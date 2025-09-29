package datasources

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/sagadana/migrator/helpers"
)

const RedisKeyspacePattern = "__keyspace@*__:"

// -----------------
// Data Structures
// -----------------

type RedisDsType string

const (
	RedisDsString RedisDsType = "string"
	RedisDsList   RedisDsType = "list"
	RedisDsSet    RedisDsType = "set"
	RedisDsHash   RedisDsType = "hash"
	RedisDsJSON   RedisDsType = "rejson-rl"
	RedisDsZSet   RedisDsType = "zset"
)

// ---------------
// Schema
// ---------------

// ------ Input -------

type RedisInputSchema struct {
	String    string
	List      []any
	Set       []string
	Hash      map[string]string
	JSON      map[string]any
	SortedSet []map[string]float64
}

// ------ Output -------

type RedisOutputStringSchema struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type RedisOutputListSchema struct {
	Key     string   `json:"key"`
	Members []string `json:"array"`
}
type RedisOutputSetSchema struct {
	Key     string   `json:"key"`
	Members []string `json:"members"`
}
type RedisOutputHashSchema map[string]string
type RedisOutputJSONSchema map[string]any
type RedisSortedSetSchema struct {
	Value string      `json:"value"`
	Score json.Number `json:"score"`
}
type RedisOutputSortedSetSchema struct {
	Key     string                 `json:"key"`
	Members []RedisSortedSetSchema `json:"members"`
}

// ---------------
// Configs
// ---------------

type RedisDatasourceTransformer func(data map[string]any) (RedisInputSchema, error)

type RedisDatasourceConfigs struct {
	// Redis connection URI
	URI string
	// Key prefix to use for saving and fetching items. E.g. `user:profile:`
	KeyPrefix string
	// ID Field for input JSON
	IDField string
	// Size of scan operations. E.g. '1000' to scan a thousand at a time. Min = 1.
	ScanSize uint64

	// Custom redis client provider
	WithClient func(ctx *context.Context, uri string) (*redis.Client, error)
	// Transform data before saving to redis
	WithTransformer RedisDatasourceTransformer
	// Perform actions on init
	OnInit func(client *redis.Client) error
}

type RedisDatasource struct {
	client    *redis.Client
	keyPrefix string
	idField   string
	scanSize  uint64

	transformer RedisDatasourceTransformer

	keys       []string
	keyMap     map[string]uint64
	keysMu     *sync.Mutex // For synchronizing keys manipulation
	lastCursor uint64
}

// Connect to Redis
func ConnectToRedis(ctx *context.Context, config RedisDatasourceConfigs) *redis.Client {
	// Try custom client
	if config.WithClient != nil {
		client, err := config.WithClient(ctx, config.URI)
		if err != nil {
			panic(err)
		}
		return client
	}

	opts, err := redis.ParseURL(config.URI)
	if err != nil {
		panic(err)
	}

	client := redis.NewClient(opts)

	if err := client.Ping(*ctx).Err(); err != nil {
		panic(err)
	}

	return client
}

// NewRedisDatasource creates and returns a new instance of RedisDatasource
func NewRedisDatasource(ctx *context.Context, config RedisDatasourceConfigs) *RedisDatasource {
	client := ConnectToRedis(ctx, config)

	// Enable keyspace notifications for key events
	if err := client.ConfigSet(*ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
		panic(fmt.Errorf("failed to enable keyspace notifications: %w", err))
	}

	if len(config.IDField) == 0 {
		panic("redis `config.IDField` is required")
	}

	ds := &RedisDatasource{
		client:    client,
		keyPrefix: config.KeyPrefix,
		idField:   config.IDField,
		scanSize:  max(1, config.ScanSize),

		keys:   make([]string, 0),
		keyMap: make(map[string]uint64),
		keysMu: new(sync.Mutex),

		transformer: config.WithTransformer,
	}

	// Initialize data source
	if config.OnInit != nil {
		if err := config.OnInit(ds.client); err != nil {
			panic(err)
		}
	}

	return ds
}

// RedisOutputSchemaToJSON converts Redis output schema to JSON
func RedisOutputSchemaToJSON[V RedisOutputStringSchema |
	RedisOutputListSchema |
	RedisOutputSetSchema |
	RedisOutputHashSchema |
	RedisOutputJSONSchema |
	RedisOutputSortedSetSchema](
	schema V,
) map[string]any {

	// Marshal the schema to JSON then unmarshal to get a generic map
	jsonBytes, err := json.Marshal(schema)
	if err != nil {
		return nil
	}

	var data map[string]any
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return nil
	}

	return data
}

// JSONToRedisJSONInputSchema converts JSON to Redis input schema (using ReJSON)
func JSONToRedisJSONInputSchema(data map[string]any) RedisInputSchema {
	schema := RedisInputSchema{
		JSON: data,
	}
	return schema
}

// JSONToRedisHashInputSchema converts JSON to Redis input schema (using Redis Hash)
func JSONToRedisHashInputSchema(data map[string]any) RedisInputSchema {
	hash := make(map[string]string)
	for k, v := range data {
		hash[k] = fmt.Sprintf("%v", v)
	}
	schema := RedisInputSchema{
		Hash: hash,
	}
	return schema
}

func (ds *RedisDatasource) getID(data map[string]any) (val string, err error) {
	id, ok := data[ds.idField]
	if ok && id != nil {
		return fmt.Sprintf("%v", id), err
	}
	return val, fmt.Errorf("missing '%s' in insert document", ds.idField)
}

func (ds *RedisDatasource) getKey(id string) string {
	prefix := strings.Trim(strings.Trim(ds.keyPrefix, "*"), ":")
	if len(prefix) == 0 {
		return id
	}
	return fmt.Sprintf("%s:%s", prefix, id)
}

func (ds *RedisDatasource) processUpsert(ctx *context.Context, pipe redis.Pipeliner, key string, schema RedisInputSchema) {

	// Process strings
	if len(schema.String) > 0 { // bytes count
		pipe.Set(*ctx, key, schema.String, 0)
	}

	// Process lists
	if len(schema.List) > 0 {
		pipe.RPush(*ctx, key, schema.List...)
	}

	// Process sets
	if len(schema.Set) > 0 {
		pipe.SAdd(*ctx, key, schema.Set)
	}

	// Process hashes
	if len(schema.Hash) > 0 {
		pipe.HMSet(*ctx, key, schema.Hash)
	}

	// Process JSON
	if len(schema.JSON) > 0 {
		b, err := json.Marshal(schema.JSON)
		if err == nil {
			pipe.JSONMerge(*ctx, key, "$", string(b))
		}
	}

	// Process sorted sets
	if len(schema.SortedSet) > 0 {
		for _, m := range schema.SortedSet {
			for member, score := range m {
				pipe.ZAdd(*ctx, key, redis.Z{Score: score, Member: member})
			}
		}
	}
}

func (ds *RedisDatasource) processFetch(ctx *context.Context, keys []string) ([]map[string]any, error) {
	docs := make([]map[string]any, 0)
	typeMap := make(map[string]string)

	pipe := ds.client.Pipeline()
	var key string
	var idx int

	if len(keys) == 0 {
		return docs, nil
	}

	for _, key = range keys {
		// Get data type
		dataType, err := ds.client.Type(*ctx, key).Result()
		if err != nil {
			return docs, fmt.Errorf("redis key error: %w", err)
		}

		// Add command
		switch RedisDsType(strings.ToLower(dataType)) {
		case RedisDsString:
			pipe.Get(*ctx, key)
		case RedisDsList:
			pipe.LRange(*ctx, key, 0, -1)
		case RedisDsSet:
			pipe.SMembers(*ctx, key)
		case RedisDsHash:
			pipe.HGetAll(*ctx, key)
		case RedisDsJSON:
			pipe.JSONGet(*ctx, key)
		case RedisDsZSet:
			pipe.ZRangeWithScores(*ctx, key, 0, -1)
		}

		typeMap[key] = dataType
	}

	// Execute pipeline
	cmds, err := pipe.Exec(*ctx)
	if err != nil {
		return docs, err
	} else if len(cmds) != len(keys) {
		return docs, fmt.Errorf("failed to fetch redis items for keys")
	}

	// Process results
	for idx, key = range keys {
		dataType, err := typeMap[key]
		if !err {
			continue
		}

		var doc map[string]any
		cmd := cmds[idx]

		switch RedisDsType(strings.ToLower(dataType)) {
		case RedisDsString:
			if val, err := cmd.(*redis.StringCmd).Result(); err == nil {
				doc = RedisOutputSchemaToJSON(RedisOutputStringSchema{
					Key:   key,
					Value: val,
				})
			}

		case RedisDsList:
			if vals, err := cmd.(*redis.StringSliceCmd).Result(); err == nil {
				doc = RedisOutputSchemaToJSON(RedisOutputListSchema{
					Key:     key,
					Members: vals,
				})
			}

		case RedisDsSet:
			if vals, err := cmd.(*redis.StringSliceCmd).Result(); err == nil {
				doc = RedisOutputSchemaToJSON(RedisOutputSetSchema{
					Key:     key,
					Members: vals,
				})
			}

		case RedisDsHash:
			if vals, err := cmd.(*redis.MapStringStringCmd).Result(); err == nil {
				doc = RedisOutputSchemaToJSON(RedisOutputHashSchema(vals))
			}

		case RedisDsJSON:
			if val, err := cmd.(*redis.JSONCmd).Result(); err == nil {
				var jsonData map[string]any
				if err := json.Unmarshal([]byte(val), &jsonData); err == nil {
					doc = RedisOutputSchemaToJSON(RedisOutputJSONSchema(jsonData))
				}
			}

		case RedisDsZSet:
			if vals, err := cmd.(*redis.ZSliceCmd).Result(); err == nil {
				members := make([]RedisSortedSetSchema, len(vals))
				for i, z := range vals {
					members[i] = RedisSortedSetSchema{
						Value: fmt.Sprint(z.Member),
						Score: json.Number(fmt.Sprint(z.Score)),
					}
				}
				doc = RedisOutputSchemaToJSON(RedisOutputSortedSetSchema{
					Key:     key,
					Members: members,
				})
			}
		}

		if doc != nil {
			docs = append(docs, doc)
		}
	}

	return docs, nil
}

func (ds *RedisDatasource) scanKeyPrefix(ctx *context.Context, offset, limit uint64) error {
	for {
		batch, cursor, err := ds.client.Scan(*ctx, ds.lastCursor, ds.getKey("*"), int64(ds.scanSize)).Result()
		if err != nil {
			return fmt.Errorf("redis fetch error: failed to scan keys: %w", err)
		}

		var k string
		for _, k = range batch {
			_, ok := ds.keyMap[k]
			if !ok {
				ds.keys = append(ds.keys, k)
				ds.keyMap[k] = max(0, uint64(len(ds.keys))-1)
			}
		}

		if cursor == 0 || (limit > 0 && uint64(len(ds.keys)) >= offset+limit) {
			break
		}

		ds.lastCursor = cursor
	}

	return nil
}

func (ds *RedisDatasource) Client() *redis.Client {
	return ds.client
}

// Get total count
func (ds *RedisDatasource) Count(ctx *context.Context, request *DatasourceFetchRequest) uint64 {

	var total uint64
	var id string

	if len(request.IDs) > 0 {
		// If specific IDs requested, count those that exist
		for _, id = range request.IDs {
			exists, err := ds.client.Exists(*ctx, ds.getKey(id)).Result()
			if err == nil && exists == 1 {
				total++
			}
		}
	} else {
		if ds.keyPrefix == "" { // No Prefix
			// Get DB Size
			size, err := ds.client.DBSize(*ctx).Result()
			if err == nil {
				total = uint64(size)
			}
		} else { // With Prefix
			ds.keysMu.Lock()

			// Scan all matching keys
			ds.keys = make([]string, 0)
			ds.keyMap = make(map[string]uint64)
			ds.lastCursor = 0

			err := ds.scanKeyPrefix(ctx, request.Offset, request.Size)
			if err != nil {
				slog.Error(fmt.Sprintf("failed to scan keys for '%s'", ds.getKey("*")), "error", err)
				ds.keysMu.Unlock()
				return total
			}

			total = uint64(len(ds.keys))

			ds.keysMu.Unlock()
		}
	}

	// Apply offset and size constraints
	if request.Offset >= total {
		return 0
	}
	remaining := total - request.Offset

	if request.Size > 0 && request.Size < remaining {
		return request.Size
	}

	return remaining
}

// Get data
func (ds *RedisDatasource) Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	keys := make([]string, 0)
	var id string

	// 1. Build base slice of IDs to page through
	if len(request.IDs) > 0 {
		// If specific IDs requested, map them to Redis keys
		for _, id = range request.IDs {
			keys = append(keys, ds.getKey(id))
		}
	} else {
		// Ensure keys is populated at least up to Offset+Size
		if len(ds.keys) == 0 || uint64(len(ds.keys)) < (request.Offset+request.Size) {
			ds.keysMu.Lock()
			err := ds.scanKeyPrefix(ctx, request.Offset, request.Size)
			if err != nil {
				ds.keysMu.Unlock()
				return DatasourceFetchResult{
					Err: fmt.Errorf("failed to scan keys for '%s': %w", ds.getKey("*"), err),
				}
			}
			ds.keysMu.Unlock()
		}

		// Always use the cache once populated
		keys = ds.keys
	}

	// 2. Slice and clone documents
	ids, start, end := helpers.Slice(keys, request.Offset, request.Size)

	// 3. Fetch actual documents by key
	docs, err := ds.processFetch(ctx, ids)
	return DatasourceFetchResult{
		Docs:  docs,
		Start: start,
		End:   end,
		Err:   err,
	}
}

// Insert/Update/Delete data
func (ds *RedisDatasource) Push(ctx *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error) {
	count := DatasourcePushCount{}
	pipe := ds.client.TxPipeline()

	var pushErr error
	var item map[string]any
	var id string

	// Handle inserts
	for _, item = range request.Inserts {
		if item == nil {
			continue
		}

		id, err := ds.getID(item)
		if err != nil {
			pushErr = fmt.Errorf("redis item id error: %w", err)
			slog.Warn(pushErr.Error())
			continue
		}

		var schema RedisInputSchema

		if ds.transformer != nil {
			schema, err = ds.transformer(item)
			if err != nil {
				pushErr = fmt.Errorf("redis item transformer error: %w", err)
				slog.Warn(pushErr.Error())
				continue
			}
		} else {
			schema = JSONToRedisHashInputSchema(item) // Default: Hash data structure
		}

		ds.processUpsert(ctx, pipe, ds.getKey(id), schema)
		count.Inserts++
	}

	// Handle updates
	for _, item = range request.Updates {
		if item == nil {
			continue
		}

		var schema RedisInputSchema
		var err error

		id, err := ds.getID(item)
		if err != nil {
			pushErr = fmt.Errorf("redis item id error: %w", err)
			slog.Warn(pushErr.Error())
			continue
		}

		if ds.transformer != nil {
			schema, err = ds.transformer(item)
			if err != nil {
				pushErr = fmt.Errorf("redis item transformer error: %w", err)
				slog.Warn(pushErr.Error())
				continue
			}
		} else {
			schema = JSONToRedisHashInputSchema(item) // Default: Hash data structure
		}

		ds.processUpsert(ctx, pipe, ds.getKey(id), schema)
		count.Updates++
	}

	// Handle deletes
	for _, id = range request.Deletes {
		pipe.Del(*ctx, ds.getKey(id))
		count.Deletes++
	}

	// Execute pipeline
	if _, err := pipe.Exec(*ctx); err != nil {
		return count, fmt.Errorf("redis push error: failed to execute pipeline: %w", err)
	}

	return count, pushErr
}

// Listen to keyspace notifications if available
func (ds *RedisDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	watcher := make(chan DatasourcePushRequest)

	// Subscribe to keyspace notifications
	pubsub := ds.client.PSubscribe(*ctx,
		RedisKeyspacePattern+ds.getKey("*"), // Key events
	)

	// Convert redis key event to Datasource Push Request
	processEvent := func(msg *redis.Message) DatasourcePushRequest {
		updates, deletes := []map[string]any{}, []string{}
		key := strings.SplitN(msg.Channel, ":", 2)[1]

		switch msg.Payload {
		case "del": // Process delete
			deletes = append(deletes, key)

		default: // Process others as updates
			docs, err := ds.processFetch(ctx, []string{key})
			if err != nil {
				updates = append(updates, nil)
			} else if len(docs) > 0 {
				updates = append(updates, docs...)
			}
		}

		return DatasourcePushRequest{
			Updates: updates,
			Deletes: deletes,
		}
	}

	// Process redis key events in the background
	go func(bgCtx context.Context) {
		defer close(watcher)
		defer pubsub.Close()

		for {
			select {
			case <-bgCtx.Done():
				return

			case msg := <-pubsub.Channel():
				watcher <- processEvent(msg)
			}
		}
	}(*ctx)

	return StreamChanges(
		ctx,
		fmt.Sprintf("redis keys matching pattern '%s'", ds.getKey("*")),
		watcher,
		request,
	)
}

// Clear data source
func (ds *RedisDatasource) Clear(ctx *context.Context) error {
	ds.keysMu.Lock()
	defer ds.keysMu.Unlock()

	// ---- No Prefix ---- //

	if ds.keyPrefix == "" {
		if err := ds.client.FlushDB(*ctx).Err(); err != nil {
			return fmt.Errorf("redis error: failed to flush DB: %s", err)
		}
		return nil
	}

	// ---- With Prefix ----//

	err := ds.scanKeyPrefix(ctx, 0, 0)
	if err != nil {
		return fmt.Errorf("redis error: failed to scan keys for '%s': %s", ds.getKey("*"), err)
	}

	// Delete in batches
	if len(ds.keys) > 0 {
		for i := 0; i < len(ds.keys); i += int(ds.scanSize) {
			end := min(i+int(ds.scanSize), len(ds.keys))
			batch := ds.keys[i:end]
			if len(batch) > 0 {
				if err := ds.client.Del(*ctx, batch...).Err(); err != nil {
					return err
				}
			}
		}
	}

	ds.keys = make([]string, 0)
	ds.keyMap = make(map[string]uint64)
	ds.lastCursor = 0
	return nil
}

// Close data source
func (ds *RedisDatasource) Close(ctx *context.Context) error {
	ds.keysMu.Lock()
	defer ds.keysMu.Unlock()

	ds.keys = make([]string, 0)
	ds.keyMap = make(map[string]uint64)
	ds.lastCursor = 0
	return ds.client.Close()
}
