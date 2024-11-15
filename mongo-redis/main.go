package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const DB_MIGRATION_KEY string = "migration:mongo"
const DB_MIGRATION_EXPIRE_AFTER time.Duration = 10 * time.Minute             // Expire migration key after this amount of time if no activity
const DB_MIGRATION_CONTINIOUS_SLEEP_DURATION time.Duration = 5 * time.Second // Replicate migration every X seconds

type MigrationConfig struct {
	MongoURI             string
	MongoDatabaseName    string
	MongoCollection      string
	RedisAddr            string
	RedisUsername        string
	RedisPassword        string
	RedisDB              int
	ParrallelLoad        int
	BatchSize            int64
	MaxSize              int64
	StartOffset          int64
	ContinousReplication bool
}

type MigrationStatusType string

const (
	MigrationStatusStarting   MigrationStatusType = "starting"
	MigrationStatusInProgress MigrationStatusType = "in_progress"
	MigrationStatusCompleted  MigrationStatusType = "completed"
	MigrationStatusFailed     MigrationStatusType = "failed"
	MigrationStatusPaused     MigrationStatusType = "paused"
)

type MigrationState struct {
	Status         MigrationStatusType `json:"status"`
	Issue          string              `json:"issue"`
	Total          json.Number         `json:"total"`
	Offset         json.Number         `json:"offset"`
	LastStartedAt  time.Time           `json:"started_at"`
	LastStopedAt   time.Time           `json:"stoped_at"`
	LastDurationMs json.Number         `json:"duration_ms"`
}

type DataType string

const (
	DataTypeHash   DataType = "hash"
	DataTypeSet    DataType = "set"
	DataTypeList   DataType = "list"
	DataTypeString DataType = "string"
	DataSortedSet  DataType = "sortedset"
)

type SourceResult struct {
	status bool
	err    error
	docs   []bson.M
	start  int64
	end    int64
}

type Dict map[string]interface{}

type DestinationData struct {
	status   bool
	key      string
	value    string
	hash     Dict
	members  []string
	score    float64
	dataType DataType
}

func parseNumber(data interface{}) float64 {
	if data == nil {
		return 0
	}
	switch v := data.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	default:
		f, err := strconv.ParseFloat(parseString(v), 64)
		if err == nil {
			return f
		}
		return 0
	}
}

func parseString(data interface{}) string {
	if data == nil {
		return ""
	}
	switch v := data.(type) {
	case string:
		return v
	case int, int32, int64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case primitive.ObjectID:
		return v.Hex()
	default:
		return ""
	}
}

func parseArray(data interface{}) []string {
	if data == nil {
		return nil
	}
	switch data := data.(type) {
	case []string:
		return data
	case []interface{}:
		var arr []string
		for _, v := range data {
			arr = append(arr, parseString(v))
		}
		return arr
	default:
		return nil
	}
}

func parseHash(data interface{}) Dict {
	if data == nil {
		return nil
	}
	switch data := data.(type) {
	case Dict:
		return data
	case bson.M:
		obj := make(Dict)
		for k, v := range data {
			obj[k] = parseString(v)
		}
		return obj
	default:
		return nil
	}
}

// Connect to MongoDB
func connectToMongoDB(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetConnectTimeout(10 * time.Second)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to MongoDB!")
	return client, nil
}

// Connect to Redis
func connectToRedisCluster(ctx context.Context, addr string, username string, password string, db int) (*redis.ClusterClient, error) {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       []string{addr},
		Username:    username,
		Password:    password,
		DialTimeout: 10 * time.Second,
		NewClient: func(opt *redis.Options) *redis.Client {
			opt.DB = db
			return redis.NewClient(opt)
		},
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to Redis!")
	return rdb, nil
}

func connectToRedis(ctx context.Context, addr string, username string, password string, db int) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:        addr,
		Username:    username,
		Password:    password,
		DB:          db,
		DialTimeout: 10 * time.Second,
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to Redis!")
	return rdb, nil
}

func generateMongoDBSourceData(ctx context.Context, client *mongo.Client, mongoDatabaseName string, mongoCollectionName string, parrallelLoad int, batchSize int64, maxSize int64, startFrom int64) (<-chan SourceResult, int64) {
	out := make(chan SourceResult, parrallelLoad)

	// Get a handle for your collection
	collection := client.Database(mongoDatabaseName).Collection(mongoCollectionName)
	fmt.Printf("MongoDb Database: %s\n", mongoDatabaseName)
	fmt.Printf("MongoDb Collection: %s\n", mongoCollectionName)

	// Get the count of documents in a collection
	if maxSize == 0 {
		// Count options
		countOption := options.EstimatedDocumentCountOptions{}
		count, err := collection.EstimatedDocumentCount(ctx, &countOption)
		if err != nil {
			log.Fatalf("Failed to get document count: %v", err)
		}
		maxSize = count
		startFrom = min(startFrom, count)
	}
	maxSize = maxSize - startFrom

	// If no documents to load
	if maxSize <= 0 {
		close(out)
		return out, maxSize
	}

	go func() {
		defer close(out)

		// Find options
		findOptions := options.Find()
		findOptions.SetSort(bson.D{{Key: "_id", Value: 1}})

		// Calculate chunk size
		chunks := parrallelLoad
		chunkSize := int64((float64(maxSize / int64(chunks))))
		chunkSizeLeft := maxSize % int64(chunks)
		if chunkSizeLeft > 0 {
			chunks++
		}

		// Calculate number of batches per chunk
		batchSize = min(batchSize, chunkSize)
		chunkBatches := int64(chunkSize / batchSize)
		if chunkSize%batchSize > 0 {
			chunkBatches++
		}

		// Calculate total number of batches
		batches := int64(maxSize / batchSize)
		if maxSize%batchSize > 0 {
			batches++
		}

		// Parallel loading of chunks and batching
		jobs := make(chan int, chunks)
		results := make(chan SourceResult, chunks)
		for c := 1; c <= chunks; c++ {
			go func(id int, jobs <-chan int, results chan<- SourceResult) {

				i := <-jobs

				// Load chunk batches
				for j := range chunkBatches {

					chunkBatchSize := batchSize
					if i == chunks-1 && j == chunkBatches-1 && chunkSizeLeft > 0 {
						chunkBatchSize = chunkSizeLeft
					}

					skip := int64(startFrom + (j * (chunkBatchSize)) + (int64(i) * chunkSize))
					findOptions.SetSkip(skip)
					findOptions.SetLimit(chunkBatchSize)
					cursor, err := collection.Find(ctx, bson.M{}, findOptions)
					if err != nil {
						log.Printf("Worker %d: Failed to find docs in collection: %v", id, err)
						results <- SourceResult{status: false, err: err, docs: nil, start: skip, end: skip + chunkBatchSize}
						continue
					}

					var batchDocs []bson.M
					for cursor.Next(ctx) {
						var doc bson.M
						if err = cursor.Decode(&doc); err != nil {
							log.Printf("Worker %d: Failed to decode doc: %v", id, err)
							continue
						}
						batchDocs = append(batchDocs, doc)
					}
					results <- SourceResult{status: true, docs: batchDocs, start: skip, end: skip + int64(len(batchDocs))}
					cursor.Close(ctx)
				}
			}(c, jobs, results)
		}

		// Send jobs to workers
		log.Printf("Starting Migration - From Offset: %d, Total: %d, Chunks: %d, Chunk Size: %d, Batch Size: %d, Parallel Load: %d\n", startFrom, maxSize, chunks, chunkSize, batchSize, parrallelLoad)
		for i := 0; i < chunks; i++ {
			jobs <- i
		}
		close(jobs)

		// Collect results
		for range batches {
			batchDocs := <-results
			if batchDocs.status {
				out <- batchDocs
			}
		}
	}()

	return out, maxSize
}

func sendToRedis(ctx context.Context, rdb *redis.Cmdable, data []DestinationData) error {

	_, pingErr := (*rdb).Ping(ctx).Result()
	if pingErr != nil {
		return pingErr
	}

	pipeline := (*rdb).Pipeline()

	for _, d := range data {
		key := d.key
		if d.status {
			switch d.dataType {
			case DataSortedSet:
				pipeline.ZAdd(ctx, key, redis.Z{Score: d.score, Member: d.value})
			case DataTypeSet:
				pipeline.SAdd(ctx, key, d.value)
			case DataTypeList:
				if len(d.members) > 0 {
					pipeline.LPush(ctx, key, d.members)
				}
			case DataTypeHash:
				if len(d.hash) > 0 {
					pipeline.HMSet(ctx, key, d.hash)
				}
			case DataTypeString:
			default:
				pipeline.Set(ctx, key, d.value, 0)
			}
		}
	}
	_, err := pipeline.Exec(ctx)
	return err
}

func getMigrationState(ctx context.Context, rdb *redis.Cmdable, collectionName string) MigrationState {
	key := fmt.Sprintf("%s:%s", DB_MIGRATION_KEY, collectionName)
	val, err := (*rdb).HGetAll(ctx, key).Result()
	if err != nil {
		log.Printf("Failed to get migration state. Error: %v", err)
		return MigrationState{Status: MigrationStatusStarting, Offset: "0", Total: "0"}
	}
	if len(val) == 0 {
		return MigrationState{Status: MigrationStatusStarting, Offset: "0", Total: "0"}
	}

	var state MigrationState
	value, err := json.Marshal(val)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal migration state. Error: %v", err))
	}

	err = json.Unmarshal([]byte(value), &state)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal migration state. Error: %v", err))
	}

	return state
}

func setMigrationState(ctx context.Context, rdb *redis.Cmdable, collectionName string, state MigrationState, persist bool) {

	_, err := (*rdb).Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Redis. Error: %v", err))
	}

	key := fmt.Sprintf("%s:%s", DB_MIGRATION_KEY, collectionName)
	value, err := json.Marshal(state)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal migration state. Error: %v", err))
	}

	var data = make(map[string]interface{})
	err = json.Unmarshal([]byte(value), &data)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal migration state. Error: %v", err))
	}

	_, err = (*rdb).HMSet(ctx, key, data).Result()
	if err != nil {
		panic(fmt.Sprintf("Failed to set migration state. Error: %v. State: %v", err, data))
	}

	if persist {
		(*rdb).Persist(ctx, key)
	} else {
		(*rdb).Expire(ctx, key, DB_MIGRATION_EXPIRE_AFTER)
	}
}

func transformData(doc bson.M) DestinationData {
	data := DestinationData{status: false}

	key, keyExists := doc["_key"].(string)
	if !keyExists {
		log.Fatalf("Key not found in document: %v", doc)
		return data
	}
	value, valueExists := doc["value"]
	score, scoreExists := doc["score"]
	members, membersExists := doc["members"]
	if !membersExists {
		array, arrayExists := doc["array"]
		members = array
		membersExists = arrayExists
	}

	data.status = true
	data.key = parseString(key)

	// If sorted set
	if scoreExists || score != nil {
		data.score = parseNumber(score)
		data.value = parseString(value)
		data.dataType = DataSortedSet
		return data
	}

	// If list
	if membersExists || members != nil {
		data.members = parseArray(members)
		data.dataType = DataTypeList
		return data
	}

	// If hash - no value or has more that just id, _key, value
	if !valueExists || len(doc) > 3 {
		delete(doc, "_key")
		delete(doc, "_id")
		data.hash = parseHash(doc)
		data.dataType = DataTypeHash
		return data
	}

	// If string
	data.value = parseString(value)
	data.dataType = DataTypeString
	return data
}

func onExit(ctx context.Context, rdb *redis.Cmdable, collectionName string) {
	migragionState := getMigrationState(ctx, rdb, collectionName)
	if migragionState.Status == MigrationStatusInProgress {
		migragionState.Status = MigrationStatusFailed
		migragionState.LastStopedAt = time.Now()
		setMigrationState(ctx, rdb, collectionName, migragionState, true)
	}
}

func handleUnexpectedExit(ctx context.Context, rdb *redis.Cmdable, collectionName string) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Printf("Migration interrupted. Updating status...")
	onExit(ctx, rdb, collectionName)

	close(c)
	os.Exit(0)
}

func handleContinousReplication(ctx context.Context, config MigrationConfig, rdb *redis.Cmdable, client *mongo.Client) bool {
	if config.ContinousReplication {
		time.Sleep(DB_MIGRATION_CONTINIOUS_SLEEP_DURATION)
		return execute(ctx, config, rdb, client)
	}
	return true
}

func execute(ctx context.Context, config MigrationConfig, rdb *redis.Cmdable, client *mongo.Client) bool {

	start := time.Now()

	// Get migration state
	migragionState := getMigrationState(ctx, rdb, config.MongoCollection)
	previousStatus := migragionState.Status
	previousStart := migragionState.LastStartedAt
	previousStop := migragionState.LastStopedAt
	previousLastDurationMs := migragionState.LastDurationMs
	previousTotal, _ := migragionState.Total.Int64()
	previousOffset, _ := migragionState.Offset.Int64()

	// Check if migration is already in progress
	if migragionState.Status == MigrationStatusInProgress {
		log.Printf("Migration already in progress. Started At: %s", migragionState.LastStartedAt.String())
		return handleContinousReplication(ctx, config, rdb, client)
	}

	// Update migration status
	migragionState.LastStartedAt = start
	migragionState.Status = MigrationStatusInProgress
	setMigrationState(ctx, rdb, config.MongoCollection, migragionState, false)

	// Get start offset
	startOffset := max(previousOffset, config.StartOffset)

	// Load data from Source
	fmt.Print("\n--- Migrating data from MongoDB to Redis --- \n\n")
	results, size := generateMongoDBSourceData(ctx, client,
		config.MongoDatabaseName,
		config.MongoCollection, config.ParrallelLoad,
		int64(config.BatchSize), int64(config.MaxSize), startOffset)

	// Check if there are no documents to load
	if size <= 0 {
		elapsed := time.Since(start)
		migragionState.Status = previousStatus
		migragionState.LastStartedAt = previousStart
		migragionState.LastStopedAt = previousStop
		migragionState.LastDurationMs = previousLastDurationMs
		migragionState.Offset = json.Number(strconv.FormatInt(previousOffset, 10))
		migragionState.Total = json.Number(strconv.FormatInt(previousTotal, 10))
		setMigrationState(ctx, rdb, config.MongoCollection, migragionState, true)

		log.Printf("No documents left to load from collection: %s. Requested Max size: %d \n", config.MongoCollection, config.MaxSize)
		log.Printf("Time taken: %s\n", elapsed)

		return handleContinousReplication(ctx, config, rdb, client)
	}

	var status bool = true
	var issue string = ""
	var count int64 = 0
	var offset int64 = previousOffset
	var total int64 = size + previousOffset

	// Update migration total
	migragionState.Total = json.Number(strconv.FormatInt(total, 10))
	setMigrationState(ctx, rdb, config.MongoCollection, migragionState, false)

	for batchDocs := range results {
		if batchDocs.status && batchDocs.docs != nil {
			// Transform data
			var destData []DestinationData
			for _, doc := range batchDocs.docs {
				destData = append(destData, transformData(doc))
			}

			// Send to destination
			err := sendToRedis(ctx, rdb, destData)
			if err != nil {
				log.Printf("Failed to load documents: %d to %d. Error: %s\n", batchDocs.start, batchDocs.end, err.Error())
				status = false
				issue = err.Error()
				break
			}

			count += int64(len(batchDocs.docs))
			offset += int64(len(batchDocs.docs))

			// Update migration amount loaded
			elapsed := time.Since(start)
			migragionState.Status = MigrationStatusInProgress
			migragionState.Issue = ""
			migragionState.LastDurationMs = json.Number(strconv.FormatInt(elapsed.Milliseconds(), 10))
			migragionState.Offset = json.Number(strconv.FormatInt(offset, 10))
			setMigrationState(ctx, rdb, config.MongoCollection, migragionState, false)

			log.Printf("Loaded documents: %d to %d = %d/%d = %d/%d (%.2f%%)\n", batchDocs.start, batchDocs.end, count, size, offset, total, float64(count*100/size))

		} else if batchDocs.err != nil {
			elapsed := time.Since(start)
			migragionState.Status = MigrationStatusPaused
			migragionState.Issue = batchDocs.err.Error()
			migragionState.LastDurationMs = json.Number(strconv.FormatInt(elapsed.Milliseconds(), 10))
			migragionState.Offset = json.Number(strconv.FormatInt(offset, 10))
			setMigrationState(ctx, rdb, config.MongoCollection, migragionState, true)
		}
	}

	if !status {
		elapsed := time.Since(start)
		migragionState.Status = MigrationStatusFailed
		migragionState.Issue = issue
		migragionState.LastStopedAt = time.Now()
		migragionState.LastDurationMs = json.Number(strconv.FormatInt(elapsed.Milliseconds(), 10))
		setMigrationState(ctx, rdb, config.MongoCollection, migragionState, true)

		log.Printf("Failed to load documents. Completed: %d/%d (%.2f%%)\n", offset, total, float64(count*100/size))
		log.Printf("Time taken: %s\n", elapsed)

		return handleContinousReplication(ctx, config, rdb, client)
	}

	elapsed := time.Since(start)
	migragionState.Status = MigrationStatusCompleted
	migragionState.LastStopedAt = time.Now()
	migragionState.LastDurationMs = json.Number(strconv.FormatInt(elapsed.Milliseconds(), 10))
	setMigrationState(ctx, rdb, config.MongoCollection, migragionState, true)

	log.Printf("Completed loading documents: %d/%d (%.2f%%)\n", offset, total, float64(count*100/size))
	log.Printf("Time taken: %s\n", elapsed)

	return handleContinousReplication(ctx, config, rdb, client)
}

func main() {

	// Load .env file from given path if no environment variable is set
	// If empty it will load .env from current directory
	enviroment := os.Getenv("ENVIRONMENT")
	if enviroment == "" {
		err := godotenv.Load(".env")
		if err != nil {
			panic(fmt.Sprintf("Error loading .env file: %v", err))
		}
		enviroment = os.Getenv("ENVIRONMENT")
	}
	// Define configuration
	mongoDatabaseName := os.Getenv("MONGO_DATABASE_NAME")
	mongoCollectionName := os.Getenv("MONGO_COLLECTION_NAME")
	mongoURI := fmt.Sprintf("mongodb://%s:%s@%s/%s?retryWrites=false&directConnection=true", os.Getenv("MONGO_USERNAME"), os.Getenv("MONGO_PASSWORD"), os.Getenv("MONGO_ADDR"), mongoDatabaseName)
	redisAddr := os.Getenv("REDIS_ADDR")
	redisUsername := os.Getenv("REDIS_USERNAME")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDB := int(parseNumber(os.Getenv("REDIS_DB")))
	redisCluster, _ := strconv.ParseBool(os.Getenv("REDIS_CLUSTER"))
	parrallelLoad := int(parseNumber(os.Getenv("PARRALLEL_LOAD")))
	batchSize := int64(parseNumber(os.Getenv("BATCH_SIZE")))
	maxSize := int64(parseNumber(os.Getenv("MAX_SIZE")))
	startOffset := int64(parseNumber(os.Getenv("START_OFFSET")))
	continousReplication, _ := strconv.ParseBool(os.Getenv("CONTINOUS_REPLICATION"))

	println("Environment: ", enviroment)
	println("Continous Replication: ", continousReplication)

	config := MigrationConfig{
		MongoURI:             mongoURI,
		MongoDatabaseName:    mongoDatabaseName,
		MongoCollection:      mongoCollectionName,
		RedisAddr:            redisAddr,
		RedisPassword:        redisPassword,
		RedisDB:              redisDB,
		ParrallelLoad:        parrallelLoad,
		BatchSize:            batchSize,
		MaxSize:              maxSize,
		StartOffset:          startOffset,
		ContinousReplication: continousReplication,
	}

	// Create context
	ctx := context.TODO()

	// Connect to MongoDB
	client, err := connectToMongoDB(ctx, mongoURI)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Connect to Redis
	var redisClient redis.Cmdable
	if redisCluster {
		rdb, err := connectToRedisCluster(ctx, redisAddr, redisUsername, redisPassword, redisDB)
		if err != nil {
			log.Fatalf("Failed to connect to Redis Cluster. Error: %v", err)
		}
		defer rdb.Close()
		redisClient = rdb
	} else {
		rdb, err := connectToRedis(ctx, redisAddr, redisUsername, redisPassword, redisDB)
		if err != nil {
			log.Fatalf("Failed to connect to Redis. Error: %v", err)
		}
		defer rdb.Close()
		redisClient = rdb
	}

	go handleUnexpectedExit(ctx, &redisClient, mongoCollectionName) // Handle unexpected exits
	defer onExit(ctx, &redisClient, mongoCollectionName)            // Handle normal exits

	// Execute migration
	execute(ctx, config, &redisClient, client)
}
