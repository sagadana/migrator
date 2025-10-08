package datasources

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const MongoDefaultIDField = "_id"

type MongoDatasourceConfigs struct {
	URI            string
	DatabaseName   string
	CollectionName string

	// MongoDB Filter Query
	Filter map[string]any
	// MongoDB Sort Query
	Sort map[string]any
	// Use accurate counting instead of estimated count (slower)
	AccurateCount bool

	// Provide custom mongo client
	WithClient func(ctx *context.Context, uri string) (*mongo.Client, error)
	// Provide custom transformer. E.g To convert item's ID to mongo ID
	WithTransformer DatasourceTransformer
	// Perform actions on init
	OnInit func(client *mongo.Client) error
}

type MongoDatasource struct {
	client         *mongo.Client
	databaseName   string
	collectionName string
	idField        string

	filter bson.M
	sort   bson.D
	// Use accurate counting instead of estimated count (slower)
	accurateCount bool

	streamRunning bool
	streamMutex   *sync.Mutex

	// Watcher channel
	watcher chan DatasourcePushRequest

	transformer DatasourceTransformer
}

// ConvertBSON converts a BSON value to a generic any value
func ConvertBSON(val any) any {
	switch v := val.(type) {
	case bson.D:
		m := make(map[string]any)
		for _, e := range v {
			m[e.Key] = ConvertBSON(e.Value)
		}
		return m
	case bson.A:
		a := make([]any, len(v))
		for i, e := range v {
			a[i] = ConvertBSON(e)
		}
		return a
	case bson.M:
		m := make(map[string]any)
		for k, e := range v {
			m[k] = ConvertBSON(e)
		}
		return m
	default:
		return v
	}
}

// Connect to MongoDB
func ConnectToMongoDB(ctx *context.Context, config MongoDatasourceConfigs) *mongo.Client {

	// Try custom client
	if config.WithClient != nil {
		client, err := config.WithClient(ctx, config.URI)
		if err != nil {
			panic(err)
		}
		return client
	}

	bsonOpts := &options.BSONOptions{
		UseJSONStructTags: true,
		NilSliceAsEmpty:   true,
		OmitEmpty:         true,
	}
	clientOptions := options.Client().
		ApplyURI(config.URI).
		SetConnectTimeout(10 * time.Second).
		SetBSONOptions(bsonOpts)

	client, err := mongo.Connect(clientOptions)
	if err != nil {
		panic(err)
	}

	err = client.Ping(*ctx, nil)
	if err != nil {
		panic(err)
	}

	return client
}

// NewMongoDatasource creates and returns a new instance of MongoDatasource.
//   - ctx: Context.
//   - uri: Database connection uri.
//   - databaseName: The database name.
//   - collectionName: The collection name.
func NewMongoDatasource(ctx *context.Context,
	config MongoDatasourceConfigs,
) *MongoDatasource {
	mongoSort := bson.D{}
	if config.Sort != nil {
		for k, v := range config.Sort {
			mongoSort = append(mongoSort, bson.E{Key: k, Value: v})
		}
	}

	client := ConnectToMongoDB(ctx, config)

	ds := &MongoDatasource{
		client:         client,
		databaseName:   config.DatabaseName,
		collectionName: config.CollectionName,
		idField:        MongoDefaultIDField, // Always use Mongo default ID field internally

		filter:        config.Filter,
		sort:          mongoSort,
		accurateCount: config.AccurateCount,

		streamRunning: false,
		streamMutex:   new(sync.Mutex),

		watcher: make(chan DatasourcePushRequest, DefaultWatcherBufferSize),

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

func (ds *MongoDatasource) Client() *mongo.Client {
	return ds.client
}

// Get total count
func (ds *MongoDatasource) Count(ctx *context.Context, request *DatasourceFetchRequest) uint64 {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	// 1. Determine the raw “total” matching items
	var total uint64
	// If IDs filter is provided
	if ds.accurateCount {
		filter := bson.M{}
		if len(ds.filter) > 0 {
			maps.Copy(filter, ds.filter)
		}
		if len(request.IDs) > 0 {
			filter[ds.idField] = bson.M{"$in": request.IDs}
		}
		count, err := collection.CountDocuments(*ctx, filter)
		if err == nil {
			total = uint64(count)
		}
	}
	if !ds.accurateCount {
		if c := len(request.IDs); c > 0 {
			total = uint64(c)
		} else {
			count, err := collection.EstimatedDocumentCount(*ctx)
			if err != nil || count == 0 {
				return 0
			}
			total = uint64(count)
		}
	}

	// 2. Apply offset: if we skip past the end, zero left
	if request.Offset >= total {
		return 0
	}
	remaining := total - request.Offset

	// 3. Apply size cap: if Size>0 and smaller than what's left, cap it
	if request.Size > 0 && request.Size < remaining {
		return request.Size
	}

	// 4. Otherwise, everything remaining counts
	return remaining
}

// Get data
func (ds *MongoDatasource) Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	filter := bson.M{}
	if len(ds.filter) > 0 {
		maps.Copy(filter, ds.filter)
	}

	findOptions := options.Find()
	findOptions.SetSort(ds.sort)

	if len(request.IDs) > 0 {
		filter[ds.idField] = bson.M{"$in": request.IDs}
	} else {
		findOptions.SetSkip(int64(request.Offset))
		findOptions.SetLimit(int64(request.Size))
	}

	cursor, err := collection.Find(*ctx, filter, findOptions)
	if err != nil {
		return DatasourceFetchResult{
			Err:  err,
			Docs: []map[string]any{},
		}
	}
	defer cursor.Close(*ctx)

	// Itterate cursor to get data
	docs := make([]map[string]any, 0)
	for cursor.Next(*ctx) {
		doc := make(map[string]any)
		if err = cursor.Decode(&doc); err != nil {
			return DatasourceFetchResult{Err: fmt.Errorf("mongodb fetch error: failed to decode doc: %w", err)}
		}
		docs = append(docs, doc)
	}

	count := len(docs)
	start := request.Offset
	end := request.Offset + uint64(max(0, count))

	if count == 0 {
		start, end = 0, 0
	}

	return DatasourceFetchResult{
		Start: start,
		End:   end,
		Docs:  docs,
	}
}

// Insert/Update/Delete data
func (ds *MongoDatasource) Push(ctx *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error) {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	docs := make([]mongo.WriteModel, 0)
	count := *new(DatasourcePushCount)
	var pushErr error

	// Insert
	if len(request.Inserts) > 0 {
		var item map[string]any
		for _, item = range request.Inserts {
			if item == nil {
				continue
			}

			// Transform
			if ds.transformer != nil {
				trans, err := ds.transformer(item)
				if err != nil {
					pushErr = fmt.Errorf("mongodb insert transformer error: %w", err)
					slog.Warn(pushErr.Error())
					continue
				}
				item = trans
			}

			docs = append(docs, mongo.NewInsertOneModel().SetDocument(item))
		}
	}

	// Update
	if len(request.Updates) > 0 {
		var item map[string]any
		for _, item = range request.Updates {
			if item == nil {
				continue
			}

			// Transform
			if ds.transformer != nil {
				trans, err := ds.transformer(item)
				if err != nil {
					pushErr = fmt.Errorf("mongodb update transformer error: %w", err)
					slog.Warn(pushErr.Error())
					continue
				}
				item = trans
			}

			// Get key
			key, ok := item[ds.idField]
			if !ok || key == nil {
				pushErr = fmt.Errorf("mongodb update error: missing '%s' field", ds.idField)
				slog.Warn(pushErr.Error())
				continue
			}

			// Remove mongo ID from update payload - prevent errors
			delete(item, ds.idField)

			docs = append(docs,
				mongo.NewUpdateOneModel().
					SetFilter(bson.M{ds.idField: key}).
					SetUpdate(bson.M{"$set": item}).
					SetUpsert(true),
			)
		}
	}

	// Delete
	if len(request.Deletes) > 0 {
		if len(request.Deletes) > 0 {
			docs = append(docs, mongo.NewDeleteManyModel().SetFilter(bson.M{ds.idField: bson.M{"$in": request.Deletes}}))
		}
	}

	if len(docs) > 0 {
		result, err := collection.BulkWrite(*ctx, docs, options.BulkWrite().SetOrdered(true))
		if err != nil {
			return count, fmt.Errorf("mongodb push error: failed to execute bulk write: %w", err)
		}
		count.Inserts = uint64(result.InsertedCount)
		count.Updates = uint64(result.ModifiedCount + result.UpsertedCount)
		count.Deletes = uint64(max(result.DeletedCount, int64(len(request.Deletes))))
	}

	return count, pushErr
}

// Listen to Change Data Streams (CDC) if available
func (ds *MongoDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {

	// Start stream only once, on first watcher
	ds.streamMutex.Lock()
	defer ds.streamMutex.Unlock()
	if !ds.streamRunning {
		ds.streamRunning = true

		collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

		const StreamOpTypeField = "operationType"
		const StreamDocKeyField = "documentKey"
		const StreamFullDocField = "fullDocument"

		// Convert mongo stream event to Datasource Push Request
		processEvent := func(event map[string]any) DatasourcePushRequest {
			inserts, updates, deletes := []map[string]any{}, []map[string]any{}, []string{}
			opType := event[StreamOpTypeField].(string)

			switch opType {
			case "insert":
				doc, ok := ConvertBSON(event[StreamFullDocField]).(map[string]any)
				if !ok {
					break
				}
				inserts = append(inserts, doc)

			case "update":
				docKey, ok := ConvertBSON(event[StreamDocKeyField]).(map[string]any)
				if !ok {
					break
				}

				doc, ok := ConvertBSON(event[StreamFullDocField]).(map[string]any)
				if !ok {
					break
				}

				id, ok := docKey[ds.idField]
				if !ok {
					doc[ds.idField] = id
				}
				updates = append(updates, doc)

			case "delete":
				docKey, ok := ConvertBSON(event[StreamDocKeyField]).(map[string]any)
				if !ok {
					break
				}

				id := docKey[ds.idField]
				deletes = append(deletes, id.(string))
			}

			return DatasourcePushRequest{
				Inserts: inserts,
				Updates: updates,
				Deletes: deletes,
			}
		}

		// Options for the change stream.
		// MaxAwaitTime tells the server how long to wait for new data before returning an empty batch.
		// This helps prevent the stream.Next() call from blocking indefinitely and allows our time window to function.
		// We set it to our batch window duration for alignment.
		streamOpts := options.ChangeStream().
			SetFullDocument(options.UpdateLookup).
			SetBatchSize(int32(max(request.BatchSize, 1))).
			SetMaxAwaitTime(time.Duration(max(request.BatchWindowSeconds, 1)) * time.Second)

		// Watch all changes for insert, update, delete
		matchFilter := bson.E{
			Key: "$match",
			Value: bson.D{
				{
					Key: StreamOpTypeField,
					Value: bson.D{{
						Key:   "$in",
						Value: bson.A{"insert", "update", "delete"},
					}},
				},
			},
		}

		// Add filter conditions if ds.filter is not empty
		if len(ds.filter) > 0 {
			for key, value := range ds.filter {
				filterKey := fmt.Sprintf("%s.%s", StreamFullDocField, key)
				matchFilter.Value = append(matchFilter.Value.(bson.D), bson.E{
					Key:   filterKey,
					Value: value,
				})
			}
		}

		// Start the change stream
		pipeline := mongo.Pipeline{{
			matchFilter,
		}}
		stream, err := collection.Watch(*ctx, pipeline, streamOpts)
		if err != nil {
			panic(fmt.Errorf("mongodb watch error: '%s", err.Error()))
		}

		// Process mongo change stream in the background
		go func(bgCtx context.Context) {
			defer func() {
				ds.streamMutex.Lock()
				if stream != nil && ds.streamRunning {
					stream.Close(bgCtx)
				}
				ds.streamRunning = false
				ds.streamMutex.Unlock()
			}()

			for {
				select {
				case <-bgCtx.Done():
					return

				default:
					var event map[string]any
					hasNext := stream.Next(*ctx)
					if hasNext {
						if err := stream.Decode(&event); err == nil {
							ds.watcher <- processEvent(event)
						}
					}
				}
			}
		}(*ctx)

	}

	// Return stream changes channel
	return StreamChanges(
		ctx,
		fmt.Sprintf("mongo datastore collection '%s'", ds.collectionName),
		ds.watcher,
		request,
	)
}

// Clear data source
func (ds *MongoDatasource) Clear(ctx *context.Context) error {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)
	_, err := collection.DeleteMany(*ctx, bson.D{})
	return err
}

// Close data source
func (ds *MongoDatasource) Close(ctx *context.Context) error {
	// Close watcher channel
	close(ds.watcher)

	return ds.client.Disconnect(*ctx)
}

// Import data into data source
func (ds *MongoDatasource) Import(ctx *context.Context, request DatasourceImportRequest) error {
	switch request.Type {
	case DatasourceImportTypeCSV:
		return LoadCSV(ctx, ds, request.Location, request.BatchSize)
	default:
		return fmt.Errorf("unsupported import type: %s", request.Type)
	}
}

// Export data from data source
func (ds *MongoDatasource) Export(ctx *context.Context, request DatasourceExportRequest) error {
	switch request.Type {
	case DatasourceExportTypeCSV:
		return SaveCSV(ctx, ds, request.Location, request.BatchSize)
	default:
		return fmt.Errorf("unsupported export type: %s", request.Type)
	}
}
