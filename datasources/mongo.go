package datasources

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const MongoIDField = "_id"

type MongoDatasourceConfigs struct {
	URI            string
	DatabaseName   string
	CollectionName string

	// Monodb Filter Query
	Filter map[string]any
	// Monodb Sort Query
	Sort map[string]any
	// Use accurate counting instead of estimated count (slower)
	AccurateCount bool

	// Provide custom mongo client
	WithClient func(ctx *context.Context, uri string) (*mongo.Client, error)
	// Perform actions on init
	OnInit func(client *mongo.Client) error
}

type MongoDatasource struct {
	client         *mongo.Client
	databaseName   string
	collectionName string
	filter         bson.M
	sort           bson.D
	// Use accurate counting instead of estimated count (slower)
	accurateCount bool

	onInit func(client *mongo.Client) error
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

	clientOptions := options.Client().ApplyURI(config.URI)
	clientOptions.SetConnectTimeout(10 * time.Second)

	client, err := mongo.Connect(*ctx, clientOptions)
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

	ds := &MongoDatasource{
		client:         ConnectToMongoDB(ctx, config),
		databaseName:   config.DatabaseName,
		collectionName: config.CollectionName,
		filter:         config.Filter,
		sort:           mongoSort,
		accurateCount:  config.AccurateCount,
		onInit:         config.OnInit,
	}

	// Initialize data source
	if ds.onInit != nil {
		if err := ds.onInit(ds.client); err != nil {
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

	// If IDs filter is provided
	if c := len(request.IDs); c > 0 {
		return uint64(c)
	}

	var err error
	var count int64

	offset := int64(request.Offset)
	size := int64(request.Size)

	if ds.accurateCount {
		filter := new(any)
		countOption := options.CountOptions{Skip: &offset, Limit: &size}
		count, err = collection.CountDocuments(*ctx, filter, &countOption)
	}

	if !ds.accurateCount || err != nil {
		countOption := options.EstimatedDocumentCountOptions{}
		count, err = collection.EstimatedDocumentCount(*ctx, &countOption)
		if err != nil || count == 0 {
			return 0
		}
	}

	if size > 0 && size < count {
		count = size
	}

	return uint64(count)
}

// Get data
func (ds *MongoDatasource) Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	filter := ds.filter

	findOptions := options.Find()
	findOptions.SetSort(ds.sort)

	if len(request.IDs) > 0 {
		filter[MongoIDField] = bson.M{"$in": request.IDs}
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

	if len(request.IDs) > 0 {
		return DatasourceFetchResult{
			Docs: docs,
		}
	}
	return DatasourceFetchResult{
		Start: request.Offset,
		End:   request.Offset + uint64(max(0, len(docs)-1)),
		Docs:  docs,
	}
}

// Insert/Update/Delete data
func (ds *MongoDatasource) Push(ctx *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error) {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	docs := make([]mongo.WriteModel, 0)
	count := *new(DatasourcePushCount)

	// Insert
	if len(request.Inserts) > 0 {
		for _, item := range request.Inserts {
			docs = append(docs, mongo.NewInsertOneModel().SetDocument(item))
		}
	}

	// Update
	if len(request.Updates) > 0 {
		for key, item := range request.Updates {
			delete(item, MongoIDField) // Remove mongo ID from update payload - prevent errors
			docs = append(docs,
				mongo.NewUpdateOneModel().
					SetFilter(bson.M{MongoIDField: key}).
					SetUpdate(bson.M{"$set": item}).
					SetUpsert(true),
			)
		}
	}

	// Delete
	if len(request.Deletes) > 0 {
		if len(request.Deletes) > 0 {
			docs = append(docs, mongo.NewDeleteManyModel().SetFilter(bson.M{MongoIDField: bson.M{"$in": request.Deletes}}))
		}
	}

	if len(docs) > 0 {
		ordered := true
		result, err := collection.BulkWrite(*ctx, docs, &options.BulkWriteOptions{
			Ordered: &ordered,
		})
		if err != nil {
			return count, fmt.Errorf("mongodb bulk write error: %w", err)
		}
		count.Inserts = uint64(result.InsertedCount)
		count.Updates = uint64(result.ModifiedCount + result.UpsertedCount)
		count.Deletes = uint64(result.DeletedCount)
	}

	return count, nil
}

// Listen to Change Data Streams (CDC) if available
func (ds *MongoDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)
	watcher := make(chan DatasourcePushRequest)

	// Convert mongo stream event to Datasource Push Request
	processEvent := func(event map[string]any) DatasourcePushRequest {
		inserts, updates, deletes := []map[string]any{}, map[string]map[string]any{}, []string{}
		opType := event["operationType"].(string)

		switch opType {
		case "insert":
			doc, ok := event["fullDocument"].(map[string]any)
			if !ok {
				break
			}
			inserts = append(inserts, doc)

		case "update":
			docKey, ok := event["documentKey"].(map[string]any)
			if !ok {
				break
			}

			doc, ok := event["fullDocument"].(map[string]any)
			if !ok {
				break
			}

			id := docKey[MongoIDField]
			updates[id.(string)] = doc

		case "delete":
			docKey, ok := event["documentKey"].(map[string]any)
			if !ok {
				break
			}

			id := docKey[MongoIDField]
			deletes = append(deletes, id.(string))
		}

		return DatasourcePushRequest{
			Inserts: inserts,
			Updates: updates,
			Deletes: deletes,
		}
	}

	// Process mongo change stream in the background
	go func(bgCtx context.Context) {
		defer close(watcher)

		batchSize := max(request.BatchSize, 1)
		batchWindow := max(request.BatchWindowSeconds, 1)

		// Options for the change stream.
		// MaxAwaitTime tells the server how long to wait for new data before returning an empty batch.
		// This helps prevent the stream.Next() call from blocking indefinitely and allows our time window to function.
		// We set it to our batch window duration for alignment.
		streamOpts := options.ChangeStream().
			SetFullDocument(options.UpdateLookup).
			SetBatchSize(int32(batchSize)).
			SetMaxAwaitTime(time.Duration(batchWindow) * time.Second)

		// Watch all changes for insert, update, delete
		pipeline := mongo.Pipeline{
			bson.D{{
				Key: "$match",
				Value: bson.D{{
					Key: "operationType",
					Value: bson.D{{
						Key:   "$in",
						Value: bson.A{"insert", "update", "delete"},
					}},
				}},
			}},
		}

		// Start the change stream
		stream, err := collection.Watch(bgCtx, pipeline, streamOpts)
		if err != nil {
			panic(fmt.Errorf("mongodb watch error: '%s", err.Error()))
		}
		defer stream.Close(bgCtx)

		for {
			select {
			case <-bgCtx.Done():
				return

			default:
				var event map[string]any
				// Use a short-lived context for TryNext to avoid blocking
				tryCtx, cancel := context.WithTimeout(bgCtx, 100*time.Millisecond)
				hasNext := stream.TryNext(tryCtx)
				cancel()
				if hasNext {
					if err := stream.Decode(&event); err == nil {
						watcher <- processEvent(event)
					}
				}
			}
		}
	}(*ctx)

	return StreamChanges(
		ctx,
		fmt.Sprintf("mongo datastore collection '%s'", ds.collectionName),
		watcher,
		request,
	)
}

// Clear data source
func (ds *MongoDatasource) Clear(ctx *context.Context) error {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)
	_, err := collection.DeleteMany(*ctx, bson.D{})
	return err
}
