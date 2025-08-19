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
	Uri            string
	DatabaseName   string
	CollectionName string
	Filter         map[string]any
	Sort           map[string]any

	OnInit func(client *mongo.Client) error
}

type MongoDatasource struct {
	client         *mongo.Client
	databaseName   string
	collectionName string
	filter         bson.D
	sort           bson.D

	onInit func(client *mongo.Client) error
}

// Connect to MongoDB
func ConnectToMongoDB(ctx *context.Context, uri string) *mongo.Client {
	clientOptions := options.Client().ApplyURI(uri)
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
// - ctx: Context.
// - uri: Database connection uri.
// - databaseName: The database name.
// - collectionName: The collection name.
func NewMongoDatasource(ctx *context.Context,
	config MongoDatasourceConfigs,
) *MongoDatasource {
	var mongoFilter bson.D
	for k, v := range config.Filter {
		mongoFilter = append(mongoFilter, bson.E{Key: k, Value: v})
	}

	var mongoSort bson.D
	for k, v := range config.Sort {
		mongoSort = append(mongoSort, bson.E{Key: k, Value: v})
	}

	ds := &MongoDatasource{
		client:         ConnectToMongoDB(ctx, config.Uri),
		databaseName:   config.DatabaseName,
		collectionName: config.CollectionName,
		filter:         mongoFilter,
		sort:           mongoSort,
		onInit:         config.OnInit,
	}

	// Initialize data source
	ds.Init()

	return ds
}

// Initialize Data source
func (ds *MongoDatasource) Init() {
	if ds.onInit != nil {
		if err := ds.onInit(ds.client); err != nil {
			panic(err)
		}
	}
}

// Get total count
func (ds *MongoDatasource) Count(ctx *context.Context, request *DatasourceFetchRequest) int64 {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	countOption := options.EstimatedDocumentCountOptions{}
	count, err := collection.EstimatedDocumentCount(*ctx, &countOption)
	if err != nil || count == 0 {
		return 0
	}

	offset := int64(0)

	if request.Size > 0 && request.Size < count {
		count = request.Size
	}
	if request.Offset > 0 && request.Offset >= count {
		offset = 0
	}

	return max(0, count-offset)
}

// Get data
func (ds *MongoDatasource) Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	findOptions := options.Find()
	findOptions.SetSort(ds.sort)
	findOptions.SetSkip(int64(request.Offset))
	findOptions.SetLimit(int64(request.Size))
	cursor, err := collection.Find(*ctx, ds.filter, findOptions)
	if err != nil {
		return DatasourceFetchResult{
			Err:   err,
			Start: int64(request.Offset),
			End:   int64(request.Offset + request.Size),
			Docs:  []map[string]any{},
		}
	}
	defer cursor.Close(*ctx)

	// Itterate cursor to get data
	docs := make([]map[string]any, 0)
	for cursor.Next(*ctx) {
		doc := make(map[string]any)
		if err = cursor.Decode(&doc); err != nil {
			doc = map[string]any{}
		}
		docs = append(docs, doc)
	}

	return DatasourceFetchResult{
		Start: int64(request.Offset),
		End:   int64(request.Offset + request.Size),
		Docs:  docs,
	}
}

// Insert/Update/Delete data
func (ds *MongoDatasource) Push(ctx *context.Context, request *DatasourcePushRequest) error {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)

	docs := make([]mongo.WriteModel, 0)

	// Insert
	if len(request.Inserts) > 0 {
		for _, item := range request.Inserts {
			docs = append(docs, mongo.NewInsertOneModel().SetDocument(item))
		}
	}

	// Update
	if len(request.Updates) > 0 {
		for key, item := range request.Updates {
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
		for _, item := range request.Deletes {
			docs = append(docs, mongo.NewDeleteOneModel().SetFilter(bson.M{MongoIDField: item}))
		}
	}

	if len(docs) > 0 {
		_, err := collection.BulkWrite(*ctx, docs)
		if err != nil {
			return fmt.Errorf("mongodb error: bulk write failed: %w", err)
		}
	}

	return nil
}

// Listen to Change Data Streams (CDC) if available
func (ds *MongoDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)
	out := make(chan DatasourceStreamResult)

	batchSize := max(request.BatchSize, 1)
	batchWindow := max(request.BatchWindowSeconds, 1)

	go func(bgCtx context.Context) {
		// Options for the change stream.
		// MaxAwaitTime tells the server how long to wait for new data before returning an empty batch.
		// This helps prevent the stream.Next() call from blocking indefinitely and allows our time window to function.
		// We set it to our batch window duration for alignment.
		streamOpts := options.ChangeStream().
			SetFullDocument(options.UpdateLookup).
			SetMaxAwaitTime(time.Duration(request.BatchWindowSeconds))

		// Can be set to nil to watch all changes.
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
			out <- DatasourceStreamResult{
				Err:  err,
				Docs: DatasourcePushRequest{},
			}
			return
		}
		defer stream.Close(bgCtx)

		// Ticker to enforce the batch processing time window
		ticker := time.NewTicker(time.Duration(batchWindow) * time.Second)
		defer ticker.Stop()

		// Buffer to hold the batch of change events
		batch := make([]map[string]any, 0, batchSize)

		// Define the Batch event Processing Logic
		// This is the callback function that handles the collected events.
		// For this example, it just prints the operation type and document key for each event.
		processEvents := func(batch []map[string]any) DatasourcePushRequest {
			inserts, updates, deletes := []map[string]any{}, map[string]map[string]any{}, []string{}
			for _, event := range batch {
				opType := event["operationType"].(string)
				switch opType {
				case "insert":
					doc, ok := event["fullDocument"].(map[string]any)
					if !ok {
						continue
					}
					inserts = append(inserts, doc)

				case "update":
					docKey, ok := event["documentKey"].(map[string]any)
					if !ok {
						continue
					}

					doc, ok := event["fullDocument"].(map[string]any)
					if !ok {
						continue
					}

					id := docKey[MongoIDField]
					updates[id.(string)] = doc

				case "delete":
					docKey, ok := event["documentKey"].(map[string]any)
					if !ok {
						continue
					}

					id := docKey[MongoIDField]
					deletes = append(deletes, id.(string))
				}
			}
			return DatasourcePushRequest{
				Inserts: inserts,
				Updates: updates,
				Deletes: deletes,
			}
		}

		for {
			// Use a select statement to handle multiple concurrent events:
			// 1. A new change event arrives.
			// 2. The batch time window expires (ticker fires).
			// 3. The context is cancelled (e.g., application shutdown).
			select {
			case <-bgCtx.Done():
				// Context has been cancelled. Process any remaining events in the batch before exiting.
				out <- DatasourceStreamResult{
					Err:  bgCtx.Err(),
					Docs: processEvents(batch),
				}
				return

			case <-ticker.C:
				// Time window has elapsed. Process the batch if it's not empty.
				if len(batch) > 0 {
					out <- DatasourceStreamResult{
						Docs: processEvents(batch),
					}
					// Reset the batch buffer
					batch = make([]map[string]any, 0, batchSize)
				}

			default:
				// Check for the next event from the change stream.
				// Pass a short-lived context to TryNext to avoid blocking the select loop for too long.
				// This makes the loop more responsive to ticker events and context cancellation.
				var event map[string]any
				if stream.Next(bgCtx) {
					if err := stream.Decode(&event); err != nil {
						event = map[string]any{}
					}
					batch = append(batch, event)

					// If batch is full, process it immediately.
					if len(batch) >= int(batchSize) {
						out <- DatasourceStreamResult{
							Docs: processEvents(batch),
						}

						// Reset the batch buffer and the ticker
						batch = make([]map[string]any, 0, batchSize)
						ticker.Reset(time.Duration(batchWindow) * time.Second)
					}
				}

				// Check for any errors on the stream itself
				if err := stream.Err(); err != nil {
					out <- DatasourceStreamResult{
						Err:  fmt.Errorf("mongodb error: change stream failed: %w", err),
						Docs: DatasourcePushRequest{},
					}
					return
				}
			}
		}
	}(*ctx)

	return out
}

// Clear data source
func (ds *MongoDatasource) Clear(ctx *context.Context) error {
	collection := ds.client.Database(ds.databaseName).Collection(ds.collectionName)
	return collection.Drop(*ctx)
}
