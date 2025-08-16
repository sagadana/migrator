package datasources

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const MongoIDField = "_id"

type MongoDatasourcePushRequest struct {
	DatasourcePushRequest
	Inserts []bson.M
	Updates map[string]bson.M
	Deletes []string
}
type MongoDatasourceFetchResult struct {
	DatasourceFetchResult
	Docs []bson.M
}
type MongoDatasourceStreamResult struct {
	DatasourceStreamResult
	Docs MongoDatasourcePushRequest
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
func NewMongoDatasource(ctx *context.Context,
	uri string,
	databaseName string,
	collectionName string,
) *MongoDatasource {
	return &MongoDatasource{
		client: ConnectToMongoDB(ctx, uri),
	}
}

// Initialize Data source
func (d *MongoDatasource) Init() {
	if err := d.onInit(d.client); err != nil {
		panic(err)
	}
}

// Get total count
func (d *MongoDatasource) Count(ctx *context.Context, request DatasourceFetchRequest) int64 {
	collection := d.client.Database(d.databaseName).Collection(d.collectionName)

	countOption := options.EstimatedDocumentCountOptions{}
	count, err := collection.EstimatedDocumentCount(*ctx, &countOption)
	if err != nil {
		count = int64(request.Size)
	}

	return max(0, count-min(int64(request.Skip), count))
}

// Get data
func (d *MongoDatasource) Fetch(ctx *context.Context, request *DatasourceFetchRequest) MongoDatasourceFetchResult {
	collection := d.client.Database(d.databaseName).Collection(d.collectionName)

	findOptions := options.Find()
	findOptions.SetSort(d.sort)
	findOptions.SetSkip(int64(request.Skip))
	findOptions.SetLimit(int64(request.Size))
	cursor, err := collection.Find(*ctx, d.filter, findOptions)
	if err != nil {
		return MongoDatasourceFetchResult{
			DatasourceFetchResult: DatasourceFetchResult{
				Err:   err,
				Start: int64(request.Skip),
				End:   int64(request.Skip + request.Size),
			},
			Docs: []bson.M{},
		}
	}
	defer cursor.Close(*ctx)

	// Itterate cursor to get data
	var docs []bson.M
	for cursor.Next(*ctx) {
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			doc = bson.M{}
		}
		docs = append(docs, doc)
	}

	return MongoDatasourceFetchResult{
		DatasourceFetchResult: DatasourceFetchResult{
			Start: int64(request.Skip),
			End:   int64(request.Skip + request.Size),
		},
		Docs: docs,
	}
}

// Insert/Update/Delete data
func (d *MongoDatasource) Push(ctx *context.Context, request *MongoDatasourcePushRequest) error {
	collection := d.client.Database(d.databaseName).Collection(d.collectionName)

	// Start session
	sessionOpt := options.SessionOptions{}
	session, err := d.client.StartSession(&sessionOpt)
	if err != nil {
		return nil
	}

	// Execute push a transaction
	_, err = session.WithTransaction(*ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {

		var docs []mongo.WriteModel

		// Insert
		if len(request.Inserts) > 0 {
			for _, item := range request.Inserts {
				docs = append(docs, mongo.NewInsertOneModel().SetDocument(item))
			}
		}

		// Update
		if len(request.Updates) > 0 {
			for key, item := range request.Updates {
				filter := options.ArrayFilters{
					Filters: []interface{}{
						bson.D{{
							Key:   MongoIDField,
							Value: key,
						}},
					},
				}
				docs = append(docs,
					mongo.NewUpdateOneModel().
						SetArrayFilters(filter).
						SetUpdate(item).
						SetUpsert(true),
				)
			}
		}

		// Delete
		if len(request.Deletes) > 0 {
			for _, item := range request.Deletes {
				filter := bson.D{{
					Key:   MongoIDField,
					Value: item,
				}}
				docs = append(docs, mongo.NewDeleteOneModel().SetFilter(filter))
			}
		}

		return collection.BulkWrite(sessCtx, docs)
	})
	if err != nil {
		return err
	}

	return nil
}

// Listen to Change Data Streams (CDC) if available
func (d *MongoDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan MongoDatasourceStreamResult {
	collection := d.client.Database(d.databaseName).Collection(d.collectionName)
	out := make(chan MongoDatasourceStreamResult)

	batchSize := request.BatchSize
	batchWindow := request.BatchWindowSeconds

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
			out <- MongoDatasourceStreamResult{
				DatasourceStreamResult: DatasourceStreamResult{
					Err: err,
				},
				Docs: MongoDatasourcePushRequest{},
			}
			return
		}
		defer stream.Close(bgCtx)

		// Ticker to enforce the batch processing time window
		ticker := time.NewTicker(time.Duration(batchWindow) * time.Second)
		defer ticker.Stop()

		// Buffer to hold the batch of change events
		batch := make([]bson.M, 0, batchSize)

		// Define the Batch event Processing Logic
		// This is the callback function that handles the collected events.
		// For this example, it just prints the operation type and document key for each event.
		processEvents := func(batch []bson.M) MongoDatasourcePushRequest {
			inserts, updates, deletes := []bson.M{}, map[string]bson.M{}, []string{}
			for _, event := range batch {
				opType := event["operationType"].(string)
				switch opType {
				case "insert":
					doc, ok := event["fullDocument"].(bson.M)
					if !ok {
						continue
					}
					inserts = append(inserts, doc)

				case "update":
					docKey, ok := event["documentKey"].(bson.M)
					if !ok {
						continue
					}

					doc, ok := event["fullDocument"].(bson.M)
					if !ok {
						continue
					}

					id := docKey[MongoIDField]
					updates[id.(string)] = doc

				case "delete":
					docKey, ok := event["documentKey"].(bson.M)
					if !ok {
						continue
					}

					id := docKey[MongoIDField]
					deletes = append(deletes, id.(string))
				}
			}
			return MongoDatasourcePushRequest{
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
				out <- MongoDatasourceStreamResult{
					DatasourceStreamResult: DatasourceStreamResult{
						Err: bgCtx.Err(),
					},
					Docs: processEvents(batch),
				}
				return

			case <-ticker.C:
				// Time window has elapsed. Process the batch if it's not empty.
				if len(batch) > 0 {
					out <- MongoDatasourceStreamResult{
						Docs: processEvents(batch),
					}
					// Reset the batch buffer
					batch = make([]bson.M, 0, batchSize)
				}

			default:
				// Check for the next event from the change stream.
				// Pass a short-lived context to TryNext to avoid blocking the select loop for too long.
				// This makes the loop more responsive to ticker events and context cancellation.
				var event bson.M
				if stream.Next(bgCtx) {
					if err := stream.Decode(&event); err != nil {
						event = bson.M{}
					}
					batch = append(batch, event)

					// If batch is full, process it immediately.
					if len(batch) >= int(batchSize) {
						out <- MongoDatasourceStreamResult{
							Docs: processEvents(batch),
						}

						// Reset the batch buffer and the ticker
						batch = make([]bson.M, 0, batchSize)
						ticker.Reset(time.Duration(batchWindow) * time.Second)
					}
				}

				// Check for any errors on the stream itself
				if err := stream.Err(); err != nil {
					out <- MongoDatasourceStreamResult{
						DatasourceStreamResult: DatasourceStreamResult{
							Err: err,
						},
						Docs: MongoDatasourcePushRequest{},
					}
					return
				}
			}
		}
	}(*ctx)

	return out
}
