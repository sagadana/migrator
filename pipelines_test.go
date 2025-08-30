package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
	"github.com/sagadana/migrator/pipelines"
	"github.com/sagadana/migrator/states"
	"go.mongodb.org/mongo-driver/mongo"
)

const IDField = "Index"
const CRUpdateField = "Test_Cr"

type TestStore struct {
	id    string
	store states.Store
}

type TestPipeline struct {
	id          string
	source      datasources.Datasource
	destination datasources.Datasource
	trasform    datasources.DatasourceTransformer
}

// --------
// Utils
// --------

// Retrieve Test Stores
// TODO: Add more state stores here to test...
func getTestStores(ctx *context.Context, basePath string) <-chan TestStore {
	out := make(chan TestStore)

	// Reusable vars
	var id string
	var store states.Store

	go func() {
		defer close(out)

		// -----------------------
		// 1. Memory
		// -----------------------
		id = "memory-state-store"
		store = states.NewMemoryStateStore()
		store.Clear(ctx)

		out <- TestStore{
			id:    id,
			store: store,
		}

		// -----------------------
		// 2. File
		// -----------------------
		id = "file-state-store"
		store = states.NewFileStateStore(basePath, "state")
		store.Clear(ctx)

		out <- TestStore{
			id:    id,
			store: store,
		}
	}()

	return out
}

// Retrieve Test Pipelines
// TODO: Add more pipelines here to test...
func getTestPipelines(ctx *context.Context, basePath string) <-chan TestPipeline {
	out := make(chan TestPipeline)

	go func() {
		defer close(out)

		// Reusable vars
		var id string
		var fromDs datasources.Datasource
		var toDs datasources.Datasource

		// -----------------------
		// 1. File
		// -----------------------

		id = "test-file-to-file-pipeline"
		fromDs = datasources.NewFileDatasource(basePath, id+"-source", IDField)
		toDs = datasources.NewFileDatasource(basePath, id+"-destination", IDField)
		fromDs.Clear(ctx)
		toDs.Clear(ctx)

		// Load sample data into source
		err := datasources.LoadCSV(ctx, fromDs, "./tests/sample-100.csv", 10, nil)
		if err != nil {
			panic(fmt.Errorf("failed to load data from CSV to file 'fromDs': %s", err))
		}

		out <- TestPipeline{
			id:          id,
			source:      fromDs,
			destination: toDs,
		}

		// -----------------------
		// 2. Mongo
		// -----------------------

		mongoURI := os.Getenv("MONGO_URI")
		mongoDB := os.Getenv("MONGO_DB")

		id = "test-file-to-mongo-pipeline"
		fromDs = datasources.NewFileDatasource(basePath, id+"-source", IDField)
		toDs = datasources.NewMongoDatasource(
			ctx,
			datasources.MongoDatasourceConfigs{
				URI:            mongoURI,
				DatabaseName:   mongoDB,
				CollectionName: id + "-destination",
				Filter:         map[string]any{},
				Sort:           map[string]any{},
				AccurateCount:  true,
				OnInit: func(client *mongo.Client) error {
					return nil
				},
			},
		)
		fromDs.Clear(ctx)
		toDs.Clear(ctx)

		// Use to transform to include default mongo ID
		mongoTransformer := func(data map[string]any) (map[string]any, error) {
			data[datasources.MongoIDField] = data[IDField]
			return data, nil
		}

		// --------------- 2.1. File to Mongo --------------- //

		// Load sample data into source
		err = datasources.LoadCSV(ctx, fromDs, "./tests/sample-100.csv", 10, nil)
		if err != nil {
			panic(fmt.Errorf("failed to load data from CSV to file 'fromDs': %s", err))
		}

		// Send test job
		out <- TestPipeline{
			id:          id,
			source:      fromDs,
			destination: toDs,
			trasform:    mongoTransformer,
		}

		// --------------- 2.2. Mongo to File --------------- //

		id = "test-mongo-to-file-pipeline"
		fromDs = datasources.NewMongoDatasource(
			ctx,
			datasources.MongoDatasourceConfigs{
				URI:            mongoURI,
				DatabaseName:   mongoDB,
				CollectionName: id + "-source",
				Filter:         map[string]any{},
				Sort:           map[string]any{},
				AccurateCount:  true,
				OnInit: func(client *mongo.Client) error {
					return nil
				},
			},
		)
		toDs = datasources.NewFileDatasource(basePath, id+"-destination", IDField)
		fromDs.Clear(ctx)
		toDs.Clear(ctx)

		// Load sample data into source
		err = datasources.LoadCSV(ctx, fromDs, "./tests/sample-100.csv", 10, mongoTransformer)
		if err != nil {
			panic(fmt.Errorf("failed to load data from CSV to 'fromMongoDs': %s", err))
		}

		// Send test job // TODO: Fix
		// out <- TestPipeline{
		// 	id:          id,
		// 	source:      fromDs,
		// 	destination: toDs,
		// 	trasform:    mongoTransformer,
		// }
	}()

	return out
}

// Get base path for temp dir
func getTempBasePath() string {
	basePath := os.Getenv("RUNNER_TEMP")
	if basePath == "" {
		basePath = os.Getenv("TEMP_BASE_PATH")
		if basePath == "" {
			basePath = os.TempDir()
		}
	}
	return basePath
}

// -----------------------------
// Testers
// -----------------------------

// Use to test migration for a pipeline
func testMigration(
	title string,
	t *testing.T,
	ctx *context.Context,
	pipeline *pipelines.Pipeline,
	config *pipelines.PipelineConfig,
) int64 {
	fmt.Printf("\n-------------------------- %s --------------------------\n", title)

	var err error
	previousOffset := int64(0)

	config.OnMigrationError = func(state states.State, err error) {
		t.Errorf("OnMigrationError triggered: %s", err.Error())
	}

	// Get inital state
	state, ok := pipeline.GetState(ctx)
	if ok {
		previousOffset, _ = state.MigrationOffset.Int64()
	}
	// Get expected total
	expectedTotal := pipeline.From.Count(ctx, &datasources.DatasourceFetchRequest{
		Size:   config.MigrationMaxSize,
		Offset: max(previousOffset, config.MigrationStartOffset),
	})

	// Start migration
	err = pipeline.Start(ctx, *config)
	if err != nil {
		t.Errorf("failed to process migration: %s", err)
	}

	// Get completion state
	state, ok = pipeline.GetState(ctx)
	if !ok {
		t.Errorf("failed to get migration state")
	}

	migrationTotal, err := state.MigrationTotal.Int64()
	if err != nil {
		t.Errorf("failed to get migration total: %s", err)
	}

	migrationOffset, err := state.MigrationOffset.Int64()
	if err != nil {
		t.Errorf("failed to get migration offset: %s", err)
	}

	toTotal := pipeline.To.Count(ctx, &datasources.DatasourceFetchRequest{})

	// Ensure all data was migrated
	if state.MigrationStatus != states.MigrationStatusCompleted {
		t.Errorf("migration failed. Expected Status: '%s', Got '%s'", string(states.MigrationStatusCompleted), string(state.MigrationStatus))
	}
	if expectedTotal != toTotal {
		t.Errorf("migration failed. Expected Migrated Total: '%d', Got '%d'", expectedTotal, toTotal)
	}
	if migrationTotal != toTotal {
		t.Errorf("migration failed. Expected State Total: '%d', Got '%d'", toTotal, migrationTotal)
	}
	if migrationOffset != config.MigrationStartOffset+migrationTotal {
		t.Errorf("migration failed. Expected State Offset: '%d', Got '%d'", config.MigrationStartOffset+migrationTotal, migrationOffset)
	}

	return migrationTotal
}

// Use to test continous replication for a pipeline
func testReplication(
	title string,
	t *testing.T,
	ctx *context.Context,
	pipeline *pipelines.Pipeline,
	config *pipelines.PipelineConfig,
	replicationOnly bool,
) {
	fmt.Printf("\n-------------------------- %s --------------------------\n", title)

	crStart := make(chan bool, 1)
	crWg := new(sync.WaitGroup)
	crCtx, crCancel := context.WithTimeout(*ctx, time.Duration(10*time.Second))

	expectdUpdateCount := 4
	expectdDeleteCount := 3
	expectdInsertCount := expectdUpdateCount + expectdDeleteCount

	insertedIDs := make([]string, 0, expectdInsertCount)
	updatedIDs := make([]string, 0, expectdUpdateCount)
	deletedIDs := make([]string, 0, expectdDeleteCount)

	// Run background updates
	crWg.Add(1)
	go func() {
		<-crStart // Start when event received
		defer func() {
			// Wait for replication batch window before ending
			<-time.After(time.Duration(max(2, config.ReplicationBatchWindowSecs)) * time.Second)
			crWg.Done() // Mark done
			crCancel()  // End continuous replication
		}()

		t.Logf("Running background updates for contiuous replication...")

		inserts := make([]map[string]any, 0)
		updates := make(map[string]map[string]any)
		deletes := make([]string, 0)

		// --- Inserts --- //
		for i := range expectdInsertCount {
			doc := map[string]any{
				IDField:     fmt.Sprintf("test-%d-%s", i+1, helpers.RandomString(16)), // Create new IDs
				"CreatedAt": time.Now(),
			}
			if pipeline.Transform != nil {
				transformed, err := pipeline.Transform(doc)
				if err != nil {
					doc = transformed
				}
			}
			inserts = append(inserts, doc)
		}
		t.Logf("Performing background push (inserts) for %d changes", len(inserts))
		count, err := pipeline.From.Push(&crCtx, &datasources.DatasourcePushRequest{
			Inserts: inserts,
		})
		if err != nil {
			t.Errorf("failed to perform background push (inserts): %s", err.Error())
			return
		}
		if count.Inserts != int64(len(inserts)) {
			t.Errorf("failed to perform background push (inserts): Expected '%d' docs inserted, Got '%d'", len(inserts), count.Inserts)
			return
		}
		// Add inserted item ids
		for _, doc := range inserts {
			id, ok := doc[IDField]
			if ok {
				insertedIDs = append(insertedIDs, id.(string))
			}
		}

		// --- Updates & Deletes --- //
		// Update and delete previously inserted items
		for _, doc := range inserts {
			if len(updates) < expectdUpdateCount { // Add Updates
				if pipeline.Transform != nil {
					transformed, err := pipeline.Transform(doc)
					if err != nil {
						doc = transformed
					}
				}
				id, ok := doc[IDField]
				if ok {
					doc[CRUpdateField] = true
					doc["UpdatedAt"] = time.Now()
					updates[id.(string)] = doc
				}
			} else { // Add Deletes
				id, ok := doc[IDField]
				if ok {
					deletes = append(deletes, id.(string))
				}
			}
		}

		t.Logf("Performing background push (updates) for %d changes", len(updates))
		count, err = pipeline.From.Push(&crCtx, &datasources.DatasourcePushRequest{
			Updates: updates,
		})
		if err != nil {
			t.Errorf("failed to perform background push (updates): %s", err.Error())
			return
		}
		if count.Updates != int64(len(updates)) {
			t.Errorf("failed to perform background push (updates): Expected '%d' docs updates, Got '%d'", len(updates), count.Updates)
			return
		}
		// Add updated item ids
		for id := range updates {
			updatedIDs = append(updatedIDs, id)
		}

		t.Logf("Performing background push (deletes) for %d changes", len(deletes))
		count, err = pipeline.From.Push(&crCtx, &datasources.DatasourcePushRequest{
			Deletes: deletes,
		})
		if err != nil {
			t.Errorf("failed to perform background push (deletes): %s", err.Error())
			return
		}
		if count.Deletes != int64(len(deletes)) {
			t.Errorf("failed to perform background push (deletes): Expected '%d' docs deleted, Got '%d'", len(deletes), count.Deletes)
			return
		}
		// Add deleted item ids
		deletedIDs = append(deletedIDs, deletes...)
	}()

	var err error
	config.ContinuousReplication = true

	config.OnReplicationError = func(state states.State, err error) {
		t.Errorf("OnReplicationError triggered: %s", err.Error())
	}
	config.OnReplicationProgress = func(state states.State, count datasources.DatasourcePushCount) {
		t.Logf("OnReplicationProgress triggered: Changes: %d", count.Inserts+count.Updates+count.Deletes)
	}

	if replicationOnly {
		config.OnReplicationStart = func(state states.State) {
			t.Log("OnReplicationStart triggered")
			<-time.After(time.Duration(200 * time.Millisecond)) // Wait a bit
			crStart <- true                                     // Send event to trigger background updates
			close(crStart)                                      // No more updates expected
		}
		err = pipeline.Stream(&crCtx, *config)
		if err != nil {
			t.Errorf("continuous replication failed: %s", err.Error())
		}
	} else {
		config.OnMigrationStopped = func(_ states.State) {
			t.Log("OnMigrationStopped triggered")
			<-time.After(time.Duration(200 * time.Millisecond)) // Wait a bit
			crStart <- true                                     // Send event to trigger background updates
			close(crStart)                                      // No more updates expected
		}
		err = pipeline.Start(&crCtx, *config)
		if err != nil {
			t.Errorf("migration + continuous replication failed: %s", err.Error())
		}
	}

	// Wait for background updates to be made
	crWg.Wait()

	// ---------------------------------------------
	// Replication Completed and 'crCtx' is closed
	// ---------------------------------------------

	// Ensure replication was stopped gracefuly
	state, ok := pipeline.GetState(ctx)
	if !ok {
		t.Errorf("failed to get replication state: %s", err)
	}
	if state.ReplicationStatus != states.ReplicationStatusPaused {
		t.Errorf("continuous replication failed. Expected status: '%s', Got '%s'", string(states.ReplicationStatusPaused), string(state.ReplicationStatus))
	}

	// Ensure all background changes to source were replicated correctly unto the destination
	// Fetch items from the destination to datasource to update

	if len(insertedIDs) != expectdInsertCount {
		t.Errorf("continuous replication (inserts) failed. Expected '%d' ids, Got '%d'", expectdInsertCount, len(insertedIDs))
	}
	if len(updatedIDs) != expectdUpdateCount {
		t.Errorf("continuous replication (updates) failed. Expected '%d' ids, Got '%d'", expectdUpdateCount, len(updatedIDs))
	}
	if len(deletedIDs) != expectdDeleteCount {
		t.Errorf("continuous replication (deletes) failed. Expected '%d' ids, Got '%d'", expectdDeleteCount, len(deletedIDs))
	}

	// --- Update -- //
	result := pipeline.To.Fetch(ctx, &datasources.DatasourceFetchRequest{
		IDs: updatedIDs,
	})
	if len(result.Docs) != len(updatedIDs) {
		t.Errorf("continuous replication results (updates) failed. Expected '%d' docs, Got '%d'", len(updatedIDs), len(result.Docs))
	}
	for _, doc := range result.Docs {
		id := doc[IDField]
		_, ok := doc[CRUpdateField]
		if !ok {
			t.Errorf("continuous replication results (updates) failed. Expected '%s' field in '%s' doc", CRUpdateField, id)
		}
	}

	// --- Deletes -- //
	result = pipeline.To.Fetch(ctx, &datasources.DatasourceFetchRequest{
		IDs: deletedIDs,
	})
	if len(result.Docs) != 0 {
		t.Errorf("continuous replication results (deletes) failed. Expected '%d' docs to be deleted", len(deletedIDs))
	}
}

// -----------------------------
// Tests
// -----------------------------

func Test_Pipelines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	basePath := getTempBasePath()

	// Run tests for different state store types
	for st := range getTestStores(&ctx, basePath) {

		basePath := filepath.Join(getTempBasePath(), string(st.id))
		os.RemoveAll(basePath) // Clean test path

		for tp := range getTestPipelines(&ctx, basePath) {

			fmt.Println("\n---------------------------------------------------------------------------------")
			t.Run(fmt.Sprintf("%s-%s", tp.id, st.id), func(t *testing.T) {
				fmt.Println("---------------------------------------------------------------------------------")

				pipeline := pipelines.Pipeline{
					ID:     tp.id,
					From:   tp.source,
					To:     tp.destination,
					Store:  st.store,
					Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
				}

				// ------------------------
				// 1. Migration only
				// ------------------------

				maxSize := int64(30)
				startOffset := int64(20)

				migrationTotal := testMigration(
					"Migration only",
					t, &ctx, &pipeline, &pipelines.PipelineConfig{
						MigrationParallelLoad: 4,
						MigrationBatchSize:    4,
						MigrationMaxSize:      maxSize,
						MigrationStartOffset:  startOffset,
					},
				)

				// --------------------------------------------------------------------
				// 2. Migration (with continuous replication enabled)
				// --------------------------------------------------------------------

				maxSize = 20
				startOffset += migrationTotal
				replicationBatchSize := int64(10)
				replicationBatchWindowSecs := int64(1)

				testReplication(
					"Migration (with continuous replication enabled)",
					t, &ctx, &pipeline, &pipelines.PipelineConfig{
						MigrationParallelLoad:      4,
						MigrationBatchSize:         5,
						MigrationMaxSize:           maxSize,
						MigrationStartOffset:       startOffset,
						ContinuousReplication:      true,
						ReplicationBatchSize:       replicationBatchSize,
						ReplicationBatchWindowSecs: replicationBatchWindowSecs,
					}, false,
				)

				// -------------------------------
				// 3. Continuous replication only
				// -------------------------------

				testReplication(
					"Continuous replication only",
					t, &ctx, &pipeline, &pipelines.PipelineConfig{
						ContinuousReplication:      true,
						ReplicationBatchSize:       replicationBatchSize,
						ReplicationBatchWindowSecs: replicationBatchWindowSecs,
					}, true,
				)

				fmt.Print("\n---------------------------------------------------------------------------------\n\n")
			})

			// Clean up
			tp.source.Clear(&ctx)
			tp.destination.Clear(&ctx)
		}

		// Clean up
		st.store.Clear(&ctx)
	}
}
