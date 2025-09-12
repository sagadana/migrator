package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
	"github.com/sagadana/migrator/pipelines"
	"github.com/sagadana/migrator/states"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const IDField = "Index"
const CRUpdateField = "Test_Cr"
const TestDatasetPath = "./datasets/sample-100.csv"

type TestStore struct {
	id    string
	store states.Store
}

type TestPipeline struct {
	id          string
	source      datasources.Datasource
	destination datasources.Datasource
	transform   datasources.DatasourceTransformer
}

// --------
// Utils
// --------

// Get base path for temp dir
func getPipelinesTempBasePath() string {
	return helpers.GetTempBasePath("test-pipelines")
}

// Retrieve Test Stores
// TODO: Add more state stores here to test...
func getTestStores(ctx *context.Context, instanceId string) <-chan TestStore {
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
		store = states.NewFileStateStore(filepath.Join(getPipelinesTempBasePath(), instanceId), id)
		store.Clear(ctx)
		out <- TestStore{
			id:    id,
			store: store,
		}

		// -----------------------
		// 3. Redis
		// -----------------------
		redisAddr := os.Getenv("REDIS_ADDR")
		redisPass := os.Getenv("REDIS_PASS")
		redisDb := 0
		if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
			db, err := strconv.Atoi(dbStr)
			if err != nil {
				redisDb = db
			}
		}

		id = "redis-state-store"
		store = states.NewRedisStateStore(ctx, redisAddr, redisPass, redisDb, fmt.Sprintf("%s:%s", id, instanceId))
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
func getTestPipelines(ctx *context.Context, instanceId string) <-chan TestPipeline {
	out := make(chan TestPipeline)

	go func() {
		defer close(out)

		// Reusable vars
		var id string
		var fromDs datasources.Datasource
		var toDs datasources.Datasource

		// Create datasource name``
		getDsName := func(id string, name string) string {
			return fmt.Sprintf("%s-%s-%s", id, instanceId, name)
		}

		// -----------------------
		// 1. Memory
		// -----------------------

		id = "test-mem-to-mem-pipeline"
		fromDs = datasources.NewMemoryDatasource(getDsName(id, "source"), IDField)
		toDs = datasources.NewMemoryDatasource(getDsName(id, "destination"), IDField)

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
		mongoTransformer := func(data map[string]any) (map[string]any, error) {
			// Use to transform to include default mongo ID
			data[datasources.MongoIDField] = data[IDField]
			return data, nil
		}

		id = "test-mem-to-mongo-pipeline"
		fromDs = datasources.NewMemoryDatasource(getDsName(id, "source"), IDField)
		toDs = datasources.NewMongoDatasource(
			ctx,
			datasources.MongoDatasourceConfigs{
				URI:            mongoURI,
				DatabaseName:   mongoDB,
				CollectionName: getDsName(id, "destination"),
				Filter:         map[string]any{},
				Sort:           map[string]any{},
				AccurateCount:  true,
				OnInit: func(client *mongo.Client) error {
					return nil
				},
			},
		)

		// --------------- 2.1. Memory to Mongo --------------- //

		// Send test job
		out <- TestPipeline{
			id:          id,
			source:      fromDs,
			destination: toDs,
			transform:   mongoTransformer,
		}

		// --------------- 2.2. Mongo to Memory --------------- //

		id = "test-mongo-to-mem-pipeline"
		fromDs = datasources.NewMongoDatasource(
			ctx,
			datasources.MongoDatasourceConfigs{
				URI:             mongoURI,
				DatabaseName:    mongoDB,
				CollectionName:  getDsName(id, "source"),
				Filter:          map[string]any{},
				Sort:            map[string]any{},
				AccurateCount:   true,
				WithTransformer: mongoTransformer, // Use transformer when loading into source
				OnInit: func(client *mongo.Client) error {
					return nil
				},
			},
		)
		toDs = datasources.NewMemoryDatasource(getDsName(id, "destination"), IDField)

		// Send test job // TODO: Fix
		out <- TestPipeline{
			id:          id,
			source:      fromDs,
			destination: toDs,
			transform:   mongoTransformer,
		}
	}()

	return out
}

// -----------------------------
// Testers
// -----------------------------

// Use to test migration for a pipeline
func testMigration(
	t *testing.T,
	ctx *context.Context,
	pipeline *pipelines.Pipeline,
	config *pipelines.PipelineConfig,
	expectedTotal uint64,
) uint64 {

	var err error
	var evStarted, evInProgress, evStopped bool

	evWg := new(sync.WaitGroup)
	evWg.Add(2)

	config.OnMigrationStart = func(state states.State) {
		evStarted = true
		defer evWg.Done()

		if state.MigrationStatus != states.MigrationStatusInProgress {
			t.Errorf("❌ expects 'ReplicationStatus' to be: %s, got: %s", states.MigrationStatusInProgress, state.MigrationStatus)
		}

		// Attempt starting migration again
		//   expects to fail with error that migration is currently running
		err := pipeline.Start(ctx, &pipelines.PipelineConfig{}, false)
		if err != pipelines.ErrPipelineMigrating {
			t.Errorf("❌ expect migration duplicate error: '%s', got: '%s'", pipelines.ErrPipelineMigrating, err)
		}
	}

	config.OnMigrationError = func(state states.State, data datasources.DatasourcePushRequest, err error) {
		if state.MigrationIssue == "" || state.MigrationIssue != err.Error() {
			t.Errorf("❌ expects 'MigrationIssue' to be populated OnMigrationError")
		}
		t.Errorf("❌ OnMigrationError triggered: %s", err.Error())
	}

	config.OnMigrationProgress = func(state states.State, count datasources.DatasourcePushCount) {
		evInProgress = true

		if state.MigrationStatus != states.MigrationStatusInProgress {
			t.Errorf("❌ expects 'MigrationStatus' to be: %s, got: %s", states.MigrationStatusInProgress, state.MigrationStatus)
		}
	}

	config.OnMigrationStopped = func(state states.State) {
		evStopped = true
		defer evWg.Done()

		if state.MigrationStoppedAt.UnixMilli() == 0 {
			t.Errorf("❌ expects 'MigrationStoppedAt' to be set, got: %s", state.MigrationStoppedAt.String())
		}
	}

	// Start migration
	err = pipeline.Start(ctx, config, false)

	// Test empty source error
	fromCount := pipeline.From.Count(ctx, &datasources.DatasourceFetchRequest{})
	if fromCount == 0 && expectedTotal == 0 {
		if err != pipelines.ErrPipelineMigrationEmptySource {
			t.Fatalf("⛔️ expected error: %s, got: %s", pipelines.ErrPipelineMigrationEmptySource, err)
		}
		return 0
	}
	if err != nil {
		t.Fatalf("⛔️ failed to process migration: %s", err)
	}

	// Wait for migration start-stop events
	evWg.Wait()

	// Wait a bit - for migration process to propagate
	<-time.After(time.Duration(500 * time.Millisecond))

	// ------------------------------
	// Migration Completed
	// ------------------------------

	// Test callbacks triggers
	if expectedTotal > 0 {
		if !evStarted {
			t.Errorf("❌ failed to trigger OnMigrationStart")
		}
		if !evInProgress {
			t.Errorf("❌ failed to trigger OnMigrationProgress")
		}
		if !evStopped {
			t.Errorf("❌ failed to trigger OnMigrationStopped")
		}
	}

	// Get completion state
	state, ok := pipeline.GetState(ctx)
	if !ok {
		t.Fatalf("⛔️ failed to get migration state")
	}

	migrationEstimateCount, err := state.MigrationTotal.Int64()
	if err != nil {
		t.Errorf("❌ failed to get migration estimate count: %s", err)
	}

	migrationTotal, err := state.MigrationTotal.Int64()
	if err != nil {
		t.Errorf("❌ failed to get migration total: %s", err)
	}

	migrationOffset, err := state.MigrationOffset.Int64()
	if err != nil {
		t.Errorf("❌ failed to get migration offset: %s", err)
	}

	toTotal := pipeline.To.Count(ctx, &datasources.DatasourceFetchRequest{})

	// Ensure all data was migrated
	if state.MigrationStatus != states.MigrationStatusCompleted {
		t.Errorf("❌ migration failed. Expected Status: '%s', Got '%s'", string(states.MigrationStatusCompleted), string(state.MigrationStatus))
	}
	if expectedTotal != toTotal {
		t.Errorf("❌ migration failed. Expected Migrated Total: '%d', Got '%d'", expectedTotal, toTotal)
	}
	if migrationTotal < migrationEstimateCount {
		t.Errorf("❌ migration failed. Expected Migrated Total to be >= Estimate Count (%d): Got '%d'", migrationEstimateCount, migrationTotal)
	}
	if uint64(migrationTotal) != config.MigrationMaxSize {
		t.Errorf("❌ migration failed. Expected State Total: '%d', Got '%d'", config.MigrationMaxSize, migrationTotal)
	}
	if uint64(migrationOffset) != config.MigrationStartOffset+uint64(migrationTotal) {
		t.Errorf("❌ migration failed. Expected State Offset: '%d', Got '%d'", config.MigrationStartOffset+uint64(migrationTotal), migrationOffset)
	}

	return uint64(migrationTotal)
}

// Use to test continous replication for a pipeline
func testReplication(
	t *testing.T,
	ctx *context.Context,
	pipeline *pipelines.Pipeline,
	config *pipelines.PipelineConfig,
	replicationOnly bool,
) {
	crStart := make(chan bool, 1)
	crWg := new(sync.WaitGroup)
	crCtx, crCancel := context.WithTimeout(context.Background(), time.Duration(60*time.Second))

	evWg := new(sync.WaitGroup)
	evWg.Add(2)

	var evStarted, evInProgress, evStopped bool

	expectdUpdateCount := 4
	expectdDeleteCount := 3
	expectdInsertCount := expectdUpdateCount + expectdDeleteCount

	insertedIDs := make([]string, 0, expectdInsertCount)
	updatedIDs := make([]string, 0, expectdUpdateCount)
	deletedIDs := make([]string, 0, expectdDeleteCount)

	// Run background updates
	crWg.Add(1)
	go func() {
		defer func() {
			// Wait for replication batch window + processing before ending
			<-time.After(time.Duration((config.ReplicationBatchWindowSecs*1000)+500) * time.Millisecond)
			crCancel()  // End continuous replication
			crWg.Done() // Mark done
		}()

		<-crStart // Start when event received

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
			t.Errorf("❌ failed to perform background push (inserts): %s", err.Error())
			return
		}
		if count.Inserts != uint64(len(inserts)) {
			t.Errorf("❌ failed to perform background push (inserts): Expected '%d' docs inserted, Got '%d'", len(inserts), count.Inserts)
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
			t.Errorf("❌ failed to perform background push (updates): %s", err.Error())
			return
		}
		if count.Updates != uint64(len(updates)) {
			t.Errorf("❌ failed to perform background push (updates): Expected '%d' docs updates, Got '%d'", len(updates), count.Updates)
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
			t.Errorf("❌ failed to perform background push (deletes): %s", err.Error())
			return
		}
		if count.Deletes != uint64(len(deletes)) {
			t.Errorf("❌ failed to perform background push (deletes): Expected '%d' docs deleted, Got '%d'", len(deletes), count.Deletes)
			return
		}
		// Add deleted item ids
		deletedIDs = append(deletedIDs, deletes...)
	}()

	var err error

	config.OnReplicationError = func(state states.State, data datasources.DatasourcePushRequest, err error) {
		if state.ReplicationIssue == "" || state.ReplicationIssue != err.Error() {
			t.Errorf("❌ expects 'ReplicationIssue' to be populated OnReplicationError")
		}
		t.Errorf("❌ OnReplicationError triggered: %s", err.Error())
	}

	config.OnReplicationProgress = func(state states.State, count datasources.DatasourcePushCount) {
		evInProgress = true

		if state.ReplicationStatus != states.ReplicationStatusStreaming {
			t.Errorf("❌ expects 'ReplicationStatus' to be: %s, got: %s", states.ReplicationStatusStreaming, state.ReplicationStatus)
		}
	}

	config.OnReplicationStopped = func(state states.State) {
		evStopped = true
		defer evWg.Done()

		if state.ReplicationStatus != states.ReplicationStatusPaused {
			t.Errorf("❌ expects 'ReplicationStatus' to be: %s, got: %s", states.ReplicationStatusPaused, state.ReplicationStatus)
		}
	}

	config.OnReplicationStart = func(state states.State) {
		evStarted = true
		defer evWg.Done()

		if state.ReplicationStatus != states.ReplicationStatusStreaming {
			t.Errorf("❌ expects 'ReplicationStatus' to be: %s, got: %s", states.ReplicationStatusStreaming, state.ReplicationStatus)
		}

		// For replication-only test
		if replicationOnly {

			// Attempt starting replication again
			//   expects to fail with error that replication is currently running
			err := pipeline.Stream(&crCtx, &pipelines.PipelineConfig{})
			if err != pipelines.ErrPipelineReplicating {
				t.Errorf("❌ expect replication duplicate error: '%s', got: '%s'", pipelines.ErrPipelineReplicating, err)
			}

			// Start background updates
			<-time.After(time.Duration(100 * time.Millisecond)) // Wait a bit
			crStart <- true                                     // Send event to trigger background updates
			close(crStart)
		}
	}

	if replicationOnly { // Test Replication Only
		err = pipeline.Stream(&crCtx, config)
		if err != nil {
			t.Errorf("❌ continuous replication failed: %s", err.Error())
		}
	} else { // Test Migration + Replication

		// Start background updates - when migration completed
		config.OnMigrationStopped = func(state states.State) {
			if state.MigrationStatus == states.MigrationStatusCompleted {
				<-time.After(time.Duration(500 * time.Millisecond)) // Wait a bit - for previous migration process
				crStart <- true                                     // Send event to trigger background updates
				close(crStart)
			}
		}

		err = pipeline.Start(&crCtx, config, true)
		if err != nil {
			t.Errorf("❌ migration + continuous replication failed: %s", err.Error())
		}
	}

	// Wait for background updates to be made
	crWg.Wait()

	// Wait for replication start-stop events
	evWg.Wait()

	// ---------------------------------------------
	// Replication Completed and 'crCtx' is closed
	// ---------------------------------------------

	// Test callbacks triggers
	if !evStarted {
		t.Errorf("❌ failed to trigger OnReplicationStart")
	}
	if !evInProgress {
		t.Errorf("❌ failed to trigger OnReplicationProgress")
	}
	if !evStopped {
		t.Errorf("❌ failed to trigger OnReplicationStopped")
	}

	// Ensure replication was stopped gracefuly
	state, ok := pipeline.GetState(ctx)
	if !ok {
		t.Fatalf("⛔️ failed to get replication state")
	} else if state.ReplicationStatus != states.ReplicationStatusPaused {
		t.Errorf("❌ continuous replication failed. Expected status: '%s', Got '%s'", string(states.ReplicationStatusPaused), string(state.ReplicationStatus))
	}

	// Ensure all background changes to source were replicated correctly unto the destination
	// Fetch items from the destination to datasource to update
	if len(insertedIDs) != expectdInsertCount {
		t.Errorf("❌ continuous replication (inserts) failed. Expected '%d' ids, Got '%d'", expectdInsertCount, len(insertedIDs))
	}
	if len(updatedIDs) != expectdUpdateCount {
		t.Errorf("❌ continuous replication (updates) failed. Expected '%d' ids, Got '%d'", expectdUpdateCount, len(updatedIDs))
	}
	if len(deletedIDs) != expectdDeleteCount {
		t.Errorf("❌ continuous replication (deletes) failed. Expected '%d' ids, Got '%d'", expectdDeleteCount, len(deletedIDs))
	}

	// --- Update -- //
	result := pipeline.To.Fetch(ctx, &datasources.DatasourceFetchRequest{
		IDs: updatedIDs,
	})
	if len(result.Docs) != len(updatedIDs) {
		t.Errorf("❌ continuous replication results (updates) failed. Expected '%d' docs, Got '%d'", len(updatedIDs), len(result.Docs))
	}
	for _, doc := range result.Docs {
		id := doc[IDField]
		_, ok := doc[CRUpdateField]
		if !ok {
			t.Errorf("❌ continuous replication results (updates) failed. Expected '%s' field in '%s' doc", CRUpdateField, id)
		}
	}

	// --- Deletes -- //
	result = pipeline.To.Fetch(ctx, &datasources.DatasourceFetchRequest{
		IDs: deletedIDs,
	})
	if len(result.Docs) != 0 {
		t.Errorf("❌ continuous replication results (deletes) failed. Expected '%d' docs to be deleted", len(deletedIDs))
	}
}

// -----------------------------
// Tests
// -----------------------------

func TestPipelineImplementations(t *testing.T) {
	testCtx := context.Background()

	instanceId := helpers.RandomString(6)
	logger := helpers.CreateTextLogger()

	for tp := range getTestPipelines(&testCtx, instanceId) {

		t.Run(tp.id, func(t *testing.T) {

			t.Parallel() // Run pipelines tests in parallel

			// Run tests for different state store types
			for st := range getTestStores(&testCtx, fmt.Sprintf("%s-%s", tp.id, instanceId)) {

				fmt.Println("\n------------------------------------------------------------------------------------------------------------------")
				t.Run(fmt.Sprintf("%s-%s", tp.id, st.id), func(t *testing.T) {
					fmt.Println("------------------------------------------------------------------------------------------------------------------")

					// Clean up - after
					t.Cleanup(func() {
						tp.source.Clear(&testCtx)
						tp.destination.Clear(&testCtx)
						st.store.Clear(&testCtx)
					})

					// Load sample data into source
					tp.source.Clear(&testCtx)
					err := datasources.LoadCSV(&testCtx, tp.source, TestDatasetPath, 10)
					if err != nil {
						panic(fmt.Errorf("failed to load data from CSV to '%s' source: %s", tp.id, err))
					}

					pipeline := pipelines.Pipeline{
						ID:     tp.id,
						From:   tp.source,
						To:     tp.destination,
						Store:  st.store,
						Logger: logger,
					}

					// ------------------------
					// 1. Migration only
					// ------------------------

					t.Run("Migration", func(t *testing.T) {
						ctx, cancel := context.WithTimeout(testCtx, time.Duration(60)*time.Second)
						defer cancel()

						// Clear first
						pipeline.To.Clear(&ctx)
						pipeline.Store.Clear(&ctx)

						// 1. First round
						maxSize := uint64(30)
						startOffset := uint64(0)
						migrationTotal := testMigration(
							t, &ctx, &pipeline, &pipelines.PipelineConfig{
								MigrationParallelWorkers: 4,
								MigrationBatchSize:       4,
								MigrationMaxSize:         maxSize,
								MigrationStartOffset:     startOffset,
							},
							maxSize,
						)

						// 2. Resume from last round
						maxSize = 20
						startOffset += migrationTotal
						testMigration(
							t, &ctx, &pipeline, &pipelines.PipelineConfig{
								MigrationParallelWorkers: 4,
								MigrationBatchSize:       4,
								MigrationMaxSize:         maxSize,
								MigrationStartOffset:     startOffset,
							},
							maxSize+migrationTotal,
						)

					})

					// --------------------------------------------------------------------
					// 2. Migration (with continuous replication enabled)
					// --------------------------------------------------------------------

					t.Run("Migration_And_Continuous_Replication)", func(t *testing.T) {
						ctx, cancel := context.WithTimeout(testCtx, time.Duration(60)*time.Second)
						defer cancel()

						// Clear first
						pipeline.To.Clear(&ctx)
						pipeline.Store.Clear(&ctx)

						maxSize := uint64(30)
						startOffset := uint64(10)
						replicationBatchSize := uint64(10)
						replicationBatchWindowSecs := uint64(1)
						testReplication(
							t, &ctx, &pipeline, &pipelines.PipelineConfig{
								MigrationParallelWorkers:   4,
								MigrationBatchSize:         5,
								MigrationMaxSize:           maxSize,
								MigrationStartOffset:       startOffset,
								ReplicationBatchSize:       replicationBatchSize,
								ReplicationBatchWindowSecs: replicationBatchWindowSecs,
							}, false,
						)
					})

					// -------------------------------
					// 3. Continuous replication only
					// -------------------------------

					t.Run("Continuous_Replication", func(t *testing.T) {
						ctx, cancel := context.WithTimeout(testCtx, time.Duration(60)*time.Second)
						defer cancel()

						// Clear first
						pipeline.To.Clear(&ctx)
						pipeline.Store.Clear(&ctx)

						replicationBatchSize := uint64(10)
						replicationBatchWindowSecs := uint64(1)
						testReplication(
							t, &ctx, &pipeline, &pipelines.PipelineConfig{
								ReplicationBatchSize:       replicationBatchSize,
								ReplicationBatchWindowSecs: replicationBatchWindowSecs,
							}, true,
						)
					})

					// ---------------------------------
					// 4. Migration (with empty source)
					// ---------------------------------

					t.Run("Migration_Empty_Source", func(t *testing.T) {
						ctx, cancel := context.WithTimeout(testCtx, time.Duration(60)*time.Second)
						defer cancel()

						// Clear first
						pipeline.From.Clear(&ctx)
						pipeline.To.Clear(&ctx)
						pipeline.Store.Clear(&ctx)

						testMigration(
							t, &ctx, &pipeline, &pipelines.PipelineConfig{
								MigrationParallelWorkers: 4,
								MigrationBatchSize:       4,
								MigrationStartOffset:     0,
							},
							0,
						)
					})
					fmt.Print("\n------------------------------------------------------------------------------------------------------------------\n\n")
				})
			}
		})
	}
}
