package pipelines

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
	"github.com/sagadana/migrator/states"
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
		if dbStr := os.Getenv("REDIS_STATE_DB"); dbStr != "" {
			db, err := strconv.Atoi(dbStr)
			if err != nil {
				redisDb = db
			}
		}

		id = "redis-state-store"
		store = states.NewRedisStateStore(ctx, redisAddr, redisPass, redisDb, fmt.Sprintf("state-%s:%s", id, instanceId))
		store.Clear(ctx)
		out <- TestStore{
			id:    id,
			store: store,
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
	pipeline *Pipeline,
	config *PipelineConfig,
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
		err := pipeline.Start(ctx, &PipelineConfig{}, false)
		if err != ErrPipelineMigrating {
			t.Errorf("❌ expect migration duplicate error: '%s', got: '%s'", ErrPipelineMigrating, err)
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
	if expectedTotal == 0 {
		if err == ErrPipelineMigrationEmptySource {
			return 0 // expected
		}
		t.Fatalf("⛔️ expected error: %s, got: %s", ErrPipelineMigrationEmptySource, err)
	}
	if err != nil {
		t.Fatalf("⛔️ failed to process migration: %s", err)
	}

	// Wait for migration start-stop events
	evWg.Wait()

	// Wait a bit - for migration process to propagate
	<-time.After(time.Duration(1000+500) * time.Millisecond)

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

// Use to test continuous replication for a pipeline
func testReplication(
	t *testing.T,
	ctx *context.Context,
	pipeline *Pipeline,
	config *PipelineConfig,
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
			<-time.After(time.Duration((config.ReplicationBatchWindowSecs*1000)+400) * time.Millisecond)
			t.Logf("Completed background updates for contiuous replication...")
			crCancel()  // End continuous replication
			crWg.Done() // Mark done
		}()

		<-crStart // Start when event received

		t.Logf("Running background updates for contiuous replication...")

		random := helpers.RandomString(6)
		getId := func(i int) string {
			return fmt.Sprintf("test-%d-%s", i+1, random)
		}

		// ---- Insert ---- //
		r := &datasources.DatasourcePushRequest{
			Inserts: make([]map[string]any, 0),
		}
		for i := range expectdInsertCount {
			doc := map[string]any{
				IDField:     getId(i + 1),
				"CreatedAt": time.Now(),
			}
			if pipeline.Transform != nil {
				transformed, err := pipeline.Transform(doc)
				if err != nil {
					doc = transformed
				}
			}
			r.Inserts = append(r.Inserts, doc)
		}
		c, err := pipeline.From.Push(&crCtx, r)
		if err != nil {
			t.Errorf("❌ background insert error: %s", err.Error())
			return
		}
		if c.Inserts != uint64(len(r.Inserts)) {
			t.Errorf("❌ failed to perform background push (inserts): Expected '%d' docs inserted, Got '%d'", len(r.Inserts), c.Inserts)
			return
		}
		// Add inserted item ids
		for _, doc := range r.Inserts {
			id, ok := doc[IDField]
			if ok {
				insertedIDs = append(insertedIDs, id.(string))
			}
		}

		// ---- Update ---- //
		r = &datasources.DatasourcePushRequest{
			Updates: make([]map[string]any, 0),
		}
		for i := range expectdUpdateCount {
			doc := map[string]any{
				IDField: getId(i + 1),
			}
			if pipeline.Transform != nil {
				transformed, err := pipeline.Transform(doc)
				if err != nil {
					doc = transformed
				}
			}
			doc[CRUpdateField] = true
			doc["UpdatedAt"] = time.Now()
			r.Updates = append(r.Updates, doc)
		}
		c, err = pipeline.From.Push(&crCtx, r)
		if err != nil {
			t.Errorf("❌ background update error: %s", err.Error())
			return
		}
		if c.Updates != uint64(len(r.Updates)) {
			t.Errorf("❌ failed to perform background push (updates): Expected '%d' docs inserted, Got '%d'", len(r.Updates), c.Updates)
			return
		}
		// Add updated item ids
		for _, doc := range r.Updates {
			id, ok := doc[IDField]
			if ok {
				updatedIDs = append(updatedIDs, id.(string))
			}
		}

		// ---- Delete ---- //
		r = &datasources.DatasourcePushRequest{
			Deletes: make([]string, 0),
		}
		for i := expectdUpdateCount; i < expectdInsertCount; i++ {
			r.Deletes = append(r.Deletes, getId(i+1))
		}
		c, err = pipeline.From.Push(&crCtx, r)
		if err != nil {
			t.Errorf("❌ background delete error: %s", err.Error())
			return
		}
		if c.Deletes != uint64(len(r.Deletes)) {
			t.Errorf("❌ failed to perform background push (deletes): Expected '%d' docs inserted, Got '%d'", len(r.Deletes), c.Deletes)
			return
		}
		// Add deleted item ids
		deletedIDs = append(deletedIDs, r.Deletes...)
	}()

	var err error

	config.OnReplicationError = func(state states.State, data datasources.DatasourcePushRequest, err error) {
		if err == nil {
			t.Errorf("❌ expects 'err' to be populated OnReplicationError")
		}
		if state.ReplicationIssue == "" || state.ReplicationIssue != err.Error() {
			t.Errorf("❌ expects 'ReplicationIssue' to be populated OnReplicationError")
		}
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
			err := pipeline.Stream(&crCtx, &PipelineConfig{})
			if err != ErrPipelineReplicating {
				t.Errorf("❌ expect replication duplicate error: '%s', got: '%s'", ErrPipelineReplicating, err)
			}

			// Start background updates
			go func() {
				<-time.After(time.Duration(200 * time.Millisecond)) // Wait a bit for replication subscription
				crStart <- true                                     // Send event to trigger background updates
				close(crStart)
			}()
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
				<-time.After(time.Duration(200 * time.Millisecond)) // Wait a bit for replication subscription
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

	// Wait a bit for async processes
	<-time.After(time.Duration(200) * time.Millisecond)

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

	// ---------------------------------------------
	// Replication Completed and 'crCtx' is closed
	// ---------------------------------------------

	// Ensure replication was stopped gracefully
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

func runPipelineTest(testCtx context.Context, t *testing.T, tp TestPipeline) {
	logger := helpers.CreateTextLogger(slog.LevelWarn)
	testDatasetPath := os.Getenv("TEST_DATASET_CSV_PATH")
	if testDatasetPath == "" {
		t.Fatalf("❌ 'TEST_DATASET_CSV_PATH' environment variable is not set")
	}

	t.Run(tp.id, func(t *testing.T) {

		t.Parallel() // Run pipelines tests in parallel

		// Close - after
		t.Cleanup(func() {
			tp.source.Close(&testCtx)
			tp.destination.Close(&testCtx)
		})

		// Run tests for different state store types
		for st := range getTestStores(&testCtx, tp.id) {

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
				err := tp.source.Import(&testCtx, datasources.DatasourceImportRequest{
					Type:      datasources.DatasourceImportTypeCSV,
					Source:    datasources.DatasourceImportSourceFile,
					Location:  testDatasetPath,
					BatchSize: 10,
				})
				if err != nil {
					panic(fmt.Errorf("failed to load data from CSV to '%s' source: %s", tp.id, err))
				}

				pipeline := Pipeline{
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
						t, &ctx, &pipeline, &PipelineConfig{
							MigrationParallelWorkers: 4,
							MigrationBatchSize:       8,
							MigrationMaxSize:         maxSize,
							MigrationStartOffset:     startOffset,
						},
						maxSize,
					)

					// 2. Resume from last round
					maxSize = 20
					startOffset += migrationTotal
					testMigration(
						t, &ctx, &pipeline, &PipelineConfig{
							MigrationParallelWorkers: 3,
							MigrationBatchSize:       7,
							MigrationMaxSize:         maxSize,
							MigrationStartOffset:     startOffset,
						},
						maxSize+migrationTotal,
					)

				})

				t.Run("Migration_From_Offset", func(t *testing.T) {
					ctx, cancel := context.WithTimeout(testCtx, time.Duration(60)*time.Second)
					defer cancel()

					// Clear first
					pipeline.To.Clear(&ctx)
					pipeline.Store.Clear(&ctx)

					// Start from offset
					startOffset := uint64(10)
					maxSize := uint64(20)
					testMigration(
						t, &ctx, &pipeline, &PipelineConfig{
							MigrationParallelWorkers: 4,
							MigrationBatchSize:       5,
							MigrationMaxSize:         maxSize,
							MigrationStartOffset:     startOffset,
						},
						maxSize,
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
						t, &ctx, &pipeline, &PipelineConfig{
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
						t, &ctx, &pipeline, &PipelineConfig{
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

					// Only test if successfully cleared
					if c := pipeline.From.Count(&ctx, &datasources.DatasourceFetchRequest{}); c == 0 {
						testMigration(
							t, &ctx, &pipeline, &PipelineConfig{
								MigrationParallelWorkers: 4,
								MigrationBatchSize:       4,
								MigrationStartOffset:     0,
							},
							0,
						)
					}

				})
			})
		}
	})
}
