package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/pipelines"
	"github.com/sagadana/migrator/states"
	"go.mongodb.org/mongo-driver/mongo"
)

const IDField = "Index"
const CIDField = "Customer Id"
const CRUpdateField = "Test_Cr"

type StoreType string

const (
	MemoryStoreType StoreType = "memory-state-store"
	FileStoreType   StoreType = "file-state-store"
)

var tests = []StoreType{
	MemoryStoreType,
	FileStoreType,
}

func getTempBasePath() string {
	basePath := os.Getenv("RUNNER_TEMP")
	if basePath == "" {
		basePath = os.Getenv("TEMP_BASE_PATH")
		if basePath == "" {
			// basePath = os.TempDir()
			basePath = "./.test"
		}
	}
	return basePath
}

func getStore(storeType StoreType, basePath string) states.Store {
	switch storeType {
	case MemoryStoreType:
		return states.NewMemoryStateStore()
	case FileStoreType:
		return states.NewFileStateStore(basePath, "state")
	}
	panic(fmt.Errorf("Store not available for type: %s", string(storeType)))
}

// Use to test migration for a pipeline
func testMigration(
	t *testing.T,
	ctx *context.Context,
	pipeline *pipelines.Pipeline,
	config *pipelines.PipelineConfig,
) int64 {

	err := pipeline.Start(ctx, *config)
	if err != nil {
		t.Errorf("failed to process migration: %s", err)
	}

	toTotal := pipeline.To.Count(ctx, &datasources.DatasourceFetchRequest{})

	state, ok := pipeline.GetState(ctx)
	if !ok {
		t.Errorf("failed to get migration state: %s", err)
	}

	migrationTotal, err := state.MigrationTotal.Int64()
	if err != nil {
		t.Errorf("failed to get migration total: %s", err)
	}
	migrationOffset, err := state.MigrationOffset.Int64()
	if err != nil {
		t.Errorf("failed to get migration offset: %s", err)
	}

	// Ensure all data was migrated
	if state.MigrationStatus != states.MigrationStatusCompleted {
		t.Errorf("migration failed. Expected Status: %s, Got %s.", string(states.MigrationStatusCompleted), string(state.MigrationStatus))
	}
	if migrationTotal != toTotal {
		t.Errorf("migration failed. Expected Total: %d, Got %d.", toTotal, migrationTotal)
	}
	if migrationOffset != config.MigrationStartOffset+migrationTotal {
		t.Errorf("migration failed. Expected Offset: %d, Got %d.", config.MigrationStartOffset+migrationTotal, migrationOffset)
	}

	return migrationTotal
}

// Use to test continous replication for a pipeline
func testReplication(
	t *testing.T,
	ctx *context.Context,
	pipeline *pipelines.Pipeline,
	config *pipelines.PipelineConfig,
	replicationOnly bool,
	replicationMaxSize int64,
) {

	crStart := make(chan bool, 1)
	crWg := new(sync.WaitGroup)
	crCtx, crCancel := context.WithTimeout(context.TODO(), time.Duration(10*time.Second))

	// Run background updates
	crWg.Add(1)
	go func(wg *sync.WaitGroup, start <-chan bool) {
		<-start          // Start when event received
		defer crCancel() // End continuous replication
		defer wg.Done()  // Mark done

		t.Logf("Running background updates for contiuous replication...")

		// Fetch items from the source to datasource to update
		result := pipeline.From.Fetch(&crCtx, &datasources.DatasourceFetchRequest{
			Size:   replicationMaxSize,
			Offset: config.MigrationStartOffset,
		})
		// Update items
		updates := make(map[string]map[string]any)
		for _, doc := range result.Docs {
			id := doc[IDField]
			doc[CRUpdateField] = true
			updates[id.(string)] = doc
		}
		err := pipeline.To.Push(&crCtx, &datasources.DatasourcePushRequest{
			Updates: updates,
		})
		if err != nil {
			t.Errorf("Failed to perform background updates: %s", err.Error())
		}

		// Wait for replication batch window before ending
		<-time.After(time.Duration(max(1, config.ReplicationBatchWindowSecs)) * time.Second)
	}(crWg, crStart)

	var err error
	config.ContinuousReplication = true

	config.OnReplicationError = func(err error) {
		t.Errorf("OnReplicationError triggered. Error: %s", err.Error())
	}

	if replicationOnly {
		config.OnReplicationStart = func() {
			t.Log("OnReplicationStart triggered")
			crStart <- true // Send event to trigger baground updates
			close(crStart)
		}
		err = pipeline.Stream(&crCtx, *config)
		if err != nil {
			t.Errorf("continuous replication failed: %s", err.Error())
		}
	} else {
		config.OnMigrationStopped = func(_ states.State) {
			t.Log("OnMigrationStopped triggered")
			crStart <- true // Send event to trigger baground updates
			close(crStart)
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
		t.Errorf("continuous replication failed. Expected Status: %s, Got %s.", string(states.ReplicationStatusPaused), string(state.ReplicationStatus))
	}

	// Ensure all background changes to source were replicated correctly unto the destination
	// Fetch items from the destination to datasource to update
	result := pipeline.To.Fetch(ctx, &datasources.DatasourceFetchRequest{
		Size:   replicationMaxSize,
		Offset: config.MigrationStartOffset,
	})
	for _, doc := range result.Docs {
		id := doc[IDField]
		_, ok := doc[CRUpdateField]
		if !ok {
			t.Errorf("continuous replication failed. Expected '%s' field in '%s' doc.", CRUpdateField, id)
		}
	}
}

// ------------------------------------------
// File to File Migration & Streaming
// ------------------------------------------

func Test_File_To_File_Migration_Pipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	// Run tests for different state store types
	for _, storeType := range tests {
		t.Run(string(storeType), func(t *testing.T) {

			fmt.Println("---------------------------------------------------------------------------------")

			basePath := filepath.Join(getTempBasePath(), "Test_File_To_File_Migration_Pipeline", string(storeType))
			os.RemoveAll(basePath)

			fromDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			defer fromDs.Clear(&ctx) // Clear data after testing

			toDs := datasources.NewFileDatasource(basePath, "test-to-customers", IDField)
			defer toDs.Clear(&ctx) // Clear data after testing

			// Load sample data into source
			err := datasources.LoadCSV(&ctx, fromDs, "./tests/sample-100.csv", 10, nil)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			pipeline := pipelines.Pipeline{
				ID:    "test-pipeline-1",
				From:  fromDs,
				To:    toDs,
				Store: getStore(storeType, basePath),
			}

			maxSize := int64(30)
			startOffset := int64(20)

			// ------------------------
			// 1. First migration
			// ------------------------

			migrationTotal := testMigration(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad: 5,
				MigrationBatchSize:    20,
				MigrationMaxSize:      maxSize,
				MigrationStartOffset:  startOffset,
			})

			// --------------------------------------------------------------------
			// 2. Second migration (with continuous replication enabled)
			// --------------------------------------------------------------------

			maxSize = 20
			startOffset += migrationTotal
			replicationBatchSize := int64(5)
			replicationBatchWindowSecs := int64(1)
			replicationMaxSize := replicationBatchSize * 2

			testReplication(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad:      5,
				MigrationBatchSize:         10,
				MigrationMaxSize:           maxSize,
				MigrationStartOffset:       startOffset,
				ContinuousReplication:      true,
				ReplicationBatchSize:       replicationBatchSize,
				ReplicationBatchWindowSecs: replicationBatchWindowSecs,
			}, false, replicationMaxSize)

			fmt.Println("---------------------------------------------------------------------------------")
		})
	}
}

func Test_File_To_File_Streaming_Pipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	// Run tests for different state store types
	for _, storeType := range tests {
		t.Run(string(storeType), func(t *testing.T) {

			fmt.Println("---------------------------------------------------------------------------------")

			basePath := filepath.Join(getTempBasePath(), "Test_File_To_File_Streaming_Pipeline", string(storeType))
			os.RemoveAll(basePath) // Clean up first

			fromDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			defer fromDs.Clear(&ctx) // Clear data after testing

			toDs := datasources.NewFileDatasource(basePath, "test-to-customers", IDField)
			defer toDs.Clear(&ctx) // Clear data after testing

			// Load sample data into source
			err := datasources.LoadCSV(&ctx, fromDs, "./tests/sample-100.csv", 10, nil)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			pipeline := pipelines.Pipeline{
				ID:    "test-pipeline-1",
				From:  fromDs,
				To:    toDs,
				Store: getStore(storeType, basePath),
			}

			maxSize := int64(10)
			startOffset := int64(20)

			// ----------------------------
			// Continuous replication
			// ----------------------------

			replicationBatchSize := int64(5)
			replicationBatchWindowSecs := int64(1)
			replicationMaxSize := replicationBatchSize * 2

			testReplication(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad:      5,
				MigrationBatchSize:         10,
				MigrationMaxSize:           maxSize,
				MigrationStartOffset:       startOffset,
				ContinuousReplication:      true,
				ReplicationBatchSize:       replicationBatchSize,
				ReplicationBatchWindowSecs: replicationBatchWindowSecs,
			}, true, replicationMaxSize)

			fmt.Println("---------------------------------------------------------------------------------")
		})
	}
}

// ------------------------------------------
// File to Mongo Migration & Streaming
// ------------------------------------------

func Test_File_To_Mongo_Migration_Pipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	// Run tests for different state store types
	for _, storeType := range tests {
		t.Run(string(storeType), func(t *testing.T) {

			fmt.Println("---------------------------------------------------------------------------------")

			basePath := filepath.Join(getTempBasePath(), "Test_File_To_Mongo_Migration_Pipeline", string(storeType))
			os.RemoveAll(basePath)

			fromDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			defer fromDs.Clear(&ctx) // Clear data after testing

			mongoURI := os.Getenv("MONGO_URI")
			mongoDB := os.Getenv("MONGO_DB")

			toDs := datasources.NewMongoDatasource(
				&ctx,
				datasources.MongoDatasourceConfigs{
					URI:            mongoURI,
					DatabaseName:   mongoDB,
					CollectionName: "test-to",
					Filter:         map[string]any{},
					Sort:           map[string]any{},
					OnInit: func(client *mongo.Client) error {
						return nil
					},
				},
			)
			defer toDs.Clear(&ctx) // Clear data after testing

			// Use to transform to Include default mongo ID
			mongoTransformer := func(data map[string]any) (map[string]any, error) {
				data[datasources.MongoIDField] = data[CIDField]
				return data, nil
			}

			// Load sample data into source
			err := datasources.LoadCSV(&ctx, fromDs, "./tests/sample-100.csv", 10, mongoTransformer)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			pipeline := pipelines.Pipeline{
				ID:        "test-pipeline-1",
				From:      fromDs,
				To:        toDs,
				Store:     getStore(storeType, basePath),
				Transform: mongoTransformer,
			}

			maxSize := int64(30)
			startOffset := int64(20)

			// ------------------------
			// 1. First migration
			// ------------------------

			migrationTotal := testMigration(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad: 5,
				MigrationBatchSize:    20,
				MigrationMaxSize:      maxSize,
				MigrationStartOffset:  startOffset,
			})

			// --------------------------------------------------------------------
			// 2. Second migration (with continuous replication enabled)
			// --------------------------------------------------------------------

			maxSize = 20
			startOffset += migrationTotal
			replicationBatchSize := int64(5)
			replicationBatchWindowSecs := int64(1)
			replicationMaxSize := replicationBatchSize * 2

			testReplication(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad:      5,
				MigrationBatchSize:         10,
				MigrationMaxSize:           maxSize,
				MigrationStartOffset:       startOffset,
				ContinuousReplication:      true,
				ReplicationBatchSize:       replicationBatchSize,
				ReplicationBatchWindowSecs: replicationBatchWindowSecs,
			}, false, replicationMaxSize)

			fmt.Println("---------------------------------------------------------------------------------")
		})
	}
}

func Test_File_To_Mongo_Streaming_Pipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	// Run tests for different state store types
	for _, storeType := range tests {
		t.Run(string(storeType), func(t *testing.T) {

			fmt.Println("---------------------------------------------------------------------------------")

			basePath := filepath.Join(getTempBasePath(), "Test_File_To_Mongo_Migration_Pipeline", string(storeType))
			os.RemoveAll(basePath)

			fromDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			defer fromDs.Clear(&ctx) // Clear data after testing

			mongoURI := os.Getenv("MONGO_URI")
			mongoDB := os.Getenv("MONGO_DB")

			toDs := datasources.NewMongoDatasource(
				&ctx,
				datasources.MongoDatasourceConfigs{
					URI:            mongoURI,
					DatabaseName:   mongoDB,
					CollectionName: "test-to",
					Filter:         map[string]any{},
					Sort:           map[string]any{},
					OnInit: func(client *mongo.Client) error {
						return nil
					},
				},
			)
			defer toDs.Clear(&ctx) // Clear data after testing

			// Use to transform to Include default mongo ID
			mongoTransformer := func(data map[string]any) (map[string]any, error) {
				data[datasources.MongoIDField] = data[CIDField]
				return data, nil
			}

			// Load sample data into source
			err := datasources.LoadCSV(&ctx, fromDs, "./tests/sample-100.csv", 10, mongoTransformer)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			pipeline := pipelines.Pipeline{
				ID:        "test-pipeline-1",
				From:      fromDs,
				To:        toDs,
				Store:     getStore(storeType, basePath),
				Transform: mongoTransformer,
			}

			maxSize := int64(10)
			startOffset := int64(20)

			// ----------------------------
			// Continuous replication
			// ----------------------------

			replicationBatchSize := int64(5)
			replicationBatchWindowSecs := int64(1)
			replicationMaxSize := replicationBatchSize * 2

			testReplication(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad:      5,
				MigrationBatchSize:         10,
				MigrationMaxSize:           maxSize,
				MigrationStartOffset:       startOffset,
				ContinuousReplication:      true,
				ReplicationBatchSize:       replicationBatchSize,
				ReplicationBatchWindowSecs: replicationBatchWindowSecs,
			}, true, replicationMaxSize)

			fmt.Println("---------------------------------------------------------------------------------")
		})
	}
}

// ------------------------------------------
// Mongo To File Migration & Streaming
// ------------------------------------------

func Test_Mongo_To_File_Migration_Pipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	// Run tests for different state store types
	for _, storeType := range tests {
		t.Run(string(storeType), func(t *testing.T) {

			fmt.Println("---------------------------------------------------------------------------------")

			basePath := filepath.Join(getTempBasePath(), "Test_Mongo_To_File_Migration_Pipeline", string(storeType))
			os.RemoveAll(basePath)

			mongoURI := os.Getenv("MONGO_URI")
			mongoDB := os.Getenv("MONGO_DB")

			fromDs := datasources.NewMongoDatasource(
				&ctx,
				datasources.MongoDatasourceConfigs{
					URI:            mongoURI,
					DatabaseName:   mongoDB,
					CollectionName: "test-to",
					Filter:         map[string]any{},
					Sort:           map[string]any{},
					OnInit: func(client *mongo.Client) error {
						return nil
					},
				},
			)
			defer fromDs.Clear(&ctx) // Clear data after testing

			toDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			defer toDs.Clear(&ctx) // Clear data after testing

			// Use to transform to Include default mongo ID
			mongoTransformer := func(data map[string]any) (map[string]any, error) {
				data[datasources.MongoIDField] = data[CIDField]
				return data, nil
			}

			// Load sample data into source
			err := datasources.LoadCSV(&ctx, fromDs, "./tests/sample-100.csv", 10, mongoTransformer)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			pipeline := pipelines.Pipeline{
				ID:    "test-pipeline-1",
				From:  fromDs,
				To:    toDs,
				Store: getStore(storeType, basePath),
			}

			maxSize := int64(30)
			startOffset := int64(20)

			// ------------------------
			// 1. First migration
			// ------------------------

			migrationTotal := testMigration(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad: 5,
				MigrationBatchSize:    20,
				MigrationMaxSize:      maxSize,
				MigrationStartOffset:  startOffset,
			})

			// --------------------------------------------------------------------
			// 2. Second migration (with continuous replication enabled)
			// --------------------------------------------------------------------

			maxSize = 20
			startOffset += migrationTotal
			replicationBatchSize := int64(5)
			replicationBatchWindowSecs := int64(1)
			replicationMaxSize := replicationBatchSize * 2

			testReplication(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad:      5,
				MigrationBatchSize:         10,
				MigrationMaxSize:           maxSize,
				MigrationStartOffset:       startOffset,
				ContinuousReplication:      true,
				ReplicationBatchSize:       replicationBatchSize,
				ReplicationBatchWindowSecs: replicationBatchWindowSecs,
			}, false, replicationMaxSize)

			fmt.Println("---------------------------------------------------------------------------------")
		})
	}
}
func Test_Mongo_To_File_Streaming_Pipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	// Run tests for different state store types
	for _, storeType := range tests {
		t.Run(string(storeType), func(t *testing.T) {

			fmt.Println("---------------------------------------------------------------------------------")

			basePath := filepath.Join(getTempBasePath(), "Test_Mongo_To_File_Migration_Pipeline", string(storeType))
			os.RemoveAll(basePath)

			mongoURI := os.Getenv("MONGO_URI")
			mongoDB := os.Getenv("MONGO_DB")

			fromDs := datasources.NewMongoDatasource(
				&ctx,
				datasources.MongoDatasourceConfigs{
					URI:            mongoURI,
					DatabaseName:   mongoDB,
					CollectionName: "test-to",
					Filter:         map[string]any{},
					Sort:           map[string]any{},
					OnInit: func(client *mongo.Client) error {
						return nil
					},
				},
			)
			defer fromDs.Clear(&ctx) // Clear data after testing

			toDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			defer toDs.Clear(&ctx) // Clear data after testing

			// Use to transform to Include default mongo ID
			mongoTransformer := func(data map[string]any) (map[string]any, error) {
				data[datasources.MongoIDField] = data[CIDField]
				return data, nil
			}

			// Load sample data into source
			err := datasources.LoadCSV(&ctx, fromDs, "./tests/sample-100.csv", 10, mongoTransformer)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			pipeline := pipelines.Pipeline{
				ID:    "test-pipeline-1",
				From:  fromDs,
				To:    toDs,
				Store: getStore(storeType, basePath),
			}

			maxSize := int64(10)
			startOffset := int64(20)

			// ----------------------------
			// Continuous replication
			// ----------------------------

			replicationBatchSize := int64(5)
			replicationBatchWindowSecs := int64(1)
			replicationMaxSize := replicationBatchSize * 2

			testReplication(t, &ctx, &pipeline, &pipelines.PipelineConfig{
				MigrationParallelLoad:      5,
				MigrationBatchSize:         10,
				MigrationMaxSize:           maxSize,
				MigrationStartOffset:       startOffset,
				ContinuousReplication:      true,
				ReplicationBatchSize:       replicationBatchSize,
				ReplicationBatchWindowSecs: replicationBatchWindowSecs,
			}, false, replicationMaxSize)

			fmt.Println("---------------------------------------------------------------------------------")
		})
	}
}
