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
)

const IDField = "Index"
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
		basePath = os.TempDir()
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
		t.Errorf("failed to process batch 1 migration: %s", err)
	}

	toTotal := pipeline.To.Count(ctx, &datasources.DatasourceFetchRequest{})

	state, ok := pipeline.GetState(ctx)
	if !ok {
		t.Errorf("failed to get batch 1 migration state: %s", err)
	}

	migrationTotal, err := state.MigrationTotal.Int64()
	if err != nil {
		t.Errorf("failed to get batch 1 migration total: %s", err)
	}
	migrationOffset, err := state.MigrationOffset.Int64()
	if err != nil {
		t.Errorf("failed to get batch 1 migration offset: %s", err)
	}

	// Ensure all data was migrated
	if migrationTotal != toTotal {
		t.Errorf("batch 1 migration failed. Expected Total: %d, Got %d.", toTotal, migrationTotal)
	}
	if migrationOffset != config.StartOffset+migrationTotal {
		t.Errorf("batch 1 migration failed. Expected Offset: %d, Got %d.", config.StartOffset+migrationTotal, migrationOffset)
	}

	return migrationTotal
}

// Use to test continous replication for a pipeline
func testReplication(
	t *testing.T,
	pipeline *pipelines.Pipeline,
	config *pipelines.PipelineConfig,
	replicationOnly bool,
	replicationMaxSize int64,
) {

	crStart := make(chan bool, 1)
	crWg := new(sync.WaitGroup)
	crCtx, crCancel := context.WithTimeout(context.TODO(), time.Duration(5*time.Second))

	// Run background updates
	crWg.Add(1)
	go func(wg *sync.WaitGroup, start <-chan bool) {
		<-start          // Start when event received
		defer wg.Done()  // Mark done
		defer crCancel() // End continuous replication

		t.Logf("Running background updates for contiuous replication...")

		// Fetch items from the source to datasource to update
		result := pipeline.From.Fetch(&crCtx, &datasources.DatasourceFetchRequest{
			Size:   replicationMaxSize,
			Offset: config.StartOffset,
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
			t.Error("Failed to perform background updates")
		}

		// Wait for replication batch window before ending
		<-time.After(time.Duration(config.ReplicationBatchWindowSecs) * time.Second)

	}(crWg, crStart)

	var err error
	config.ContinuousReplication = true
	config.OnStop = func(_ states.State) {
		t.Log("OnStop triggered")
		crStart <- true // Send event to trigger baground updates
		close(crStart)
	}
	if replicationOnly {
		err = pipeline.Stream(&crCtx, *config)
		if err != nil {
			t.Errorf("continuous replication failed: %s", err.Error())
		}

	} else {
		err = pipeline.Start(&crCtx, *config)
		if err != nil {
			t.Errorf("migration + continuous replication failed: %s", err.Error())
		}
	}

	// Wait for background updates to be made
	crWg.Wait()

	// Ensure all background changes to source were replicated correctly unto the destination
	// Fetch items from the destination to datasource to update
	result := pipeline.To.Fetch(&crCtx, &datasources.DatasourceFetchRequest{
		Size:   replicationMaxSize,
		Offset: config.StartOffset,
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

			basePath := filepath.Join(getTempBasePath(), string(storeType))
			os.RemoveAll(basePath)

			fromDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			err := fromDs.LoadCSV(&ctx, "./tests/sample-100.csv", 10)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			toDs := datasources.NewFileDatasource(basePath, "test-to-customers", IDField)

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
				ParallelLoad: 5,
				BatchSize:    20,
				MaxSize:      maxSize,
				StartOffset:  startOffset,
			})

			// --------------------------------------------------------------------
			// 2. Second migration (with continuous replication enabled)
			// --------------------------------------------------------------------

			maxSize = 20
			startOffset += migrationTotal
			replicationBatchSize := int64(5)
			replicationBatchWindowSecs := int64(1)
			replicationMaxSize := replicationBatchSize * 2

			testReplication(t, &pipeline, &pipelines.PipelineConfig{
				ParallelLoad:               5,
				BatchSize:                  10,
				MaxSize:                    maxSize,
				StartOffset:                startOffset,
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

			basePath := filepath.Join(getTempBasePath(), string(storeType))
			os.RemoveAll(basePath) // Clean up first

			fromDs := datasources.NewFileDatasource(basePath, "test-from-customers", IDField)
			err := fromDs.LoadCSV(&ctx, "./tests/sample-100.csv", 10)
			if err != nil {
				t.Errorf("failed to load data from CSV: %s", err)
			}

			toDs := datasources.NewFileDatasource(basePath, "test-to-customers", IDField)

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

			testReplication(t, &pipeline, &pipelines.PipelineConfig{
				ParallelLoad:               5,
				BatchSize:                  10,
				MaxSize:                    maxSize,
				StartOffset:                startOffset,
				ContinuousReplication:      true,
				ReplicationBatchSize:       replicationBatchSize,
				ReplicationBatchWindowSecs: replicationBatchWindowSecs,
			}, true, replicationMaxSize)

			fmt.Println("---------------------------------------------------------------------------------")
		})
	}
}
