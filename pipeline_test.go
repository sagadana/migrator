package main

import (
	"context"
	"sagadana/migrator/datasources"
	"sagadana/migrator/pipelines"
	"sagadana/migrator/states"
	"testing"
	"time"
)

func TestMigrationPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	basePath := "./.test"
	fileFromDs := datasources.NewFileDatasource(basePath, "test-from-customers", "Index")
	err := fileFromDs.LoadCSV(&ctx, "./tests/sample-100.csv", 10)
	if err != nil {
		t.Errorf("failed to load data from CSV: %s", err)
	}

	fileToDs := datasources.NewFileDatasource(basePath, "test-to-customers", "Index")
	pipeline := pipelines.Pipeline{
		ID:   "test-pipeline-1",
		From: fileFromDs,
		To:   fileToDs,
		// Store: states.NewMemoryStateStore(),
		Store: states.NewFileStateStore(basePath, "state.json"),
	}

	pipelineConfig := pipelines.PipelineConfig{
		ParrallelLoad:              5,
		BatchSize:                  10,
		ReplicationBatchSize:       10,
		ReplicationBatchWindowSecs: 10,
	}
	err = pipeline.Start(&ctx, pipelineConfig)
	if err != nil {
		t.Errorf("failed to process migration: %s", err)
	}
}
