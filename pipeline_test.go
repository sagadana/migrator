package main

import (
	"context"
	"sagadana/migrator/datasources"
	"sagadana/migrator/pipelines"
	"sagadana/migrator/states"
	"testing"
	"time"
)

func TestPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	type FileToFilePipeline = pipelines.Pipeline[*datasources.FileDatasource, *datasources.FileDatasource]

	// client := datasources.ConnectToMongoDB(&ctx)
	fileFromDs := datasources.NewFileDatasource("./test-from.json")
	fileToDs := datasources.NewFileDatasource("./test-to.json")
	pipeline := FileToFilePipeline{
		ID:   "test-pipeline-1",
		From: fileFromDs,
		To:   fileToDs,
		// Store: states.CreateMemoryStateStore(),
		Store: states.CreateFileStateStore("./state.json"),
	}

	pipelineConfig := pipelines.PipelineConfig{
		ParrallelLoad: 2,
		BatchSize:     4,
	}
	pipeline.Start(&ctx, pipelineConfig)
}
