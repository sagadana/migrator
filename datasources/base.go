package datasources

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/sagadana/migrator/helpers"
)

type DatasourcePushRequest struct {
	Inserts []map[string]any
	Updates map[string]map[string]any
	Deletes []string
}

type DatasourcePushCount struct {
	Inserts uint64
	Updates uint64
	Deletes uint64
}

type DatasourceFetchRequest struct {
	// List of IDs to fetch (optional)
	IDs []string
	// Number of items to fetch. 0 to fetch all (optional)
	Size uint64
	// Offset to start fetching from. 0 to start from beginning (optional)
	Offset uint64
}

type DatasourceFetchResult struct {
	Err  error
	Docs []map[string]any
	// Start offset: where first offset = 0
	Start uint64
	// End offset: where first offset = 0
	End uint64
}

type DatasourceStreamRequest struct {
	// TODO: Add support for StartFromTimestamp
	// Number of items to batch. 0 to disable batching (optional)
	BatchSize uint64
	// How long to wait to accumulate batch (optional)
	BatchWindowSeconds uint64
}

type DatasourceStreamResult struct {
	Err  error
	Docs DatasourcePushRequest
}

type ImportType string

const (
	ImportTypeCSV ImportType = "csv"
)

type ImportSource string

const (
	ImportSourceFile ImportType = "file"
)

type DatasourceImportRequest struct {
	Type     ImportType
	Source   ImportSource
	Location string
}

type DatasourceTransformer func(data map[string]any) (map[string]any, error)

type Datasource interface {
	// Get total count of items based on the provided request.
	// Note: The count should reflect the given 'Size' and number of 'IDs'
	Count(ctx *context.Context, request *DatasourceFetchRequest) uint64
	// Fetch data
	Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult
	// Insert/Update/Delete data
	Push(ctx *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error)
	// Listen to Change Data Streams or periodically watch for changes
	Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult
	// Clear data source
	Clear(ctx *context.Context) error

	// Import contents
	// Import(ctx *context.Context, request DatasourceImportRequest) error
}

// Load CSV data into datasource
func LoadCSV(
	ctx *context.Context,
	ds Datasource,
	path string,
	batchSize uint64,
) error {
	result, err := helpers.StreamReadCSV(path, batchSize)
	if err != nil {
		return err
	}

	for data := range result {
		_, err = ds.Push(ctx, &DatasourcePushRequest{
			Inserts: data,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Save datasource into CSV
func SaveCSV(
	ctx *context.Context,
	ds Datasource,
	path string,
	batchSize uint64,
) error {
	result, err := helpers.StreamReadCSV(path, batchSize)
	if err != nil {
		return err
	}

	for data := range result {
		_, err = ds.Push(ctx, &DatasourcePushRequest{
			Inserts: data,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Process datasource change stream with additional features such as batching
func StreamChanges(
	ctx *context.Context,
	title string,
	watcher <-chan DatasourcePushRequest,
	request *DatasourceStreamRequest) <-chan DatasourceStreamResult {

	out := make(chan DatasourceStreamResult)

	go func(bgCtx context.Context) {
		defer close(out)

		batchSize := max(request.BatchSize, 1)
		batchWindow := max(request.BatchWindowSeconds, 1)

		ticker := time.NewTicker(time.Duration(batchWindow) * time.Second)
		defer ticker.Stop()

		batch := DatasourcePushRequest{
			Inserts: make([]map[string]any, 0),
			Updates: make(map[string]map[string]any),
			Deletes: make([]string, 0),
		}

		slog.Info(fmt.Sprintf("Watching %s for changes...", title))

		for {
			select {
			case <-bgCtx.Done():
				slog.Info(fmt.Sprintf("Closing %s watcher...", title))

				// Watcher closed - Drain channel
				for {
					select {
					case event := <-watcher:
						// Drain
						batch.Inserts = append(batch.Inserts, event.Inserts...)
						maps.Copy(batch.Updates, event.Updates)
						batch.Deletes = append(batch.Deletes, event.Deletes...)
					default:
						// Wait for batch window before ending
						<-time.After(time.Duration(batchWindow) * time.Second)
						// Send remaining batch
						// TODO: Investigate if this could be causing downstream to
						//   process logic that require context even when it is closed
						if len(batch.Inserts)+len(batch.Updates)+len(batch.Deletes) > 0 {
							out <- DatasourceStreamResult{Docs: batch}
							batch = *new(DatasourcePushRequest) // clean up batch
						}
						return
					}
				}

			case <-ticker.C:
				// Time elapsed - send batch
				if len(batch.Inserts)+len(batch.Updates)+len(batch.Deletes) > 0 {
					out <- DatasourceStreamResult{Docs: batch}
					// Reset batch
					batch = DatasourcePushRequest{
						Inserts: make([]map[string]any, 0),
						Updates: make(map[string]map[string]any),
						Deletes: make([]string, 0),
					}
				}

			case event, ok := <-watcher:
				if !ok {
					return
				}

				// Append items to batch
				batch.Inserts = append(batch.Inserts, event.Inserts...)
				maps.Copy(batch.Updates, event.Updates)
				batch.Deletes = append(batch.Deletes, event.Deletes...)

				// Batch full - send batch
				if len(batch.Inserts)+len(batch.Updates)+len(batch.Deletes) >= int(batchSize) {
					out <- DatasourceStreamResult{Docs: batch}
					// Reset batch
					batch = DatasourcePushRequest{
						Inserts: make([]map[string]any, 0),
						Updates: make(map[string]map[string]any),
						Deletes: make([]string, 0),
					}
					// Reset timer
					ticker.Reset(time.Duration(batchWindow) * time.Second)
				}
			}
		}

	}(*ctx)

	return out
}
