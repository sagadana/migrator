package datasources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sagadana/migrator/helpers"
)

var (
	ErrDatastoreClosed = errors.New("datasource closed")
)

type DatasourcePushRequest struct {
	Inserts []map[string]any
	Updates []map[string]any
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
	// Number of items to batch. 0 to disable batching (optional)
	BatchSize uint64
	// How long to wait to accumulate batch (optional)
	BatchWindowSeconds uint64
}

type DatasourceStreamResult struct {
	Err  error
	Docs DatasourcePushRequest
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
	// Close data source
	Close(ctx *context.Context) error
}

// Load CSV data into datasource
func LoadCSV(
	ctx *context.Context,
	ds Datasource,
	path string,
	batchSize uint64,
) error {
	result, err := helpers.StreamCSV(path, batchSize)
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
			Inserts: []map[string]any{},
			Updates: []map[string]any{},
			Deletes: []string{},
		}

		slog.Info(fmt.Sprintf("Watching %s for changes...", title))

		for {
			select {
			case <-bgCtx.Done():
				slog.Info(fmt.Sprintf("Closing %s watcher...", title))

				// Watcher closed - Drain channel
				timer := time.After(time.Duration(batchWindow) * time.Second)
				for {
					select {
					case event := <-watcher:
						// Drain
						if event.Inserts != nil {
							batch.Inserts = append(batch.Inserts, event.Inserts...)
						}
						if event.Updates != nil {
							batch.Updates = append(batch.Updates, event.Updates...)
						}
						if event.Deletes != nil {
							batch.Deletes = append(batch.Deletes, event.Deletes...)
						}

					case <-timer:
						// Send remaining batch after waiting for batch window
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
						Inserts: []map[string]any{},
						Updates: []map[string]any{},
						Deletes: []string{},
					}
				}

			case event := <-watcher:

				// Append items to batch
				if event.Inserts != nil {
					batch.Inserts = append(batch.Inserts, event.Inserts...)
				}
				if event.Updates != nil {
					batch.Updates = append(batch.Updates, event.Updates...)
				}
				if event.Deletes != nil {
					batch.Deletes = append(batch.Deletes, event.Deletes...)
				}

				// Batch full - send batch
				count := len(batch.Inserts) + len(batch.Updates) + len(batch.Deletes)
				if count > 0 && count >= int(batchSize) {
					out <- DatasourceStreamResult{Docs: batch}
					// Reset batch
					batch = DatasourcePushRequest{
						Inserts: []map[string]any{},
						Updates: []map[string]any{},
						Deletes: []string{},
					}
					// Reset timer
					ticker.Reset(time.Duration(batchWindow) * time.Second)
				}
			}
		}

	}(*ctx)

	return out
}
