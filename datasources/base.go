package datasources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/sagadana/migrator/helpers"
)

var (
	ErrDatastoreClosed = errors.New("datasource closed")
)

// ----------------------------
// Types: Push, Fetch, Stream
// ----------------------------

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

// ----------------------------
// Types: Import, Export
// ----------------------------

type DatasourceImportType string

const (
	DatasourceImportTypeCSV DatasourceImportType = "csv"
)

type DatasourceImportSource string

const (
	DatasourceImportSourceFile DatasourceImportSource = "file"
)

type DatasourceImportRequest struct {
	Type      DatasourceImportType
	Source    DatasourceImportSource
	Location  string
	BatchSize uint64
}

type DatasourceExportType string

const (
	DatasourceExportTypeCSV DatasourceExportType = "csv"
)

type DatasourceExportDestination string

const (
	DatasourceExportDestinationFile DatasourceExportDestination = "file"
)

type DatasourceExportRequest struct {
	Type        DatasourceExportType
	Destination DatasourceExportDestination
	Location    string
	BatchSize   uint64
}

// -------------------------------
// Types: Transformer, Interface
// -------------------------------

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

	// Import data
	Import(ctx *context.Context, request DatasourceImportRequest) error
	// Export data
	Export(ctx *context.Context, request DatasourceExportRequest) error
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
	// Normalize batch size
	batchSize = max(batchSize, 1)

	// Get total count to determine if we have data
	totalCount := ds.Count(ctx, &DatasourceFetchRequest{})
	if totalCount == 0 {
		// Create empty file for consistency
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		file.Close()
		return nil
	}

	// Create a channel to stream data batches
	dataChan := make(chan []map[string]any)
	errChan := make(chan error, 1)

	// Start a goroutine to fetch data in batches and send to channel
	go func() {
		defer close(dataChan)
		defer close(errChan)

		var offset uint64 = 0
		var result DatasourceFetchResult
		for {
			// Fetch batch
			result = ds.Fetch(ctx, &DatasourceFetchRequest{
				Size:   batchSize,
				Offset: offset,
			})

			if result.Err != nil {
				errChan <- fmt.Errorf("failed to fetch data at offset %d: %w", offset, result.Err)
				return
			}

			if len(result.Docs) == 0 {
				// No more data
				break
			}

			// Send batch to channel
			dataChan <- result.Docs

			// Move to next batch
			offset += uint64(len(result.Docs))

			// If we got less than requested, we're at the end
			if uint64(len(result.Docs)) < batchSize {
				break
			}
		}
	}()

	// Check for errors first
	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
	default:
	}

	// Determine headers from first batch
	var headers []string
	firstBatch := <-dataChan

	// Check for errors after getting first batch
	select {
	case err := <-errChan:
		if err != nil {
			return err
		}
	default:
	}

	if len(firstBatch) > 0 {
		// Collect all unique keys from first batch and sort them for consistent output
		headerSet := make(map[string]bool)
		var doc map[string]any
		for _, doc = range firstBatch {
			var key string
			for key = range doc {
				headerSet[key] = true
			}
		}

		// Convert to sorted slice
		for key := range headerSet {
			headers = append(headers, key)
		}
		sort.Strings(headers)

		// Create new channel that includes the first batch
		newDataChan := make(chan []map[string]any)
		go func() {
			defer close(newDataChan)
			// Send first batch
			newDataChan <- firstBatch
			// Send remaining batches
			var batch []map[string]any
			for batch = range dataChan {
				newDataChan <- batch
			}
		}()

		// Use StreamWriteCSV to write the data
		return helpers.StreamWriteCSV(path, headers, newDataChan)
	}

	// If no first batch, create empty file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	file.Close()
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
