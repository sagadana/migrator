package datasources

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/sagadana/migrator/helpers"
	"gorm.io/gorm/schema"
)

var (
	ErrDatastoreClosed = errors.New("datasource closed")
)

const DefaultWatcherBufferSize = 1024

type DatasourceActionResult struct {
	Read   bool
	Write  bool
	Stream bool
}

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
	DatasourceImportTypeCSV     DatasourceImportType = "csv"
	DatasourceImportTypeParquet DatasourceImportType = "parquet"
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
	DatasourceExportTypeCSV     DatasourceExportType = "csv"
	DatasourceExportTypeParquet DatasourceExportType = "parquet"
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
	// Allowed actions
	Actions() *DatasourceActionResult

	// Get total count of items based on the provided request.
	// Note: The count should reflect the given 'Size' and number of 'IDs'
	Count(ctx *context.Context, request *DatasourceFetchRequest) uint64
	// Fetch data
	Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult
	// Insert, Update, Delete data (in that order)
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

// Load Parquet data into datasource
func LoadParquet(
	ctx *context.Context,
	ds Datasource,
	path string,
	batchSize uint64,
) error {
	result, err := helpers.StreamReadParquet(path, batchSize)
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

// Save datasource into Parquet
func SaveParquet(
	ctx *context.Context,
	ds Datasource,
	path string,
	batchSize uint64,
	fields map[string]reflect.Type,
) error {
	// Normalize batch size
	batchSize = max(batchSize, 1)

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

		// Auto-detect schema from first batch if not provided
		// Note: This is a best-effort implementation and may not cover all edge cases.
		// It is recommended to provide schema for consistent results.
		if len(fields) == 0 {
			fields = make(map[string]reflect.Type)
			for _, doc := range firstBatch {
				for key, value := range doc {
					if value != nil {
						fields[key] = reflect.TypeOf(value)
					}
				}
			}
		}

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

		// Use StreamWriteParquet to write the data
		return helpers.StreamWriteParquet(path, fields, newDataChan)
	}

	// If no first batch, create empty file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}

// Process datasource change stream with additional features such as batching and de-duplication within a batch window
func StreamChanges(
	ctx *context.Context,
	name string,
	watcher <-chan DatasourcePushRequest,
	request *DatasourceStreamRequest) <-chan DatasourceStreamResult {

	out := make(chan DatasourceStreamResult)

	// Helper function to calculate hash for insert/update documents
	calculateDocumentHash := func(doc map[string]any) string {
		docBytes, err := json.Marshal(doc)
		if err != nil {
			// Fallback to string representation if JSON encoding fails
			docBytes = fmt.Appendf(nil, "%v", doc)
		}
		// Include name in the hash to make it unique per stream
		hashData := append(docBytes, []byte(name)...)
		hash := sha256.Sum256(hashData)
		return hex.EncodeToString(hash[:])
	}

	// Helper function to calculate hash for delete IDs
	calculateDeleteHash := func(id string) string {
		// Include title in the hash to make it unique per stream
		hashData := append([]byte(id), []byte(name)...)
		hash := sha256.Sum256(hashData)
		return hex.EncodeToString(hash[:])
	}

	// Helper function to process events with deduplication
	processEvent := func(event DatasourcePushRequest, seenHashes map[string]bool, batchInserts *[]map[string]any, batchUpdates *[]map[string]any, batchDeletes *[]string) {

		// Process inserts with hash-based deduplication
		for _, doc := range event.Inserts {
			if doc == nil {
				continue
			}
			hash := calculateDocumentHash(doc)
			exist, ok := seenHashes[hash]
			if !ok && !exist {
				seenHashes[hash] = true
				*batchInserts = append(*batchInserts, doc)
			}
		}
		// Process updates with hash-based deduplication
		for _, doc := range event.Updates {
			if doc == nil {
				continue
			}
			hash := calculateDocumentHash(doc)
			exist, ok := seenHashes[hash]
			if !ok && !exist {
				seenHashes[hash] = true
				*batchUpdates = append(*batchUpdates, doc)
			}
		}
		// Process deletes with hash-based deduplication
		for _, id := range event.Deletes {
			hash := calculateDeleteHash(id)
			exist, ok := seenHashes[hash]
			if !ok && !exist {
				seenHashes[hash] = true
				*batchDeletes = append(*batchDeletes, id)
			}
		}
	}

	go func(bgCtx context.Context) {
		defer close(out)

		batchSize := max(request.BatchSize, 1)
		batchWindow := max(request.BatchWindowSeconds, 1)

		ticker := time.NewTicker(time.Duration(batchWindow) * time.Second)
		defer ticker.Stop()

		// Use maps to track hashes and prevent duplicates
		seenHashes := make(map[string]bool)
		var batchInserts []map[string]any
		var batchUpdates []map[string]any
		var batchDeletes []string

		slog.Info(fmt.Sprintf("Watching %s for changes...", name))

		for {
			select {
			case <-bgCtx.Done():
				slog.Info(fmt.Sprintf("Closing %s watcher...", name))

				// Watcher closed - Drain channel
				timer := time.After(time.Duration(batchWindow) * time.Second)
				for {
					select {
					case event := <-watcher:
						// Drain and deduplicate using the helper function
						processEvent(event, seenHashes, &batchInserts, &batchUpdates, &batchDeletes)

					case <-timer:
						// Send remaining batch after waiting for batch window
						// TODO: Investigate if this could be causing downstream to
						//   process logic that require context even when it is closed
						if len(batchInserts)+len(batchUpdates)+len(batchDeletes) > 0 {
							out <- DatasourceStreamResult{Docs: DatasourcePushRequest{
								Inserts: batchInserts,
								Updates: batchUpdates,
								Deletes: batchDeletes,
							}}
							// Clean up batch slices and hashes
							batchInserts = nil
							batchUpdates = nil
							batchDeletes = nil
						}
						return
					}
				}

			case <-ticker.C:
				// Time elapsed - send batch
				if len(batchInserts)+len(batchUpdates)+len(batchDeletes) > 0 {
					out <- DatasourceStreamResult{Docs: DatasourcePushRequest{
						Inserts: batchInserts,
						Updates: batchUpdates,
						Deletes: batchDeletes,
					}}
					// Reset batch slices and hashes
					batchInserts = make([]map[string]any, 0)
					batchUpdates = make([]map[string]any, 0)
					batchDeletes = make([]string, 0)
					seenHashes = make(map[string]bool)
				}

			case event := <-watcher:
				// Process event with hash-based de-duplication using helper function
				processEvent(event, seenHashes, &batchInserts, &batchUpdates, &batchDeletes)

				// Batch full - send batch
				count := len(batchInserts) + len(batchUpdates) + len(batchDeletes)
				if count > 0 && count >= int(batchSize) {
					batch := DatasourcePushRequest{
						Inserts: batchInserts,
						Updates: batchUpdates,
						Deletes: batchDeletes,
					}
					out <- DatasourceStreamResult{Docs: batch}
					// Reset batch slices and hashes
					batchInserts = make([]map[string]any, 0)
					batchUpdates = make([]map[string]any, 0)
					batchDeletes = make([]string, 0)
					seenHashes = make(map[string]bool)
					// Reset timer
					ticker.Reset(time.Duration(batchWindow) * time.Second)
				}
			}
		}

	}(*ctx)

	return out
}

// Parse value based on GORM field type
// Returns parsed value or error if parsing fails
// Note: This is a best-effort implementation and may not cover all edge cases.
// It is recommended to validate the parsed values before using them in database operations.
func ParseGormFieldValue(field *schema.Field, value any) (any, error) {

	var result any
	var err error

	if value == nil {
		return nil, nil
	}

	switch field.DataType {
	case schema.String:
		switch v := value.(type) {
		case string:
			// Try convert from JSON string
			var jsonData map[string]any
			if jsonErr := json.Unmarshal([]byte(v), &jsonData); jsonErr == nil {
				result = jsonData
			} else {
				result = v
			}
		case []byte:
			// Try convert from JSON bytes
			var jsonData map[string]any
			if jsonErr := json.Unmarshal(v, &jsonData); jsonErr == nil {
				result = jsonData
			} else {
				result = string(v)
			}
		}

	case schema.Bool:
		if b, ok := value.(bool); ok {
			result = b
		} else if result, err = strconv.ParseBool(fmt.Sprintf("%v", value)); err != nil {
			err = fmt.Errorf("invalid value type for bool field %s: %T", field.Name, value)
		}

	case schema.Float:
		switch v := value.(type) {
		case float32:
			result = float64(v)
		case float64:
			result = v
		default:
			if result, err = strconv.ParseFloat(fmt.Sprintf("%v", v), 64); err != nil {
				err = fmt.Errorf("invalid value type for float field %s: %w", field.Name, err)
			}
		}

	case schema.Time:
		if t, ok := value.(time.Time); ok {
			result = t
		} else if result, err = time.Parse(time.RFC3339, fmt.Sprintf("%v", value)); err != nil {
			if result, err = time.Parse(time.RFC3339Nano, fmt.Sprintf("%v", value)); err != nil {
				if result, err = time.Parse("2006-01-02 15:04:05.000000+00", fmt.Sprintf("%v", value)); err != nil {
					err = fmt.Errorf("invalid value type for time field %s: %w", field.Name, err)
				}
			}
		}

	case schema.Int:
		switch v := value.(type) {
		case int, int8, int16, int32:
			if result, err = strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64); err != nil {
				err = fmt.Errorf("invalid value type for int field %s: %w", field.Name, err)
			}
		case int64:
			result = v
		default:
			if result, err = strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64); err != nil {
				err = fmt.Errorf("invalid value type for int field %s: %w", field.Name, err)
			}
		}

	case schema.Uint:
		switch v := value.(type) {
		case uint, uint8, uint16, uint32:
			if result, err = strconv.ParseUint(fmt.Sprintf("%v", v), 10, 64); err != nil {
				err = fmt.Errorf("invalid value type for uint field %s: %w", field.Name, err)
			}
		case uint64:
			result = v
		default:
			if result, err = strconv.ParseUint(fmt.Sprintf("%v", v), 10, 64); err != nil {
				err = fmt.Errorf("invalid value type for uint field %s: %w", field.Name, err)
			}
		}

	case schema.Bytes:
		if b, ok := value.([]byte); ok {
			result = b
		} else if s, ok := value.(string); ok {
			result = []byte(s)
		} else {
			err = fmt.Errorf("invalid value type for bytes field %s: %T", field.Name, value)
		}

	default:
		err = fmt.Errorf("unsupported field type %s for field %s", field.DataType, field.Name)
	}

	return result, err
}
