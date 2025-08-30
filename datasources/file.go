package datasources

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sagadana/migrator/helpers"

	"github.com/fsnotify/fsnotify"
)

const FilePrefix = ".json"
const IndexFileName = "_index" + FilePrefix

// FileDatasource provides a Datasource implementation that stores records
// as individual files in a collection directory, managed by a central index file.
type FileDatasource struct {
	collectionName string
	idField        string
	collectionPath string
	indexPath      string
	mode           os.FileMode
	mu             sync.RWMutex
	// index stores a map of documentID -> content hash for quick lookups and change detection.
	index     *sync.Map
	indexKeys []string
}

// NewFileDatasource creates a new instance of the indexed file datasource.
//   - collectionName: The name of the folder to store data files.
//   - idField: The name of the field within each document to use as its unique ID.
func NewFileDatasource(basePath, collectionName, idField string) *FileDatasource {
	collectionPath, err := filepath.Abs(filepath.Join(basePath, collectionName))
	if err != nil {
		collectionPath = filepath.Join(basePath, collectionName)
	}
	indexPath, err := filepath.Abs(filepath.Join(basePath, collectionName, IndexFileName))
	if err != nil {
		indexPath = filepath.Join(basePath, collectionName, IndexFileName)
	}

	ds := &FileDatasource{
		collectionName: collectionName,
		idField:        idField,
		collectionPath: collectionPath,
		indexPath:      indexPath,
		index:          new(sync.Map),
		indexKeys:      make([]string, 0),
		mode:           0644,
	}

	// Initialize data source
	ds.Init()

	return ds
}

// --- Private Helper Methods ---

// processIndex sorts index and loads index keys for pagination use
func (ds *FileDatasource) processIndex() {
	// Sort index
	keys := make([]string, 0)
	ds.index.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	helpers.NumberAwareSort(keys)
	ds.indexKeys = keys
}

// loadIndex reads the index file from disk into the in-memory map.
func (ds *FileDatasource) loadIndex() error {
	if _, err := os.Stat(ds.indexPath); os.IsNotExist(err) {
		return nil // Index doesn't exist yet, which is fine.
	}

	bytes, err := os.ReadFile(ds.indexPath)
	if err != nil {
		return fmt.Errorf("failed to read index file: %w", err)
	}
	if len(bytes) == 0 {
		return nil // Empty index file
	}

	if err = json.Unmarshal(bytes, &ds.index); err != nil {
		return err
	}

	// Process index after loading
	ds.processIndex()
	return nil
}

// saveIndex writes the current in-memory index to the _index.json file.
func (ds *FileDatasource) saveIndex() error {
	// Re-process index before saving
	ds.processIndex()

	bytes, err := json.MarshalIndent(ds.index, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}
	return os.WriteFile(ds.indexPath, bytes, ds.mode)
}

// buildIndex scans the collection directory and builds the index from scratch.
// This is a recovery mechanism if the index file is missing or corrupt.
func (ds *FileDatasource) buildIndex() error {
	entries, err := os.ReadDir(ds.collectionPath)
	if err != nil {
		return fmt.Errorf("failed to read collection directory: %w", err)
	}

	// index := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == IndexFileName {
			continue
		}

		path := filepath.Join(ds.collectionPath, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			slog.Error(fmt.Sprintf("Warning: could not read file %s during index build: %v", path, err))
			continue
		}
		hash := helpers.CalculateHash(content)
		docID := strings.TrimSuffix(entry.Name(), FilePrefix)

		ds.index.Store(docID, hash)
	}

	return ds.saveIndex()
}

func (ds *FileDatasource) getFile(docID string) (map[string]any, error) {

	doc := make(map[string]any)

	filePath := filepath.Join(ds.collectionPath, docID+FilePrefix)
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return doc, fmt.Errorf("failed to read file for doc ID %s: %w", docID, err)
	}
	if err := json.Unmarshal(bytes, &doc); err != nil {
		return doc, fmt.Errorf("failed to unmarshal doc ID %s: %w", docID, err)
	}

	return doc, nil
}

// --- Datasource Interface Implementation ---

// Init sets up the datasource by creating the collection directory and loading/building the index.
func (ds *FileDatasource) Init() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := os.MkdirAll(ds.collectionPath, ds.mode); err != nil {
		panic(fmt.Sprintf("FATAL: could not create collection directory: %v", err))
	}

	if err := ds.loadIndex(); err != nil {
		panic(fmt.Sprintf("FATAL: could not load index file: %v", err))
	}

	// If the index is empty but the directory might have files, build it.
	if len(ds.indexKeys) == 0 {
		slog.Info(fmt.Sprintf("Building index for '%s' collection from data files...", ds.collectionName))
		if err := ds.buildIndex(); err != nil {
			panic(fmt.Sprintf("FATAL: could not build index: %v", err))
		}
	}
	slog.Info(fmt.Sprintf("File datasource '%s' initialized with %d records.", ds.collectionName, len(ds.indexKeys)))
}

// Count returns the total number of records by checking the index size.
func (ds *FileDatasource) Count(_ *context.Context, request *DatasourceFetchRequest) int64 {

	count := int64(len(ds.indexKeys))
	offset := int64(0)

	if count == 0 {
		return 0
	}

	if request.Size > 0 && request.Size < count {
		count = request.Size
	}
	if request.Offset > 0 && request.Offset >= count {
		offset = 0
	}

	return max(0, count-offset)
}

// Fetch retrieves a paginated set of documents by reading individual files based on the index.
func (ds *FileDatasource) Fetch(_ *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {

	if len(request.IDs) > 0 { // Fetch files for IDs
		docs := make([]map[string]any, 0, len(request.IDs))

		for _, docID := range request.IDs {
			_, ok := ds.index.Load(docID)
			if !ok {
				return DatasourceFetchResult{Err: fmt.Errorf("file fetch error: file for ID %s not found", docID)}
			}

			doc, err := ds.getFile(docID)
			if err != nil {
				return DatasourceFetchResult{Err: err}
			}
			docs = append(docs, doc)
		}

		return DatasourceFetchResult{Docs: docs}
	} else { // Itterate through index to fetch files

		total := int64(len(ds.indexKeys))
		last := max(0, total-1)
		size := request.Size
		if size <= 0 {
			size = total
		}

		start := max(0, request.Offset)
		end := max(0, (start+size)-1)
		if end == 0 || end >= last {
			end = last
		}

		if total == 0 || start >= last {
			return DatasourceFetchResult{Docs: []map[string]any{}, Start: start, End: end}
		}
		docs := make([]map[string]any, 0, size)

		for i := start; i <= end; i++ {
			docID := ds.indexKeys[i]
			doc, err := ds.getFile(docID)
			if err != nil {
				return DatasourceFetchResult{Err: err}
			}
			docs = append(docs, doc)
		}

		return DatasourceFetchResult{Docs: docs, Start: start, End: end}
	}
}

// Push applies inserts, updates, and deletes to the file system and updates the index.
func (ds *FileDatasource) Push(_ *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	count := *new(DatasourcePushCount)

	// 1. Handle Inserts
	for i, record := range request.Inserts {
		if len(record) == 0 {
			slog.Warn(fmt.Sprintf("File insert: failed to insert record. Empty record at index '%d'", i))
			continue
		}

		id, ok := record[ds.idField]
		if !ok {
			slog.Warn(fmt.Sprintf("File insert: failed to get record ID. '%s' not found in record", ds.idField))
			continue
		}

		docID := id.(string)
		_, ok = ds.index.Load(docID)
		if ok {
			slog.Info(fmt.Sprintf("File insert: skipping inserts to '%s' for ID '%s': file already exists", ds.collectionName, docID))
			continue
		}

		bytes, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			slog.Warn(fmt.Sprintf("File insert: failed to marshal insert for ID %s: %s", docID, err.Error()))
			continue
		}
		filePath := filepath.Join(ds.collectionPath, docID+FilePrefix)
		if err := os.WriteFile(filePath, bytes, ds.mode); err != nil {
			slog.Error(fmt.Sprintf("File insert: failed to write insert for ID %s: %s", docID, err.Error()))
			continue
		}

		ds.index.Store(docID, helpers.CalculateHash(bytes))
		count.Inserts++
	}

	// 2. Handle Updates
	for docID, record := range request.Updates {
		bytes, err := json.MarshalIndent(record, "", "  ")
		newHash := helpers.CalculateHash(bytes)
		oldHash, ok := ds.index.Load(docID)
		if ok && newHash == oldHash {
			slog.Info(fmt.Sprintf("File update: skipping updates to '%s' for ID '%s': no changes made", ds.collectionName, docID))
			continue
		}

		if err != nil {
			slog.Warn(fmt.Sprintf("File update: failed to marshal update for ID %s: %s", docID, err.Error()))
			continue
		}
		filePath := filepath.Join(ds.collectionPath, docID+FilePrefix)
		if err := os.WriteFile(filePath, bytes, ds.mode); err != nil {
			slog.Warn(fmt.Sprintf("File update: failed to write update for ID %s: %s", docID, err.Error()))
			continue
		}

		ds.index.Store(docID, newHash)
		count.Updates++
	}

	// 3. Handle Deletes
	for _, docID := range request.Deletes {
		filePath := filepath.Join(ds.collectionPath, docID+FilePrefix)
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			slog.Warn(fmt.Sprintf("File delete: failed to delete file for ID %s: %s", docID, err.Error()))
			continue
		}

		ds.index.Delete(docID)
		count.Deletes++
	}

	// 4. Persist the updated index
	return count, ds.saveIndex()
}

// Watch listens for file system events in the collection directory to report changes.
func (ds *FileDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	out := make(chan DatasourceStreamResult)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		out <- DatasourceStreamResult{Err: fmt.Errorf("file watch error: failed to create file watcher: %w", err)}
		defer close(out)
		return out
	}

	// Watch the directory
	if err := watcher.Add(ds.collectionPath); err != nil {
		out <- DatasourceStreamResult{Err: fmt.Errorf("file watch error: failed to add collection to watcher: %w", err)}
		defer close(out)
		return out
	}

	go func(bgCtx context.Context) {
		defer close(out)
		defer watcher.Close()

		batchSize := max(request.BatchSize, 1)
		batchWindow := max(request.BatchWindowSeconds, 1)

		// Ticker to enforce the batch processing time window
		ticker := time.NewTicker(time.Duration(batchWindow) * time.Second)
		defer ticker.Stop()

		// Buffer to hold the batch of change events
		batch := DatasourcePushRequest{
			Inserts: make([]map[string]any, 0),
			Updates: make(map[string]map[string]any),
			Deletes: make([]string, 0),
		}

		slog.Info(fmt.Sprintf("Watching '%s' for changes...", ds.collectionPath))
		for {
			select {
			case <-bgCtx.Done():
				slog.Info("Canceled File Watcher")
				// Context has been cancelled. Process any remaining events in the batch before exiting.
				if len(batch.Inserts)+len(batch.Updates)+len(batch.Deletes) > 0 {
					out <- DatasourceStreamResult{Docs: batch}
				}
				return

			case <-ticker.C:
				// Time window has elapsed. Process the batch if it's not empty.
				if len(batch.Inserts)+len(batch.Updates)+len(batch.Deletes) > 0 {
					out <- DatasourceStreamResult{Docs: batch}
					// Reset the batch buffer
					batch = DatasourcePushRequest{
						Inserts: make([]map[string]any, 0),
						Updates: make(map[string]map[string]any),
						Deletes: make([]string, 0),
					}
				}

			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				// Ignore events for the index file itself
				if filepath.Base(event.Name) == IndexFileName {
					continue
				}

				docID := strings.TrimSuffix(filepath.Base(event.Name), FilePrefix)

				if event.Has(fsnotify.Create) { // CREATE event
					content, err := os.ReadFile(event.Name)
					if err != nil {
						continue // File might be gone again, skip
					}

					var doc map[string]any
					if json.Unmarshal(content, &doc) == nil {
						batch.Inserts = append(batch.Inserts, doc)
					}
				} else if event.Has(fsnotify.Write) { // WRITE event
					content, err := os.ReadFile(event.Name)
					if err != nil {
						continue // File might be gone again, skip
					}

					var doc map[string]any
					if json.Unmarshal(content, &doc) == nil {
						batch.Updates[docID] = doc
					}
				} else if event.Has(fsnotify.Remove) { // REMOVE event
					batch.Deletes = append(batch.Deletes, docID)
				}

				// If batch is full, process it immediately.
				if len(batch.Inserts)+len(batch.Updates)+len(batch.Deletes) >= int(batchSize) {
					out <- DatasourceStreamResult{Docs: batch}
					// Reset the batch buffer and the ticker
					batch = DatasourcePushRequest{
						Inserts: make([]map[string]any, 0),
						Updates: make(map[string]map[string]any),
						Deletes: make([]string, 0),
					}
					ticker.Reset(time.Duration(batchWindow) * time.Second)
				}

			case err, ok := <-watcher.Errors:
				if ok {
					out <- DatasourceStreamResult{Err: fmt.Errorf("file watcher error: %w", err)}
				}
			}
		}
	}(*ctx)

	return out
}

// Clear data source
func (ds *FileDatasource) Clear(ctx *context.Context) error {
	err := os.RemoveAll(ds.collectionPath)
	if err != nil {
		return err
	}
	err = os.RemoveAll(ds.indexPath)
	if err != nil {
		return err
	}
	ds.index.Clear()
	ds.indexKeys = make([]string, 0)

	// Re-init
	ds.Init()

	return nil
}
