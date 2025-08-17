package datasources

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sagadana/migrator/helpers"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const filePrefix = ".json"
const indexFileName = "_index" + filePrefix

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
	index     map[string]string
	indexKeys []string
}

// NewFileDatasource creates a new instance of the indexed file datasource.
// - collectionName: The name of the folder to store data files.
// - idField: The name of the field within each document to use as its unique ID.
func NewFileDatasource(basePath, collectionName, idField string) *FileDatasource {
	collectionPath, err := filepath.Abs(filepath.Join(basePath, collectionName))
	if err != nil {
		collectionPath = filepath.Join(basePath, collectionName)
	}
	indexPath, err := filepath.Abs(filepath.Join(basePath, collectionName, indexFileName))
	if err != nil {
		indexPath = filepath.Join(basePath, collectionName, indexFileName)
	}

	return &FileDatasource{
		collectionName: collectionName,
		idField:        idField,
		collectionPath: collectionPath,
		indexPath:      indexPath,
		index:          make(map[string]string),
		indexKeys:      []string{},
		mode:           0644,
	}
}

// --- Private Helper Methods ---

// calculateHash computes a SHA256 hash for a given byte slice.
func calculateHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// getID extracts the document ID from a record using reflection.
func (ds *FileDatasource) getID(record any) (string, error) {
	val := reflect.ValueOf(record)
	// If the record is a map
	if val.Kind() == reflect.Map {
		for _, key := range val.MapKeys() {
			if key.String() == ds.idField {
				return fmt.Sprintf("%v", val.MapIndex(key).Interface()), nil
			}
		}
	}
	return "", fmt.Errorf("idField '%s' not found in record", ds.idField)
}

// processIndex sorts index and loads index keys for pagination use
func (ds *FileDatasource) processIndex() {
	// Sort index
	keys := make([]string, 0, len(ds.index))
	for id := range ds.index {
		keys = append(keys, id)
	}
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
	log.Printf("Index file for %s not found. Building index from data files...", ds.collectionName)
	entries, err := os.ReadDir(ds.collectionPath)
	if err != nil {
		return fmt.Errorf("failed to read collection directory: %w", err)
	}

	index := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == indexFileName {
			continue
		}

		path := filepath.Join(ds.collectionPath, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			log.Printf("Warning: could not read file %s during index build: %v", path, err)
			continue
		}
		hash := calculateHash(content)
		docID := strings.TrimSuffix(entry.Name(), filePrefix)

		index[docID] = hash
	}

	ds.index = index
	return ds.saveIndex()
}

// Load content of CSV file into datasource
func (ds *FileDatasource) LoadCSV(ctx *context.Context, path string, batchSize int64) error {
	ds.Init()

	result, err := helpers.StreamCSV(path, batchSize)
	if err != nil {
		return err
	}

	for data := range result {
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}

		converted := []map[string]any{}
		err = json.Unmarshal(bytes, &converted)
		if err != nil {
			return err
		}

		err = ds.Push(ctx, &DatasourcePushRequest{
			Inserts: converted,
		})
		if err != nil {
			return err
		}
	}

	return nil
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
	if len(ds.index) == 0 {
		if err := ds.buildIndex(); err != nil {
			panic(fmt.Sprintf("FATAL: could not build index: %v", err))
		}
	}
	log.Printf("File datasource '%s' initialized with %d records.", ds.collectionName, len(ds.index))
}

// Count returns the total number of records by checking the index size.
func (ds *FileDatasource) Count(_ *context.Context, _ *DatasourceFetchRequest) int64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return int64(len(ds.index))
}

// Fetch retrieves a paginated set of documents by reading individual files based on the index.
func (ds *FileDatasource) Fetch(_ *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

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

	// Itterate through index to fetch files
	docs := make([]map[string]any, 0, size)
	for i := start; i <= end; i++ {
		docID := ds.indexKeys[i]

		filePath := filepath.Join(ds.collectionPath, docID+filePrefix)
		bytes, err := os.ReadFile(filePath)
		if err != nil {
			return DatasourceFetchResult{Err: fmt.Errorf("failed to read file for doc ID %s: %w", docID, err)}
		}
		var doc map[string]any
		if err := json.Unmarshal(bytes, &doc); err != nil {
			return DatasourceFetchResult{Err: fmt.Errorf("failed to unmarshal doc ID %s: %w", docID, err)}
		}
		docs = append(docs, doc)
	}

	return DatasourceFetchResult{Docs: docs, Start: start, End: end}
}

// Push applies inserts, updates, and deletes to the file system and updates the index.
func (ds *FileDatasource) Push(_ *context.Context, request *DatasourcePushRequest) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// 1. Handle Inserts
	for _, record := range request.Inserts {
		docID, err := ds.getID(record)
		if err != nil {
			return fmt.Errorf("failed to get record ID: %w", err)
		}
		bytes, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal insert for ID %s: %w", docID, err)
		}
		filePath := filepath.Join(ds.collectionPath, docID+filePrefix)
		if err := os.WriteFile(filePath, bytes, ds.mode); err != nil {
			return fmt.Errorf("failed to write insert for ID %s: %w", docID, err)
		}
		ds.index[docID] = calculateHash(bytes)
	}

	// 2. Handle Updates
	for docID, record := range request.Updates {
		bytes, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal update for ID %s: %w", docID, err)
		}
		filePath := filepath.Join(ds.collectionPath, docID+filePrefix)
		if err := os.WriteFile(filePath, bytes, ds.mode); err != nil {
			return fmt.Errorf("failed to write update for ID %s: %w", docID, err)
		}
		ds.index[docID] = calculateHash(bytes)
	}

	// 3. Handle Deletes
	for _, docID := range request.Deletes {
		filePath := filepath.Join(ds.collectionPath, docID+filePrefix)
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete file for ID %s: %w", docID, err)
		}
		delete(ds.index, docID)
	}

	// 4. Persist the updated index
	return ds.saveIndex()
}

// Watch listens for file system events in the collection directory to report changes.
func (ds *FileDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	out := make(chan DatasourceStreamResult)

	batchSize := max(request.BatchSize, 1)
	batchWindow := max(request.BatchWindowSeconds, 1)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		out <- DatasourceStreamResult{Err: fmt.Errorf("failed to create file watcher: %w", err)}
		defer close(out)
		return out
	}

	go func(bgCtx context.Context) {
		defer close(out)
		defer watcher.Close()

		// Ticker to enforce the batch processing time window
		ticker := time.NewTicker(time.Duration(batchWindow) * time.Second)
		defer ticker.Stop()

		// Buffer to hold the batch of change events
		batch := DatasourcePushRequest{Updates: make(map[string]map[string]any)}

		log.Printf("Watching directory '%s' for changes...", ds.collectionPath)
		for {
			select {
			case <-bgCtx.Done():
				log.Println("Watching: Context cancelled")

				// Context has been cancelled. Process any remaining events in the batch before exiting.
				out <- DatasourceStreamResult{Docs: batch}
				return

			case <-ticker.C:

				// Time window has elapsed. Process the batch if it's not empty.
				if len(batch.Inserts) > 0 || len(batch.Updates) > 0 || len(batch.Deletes) > 0 {
					out <- DatasourceStreamResult{Docs: batch}
					// Reset the batch buffer
					batch = DatasourcePushRequest{Updates: make(map[string]map[string]any)}
				}

			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				// Ignore events for the index file itself
				if filepath.Base(event.Name) == indexFileName {
					continue
				}

				docID := strings.TrimSuffix(filepath.Base(event.Name), filePrefix)

				// CREATE or WRITE event
				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
					content, err := os.ReadFile(event.Name)
					if err != nil {
						continue // File might be gone again, skip
					}
					newHash := calculateHash(content)
					oldHash, exists := ds.index[docID]

					if !exists { // It's a new file -> Insert
						var doc map[string]any
						if json.Unmarshal(content, &doc) == nil {
							batch.Inserts = append(batch.Inserts, doc)
						}
					} else if newHash != oldHash { // File changed -> Update
						var doc map[string]any
						if json.Unmarshal(content, &doc) == nil {
							batch.Updates[docID] = doc
						}
					}
				} else if event.Has(fsnotify.Remove) { // REMOVE event
					if _, exists := ds.index[docID]; exists {
						batch.Deletes = append(batch.Deletes, docID)
					}
				}

				// If batch is full, process it immediately.
				if len(batch.Inserts)+len(batch.Updates)+len(batch.Deletes) >= int(batchSize) {
					out <- DatasourceStreamResult{Docs: batch}
					// Reset the batch buffer and the ticker
					batch = DatasourcePushRequest{Updates: make(map[string]map[string]any)}
					ticker.Reset(time.Duration(batchWindow) * time.Second)
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("File watcher error: %v", err)
			}
		}
	}(*ctx)

	if err := watcher.Add(ds.collectionPath); err != nil {
		out <- DatasourceStreamResult{Err: fmt.Errorf("failed to add collection to watcher: %w", err)}
	}

	return out
}
