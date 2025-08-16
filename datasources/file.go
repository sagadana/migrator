package datasources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const FileIDField = "_id"

// FileDatasource provides a Datasource implementation based on a single JSON file.
// Note: This implementation is for demonstration and may not be efficient for very large files.
type FileDatasource struct {
	path string
	mode os.FileMode
	// Mutex to handle concurrent access to the file and in-memory cache
	mu sync.RWMutex
	// In-memory representation of the JSON file's data
	cache []any
}

// NewFileDatasource creates and returns a new instance of FileDatasource.
func NewFileDatasource(path string) *FileDatasource {
	return &FileDatasource{
		path:  path,
		mode:  0644,
		cache: make([]any, 0),
	}
}

// save writes the current in-memory cache to the file, overwriting its contents.
func (ds *FileDatasource) save() error {
	bytes, err := json.MarshalIndent(ds.cache, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cache to JSON: %w", err)
	}
	return os.WriteFile(ds.path, bytes, ds.mode)
}

// load reads the file and populates the in-memory cache.
func (ds *FileDatasource) load() error {
	file, err := os.Open(ds.path)
	if err != nil {
		// If file doesn't exist, create it with an empty JSON array
		if os.IsNotExist(err) {
			return os.WriteFile(ds.path, []byte("[]"), ds.mode)
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Handle empty file case
	if len(bytes) == 0 {
		ds.cache = make([]any, 0)
		return nil
	}

	var data []any
	if err := json.Unmarshal(bytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	ds.cache = data
	return nil
}

// Init loads data from the JSON file into memory. It creates the file if it doesn't exist.
func (ds *FileDatasource) Init() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.load(); err != nil {
		// Using panic because Init is critical for the datasource's operation
		panic(fmt.Sprintf("FATAL: could not initialize file datasource: %v", err))
	}
	log.Printf("File datasource initialized from '%s'", ds.path)
}

// Count returns the total number of documents in the file.
func (ds *FileDatasource) Count(_ *context.Context, _ *DatasourceFetchRequest) int64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return int64(len(ds.cache))
}

// Fetch retrieves a paginated list of documents from the file.
func (ds *FileDatasource) Fetch(_ *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	total := int64(len(ds.cache))
	start := request.Skip
	end := start + request.Size

	if start < 0 {
		start = 0
	}
	if start >= total {
		// If the starting point is beyond the data, return empty
		return DatasourceFetchResult{Docs: []any{}, Start: start, End: start}
	}

	// If size is 0 or extends beyond the cache length, fetch until the end
	if request.Size == 0 || end > total {
		end = total
	}

	return DatasourceFetchResult{
		Docs:  ds.cache[start:end],
		Start: start,
		End:   end,
	}
}

// Push applies inserts, updates, and deletes to the file.
// For simplicity, updates and deletes are identified by matching the entire object.
// A more robust implementation would match based on a unique ID.
func (ds *FileDatasource) Push(_ *context.Context, request *DatasourcePushRequest) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// 1. Handle Deletes
	if len(request.Deletes) > 0 {
		newCache := make([]any, 0, len(ds.cache))
		deleteSet := make(map[string]struct{})
		for _, item := range request.Deletes {
			// Create a comparable representation (e.g., JSON string)
			bytes, _ := json.Marshal(item)
			deleteSet[string(bytes)] = struct{}{}
		}

		for _, item := range ds.cache {
			bytes, _ := json.Marshal(item)
			if _, found := deleteSet[string(bytes)]; !found {
				newCache = append(newCache, item)
			}
		}
		ds.cache = newCache
	}

	// 2. Handle Updates
	if len(request.Updates) > 0 {
		// This is a naive implementation: it replaces the matching old doc with the new one.
		// Assumes updates are provided as complete new documents.
		// It requires a unique identifier (e.g., `_id`) in the documents.
		updateMap := make(map[any]any)
		for _, item := range request.Updates {
			if m, ok := item.(map[string]any); ok {
				if id, exists := m[FileIDField]; exists {
					updateMap[id] = item
				}
			}
		}

		for i, item := range ds.cache {
			if m, ok := item.(map[string]any); ok {
				if id, exists := m[FileIDField]; exists {
					if updatedDoc, found := updateMap[id]; found {
						ds.cache[i] = updatedDoc
					}
				}
			}
		}
	}

	// 3. Handle Inserts
	if len(request.Inserts) > 0 {
		ds.cache = append(ds.cache, request.Inserts...)
	}

	return ds.save()
}

// Watch listens for changes to the underlying file and streams them.
func (ds *FileDatasource) Watch(ctx *context.Context, _ *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	results := make(chan DatasourceStreamResult)

	go func(bgCtx context.Context) {
		defer close(results)

		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			results <- DatasourceStreamResult{Err: fmt.Errorf("failed to create file watcher: %w", err)}
			return
		}
		defer watcher.Close()

		if err := watcher.Add(ds.path); err != nil {
			results <- DatasourceStreamResult{Err: fmt.Errorf("failed to add file to watcher: %w", err)}
			return
		}

		log.Printf("Watching file '%s' for changes...", ds.path)

		for {
			select {
			case <-bgCtx.Done():
				// Context was cancelled, so we stop watching.
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return // Channel closed
				}
				// We only care about writes to the file.
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Printf("Detected file write event: %s", event.Name)

					// Debounce to avoid rapid firing
					time.Sleep(100 * time.Millisecond)

					ds.mu.Lock()
					oldCache := ds.cache // Keep old data for comparison
					if err := ds.load(); err != nil {
						log.Printf("Error reloading file after change: %v", err)
						results <- DatasourceStreamResult{
							Err: err,
						}
						ds.mu.Unlock()
						continue
					}
					newCache := ds.cache
					ds.mu.Unlock()

					// Diff the old and new cache to find changes
					changes := diff(oldCache, newCache)
					if len(changes.Inserts) > 0 || len(changes.Updates) > 0 || len(changes.Deletes) > 0 {
						results <- DatasourceStreamResult{
							Docs: changes,
						}
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return // Channel closed
				}
				log.Printf("File watcher error: %v", err)
				results <- DatasourceStreamResult{
					Err: err,
				}
			}
		}
	}(*ctx)

	return results
}

// diff compares two slices of documents and identifies inserts, updates, and deletes.
// This helper assumes documents are maps with an FileIDField field for identification.
func diff(oldDocs, newDocs []any) DatasourcePushRequest {
	oldMap := make(map[string]any)
	for _, doc := range oldDocs {
		if m, ok := doc.(map[string]any); ok {
			if id, exists := m[FileIDField]; exists {
				oldMap[id.(string)] = doc
			}
		}
	}

	newMap := make(map[string]any)
	for _, doc := range newDocs {
		if m, ok := doc.(map[string]any); ok {
			if id, exists := m[FileIDField]; exists {
				newMap[id.(string)] = doc
			}
		}
	}

	var req DatasourcePushRequest

	// Check for updates and deletes
	for id, oldDoc := range oldMap {
		newDoc, found := newMap[id]
		if !found {
			req.Deletes = append(req.Deletes, id)
		} else if !reflect.DeepEqual(oldDoc, newDoc) {
			req.Updates[id] = newDoc
		}
	}

	// Check for inserts
	for id, newDoc := range newMap {
		if _, found := oldMap[id]; !found {
			req.Inserts = append(req.Inserts, newDoc)
		}
	}

	return req
}
