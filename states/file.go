package states

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const FilePrefix = ".json"

type FileStore[V any] struct {
	mutex    sync.Mutex
	path     string
	mode     os.FileMode
	isClosed bool
}

func (sm *FileStore[V]) getItemPath(key string) string {
	return filepath.Join(sm.path, strings.ReplaceAll(strings.ToLower(key), " ", "-")+FilePrefix)
}

// Sets the value for a key.
func (sm *FileStore[V]) Store(ctx *context.Context, key string, value V) error {
	if sm.isClosed {
		return ErrClosed
	}

	// To JSON
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling state: Error: %w", err)
	}
	// Save to file
	path := sm.getItemPath(key)
	err = os.WriteFile(path, data, sm.mode)
	if err != nil {
		return fmt.Errorf("error saving state file: %s. Error: %w", path, err)
	}
	return nil
}

// Load returns the value stored in the map for a key, or nil if no value is present.
// The ok result indicates whether a value was found in the map.
func (sm *FileStore[V]) Load(ctx *context.Context, key string) (value V, ok bool) {
	if sm.isClosed {
		return value, false
	}

	path := sm.getItemPath(key)

	// Read from the file
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn(fmt.Sprintf("Failed reading state file: %s. Error: %v", path, err))
		}
		return value, false
	}
	// Parse JSON
	var result V
	if err := json.Unmarshal(data, &result); err != nil {
		slog.Warn(fmt.Sprintf("Failed unmarshaling state file: %s. Error: %v", path, err))
		return value, false
	}
	return result, true
}

// Deletes the value for a key.
func (sm *FileStore[V]) Delete(ctx *context.Context, key string) error {
	if sm.isClosed {
		return ErrClosed
	}

	path := sm.getItemPath(key)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error deleting state file: %s. Error: %v", path, err)
	}
	return nil
}

// Deletes the value for a key.
func (sm *FileStore[V]) Close(ctx *context.Context) error {
	sm.isClosed = true
	return nil
}

// Deletes the value for a key.
func (sm *FileStore[V]) Clear(ctx *context.Context) error {
	if sm.isClosed {
		return ErrClosed
	}

	return os.RemoveAll(sm.path)
}

// Creates and returns a new state store.
func NewFileStateStore(basePath, storeName string) *FileStore[State] {
	path, err := filepath.Abs(filepath.Join(basePath, storeName))
	if err != nil {
		path = filepath.Join(os.TempDir(), basePath, storeName)
	}

	if err := os.MkdirAll(path, 0644); err != nil {
		panic(fmt.Sprintf("FATAL: could not create store directory: %v", err))
	}

	return &FileStore[State]{
		mutex: sync.Mutex{},
		path:  path,
		mode:  0644,
	}
}
