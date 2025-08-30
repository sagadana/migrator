package states

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileStore[V any] struct {
	mutex     sync.Mutex
	path      string
	mode      os.FileMode
	_fullPath string
}

func addSuffixToPath(path, suffix string) string {
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext) // Remove original extension
	return base + suffix + ext
}

func (sm *FileStore[V]) getFullPath(key string) string {
	if len(sm._fullPath) == 0 {
		sm._fullPath = addSuffixToPath(sm.path, "-"+strings.ReplaceAll(strings.ToLower(key), " ", "-"))
	}
	return sm._fullPath
}

// Sets the value for a key.
func (sm *FileStore[V]) Store(ctx *context.Context, key string, value V) error {
	// To JSON
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling state: Error: %w", err)
	}
	// Save to file
	path := sm.getFullPath(key)
	err = os.WriteFile(path, data, sm.mode)
	if err != nil {
		return fmt.Errorf("error saving state file: %s. Error: %w", path, err)
	}
	return nil
}

// Load returns the value stored in the map for a key, or nil if no value is present.
// The ok result indicates whether a value was found in the map.
func (sm *FileStore[V]) Load(ctx *context.Context, key string) (value V, ok bool) {
	path := sm.getFullPath(key)

	// Read from the file
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Failed reading state file: %s. Error: %v", path, err)
		}
		return value, false
	}
	// Parse JSON
	var result V
	if err := json.Unmarshal(data, &result); err != nil {
		log.Printf("Failed unmarshaling state file: %s. Error: %v", path, err)
		return value, false
	}
	return result, true
}

// Deletes the value for a key.
func (sm *FileStore[V]) Delete(ctx *context.Context, key string) error {
	path := sm.getFullPath(key)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error deleting state file: %s. Error: %v", path, err)
	}
	return nil
}

// Deletes the value for a key.
func (sm *FileStore[V]) Close(ctx *context.Context) error {
	return nil
}

// Deletes the value for a key.
func (sm *FileStore[V]) Clear(ctx *context.Context) error {
	return nil
}

// Creates and returns a new state store.
func NewFileStateStore(basePath, storeName string) *FileStore[State] {
	stateFilePath, err := filepath.Abs(filepath.Join(basePath, storeName))
	if err != nil {
		stateFilePath = storeName
	}
	return &FileStore[State]{
		mutex: sync.Mutex{},
		path:  stateFilePath,
		mode:  0644,
	}
}
