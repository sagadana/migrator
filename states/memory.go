package states

import (
	"context"
	"sync"
)

type MemoryStore[K comparable, V any] struct {
	m *sync.Map

	isClosed bool
}

// Sets the value for a key.
func (ms *MemoryStore[K, V]) Store(ctx *context.Context, key K, value V) error {
	if ms.isClosed {
		return ErrStoreClosed
	}

	ms.m.Store(key, value)
	return nil
}

// Load returns the value stored in the map for a key, or nil if no value is present.
// The ok result indicates whether a value was found in the map.
func (ms *MemoryStore[K, V]) Load(ctx *context.Context, key K) (value V, ok bool) {
	if ms.isClosed {
		return value, false
	}

	val, loaded := ms.m.Load(key)
	if loaded {
		return val.(V), true // Type assertion here, but within the generic wrapper
	}
	return value, false
}

// Deletes the value for a key.
func (ms *MemoryStore[K, V]) Delete(ctx *context.Context, key K) error {
	if ms.isClosed {
		return ErrStoreClosed
	}

	ms.m.Delete(key)
	return nil
}

// Close store.
func (ms *MemoryStore[K, V]) Close(ctx *context.Context) error {
	ms.isClosed = true
	return nil
}

// Clear all values
func (ms *MemoryStore[K, V]) Clear(ctx *context.Context) error {
	if ms.isClosed {
		return ErrStoreClosed
	}

	ms.m.Range(func(key, val any) bool {
		ms.m.Delete(key)
		return true
	})
	return nil
}

// Creates and returns a new generic memory store.
func NewMemoryStore[V any]() *MemoryStore[string, V] {
	return &MemoryStore[string, V]{
		m: new(sync.Map),
	}
}

// Creates and returns a new memory state store.
func NewMemoryStateStore() *MemoryStore[string, State] {
	return NewMemoryStore[State]()
}
