package datasources

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"github.com/sagadana/migrator/helpers"
)

// -------------------------------------------------------------------------------
// MemoryDatasource: in-memory implementation of Datasource interface for testing
// -------------------------------------------------------------------------------

type MemoryDatasource struct {
	name string

	idField  string
	isClosed bool

	ids  []string
	data *sync.Map

	// Watcher channel
	watcher       chan DatasourcePushRequest
	watcherActive bool
}

func NewMemoryDatasource(name string, idField string) *MemoryDatasource {
	return &MemoryDatasource{
		name:          name,
		idField:       idField,
		data:          new(sync.Map),
		watcher:       make(chan DatasourcePushRequest, DefaultWatcherBufferSize),
		watcherActive: false,
	}
}

// clone makes a shallow copy of a map[string]any
func clone[T any](src map[string]T) map[string]T {
	dst := make(map[string]T, len(src))
	maps.Copy(dst, src)
	return dst
}

func (m *MemoryDatasource) getID(data map[string]any) (val string, err error) {
	id, ok := data[m.idField]
	if ok {
		return fmt.Sprintf("%v", id), err
	}
	return val, fmt.Errorf("missing '%s' in insert document", m.idField)
}

func (m *MemoryDatasource) Count(_ *context.Context, request *DatasourceFetchRequest) uint64 {
	if m.isClosed {
		panic(ErrDatastoreClosed)
	}

	// 1. Determine the raw “total” matching items
	var total uint64
	if len(request.IDs) > 0 {
		// Only count IDs that actually exist
		for _, id := range request.IDs {
			if _, ok := m.data.Load(id); ok {
				total++
			}
		}
	} else {
		// No IDs filter → all stored IDs
		total = uint64(len(m.ids))
	}

	// 2. Apply offset: if we skip past the end, zero left
	if request.Offset >= total {
		return 0
	}
	remaining := total - request.Offset

	// 3. Apply size cap: if Size>0 and smaller than what's left, cap it
	if request.Size > 0 && request.Size < remaining {
		return request.Size
	}

	// 4. Otherwise, everything remaining counts
	return remaining
}

func (m *MemoryDatasource) Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult {
	if m.isClosed {
		panic(ErrDatastoreClosed)
	}

	// 1. Build base slice of IDs to page through
	var keys []string
	if len(request.IDs) > 0 {
		var id string
		keys = make([]string, 0, len(request.IDs))
		for _, id = range request.IDs {
			if _, exists := m.data.Load(id); exists {
				keys = append(keys, id)
			}
		}
	} else {
		keys = m.ids
	}

	// 2. Slice and clone documents
	ids, start, end := helpers.Slice(keys, request.Offset, request.Size)

	// 3. Fetch actual documents by key
	var id string
	docs := make([]map[string]any, 0, len(ids))
	for _, id = range ids {
		if raw, ok := m.data.Load(id); ok {
			if doc, ok := raw.(map[string]any); ok {
				docs = append(docs, clone(doc))
			}
		}
	}
	return DatasourceFetchResult{
		Docs:  docs,
		Start: start,
		End:   end,
	}
}

func (m *MemoryDatasource) Push(_ *context.Context, request *DatasourcePushRequest) (DatasourcePushCount, error) {

	count := *new(DatasourcePushCount)
	event := &DatasourcePushRequest{
		Inserts: []map[string]any{},
		Updates: []map[string]any{},
		Deletes: []string{},
	}

	var pushErr error
	var doc map[string]any
	var id string

	if m.isClosed {
		return count, ErrDatastoreClosed
	}

	// Inserts
	for _, doc = range request.Inserts {
		if doc == nil {
			continue
		}

		id, err := m.getID(doc)
		if err != nil {
			pushErr = fmt.Errorf("memory item id error: %w", err)
			slog.Warn(pushErr.Error())
			continue
		}

		m.data.Store(id, clone(doc))
		count.Inserts++
		event.Inserts = append(event.Inserts, clone(doc))
	}

	// Updates
	for _, doc = range request.Updates {
		if doc == nil {
			continue
		}

		id, err := m.getID(doc)
		if err != nil {
			pushErr = fmt.Errorf("memory item id error: %w", err)
			slog.Warn(pushErr.Error())
			continue
		}

		if existingAny, ok := m.data.Load(id); ok {
			if existing, ok := existingAny.(map[string]any); ok {
				maps.Copy(existing, doc)
				m.data.Store(id, existing)
			}
		} else {
			m.data.Store(id, doc)
		}

		count.Updates++
		event.Updates = append(event.Updates, clone(doc))
	}

	// Deletes
	for _, id = range request.Deletes {
		m.data.Delete(id)
		count.Deletes++
		event.Deletes = append(event.Deletes, id)
	}

	// Regenerate ids on inserts & deletes
	if count.Inserts > 0 || count.Deletes > 0 {
		ids := []string{}
		m.data.Range(func(key, _ any) bool {
			if id, ok := key.(string); ok {
				ids = append(ids, id)
			}
			return true
		})
		m.ids = ids
	}

	// Notify watchers
	if m.watcherActive && m.watcher != nil {
		m.watcher <- *event
	}

	return count, pushErr
}

func (m *MemoryDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	if m.isClosed {
		panic(ErrDatastoreClosed)
	}

	// Return stream changes channel
	m.watcherActive = true
	return StreamChanges(
		ctx,
		fmt.Sprintf("memory datastore '%s'", m.name),
		m.watcher,
		request,
	)
}

func (m *MemoryDatasource) Clear(_ *context.Context) error {
	if m.isClosed {
		return ErrDatastoreClosed
	}

	m.data.Clear()
	m.ids = make([]string, 0)
	return nil
}

func (m *MemoryDatasource) Close(ctx *context.Context) error {
	m.data = new(sync.Map)
	m.ids = make([]string, 0)
	m.isClosed = true

	// Close watcher channel
	close(m.watcher)

	m.watcherActive = false
	return nil
}

func (m *MemoryDatasource) Import(ctx *context.Context, request DatasourceImportRequest) error {
	if m.isClosed {
		return ErrDatastoreClosed
	}

	switch request.Type {
	case DatasourceImportTypeCSV:
		return LoadCSV(ctx, m, request.Location, request.BatchSize)
	default:
		return fmt.Errorf("unsupported import type: %s", request.Type)
	}
}

func (m *MemoryDatasource) Export(ctx *context.Context, request DatasourceExportRequest) error {
	if m.isClosed {
		return ErrDatastoreClosed
	}
	switch request.Type {
	case DatasourceExportTypeCSV:
		return SaveCSV(ctx, m, request.Location, request.BatchSize)
	default:
		return fmt.Errorf("unsupported export type: %s", request.Type)
	}
}
