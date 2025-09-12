package datasources

import (
	"context"
	"fmt"
	"maps"
	"sync"
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

	watcher       chan DatasourcePushRequest
	watcherActive bool
}

func NewMemoryDatasource(name string, idField string) *MemoryDatasource {
	return &MemoryDatasource{
		name:          name,
		idField:       idField,
		data:          new(sync.Map),
		watcher:       make(chan DatasourcePushRequest),
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
	var baseIDs []string
	if len(request.IDs) > 0 {
		baseIDs = make([]string, 0, len(request.IDs))
		for _, id := range request.IDs {
			if _, exists := m.data.Load(id); exists {
				baseIDs = append(baseIDs, id)
			}
		}
	} else {
		baseIDs = m.ids
	}

	total := uint64(len(baseIDs))
	var start, end uint64

	// 2. If offset beyond available entries, return empty result
	if request.Offset >= total || total == 0 {
		return DatasourceFetchResult{
			Docs:  make([]map[string]any, 0),
			Start: 0,
			End:   0,
		}
	}

	// 3. Compute start index
	start = request.Offset

	// 4. Determine page size (zero means “no cap”)
	var pageSize uint64
	if request.Size == 0 {
		pageSize = total
	} else {
		pageSize = request.Size
	}

	// 5. Compute exclusive end index, then inclusive end
	endExcl := min(start+pageSize, total)
	end = endExcl - 1

	// 6. Slice and clone documents
	ids := baseIDs[start:endExcl]
	docs := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
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

	if m.isClosed {
		return count, ErrDatastoreClosed
	}

	// Inserts
	for _, doc := range request.Inserts {
		id, err := m.getID(doc)
		if err != nil {
			return count, err
		}
		m.data.Store(id, clone(doc))
		count.Inserts++
	}

	// Updates
	for id, fields := range request.Updates {
		if existingAny, ok := m.data.Load(id); ok {
			if existing, ok := existingAny.(map[string]any); ok {
				maps.Copy(existing, fields)
				m.data.Store(id, existing)
			}
		} else {
			m.data.Store(id, fields)
		}
		count.Updates++
	}

	// Deletes
	for _, id := range request.Deletes {
		m.data.Delete(id)
		count.Deletes++
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
		go func(r DatasourcePushRequest) {
			m.watcher <- r
		}(*request)
	}

	return count, nil
}

func (m *MemoryDatasource) Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult {
	if m.isClosed {
		panic(ErrDatastoreClosed)
	}

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
	return nil
}
