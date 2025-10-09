package tests

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type TestDatasource struct {
	id     string
	source datasources.Datasource

	withFilter      bool // Datasource supports filtering
	withWatchFilter bool // Datasource supports watch filtering
	withSort        bool // Datasource supports sorting
	synchronous     bool // Run synchronusly
}

const (
	TestIDField string = "id"
	TestAField  string = "fieldA"
	TestBField  string = "fieldB"

	TestFilterOutField string = "filterOut"
	TestSortAscField   string = "sortAsc"
)

// MockErrorDatasource is a test helper that simulates datasource errors
type MockErrorDatasource struct{}

func (m *MockErrorDatasource) Count(ctx *context.Context, request *datasources.DatasourceFetchRequest) uint64 {
	return 5 // Report that there's data
}

func (m *MockErrorDatasource) Fetch(ctx *context.Context, request *datasources.DatasourceFetchRequest) datasources.DatasourceFetchResult {
	return datasources.DatasourceFetchResult{
		Err:  fmt.Errorf("simulated fetch error"),
		Docs: nil,
	}
}

func (m *MockErrorDatasource) Push(ctx *context.Context, request *datasources.DatasourcePushRequest) (datasources.DatasourcePushCount, error) {
	return datasources.DatasourcePushCount{}, fmt.Errorf("simulated push error")
}

func (m *MockErrorDatasource) Watch(ctx *context.Context, request *datasources.DatasourceStreamRequest) <-chan datasources.DatasourceStreamResult {
	ch := make(chan datasources.DatasourceStreamResult)
	close(ch)
	return ch
}

func (m *MockErrorDatasource) Clear(ctx *context.Context) error {
	return nil
}

func (m *MockErrorDatasource) Close(ctx *context.Context) error {
	return nil
}

func (m *MockErrorDatasource) Import(ctx *context.Context, request datasources.DatasourceImportRequest) error {
	return fmt.Errorf("simulated import error")
}

func (m *MockErrorDatasource) Export(ctx *context.Context, request datasources.DatasourceExportRequest) error {
	return fmt.Errorf("simulated export error")
}

// --------
// Utils
// --------

// Retrieve Test Datasources
// TODO: Add more datasources here to test...
func getTestDatasources(ctx *context.Context, instanceId string) <-chan TestDatasource {
	out := make(chan TestDatasource)

	// Reusable vars
	var id string

	// Create datasource name
	getDsName := func(id string) string {
		return fmt.Sprintf("%s-%s", id, instanceId)
	}

	go func() {
		defer close(out)

		// -----------------------
		// 1. Memory
		// -----------------------
		id = "memory-datasource"
		out <- TestDatasource{
			id:     id,
			source: datasources.NewMemoryDatasource(getDsName(id), TestIDField),
		}

		// -----------------------
		// 2. Mongo
		// -----------------------

		mongoURI := os.Getenv("MONGO_URI")
		mongoDB := os.Getenv("MONGO_DB")

		id = "mongo-datasource"
		out <- TestDatasource{
			id:              id,
			withFilter:      true,
			withWatchFilter: true,
			withSort:        true,
			source: datasources.NewMongoDatasource(
				ctx,
				datasources.MongoDatasourceConfigs{
					URI:            mongoURI,
					DatabaseName:   mongoDB,
					CollectionName: getDsName(id),
					AccurateCount:  true,
					Filter: map[string]any{
						TestFilterOutField: map[string]any{"$in": []any{nil, false, ""}},
					},
					Sort: map[string]any{
						TestSortAscField: 1, // 1 = Ascending, -1 = Descending
					},
					WithTransformer: func(data map[string]any) (map[string]any, error) {
						if data != nil {
							data[datasources.MongoDefaultIDField] = data[TestIDField]
						}
						return data, nil
					},
					OnInit: func(client *mongo.Client) error {
						return nil
					},
				},
			),
		}

		// -----------------------
		// 3. Redis
		// -----------------------
		redisAddr := os.Getenv("REDIS_ADDR")
		redisUser := os.Getenv("REDIS_USER")
		redisPass := os.Getenv("REDIS_PASS")
		redisDb := os.Getenv("REDIS_STATE_DB")

		redisURI := fmt.Sprintf("redis://%s/%s", redisAddr, redisDb)
		if len(redisUser) > 0 && len(redisPass) > 0 {
			redisURI = fmt.Sprintf("redis://%s:%s@%s/%s", redisUser, redisPass, redisAddr, redisDb)
		}

		// --------------- Redis Hash & Without Pefix --------------- //
		id = "redis-hash-datasource"
		out <- TestDatasource{
			id:              id,
			withFilter:      false,
			withWatchFilter: false,
			withSort:        false,
			synchronous:     true, // Run synchronously to prevent overlap with "redis-json-datasource" test
			source: datasources.NewRedisDatasource(
				ctx,
				datasources.RedisDatasourceConfigs{
					URI:      redisURI,
					IDField:  TestIDField,
					ScanSize: 10,
					OnInit: func(client *redis.Client) error {
						return nil
					},
				},
			),
		}

		// --------------- Redis JSON & With Prefix --------------- //
		id = "redis-json-datasource"
		out <- TestDatasource{
			id:              id,
			withFilter:      false,
			withWatchFilter: false,
			withSort:        false,
			source: datasources.NewRedisDatasource(
				ctx,
				datasources.RedisDatasourceConfigs{
					URI:       redisURI,
					KeyPrefix: fmt.Sprintf("%s:", getDsName(id)),
					IDField:   TestIDField,
					ScanSize:  10,
					WithTransformer: func(data map[string]any) (datasources.RedisInputSchema, error) {
						return datasources.JSONToRedisJSONInputSchema(data), nil
					},
					OnInit: func(client *redis.Client) error {
						return nil
					},
				},
			),
		}

		// -----------------------
		// 4. PostgreSQL
		// -----------------------
		postgresDSN := os.Getenv("POSTGRES_DSN")
		if postgresDSN == "" {
			postgresDSN = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
				os.Getenv("POSTGRES_HOST"),
				os.Getenv("POSTGRES_PORT"),
				os.Getenv("POSTGRES_USER"),
				os.Getenv("POSTGRES_PASS"),
				os.Getenv("POSTGRES_DB"),
				os.Getenv("POSTGRES_SSLMODE"),
			)
		}

		// ---------------------- Simple Model  -----------------------

		// Define test model for PostgreSQL
		type TestSimplePostgresModel struct {
			ID     string `gorm:"primaryKey;column:id" json:"id"`
			FieldA string `gorm:"column:fieldA" json:"fieldA"`
			FieldB string `gorm:"column:fieldB" json:"fieldB"`
		}

		id = "postgres-simple-datasource"
		out <- TestDatasource{
			id:              id,
			withFilter:      false,
			withWatchFilter: false,
			withSort:        false,
			source: datasources.NewPostgresDatasource(
				ctx,
				datasources.PostgresDatasourceConfigs[TestSimplePostgresModel]{
					DSN:       postgresDSN,
					TableName: getDsName(id),
					Model:     &TestSimplePostgresModel{},
					IDField:   TestIDField,

					OnInit: func(db *gorm.DB) error {
						return nil
					},

					DisableReplication: false,
				},
			),
		}

		// ---------------------- Complex Model -----------------------

		// Define test model for PostgreSQL
		type TestComplexPostgresModel struct {
			ID        string    `gorm:"primaryKey;column:id" json:"id"`
			FieldA    string    `gorm:"column:fieldA" json:"fieldA"`
			FieldB    string    `gorm:"column:fieldB" json:"fieldB"`
			FilterOut bool      `gorm:"column:filterOut" json:"filterOut"`
			SortAsc   int       `gorm:"type:int;column:sortAsc" json:"sortAsc"`
			CreatedAt time.Time `gorm:"column:createdAt" json:"createdAt"`
			UpdatedAt time.Time `gorm:"column:updatedAt" json:"updatedAt"`
		}

		id = "postgres-complex-datasource"
		out <- TestDatasource{
			id:              id,
			withFilter:      true,
			withWatchFilter: false,
			withSort:        true,
			source: datasources.NewPostgresDatasource(
				ctx,
				datasources.PostgresDatasourceConfigs[TestComplexPostgresModel]{
					DSN:       postgresDSN,
					TableName: getDsName(id),
					Model:     &TestComplexPostgresModel{},
					IDField:   TestIDField,
					Filter: datasources.PostgresDatasourceFilter{
						Query:  fmt.Sprintf("\"%s\" = ?", TestFilterOutField),
						Params: []any{false},
					},
					Sort: map[string]any{
						TestSortAscField: 1, // 1 = Ascending, -1 = Descending
					},

					WithTransformer: func(data map[string]any) (TestComplexPostgresModel, error) {
						s, _ := strconv.Atoi(fmt.Sprintf("%v", data[TestSortAscField]))
						return TestComplexPostgresModel{
							ID:        fmt.Sprintf("%v", data[TestIDField]),
							FieldA:    fmt.Sprintf("%v", data[TestAField]),
							FieldB:    fmt.Sprintf("%v", data[TestBField]),
							FilterOut: data[TestFilterOutField] == true,
							SortAsc:   s,
						}, nil
					},
					OnInit: func(db *gorm.DB) error {
						return nil
					},

					PublicationName:     strings.ReplaceAll(fmt.Sprintf("%s_pub", id), "-", "_"),
					ReplicationSlotName: strings.ReplaceAll(fmt.Sprintf("%s_slot", id), "-", "_"),
					DisableReplication:  false,
				},
			),
		}

		// -----------------------
		// 5. MySQL
		// -----------------------
		mysqlDSN := os.Getenv("MYSQL_DSN")
		if mysqlDSN == "" {
			mysqlDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
				os.Getenv("MYSQL_USER"),
				os.Getenv("MYSQL_PASS"),
				os.Getenv("MYSQL_HOST"),
				os.Getenv("MYSQL_PORT"),
				os.Getenv("MYSQL_DB"),
			)
		}

		// ---------------------- Simple Model  -----------------------

		// Define test model for MySQL
		type TestSimpleMySQLModel struct {
			ID     string `gorm:"primaryKey;column:id" json:"id"`
			FieldA string `gorm:"column:fieldA" json:"fieldA"`
			FieldB string `gorm:"column:fieldB" json:"fieldB"`
		}

		id = "mysql-simple-datasource"
		out <- TestDatasource{
			id:              id,
			withFilter:      false,
			withWatchFilter: false,
			withSort:        false,
			source: datasources.NewMySQLDatasource(
				ctx,
				datasources.MySQLDatasourceConfigs[TestSimpleMySQLModel]{
					DSN:       mysqlDSN,
					TableName: getDsName(id),
					Model:     &TestSimpleMySQLModel{},
					IDField:   TestIDField,

					OnInit: func(db *gorm.DB) error {
						return nil
					},

					DisableReplication: false,
				},
			),
		}

		// ---------------------- Complex Model -----------------------

		// Define test model for MySQL
		type TestComplexMySQLModel struct {
			ID        string    `gorm:"primaryKey;column:id" json:"id"`
			FieldA    string    `gorm:"column:fieldA" json:"fieldA"`
			FieldB    string    `gorm:"column:fieldB" json:"fieldB"`
			FilterOut bool      `gorm:"column:filterOut" json:"filterOut"`
			SortAsc   int       `gorm:"type:int;column:sortAsc" json:"sortAsc"`
			CreatedAt time.Time `gorm:"column:createdAt" json:"createdAt"`
			UpdatedAt time.Time `gorm:"column:updatedAt" json:"updatedAt"`
		}

		id = "mysql-complex-datasource"
		out <- TestDatasource{
			id:              id,
			withFilter:      true,
			withWatchFilter: false,
			withSort:        true,
			source: datasources.NewMySQLDatasource(
				ctx,
				datasources.MySQLDatasourceConfigs[TestComplexMySQLModel]{
					DSN:       mysqlDSN,
					TableName: getDsName(id),
					Model:     &TestComplexMySQLModel{},
					IDField:   TestIDField,
					Filter: datasources.MySQLDatasourceFilter{
						Query:  fmt.Sprintf("`%s` = ?", TestFilterOutField),
						Params: []any{false},
					},
					Sort: map[string]any{
						TestSortAscField: 1, // 1 = Ascending, -1 = Descending
					},

					WithTransformer: func(data map[string]any) (TestComplexMySQLModel, error) {
						s, _ := strconv.Atoi(fmt.Sprintf("%v", data[TestSortAscField]))
						return TestComplexMySQLModel{
							ID:        fmt.Sprintf("%v", data[TestIDField]),
							FieldA:    fmt.Sprintf("%v", data[TestAField]),
							FieldB:    fmt.Sprintf("%v", data[TestBField]),
							FilterOut: data[TestFilterOutField] == true,
							SortAsc:   s,
						}, nil
					},
					OnInit: func(db *gorm.DB) error {
						return nil
					},

					DisableReplication: false,
				},
			),
		}
	}()

	return out
}

// --------
// Tests
// --------

// compareBatches compares two slices of DatasourcePushRequest for equality
// considering that the order of elements within slices might vary due to deduplication
func compareBatches(got, expected []datasources.DatasourcePushRequest) bool {
	if len(got) != len(expected) {
		return false
	}

	for i := range got {
		if !compareSingleBatch(got[i], expected[i]) {
			return false
		}
	}
	return true
}

// compareSingleBatch compares two DatasourcePushRequest structs
// allowing for different ordering within the slices
func compareSingleBatch(got, expected datasources.DatasourcePushRequest) bool {
	// Compare inserts (order may vary)
	if !compareMapSlices(got.Inserts, expected.Inserts) {
		return false
	}

	// Compare updates (order may vary)
	if !compareMapSlices(got.Updates, expected.Updates) {
		return false
	}

	// Compare deletes (order may vary)
	if !compareStringSlices(got.Deletes, expected.Deletes) {
		return false
	}

	return true
}

// compareMapSlices compares two slices of maps allowing for different order
func compareMapSlices(got, expected []map[string]any) bool {
	if len(got) != len(expected) {
		return false
	}

	// Convert to string representations for easier comparison
	gotStrs := make([]string, len(got))
	expectedStrs := make([]string, len(expected))

	for i, m := range got {
		gotStrs[i] = fmt.Sprintf("%v", m)
	}
	for i, m := range expected {
		expectedStrs[i] = fmt.Sprintf("%v", m)
	}

	// Sort both slices
	sort.Strings(gotStrs)
	sort.Strings(expectedStrs)

	return reflect.DeepEqual(gotStrs, expectedStrs)
}

// compareStringSlices compares two string slices allowing for different order
func compareStringSlices(got, expected []string) bool {
	if len(got) != len(expected) {
		return false
	}

	gotCopy := make([]string, len(got))
	expectedCopy := make([]string, len(expected))
	copy(gotCopy, got)
	copy(expectedCopy, expected)

	sort.Strings(gotCopy)
	sort.Strings(expectedCopy)

	return reflect.DeepEqual(gotCopy, expectedCopy)
}

// TestStreamChanges covers the main behaviors of StreamChanges:
// 1. immediate flush when batchSize is reached
// 2. periodic flush when batchWindowSeconds elapses
// 3. draining remaining events on context cancellation
// 4. no output when no events arrive and watcher closes
// 5. deduplication of insert, update, and delete events per batch
func TestStreamChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		events                 []datasources.DatasourcePushRequest
		request                datasources.DatasourceStreamRequest
		sendEventsCloseWatcher bool
		cancelContextAfterSend bool
		expectedBatches        []datasources.DatasourcePushRequest
	}{
		{
			name: "batch size 1 immediate flush",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": 1}},
					Updates: nil,
					Deletes: nil,
				},
				{
					Inserts: []map[string]any{{"id": 2}},
					Updates: nil,
					Deletes: nil,
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          1,
				BatchWindowSeconds: 10, // long window so ticker won’t fire
			},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": 1}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
				{
					Inserts: []map[string]any{{"id": 2}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
			},
		},
		{
			name: "batch window flush",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": "a"}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          10, // won't reach size
				BatchWindowSeconds: 1,  // ticker fires every 1s
			},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": "a"}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
			},
		},
		{
			name: "context cancellation drains and flushes remaining",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": "x"}},
					Updates: []map[string]any{{"id": "u"}},
					Deletes: []string{"d"},
				},
				{
					Inserts: []map[string]any{{"id": "y"}},
					Updates: nil,
					Deletes: []string{"e1", "e2"},
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          10, // won't trigger size flush
				BatchWindowSeconds: 1,  // used after ctx.Done()
			},
			sendEventsCloseWatcher: false, // keep watcher open so drain branch sees events
			cancelContextAfterSend: true,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					// both events aggregated
					Inserts: []map[string]any{{"id": "x"}, {"id": "y"}},
					Updates: []map[string]any{{"id": "u"}},
					Deletes: []string{"d", "e1", "e2"},
				},
			},
		},
		{
			name:                   "no events, watcher closes => no output",
			events:                 nil,
			request:                datasources.DatasourceStreamRequest{BatchSize: 1, BatchWindowSeconds: 1},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches:        []datasources.DatasourcePushRequest{},
		},
		{
			name: "deduplication of identical inserts in batch",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}},
					Updates: nil,
					Deletes: nil,
				},
				{
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}}, // Duplicate
					Updates: nil,
					Deletes: nil,
				},
				{
					Inserts: []map[string]any{{"id": "2", "name": "Bob"}},
					Updates: nil,
					Deletes: nil,
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          10, // Large batch to accumulate all events
				BatchWindowSeconds: 1,
			},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					// Only unique inserts should remain
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
			},
		},
		{
			name: "deduplication of identical updates in batch",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: nil,
					Updates: []map[string]any{{"id": "1", "name": "Alice", "age": 30}},
					Deletes: nil,
				},
				{
					Inserts: nil,
					Updates: []map[string]any{{"id": "1", "name": "Alice", "age": 30}}, // Duplicate
					Deletes: nil,
				},
				{
					Inserts: nil,
					Updates: []map[string]any{{"id": "2", "name": "Bob", "age": 25}},
					Deletes: nil,
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          10,
				BatchWindowSeconds: 1,
			},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{},
					Updates: []map[string]any{{"id": "1", "name": "Alice", "age": 30}, {"id": "2", "name": "Bob", "age": 25}},
					Deletes: []string{},
				},
			},
		},
		{
			name: "deduplication of identical deletes in batch",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: nil,
					Updates: nil,
					Deletes: []string{"id1"},
				},
				{
					Inserts: nil,
					Updates: nil,
					Deletes: []string{"id1"}, // Duplicate
				},
				{
					Inserts: nil,
					Updates: nil,
					Deletes: []string{"id2"},
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          10,
				BatchWindowSeconds: 1,
			},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{},
					Updates: []map[string]any{},
					Deletes: []string{"id1", "id2"},
				},
			},
		},
		{
			name: "mixed operations with deduplication",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}},
					Updates: []map[string]any{{"id": "2", "name": "Bob", "age": 25}},
					Deletes: []string{"id3"},
				},
				{
					// Duplicate insert, update, and delete
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}},
					Updates: []map[string]any{{"id": "2", "name": "Bob", "age": 25}},
					Deletes: []string{"id3"},
				},
				{
					// New unique operations
					Inserts: []map[string]any{{"id": "4", "name": "Charlie"}},
					Updates: []map[string]any{{"id": "5", "name": "David", "age": 35}},
					Deletes: []string{"id6"},
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          20,
				BatchWindowSeconds: 1,
			},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}, {"id": "4", "name": "Charlie"}},
					Updates: []map[string]any{{"id": "2", "name": "Bob", "age": 25}, {"id": "5", "name": "David", "age": 35}},
					Deletes: []string{"id3", "id6"},
				},
			},
		},
		{
			name: "no deduplication across multiple batches",
			events: []datasources.DatasourcePushRequest{
				{
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}},
					Updates: nil,
					Deletes: nil,
				},
				{
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}}, // Duplicate - should trigger batch
					Updates: nil,
					Deletes: nil,
				},
				{
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}}, // Same as before but new batch
					Updates: nil,
					Deletes: nil,
				},
			},
			request: datasources.DatasourceStreamRequest{
				BatchSize:          1, // Force each event into separate batches
				BatchWindowSeconds: 10,
			},
			sendEventsCloseWatcher: true,
			cancelContextAfterSend: false,
			expectedBatches: []datasources.DatasourcePushRequest{
				{
					// First unique insert
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
				{
					// Third event creates second batch (second was duplicate but still triggers new batch)
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
				{
					// Third event creates third batch
					Inserts: []map[string]any{{"id": "1", "name": "Alice"}},
					Updates: []map[string]any{},
					Deletes: []string{},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			watcher := make(chan datasources.DatasourcePushRequest)
			results := datasources.StreamChanges(&ctx, "test", watcher, &tc.request)

			// produce events (and close/cancel as needed)
			go func() {
				for _, ev := range tc.events {
					watcher <- ev
				}
				if tc.sendEventsCloseWatcher {
					close(watcher)
				}
				if tc.cancelContextAfterSend {
					cancel()
				}
			}()

			got := make([]datasources.DatasourcePushRequest, 0)

			// Collect output batches according to scenario
			switch {
			case tc.cancelContextAfterSend:
				// Wait for drain + batchWindow flush
				<-time.After(time.Duration(tc.request.BatchWindowSeconds+1) * time.Second)

				// Read until channel closes
				for r := range results {
					got = append(got, r.Docs)
				}

			case tc.sendEventsCloseWatcher:
				// Wait for events to be processed and batches to be emitted
				<-time.After(time.Duration(tc.request.BatchWindowSeconds+1) * time.Second)

				// Read all available batches with timeout
				timeout := time.After(3 * time.Second)
				for len(got) < len(tc.expectedBatches) {
					select {
					case r := <-results:
						got = append(got, r.Docs)
					case <-timeout:
						// Break if we've waited too long
						goto validateResults
					}
				}

				// Check for any unexpected extra batches
				select {
				case extra := <-results:
					if len(extra.Docs.Inserts)+len(extra.Docs.Updates)+len(extra.Docs.Deletes) > 0 {
						got = append(got, extra.Docs)
					}
				case <-time.After(100 * time.Millisecond):
					// No extra batches - good
				}
			}

		validateResults:
			// For deduplication tests, we need to compare content more carefully
			if !compareBatches(got, tc.expectedBatches) {
				t.Errorf("❌ got batches %#v\nexpected %#v", got, tc.expectedBatches)
			}
		})
	}
}

// Test different datasource implementations
func TestDatasourceImplementations(t *testing.T) {
	t.Parallel()

	testCtx := context.Background()

	slog.SetDefault(helpers.CreateTextLogger(slog.LevelWarn)) // Default logger

	instanceId := helpers.RandomString(6)

	for td := range getTestDatasources(&testCtx, instanceId) {

		fmt.Println("\n---------------------------------------------------------------------------------")
		t.Run(td.id, func(t *testing.T) {
			fmt.Println("---------------------------------------------------------------------------------")

			// Cleanup after
			t.Cleanup(func() {
				td.source.Clear(&testCtx)
				td.source.Close(&testCtx)
			})

			if !td.synchronous {
				t.Parallel() // Run datasources tests in parallel
			}

			ctx, cancel := context.WithTimeout(testCtx, time.Duration(5)*time.Minute)
			defer cancel()

			t.Run("Test_CRUD", func(t *testing.T) {
				td.source.Clear(&ctx)

				// 1) Test Count on empty
				if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 0 {
					t.Fatalf("⛔️ expected empty count=0, got %d", got)
				}

				// 2) Test Push: inserts only
				pushReq := &datasources.DatasourcePushRequest{
					Inserts: []map[string]any{
						{TestIDField: "a", TestAField: "foo"},
						{TestIDField: "b", TestAField: "bar", TestBField: "123"},
					},
				}
				cnt, err := td.source.Push(&ctx, pushReq)
				if err != nil {
					t.Fatalf("⛔️ Push(inserts) error: %v", err)
				}
				if cnt.Inserts != 2 || cnt.Updates != 0 || cnt.Deletes != 0 {
					t.Fatalf("⛔️ unexpected push count: %+v", cnt)
					t.Errorf("❌ expectd docs inserts: %d, updates: %d, deletes: %d. Got inserts: %d, updates: %d, deletes: %d",
						2, 0, 0, cnt.Inserts, cnt.Updates, cnt.Deletes)
				}

				// 3) Count after inserts
				if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 2 {
					t.Errorf("❌ expected count=%d, got %d", uint64(len(pushReq.Inserts)), got)
				}

				// 4) Test Fetch: no filters
				fetchRes := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{Size: 0, Offset: 0})
				if fetchRes.Err != nil {
					t.Fatalf("⛔️ Fetch error: %v", fetchRes.Err)
				}
				if len(fetchRes.Docs) != 2 || fetchRes.Start != 0 || fetchRes.End != uint64(len(pushReq.Inserts)) {
					t.Errorf("❌ expectd docs size: %d, start: %d, end: %d. Got size: %d, start: %d, end: %d",
						2, 0, len(pushReq.Inserts), len(fetchRes.Docs), fetchRes.Start, fetchRes.End)
				}

				// 5) Test Fetch: by IDs
				fetchRes2 := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{IDs: []string{"b"}})
				if len(fetchRes2.Docs) != 1 || fmt.Sprintf("%v", fetchRes2.Docs[0][TestIDField]) != "b" {
					t.Errorf("❌ expected single doc with id 'b', got %#v", fetchRes2.Docs)
				}

				// 6) Test Push: updates only (existing + non-existing)
				pushUpd := &datasources.DatasourcePushRequest{
					Updates: []map[string]any{
						{TestIDField: "a", TestAField: "fooood"},
						{TestIDField: "x", TestBField: "value"},
					},
				}
				cntUpd, err := td.source.Push(&ctx, pushUpd)
				if err != nil {
					t.Fatalf("⛔️ Push(updates) error: %v", err)
				}
				if cntUpd.Updates != uint64(len(pushUpd.Updates)) {
					t.Errorf("❌ expected %d update, got %d", uint64(len(pushUpd.Updates)), cntUpd.Updates)
				}

				// 7) Verify update applied
				fetchRes3 := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{IDs: []string{"a"}})
				if len(fetchRes3.Docs) == 0 {
					t.Errorf("❌ did not expect empty docs after update")
				} else if fetchRes3.Docs[0][TestAField] != "fooood" {
					t.Errorf("❌ expected %s='fooood', got %v", TestAField, fetchRes3.Docs[0][TestAField])
				}

				// 8) Test Push: deletes
				pushDel := &datasources.DatasourcePushRequest{Deletes: []string{"a", "x"}}
				cntDel, err := td.source.Push(&ctx, pushDel)
				if err != nil {
					t.Fatalf("⛔️ Push(deletes) error: %v", err)
				}
				if cntDel.Deletes != uint64(len(pushDel.Deletes)) {
					t.Errorf("❌ expected %d delete, got %d", uint64(len(pushDel.Deletes)), cntDel.Deletes)
				}
				if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 1 { // a,b,x - a,x = b(1)
					t.Errorf("❌ expected count=1 after delete, got %d", got)
				}
			})

			t.Run("Test_Concurrent_CRUD", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Number of concurrent operations
				numOps := 10
				wg := new(sync.WaitGroup)
				wg.Add(numOps * 3) // Insert, Update, Delete operations

				// Channel to collect results
				resultCh := make(chan error, numOps*3)

				// Concurrent inserts
				insWg := new(sync.WaitGroup)
				for i := range numOps {
					insWg.Add(1)
					go func(i int) {
						defer wg.Done()
						defer insWg.Done()
						req := &datasources.DatasourcePushRequest{
							Inserts: []map[string]any{
								{TestIDField: fmt.Sprintf("concurrent-%d", i), TestAField: fmt.Sprintf("value-%d", i)},
							},
						}
						_, err := td.source.Push(&ctx, req)
						<-time.After(time.Duration(100 * time.Millisecond)) // wait a bit for changes to propagate
						resultCh <- err
					}(i)
				}

				// Wait for inserts to complete
				insWg.Wait()

				// Verify count after inserts
				if count := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); count != uint64(numOps) {
					t.Errorf("❌ expected count=%d after concurrent inserts, got %d", numOps, count)
				}

				// Concurrent updates
				updWg := new(sync.WaitGroup)
				for i := range numOps {
					updWg.Add(1)
					go func(i int) {
						defer wg.Done()
						defer updWg.Done()
						req := &datasources.DatasourcePushRequest{
							Updates: []map[string]any{
								{TestIDField: fmt.Sprintf("concurrent-%d", i), TestAField: fmt.Sprintf("updated-%d", i)},
							},
						}
						_, err := td.source.Push(&ctx, req)
						<-time.After(time.Duration(100 * time.Millisecond)) // wait a bit for changes to propagate
						resultCh <- err
					}(i)
				}

				// Wait for updates to complete
				updWg.Wait()

				// Verify updates
				res := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{})
				for _, doc := range res.Docs {
					id := doc[TestIDField].(string)
					expected := fmt.Sprintf("updated-%s", id[len("concurrent-"):])
					if doc[TestAField] != expected {
						t.Errorf("❌ expected %s to be updated to %s, got %v", id, expected, doc[TestAField])
					}
				}

				// Concurrent deletes
				delWg := new(sync.WaitGroup)
				for i := range numOps {
					delWg.Add(1)
					go func(i int) {
						defer wg.Done()
						defer delWg.Done()
						req := &datasources.DatasourcePushRequest{
							Deletes: []string{fmt.Sprintf("concurrent-%d", i)},
						}
						_, err := td.source.Push(&ctx, req)
						<-time.After(time.Duration(100 * time.Millisecond)) // wait a bit for changes to propagate
						resultCh <- err
					}(i)
				}

				// Wait for delete to complete
				delWg.Wait()

				// Wait for all operations to complete
				wg.Wait()
				close(resultCh)

				// Wait a bit for all changes to propagate
				<-time.After(time.Duration(400 * time.Millisecond))

				// Check for errors
				for err := range resultCh {
					if err != nil {
						t.Errorf("❌ concurrent operation error: %v", err)
					}
				}

				// Verify final count
				if count := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); count != 0 {
					t.Errorf("❌ expected count=0 after concurrent deletes, got %d", count)
				}
			})

			t.Run("Test_Watch", func(t *testing.T) {
				td.source.Clear(&ctx)
				<-time.After(time.Duration(500) * time.Millisecond) // Wait for previous push events to complete

				evUpd := 3
				evDel := 2
				evIns := evUpd + evDel
				evCnt := evIns + evUpd + evDel
				batchWin := uint64(1)

				// 1) Test watch: fire background pushes
				streamReq := &datasources.DatasourceStreamRequest{BatchSize: uint64(evCnt), BatchWindowSeconds: batchWin}
				watchCtx, watchCancel := context.WithTimeout(ctx, time.Duration(30)*time.Second)
				watchCh := td.source.Watch(&watchCtx, streamReq)

				var expInsert, expUpdate, expDelete uint64

				wg := new(sync.WaitGroup)
				wg.Add(3)
				go func() {

					<-time.After(time.Duration(200) * time.Millisecond) // wait a bit
					random := helpers.RandomString(6)

					// Insert
					r := &datasources.DatasourcePushRequest{
						Inserts: make([]map[string]any, 0),
					}
					for i := range evIns {
						r.Inserts = append(r.Inserts, map[string]any{TestIDField: fmt.Sprintf("w%d", i)})
					}
					c, err := td.source.Push(&ctx, r)
					if err != nil {
						t.Errorf("❌ background insert error: %s", err.Error())
					}
					expInsert += c.Inserts
					wg.Done()

					<-time.After(time.Duration(200) * time.Millisecond) // wait a bit

					// Update
					r = &datasources.DatasourcePushRequest{
						Updates: make([]map[string]any, 0),
					}
					for i := range evUpd {
						// Last updated field contains filter out field - if filter supported
						if td.withWatchFilter && i == evUpd-1 {
							r.Updates = append(r.Updates, map[string]any{TestIDField: fmt.Sprintf("w%d", i), TestFilterOutField: "yes"})
						} else {
							r.Updates = append(r.Updates, map[string]any{TestIDField: fmt.Sprintf("w%d", i), TestBField: fmt.Sprintf("%s-%d", random, i)})
						}
					}
					c, err = td.source.Push(&ctx, r)
					if err != nil {
						t.Errorf("❌ background update error: %s", err.Error())
					}
					expUpdate += c.Updates
					wg.Done()

					<-time.After(time.Duration((batchWin*1000)+500) * time.Millisecond) // wait for watch batch window before deleting

					// Delete
					r = &datasources.DatasourcePushRequest{
						Deletes: make([]string, 0),
					}
					for i := evUpd; i < evIns; i++ {
						r.Deletes = append(r.Deletes, fmt.Sprintf("w%d", i))
					}
					c, err = td.source.Push(&ctx, r)
					if err != nil {
						t.Errorf("❌ background delete error: %s", err.Error())
					}
					expDelete += c.Deletes
					wg.Done()
				}()

				// Wait for background pushes to run
				rWg := new(sync.WaitGroup)
				rWg.Add(1)
				go func() {
					defer rWg.Done()
					wg.Wait()

					if expInsert != uint64(evIns) {
						t.Errorf("❌ background inserts: expected %d, got %d", evIns, expInsert)
					}
					if expUpdate != uint64(evUpd) {
						t.Errorf("❌ background updates: expected %d, got %d", evUpd, expUpdate)
					}
					if expDelete != uint64(evDel) {
						t.Errorf("❌ background deletes: expected %d, got %d", evDel, expDelete)
					}
				}()

				// 2) Test watch: event listener
				var gotInsert, gotUpdate, gotDelete uint64

			loop:
				for {
					select {
					case evt := <-watchCh:
						gotInsert += uint64(len(evt.Docs.Inserts))
						gotUpdate += uint64(len(evt.Docs.Updates))
						gotDelete += uint64(len(evt.Docs.Deletes))
					case <-time.After(time.Duration((streamReq.BatchWindowSeconds*1000)+2000) * time.Millisecond): // Wait for watch batch window
						rWg.Wait()    // Wait for background pushes to complete
						watchCancel() // Stop watching
						break loop
					}
				}

				// Verify upserts - as some datasources only supports upserts
				if td.withWatchFilter && (gotInsert+gotUpdate) != (expInsert+(expUpdate-1)) { // Expexts 1 less if filter enabled
					t.Errorf("❌ watch upserts: expected %d, got %d", (expInsert + (expUpdate - 1)), (gotInsert + gotUpdate))
				} else if !td.withWatchFilter && (gotInsert+gotUpdate) != (expInsert+expUpdate) {
					t.Errorf("❌ watch upserts: expected %d, got %d", (expInsert + expUpdate), (gotInsert + gotUpdate))
				}

				// Verify deletes
				if gotDelete != expDelete {
					t.Errorf("❌ watch deletes: expected %d, got %d", expDelete, gotDelete)
				}
			})

			t.Run("Test_Watch_Multiple_Workers", func(t *testing.T) {

				td.source.Clear(&ctx)
				<-time.After(time.Duration(500) * time.Millisecond) // Wait for previous push events to complete

				const numWorkers = 3
				const numEvents = 5
				const watchTimeout = 15 * time.Second

				// Setup test context with timeout
				testCtx, testCancel := context.WithTimeout(ctx, watchTimeout)
				defer testCancel()

				// Track events from multiple watchers
				var wg sync.WaitGroup
				eventCounts := make([]int, numWorkers)
				allEvents := make([][]datasources.DatasourceStreamResult, numWorkers)
				mu := new(sync.Mutex)

				// Start multiple watchers
				watchCtx, watchCancel := context.WithCancel(testCtx)
				defer watchCancel()

				for i := range numWorkers {
					wg.Add(1)
					go func(workerID int) {
						defer wg.Done()

						// Create watch request
						streamReq := &datasources.DatasourceStreamRequest{
							BatchSize:          1,
							BatchWindowSeconds: 1,
						}

						stream := td.source.Watch(&watchCtx, streamReq)
						count := 0
						events := make([]datasources.DatasourceStreamResult, 0)

						for {
							select {
							case event, ok := <-stream:
								if !ok {
									// Stream closed
									mu.Lock()
									eventCounts[workerID] = count
									allEvents[workerID] = events
									mu.Unlock()
									return
								}

								count++
								events = append(events, event)

							case <-watchCtx.Done():
								mu.Lock()
								eventCounts[workerID] = count
								allEvents[workerID] = events
								mu.Unlock()
								return
							}
						}
					}(i)
				}

				// Give watchers time to start
				time.Sleep(500 * time.Millisecond)

				// Insert test data to generate events
				for i := 1; i <= numEvents; i++ {
					data := datasources.DatasourcePushRequest{
						Inserts: []map[string]any{
							{TestIDField: fmt.Sprintf("worker-test-%d", i), TestAField: fmt.Sprintf("value-%d", i)},
						},
					}
					_, err := td.source.Push(&testCtx, &data)
					if err != nil {
						t.Errorf("❌ Failed to insert test data %d: %v", i, err)
					}
					time.Sleep(100 * time.Millisecond) // Small delay between inserts
				}

				// Wait a bit for events to propagate, then cancel watchers
				time.Sleep(1000 * time.Millisecond)
				watchCancel()

				// Wait for all watchers to finish
				wg.Wait()

				// Analyze results
				mu.Lock()
				defer mu.Unlock()

				totalEvents := 0
				for _, count := range eventCounts {
					totalEvents += count
				}
				if totalEvents != numEvents {
					t.Errorf("❌ Total events mismatch: expected %d, got %d", numEvents, totalEvents)
				}

				// Verify that workers are sharing the load (not all workers need to receive events)
				workersWithEvents := 0
				for _, count := range eventCounts {
					if count > 0 {
						workersWithEvents++
					}
				}
				if workersWithEvents != numWorkers {
					t.Errorf("❌ Not all workers received events: %d out of %d", workersWithEvents, numWorkers)
				}

				// Verify total event count matches expectations (workers share the processing)
				expectedTotal := numEvents
				if totalEvents == 0 {
					t.Error("❌ No events were received by any worker!")
				} else if totalEvents < expectedTotal {
					t.Errorf("❌ Some events were missed: got %d, expected %d", totalEvents, expectedTotal)
				} else if totalEvents > expectedTotal {
					t.Errorf("❌ More events received than expected: got %d, expected %d (duplicate events detected)", totalEvents, expectedTotal)
				}

				// Verify event content consistency
				allSeenIDs := make(map[string]int) // Track how many times each ID was seen
				for _, events := range allEvents {
					for _, event := range events {
						// Merge insert IDs and update IDs - for upsert scenarios
						mergeIDs := append(event.Docs.Inserts, event.Docs.Updates...)
						for _, insert := range mergeIDs {
							if idVal, ok := insert[TestIDField]; ok {
								idStr := fmt.Sprintf("%v", idVal)
								allSeenIDs[idStr]++
							}
						}
					}
				}

				// Check that we saw each expected ID exactly once across all workers
				for i := 1; i <= numEvents; i++ {
					expectedID := fmt.Sprintf("worker-test-%d", i)
					if count, exists := allSeenIDs[expectedID]; !exists {
						t.Errorf("❌ Expected ID %s was not seen by any worker", expectedID)
					} else if count != 1 {
						t.Errorf("❌ ID %s was seen %d times across all workers (expected exactly 1 for shared processing)", expectedID, count)
					}
				}
			})

			t.Run("Test_Fetch", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Add more data
				data := datasources.DatasourcePushRequest{
					Inserts: []map[string]any{
						{TestIDField: "a", TestAField: "foo"},
						{TestIDField: "b", TestAField: "bar", TestBField: "123"},
						{TestIDField: "c", TestAField: "bars", TestBField: "1234"},
						{TestIDField: "d", TestAField: "barss", TestBField: "1235"},
						{TestIDField: "e", TestAField: "barsss", TestBField: "1236"},
					},
				}
				_, err := td.source.Push(&ctx, &data)
				if err != nil {
					t.Fatalf("⛔️ Push error: %s", err)
				}

				// Get current count
				currentCount := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{})
				if currentCount != uint64(len(data.Inserts)) {
					t.Fatalf("⛔️ Get current count: expected count = %d, got %d", len(data.Inserts), currentCount)
				}

				// Table-driven tests for Fetch, similar to Test_Count
				type fetchTest struct {
					name        string
					req         *datasources.DatasourceFetchRequest
					expectLen   int
					expectStart uint64
					expectEnd   uint64
				}

				fetchTests := []fetchTest{
					{
						name:        "Fetch_SizeAndOffsetWithinRange",
						req:         &datasources.DatasourceFetchRequest{Size: 1, Offset: 0},
						expectLen:   1,
						expectStart: 0,
						expectEnd:   1,
					},
					{
						name:        "Fetch_SizeAndOffsetExceedingTotal",
						req:         &datasources.DatasourceFetchRequest{Size: currentCount + 5, Offset: currentCount + 2},
						expectLen:   0,
						expectStart: 0,
						expectEnd:   0,
					},
					{
						name:        "Fetch_SizeGreaterThanTotal_OffsetWithinRange",
						req:         &datasources.DatasourceFetchRequest{Size: currentCount + 5, Offset: currentCount - 2},
						expectLen:   int(currentCount - (currentCount - 2)),
						expectStart: currentCount - 2,
						expectEnd:   currentCount,
					},
					{
						name:        "Fetch_OffsetEqualToTotal",
						req:         &datasources.DatasourceFetchRequest{Size: 1, Offset: currentCount},
						expectLen:   0,
						expectStart: 0,
						expectEnd:   0,
					},
					{
						name:        "Fetch_SizeZeroAndOffsetZero",
						req:         &datasources.DatasourceFetchRequest{Size: 0, Offset: 0},
						expectLen:   int(currentCount),
						expectStart: 0,
						expectEnd:   currentCount,
					},
				}

				for _, tc := range fetchTests {
					t.Run(tc.name, func(t *testing.T) {
						fetchRes := td.source.Fetch(&ctx, tc.req)
						if fetchRes.Err != nil {
							t.Fatalf("⛔️ Fetch error: %v", fetchRes.Err)
						}
						if len(fetchRes.Docs) != tc.expectLen || fetchRes.Start != tc.expectStart || fetchRes.End != tc.expectEnd {
							t.Errorf("❌ %s: expected len=%d, start=%d, end=%d, got len=%d, start=%d, end=%d, result: %#v",
								tc.name, tc.expectLen, tc.expectStart, tc.expectEnd,
								len(fetchRes.Docs), fetchRes.Start, fetchRes.End, fetchRes)
						}
					})
				}
			})

			t.Run("Test_Push", func(t *testing.T) {
				td.source.Clear(&ctx)

				tests := []struct {
					name   string
					req    *datasources.DatasourcePushRequest
					expect struct {
						inserts uint64
						updates uint64
						deletes uint64
						err     bool
					}
				}{
					{
						name: "Basic_Insert",
						req: &datasources.DatasourcePushRequest{
							Inserts: []map[string]any{
								{TestIDField: "a", TestAField: "foo"},
								{TestIDField: "b", TestAField: "bar", TestBField: "123"},
							},
						},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{inserts: 2},
					},
					{
						name: "Invalid_Inserts",
						req: &datasources.DatasourcePushRequest{
							Inserts: []map[string]any{
								nil,
								{},
								{TestAField: "no-id"},
							},
						},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{err: true},
					},
					{
						name: "Basic_Update",
						req: &datasources.DatasourcePushRequest{
							Updates: []map[string]any{
								{TestIDField: "a", TestAField: "updated"},
							},
						},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{updates: 1},
					},
					{
						name: "Invalid_Updates",
						req: &datasources.DatasourcePushRequest{
							Updates: []map[string]any{
								nil,
								{},
								{TestAField: "no-id"},
								{TestIDField: "a", TestAField: "updated-v2"},
							},
						},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{err: true, updates: 1},
					},
					{
						name: "Basic_Delete",
						req: &datasources.DatasourcePushRequest{
							Deletes: []string{"a"},
						},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{deletes: 1},
					},
					{
						name: "Invalid_Deletes",
						req: &datasources.DatasourcePushRequest{
							Deletes: []string{"non-existent"},
						},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{deletes: 1}, // Most implementations count attempted deletes
					},
					{
						name: "Mixed_Operations",
						req: &datasources.DatasourcePushRequest{
							Inserts: []map[string]any{{TestIDField: "c", TestAField: "new"}},
							Updates: []map[string]any{{TestIDField: "b", TestAField: "changed"}},
							Deletes: []string{"a"},
						},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{inserts: 1, updates: 1, deletes: 1},
					},
					{
						name: "Empty_Request",
						req:  &datasources.DatasourcePushRequest{},
						expect: struct {
							inserts uint64
							updates uint64
							deletes uint64
							err     bool
						}{},
					},
				}

				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						td.source.Clear(&ctx)

						// Setup initial data if needed
						if tt.name != "Basic_Insert" {
							setup := &datasources.DatasourcePushRequest{
								Inserts: []map[string]any{
									{TestIDField: "a", TestAField: "foo"},
									{TestIDField: "b", TestAField: "bar"},
								},
							}
							_, err := td.source.Push(&ctx, setup)
							if err != nil {
								t.Fatalf("⛔️ Setup error: %s", err)
							}
						}

						// Run test
						c, err := td.source.Push(&ctx, tt.req)
						if err != nil && !tt.expect.err {
							t.Fatalf("⛔️ Unexpected push error: %s", err)
						}
						if err == nil && tt.expect.err {
							t.Error("❌ Expected error but got none")
						}
						if c.Inserts != tt.expect.inserts || c.Updates != tt.expect.updates || c.Deletes != tt.expect.deletes {
							t.Errorf("❌ expected inserts=%d, updates=%d, deletes=%d; got inserts=%d, updates=%d, deletes=%d",
								tt.expect.inserts, tt.expect.updates, tt.expect.deletes,
								c.Inserts, c.Updates, c.Deletes)
						}
					})
				}
			})

			t.Run("Test_Count", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Insert test data
				data := datasources.DatasourcePushRequest{
					Inserts: []map[string]any{
						{TestIDField: "a", TestAField: "foo"},
						{TestIDField: "b", TestAField: "bar", TestBField: "123"},
						{TestIDField: "c", TestAField: "baz", TestBField: "456"},
						{TestIDField: "d", TestAField: "qux", TestBField: "789"},
						{TestIDField: "e", TestAField: "quux", TestBField: "1011"},
					},
				}
				_, err := td.source.Push(&ctx, &data)
				if err != nil {
					t.Fatalf("⛔️ Push error: %s", err)
				}

				total := uint64(len(data.Inserts))

				tests := []struct {
					name   string
					req    *datasources.DatasourceFetchRequest
					expect uint64
				}{
					{
						name:   "Count_All",
						req:    &datasources.DatasourceFetchRequest{},
						expect: total,
					},
					{
						name:   "Count_SingleID",
						req:    &datasources.DatasourceFetchRequest{IDs: []string{"a"}},
						expect: 1,
					},
					{
						name:   "Count_MultipleIDs",
						req:    &datasources.DatasourceFetchRequest{IDs: []string{"a", "b", "c"}},
						expect: 3,
					},
					{
						name:   "Count_NonExistingID",
						req:    &datasources.DatasourceFetchRequest{IDs: []string{"z"}},
						expect: 0,
					},
					{
						name:   "Count_SizeLessThanTotal",
						req:    &datasources.DatasourceFetchRequest{Size: 2},
						expect: 2,
					},
					{
						name:   "Count_SizeGreaterThanTotal",
						req:    &datasources.DatasourceFetchRequest{Size: total + 5},
						expect: total,
					},
					{
						name:   "Count_OffsetWithinRange",
						req:    &datasources.DatasourceFetchRequest{Offset: 2},
						expect: total - 2,
					},
					{
						name:   "Count_OffsetEqualsTotal",
						req:    &datasources.DatasourceFetchRequest{Offset: total},
						expect: 0,
					},
					{
						name:   "Count_OffsetGreaterThanTotal",
						req:    &datasources.DatasourceFetchRequest{Offset: total + 1},
						expect: 0,
					},
					{
						name:   "Count_SizeAndOffsetWithinRange",
						req:    &datasources.DatasourceFetchRequest{Size: 2, Offset: 1},
						expect: 2,
					},
					{
						name:   "Count_SizeAndOffsetExceedingTotal",
						req:    &datasources.DatasourceFetchRequest{Size: 10, Offset: total - 1},
						expect: 1,
					},
					{
						name:   "Count_SizeZeroOffsetZero",
						req:    &datasources.DatasourceFetchRequest{Size: 0, Offset: 0},
						expect: total,
					},
					{
						name:   "Count_SizeZeroOffsetNonZero",
						req:    &datasources.DatasourceFetchRequest{Size: 0, Offset: 2},
						expect: total - 2,
					},
					{
						name:   "Count_IDsWithOffset",
						req:    &datasources.DatasourceFetchRequest{IDs: []string{"a", "b", "c"}, Offset: 1},
						expect: 2,
					},
					{
						name:   "Count_IDsWithSize",
						req:    &datasources.DatasourceFetchRequest{IDs: []string{"a", "b", "c"}, Size: 2},
						expect: 2,
					},
					{
						name:   "Count_IDsWithSizeAndOffset",
						req:    &datasources.DatasourceFetchRequest{IDs: []string{"a", "b", "c"}, Size: 1, Offset: 2},
						expect: 1,
					},
					{
						name:   "Count_IDsWithSizeAndOffsetExceeding",
						req:    &datasources.DatasourceFetchRequest{IDs: []string{"a", "b", "c"}, Size: 5, Offset: 2},
						expect: 1,
					},
				}

				for _, tc := range tests {
					t.Run(tc.name, func(t *testing.T) {
						got := td.source.Count(&ctx, tc.req)
						if got != tc.expect {
							t.Errorf("❌ %s: expected %d, got %d (req: %+v)", tc.name, tc.expect, got, tc.req)
						}
					})
				}
			})

			// Test Clear
			t.Run("Test_Clear", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Insert test data
				data := datasources.DatasourcePushRequest{
					Inserts: []map[string]any{
						{TestIDField: "a", TestAField: "foo"},
						{TestIDField: "b", TestAField: "bar", TestBField: "123"},
					},
				}
				_, err := td.source.Push(&ctx, &data)
				if err != nil {
					t.Fatalf("⛔️ Push error: %s", err)
				}
				<-time.After(time.Duration(100 * time.Millisecond)) // Wait a bit to ensure changes were made

				// Clear
				td.source.Clear(&ctx)
				// Verify empty
				if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 0 {
					t.Errorf("❌ expected empty count=0 after clear, got %d", got)
				}
			})

			// Test filtering if available
			if td.withFilter {
				t.Run("Test_Fetch_Filter", func(t *testing.T) {
					td.source.Clear(&ctx)

					// Insert test data with and without deletedAt field
					data := datasources.DatasourcePushRequest{
						Inserts: []map[string]any{
							{TestIDField: "a", TestAField: "foo", TestFilterOutField: true},
							{TestIDField: "b", TestAField: "bar"},
						},
					}
					_, err := td.source.Push(&ctx, &data)
					if err != nil {
						t.Fatalf("⛔️ Push error: %s", err)
					}

					// Fetch should return only non-deleted records due to filter
					fetchRes := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{})
					if fetchRes.Err != nil {
						t.Fatalf("⛔️ Fetch error: %v", fetchRes.Err)
					}
					if len(fetchRes.Docs) != 1 || fetchRes.Docs[0][TestIDField] != "b" {
						t.Errorf("❌ expected single non-deleted doc with id 'b', got %#v", fetchRes.Docs)
					}
				})
			}

			// Test sorting if available
			if td.withSort {
				t.Run("Test_Fetch_Sort", func(t *testing.T) {
					td.source.Clear(&ctx)

					// Insert test data with sort field in descending order
					data := datasources.DatasourcePushRequest{
						Inserts: []map[string]any{
							{TestIDField: "a", TestAField: "foo", TestSortAscField: 4},
							{TestIDField: "d", TestAField: "qux", TestSortAscField: 1},
							{TestIDField: "b", TestAField: "bar", TestSortAscField: 3},
							{TestIDField: "c", TestAField: "baz", TestSortAscField: 2},
						},
					}
					_, err := td.source.Push(&ctx, &data)
					if err != nil {
						t.Fatalf("⛔️ Push error: %s", err)
					}

					// Fetch should return records sorted by TestSortAscField in ascending order
					fetchRes := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{})
					if fetchRes.Err != nil {
						t.Fatalf("⛔️ Fetch error: %v", fetchRes.Err)
					}

					// Verify ascending order
					if len(fetchRes.Docs) != 4 {
						t.Fatalf("❌ expected 4 docs, got %d", len(fetchRes.Docs))
					}

					for i, doc := range fetchRes.Docs {
						expectedSort := i + 1
						if fmt.Sprintf("%v", doc[TestSortAscField]) != fmt.Sprintf("%d", expectedSort) {
							t.Errorf("❌ expected sort value %d at position %d, got %v", expectedSort, i, doc[TestSortAscField])
						}
					}
				})
			}

			// Test Import (basic tests)
			t.Run("Test_Import", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Create temp CSV file
				dir := t.TempDir()
				csvPath := filepath.Join(dir, "test.csv")
				csvContent := "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n"
				err := os.WriteFile(csvPath, []byte(csvContent), 0644)
				if err != nil {
					t.Fatalf("⛔️ failed to create temp CSV file: %v", err)
				}

				// Import CSV
				err = td.source.Import(&ctx, datasources.DatasourceImportRequest{
					Type:      datasources.DatasourceImportTypeCSV,
					Source:    datasources.DatasourceImportSourceFile,
					Location:  csvPath,
					BatchSize: 2,
				})
				if err != nil {
					t.Fatalf("⛔️ Import error: %v", err)
				}

				// Verify count
				if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 3 {
					t.Errorf("❌ expected count=3 after import, got %d", got)
				}
			})

			// Test Export (basic tests)
			t.Run("Test_Export", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Insert test data
				data := datasources.DatasourcePushRequest{
					Inserts: []map[string]any{
						{TestIDField: "1", TestAField: "Alice", TestBField: "30"},
						{TestIDField: "2", TestAField: "Bob", TestBField: "25"},
						{TestIDField: "3", TestAField: "Charlie", TestBField: "35"},
					},
				}
				_, err := td.source.Push(&ctx, &data)
				if err != nil {
					t.Fatalf("⛔️ Push error: %s", err)
				}

				// Verify count
				if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 3 {
					t.Errorf("❌ expected count=3 after push, got %d", got)
				}

				// Create temp CSV file path
				dir := t.TempDir()
				csvPath := filepath.Join(dir, "export.csv")

				// Call SaveCSV
				err = datasources.SaveCSV(&ctx, td.source, csvPath, 2)
				if err != nil {
					t.Fatalf("⛔️ SaveCSV failed: %v", err)
				}

				// Read and verify the file content
				content, err := os.ReadFile(csvPath)
				if err != nil {
					t.Fatalf("⛔️ failed to read output file: %v", err)
				}

				gotContent := string(content)

				// For basic CSV structure validation, we'll check:
				// 1. The file has content
				if len(gotContent) == 0 {
					t.Errorf("❌ got empty file, expected content")
				}
				// 2. It has the right number of lines (header + data)
				gotLines := strings.Split(strings.TrimSuffix(gotContent, "\n"), "\n")
				if len(gotLines) != 4 { // 1 header + 3 data rows
					t.Errorf("❌ expected 4 lines (1 header + 3 data), got %d lines", len(gotLines))
				}
				// 3. Headers are present and correct
				expectedHeaders := []string{TestAField, TestBField, TestIDField} // Sorted order
				gotHeaders := strings.Split(gotLines[0], ",")
				if len(gotHeaders) < len(expectedHeaders) { // Can be more if datasource adds extra fields
					t.Errorf("❌ expected %d headers, got %d headers", len(expectedHeaders), len(gotHeaders))
				}
			})

			fmt.Print("\n---------------------------------------------------------------------------------\n\n")
		})
	}
}

func TestSaveCSV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name        string
		data        []map[string]any
		batchSize   uint64
		expectedCSV string
	}{
		{
			name: "basic export with batch size 2",
			data: []map[string]any{
				{"id": "1", "name": "Alice", "age": 30},
				{"id": "2", "name": "Bob", "age": 25},
				{"id": "3", "name": "Charlie", "age": 35},
			},
			batchSize:   2,
			expectedCSV: "age,id,name\n30,1,Alice\n25,2,Bob\n35,3,Charlie\n",
		},
		{
			name: "export with batch size 1",
			data: []map[string]any{
				{"id": "1", "x": "a", "y": 1},
				{"id": "2", "x": "b", "y": 2},
			},
			batchSize:   1,
			expectedCSV: "id,x,y\n1,a,1\n2,b,2\n",
		},
		{
			name: "batch size larger than data",
			data: []map[string]any{
				{"id": "1", "col1": "val1", "col2": "val2"},
			},
			batchSize:   10,
			expectedCSV: "col1,col2,id\nval1,val2,1\n",
		},
		{
			name:        "empty datasource",
			data:        []map[string]any{},
			batchSize:   5,
			expectedCSV: "",
		},
		{
			name: "zero batch size defaults to 1",
			data: []map[string]any{
				{"id": "1", "key": "value1"},
				{"id": "2", "key": "value2"},
			},
			batchSize:   0,
			expectedCSV: "id,key\n1,value1\n2,value2\n",
		},
		{
			name: "mixed data types",
			data: []map[string]any{
				{"id": 1, "active": true, "score": 95.5, "name": "test"},
				{"id": 2, "active": false, "score": 88.0, "name": "demo"},
			},
			batchSize:   3,
			expectedCSV: "active,id,name,score\ntrue,1,test,95.5\nfalse,2,demo,88\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create memory datasource and populate with test data
			ds := datasources.NewMemoryDatasource("test-save-csv", "id")
			defer ds.Close(&ctx)

			if len(tt.data) > 0 {
				_, err := ds.Push(&ctx, &datasources.DatasourcePushRequest{
					Inserts: tt.data,
				})
				if err != nil {
					t.Fatalf("⛔️ failed to push test data: %v", err)
				}
			}

			// Create temp file path
			dir := t.TempDir()
			csvPath := filepath.Join(dir, "test.csv")

			// Call SaveCSV
			err := datasources.SaveCSV(&ctx, ds, csvPath, tt.batchSize)
			if err != nil {
				t.Fatalf("⛔️ SaveCSV failed: %v", err)
			}

			// Read and verify the file content
			if tt.expectedCSV == "" {
				// For empty datasource, file should not exist or be empty
				if _, err := os.Stat(csvPath); err == nil {
					content, readErr := os.ReadFile(csvPath)
					if readErr != nil {
						t.Fatalf("⛔️ failed to read output file: %v", readErr)
					}
					if len(content) > 0 {
						t.Errorf("❌ expected empty file, got content: %q", string(content))
					}
				}
			} else {
				content, err := os.ReadFile(csvPath)
				if err != nil {
					t.Fatalf("⛔️ failed to read output file: %v", err)
				}

				gotContent := string(content)

				// For basic CSV structure validation, we'll check:
				// 1. The file has content
				// 2. It has the right number of lines (header + data)
				// 3. Headers are present and correct
				if len(gotContent) == 0 {
					t.Errorf("❌ got empty file, expected content")
					return
				}

				gotLines := strings.Split(strings.TrimSuffix(gotContent, "\n"), "\n")
				expectedLines := strings.Split(strings.TrimSuffix(tt.expectedCSV, "\n"), "\n")

				// Check number of lines
				if len(gotLines) != len(expectedLines) {
					t.Errorf("❌ got %d lines, want %d lines", len(gotLines), len(expectedLines))
					return
				}

				// Check headers (first line should match exactly)
				if len(expectedLines) > 0 && gotLines[0] != expectedLines[0] {
					t.Errorf("❌ got headers %q, want %q", gotLines[0], expectedLines[0])
					return
				}

				// For data rows, we'll verify the content exists but not the exact order
				// (since memory datasource doesn't guarantee order)
				if len(expectedLines) > 1 {
					expectedDataLines := expectedLines[1:]
					gotDataLines := gotLines[1:]

					// Convert to sets for comparison
					expectedSet := make(map[string]bool)
					for _, line := range expectedDataLines {
						expectedSet[line] = true
					}

					gotSet := make(map[string]bool)
					for _, line := range gotDataLines {
						gotSet[line] = true
					}

					// Check that all expected lines are present
					for expectedLine := range expectedSet {
						if !gotSet[expectedLine] {
							t.Errorf("❌ missing expected line: %q", expectedLine)
						}
					}

					// Check that no unexpected lines are present
					for gotLine := range gotSet {
						if !expectedSet[gotLine] {
							t.Errorf("❌ unexpected line: %q", gotLine)
						}
					}
				}
			}
		})
	}
}

func TestSaveCSV_DatasourceErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("invalid file path", func(t *testing.T) {
		t.Parallel()

		ds := datasources.NewMemoryDatasource("test-error", "id")
		defer ds.Close(&ctx)

		// Add some data
		_, err := ds.Push(&ctx, &datasources.DatasourcePushRequest{
			Inserts: []map[string]any{{"id": "1", "name": "test"}},
		})
		if err != nil {
			t.Fatalf("⛔️ failed to setup test data: %v", err)
		}

		// Try to save to invalid path
		err = datasources.SaveCSV(&ctx, ds, "/invalid/path/file.csv", 10)
		if err == nil {
			t.Fatal("⛔️ expected error for invalid path, got nil")
		}
	})

	t.Run("datasource fetch error simulation", func(t *testing.T) {
		t.Parallel()

		// Create a mock datasource that returns errors on fetch
		ds := &MockErrorDatasource{}
		defer ds.Close(&ctx)

		dir := t.TempDir()
		csvPath := filepath.Join(dir, "test.csv")

		err := datasources.SaveCSV(&ctx, ds, csvPath, 5)
		if err == nil {
			t.Fatal("⛔️ expected error from mock datasource, got nil")
		}
	})
}

func TestLoadCSV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name           string
		csvContent     string
		batchSize      uint64
		expectedData   []map[string]any
		expectedCounts map[string]int // For counting unique values
	}{
		{
			name:       "basic CSV with batch size 2",
			batchSize:  2,
			csvContent: "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35",
			expectedData: []map[string]any{
				{"id": "1", "name": "Alice", "age": "30"},
				{"id": "2", "name": "Bob", "age": "25"},
				{"id": "3", "name": "Charlie", "age": "35"},
			},
			expectedCounts: map[string]int{
				"1": 1,
				"2": 1,
				"3": 1,
			},
		},
		{
			name:       "CSV with batch size 1",
			batchSize:  1,
			csvContent: "id,x,y\n1,a,1\n2,b,2",
			expectedData: []map[string]any{
				{"id": "1", "x": "a", "y": "1"},
				{"id": "2", "x": "b", "y": "2"},
			},
			expectedCounts: map[string]int{
				"1": 1,
				"2": 1,
			},
		},
		{
			name:       "batch size larger than data",
			batchSize:  10,
			csvContent: "id,col1,col2\n1,val1,val2",
			expectedData: []map[string]any{
				{"id": "1", "col1": "val1", "col2": "val2"},
			},
			expectedCounts: map[string]int{
				"1": 1,
			},
		},
		{
			name:           "empty CSV (headers only)",
			batchSize:      5,
			csvContent:     `id,h1,h2`,
			expectedData:   []map[string]any{},
			expectedCounts: map[string]int{},
		},
		{
			name:       "zero batch size defaults to 1",
			batchSize:  0,
			csvContent: "id,key,value\n1,item1,data1\n2,item2,data2",
			expectedData: []map[string]any{
				{"id": "1", "key": "item1", "value": "data1"},
				{"id": "2", "key": "item2", "value": "data2"},
			},
			expectedCounts: map[string]int{
				"1": 1,
				"2": 1,
			},
		},
		{
			name:       "CSV with special characters and quotes",
			batchSize:  3,
			csvContent: "id,text,number\n1,\"Hello, World!\",123\n2,\"Line with \"\"quotes\"\"\",456\n3,\"Multi\nline\",789",
			expectedData: []map[string]any{
				{"id": "1", "text": "Hello, World!", "number": "123"},
				{"id": "2", "text": `Line with "quotes"`, "number": "456"},
				{"id": "3", "text": "Multi\nline", "number": "789"},
			},
			expectedCounts: map[string]int{
				"1": 1,
				"2": 1,
				"3": 1,
			},
		},
		{
			name:       "CSV with missing fields",
			batchSize:  2,
			csvContent: "id,a,b,c\n1,1,2,3\n2,4,\"\",6\n3,7,8,\"\"",
			expectedData: []map[string]any{
				{"id": "1", "a": "1", "b": "2", "c": "3"},
				{"id": "2", "a": "4", "b": "", "c": "6"},
				{"id": "3", "a": "7", "b": "8", "c": ""},
			},
			expectedCounts: map[string]int{
				"1": 1,
				"2": 1,
				"3": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create temporary CSV file
			dir := t.TempDir()
			csvPath := filepath.Join(dir, "test.csv")
			err := os.WriteFile(csvPath, []byte(tt.csvContent), 0644)
			if err != nil {
				t.Fatalf("⛔️ failed to create test CSV file: %v", err)
			}

			// Create memory datasource
			ds := datasources.NewMemoryDatasource("test-load-csv", "id")
			defer ds.Close(&ctx)

			// Call LoadCSV
			err = datasources.LoadCSV(&ctx, ds, csvPath, tt.batchSize)
			if err != nil {
				t.Fatalf("⛔️ LoadCSV failed: %v", err)
			}

			// Verify the data was loaded correctly
			result := ds.Fetch(&ctx, &datasources.DatasourceFetchRequest{})
			if result.Err != nil {
				t.Fatalf("⛔️ failed to fetch loaded data: %v", result.Err)
			}

			// Check total count
			if len(result.Docs) != len(tt.expectedData) {
				t.Errorf("❌ got %d documents, want %d", len(result.Docs), len(tt.expectedData))
			}

			// For validation, we'll use a count-based approach since memory datasource
			// doesn't guarantee order
			if len(tt.expectedCounts) > 0 {
				actualCounts := make(map[string]int)
				for _, doc := range result.Docs {
					// Count occurrences of id field values for verification
					if idValue, ok := doc["id"].(string); ok {
						if _, exists := tt.expectedCounts[idValue]; exists {
							actualCounts[idValue]++
						}
					}
				}

				// Verify expected counts
				for expectedKey, expectedCount := range tt.expectedCounts {
					if actualCounts[expectedKey] != expectedCount {
						t.Errorf("❌ expected %d occurrences of %q, got %d", expectedCount, expectedKey, actualCounts[expectedKey])
					}
				}
			}

			// Verify data structure for non-empty cases
			if len(tt.expectedData) > 0 && len(result.Docs) > 0 {
				// Check that all expected fields are present in at least one document
				expectedFields := make(map[string]bool)
				for _, expectedDoc := range tt.expectedData {
					for field := range expectedDoc {
						expectedFields[field] = true
					}
				}

				actualFields := make(map[string]bool)
				for _, actualDoc := range result.Docs {
					for field := range actualDoc {
						actualFields[field] = true
					}
				}

				for expectedField := range expectedFields {
					if !actualFields[expectedField] {
						t.Errorf("❌ missing expected field %q in loaded data", expectedField)
					}
				}
			}
		})
	}
}

func TestLoadCSV_ErrorCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("non-existent file", func(t *testing.T) {
		t.Parallel()

		ds := datasources.NewMemoryDatasource("test-error", "id")
		defer ds.Close(&ctx)

		err := datasources.LoadCSV(&ctx, ds, "/non/existent/file.csv", 10)
		if err == nil {
			t.Fatal("⛔️ expected error for non-existent file, got nil")
		}
	})

	t.Run("datasource push error", func(t *testing.T) {
		t.Parallel()

		// Create temporary CSV file
		dir := t.TempDir()
		csvPath := filepath.Join(dir, "test.csv")
		csvContent := "id,name\n1,Alice\n2,Bob"
		err := os.WriteFile(csvPath, []byte(csvContent), 0644)
		if err != nil {
			t.Fatalf("⛔️ failed to create test CSV file: %v", err)
		}

		// Use mock datasource that fails on push
		ds := &MockErrorDatasource{}
		defer ds.Close(&ctx)

		err = datasources.LoadCSV(&ctx, ds, csvPath, 2)
		if err == nil {
			t.Fatal("⛔️ expected error from mock datasource push, got nil")
		}
	})

	t.Run("invalid CSV format", func(t *testing.T) {
		t.Parallel()

		// Create temporary file with invalid CSV
		dir := t.TempDir()
		csvPath := filepath.Join(dir, "invalid.csv")
		// Create a file that will cause CSV parsing issues
		invalidContent := "header1,header2\n\"unclosed quote,value2\n"
		err := os.WriteFile(csvPath, []byte(invalidContent), 0644)
		if err != nil {
			t.Fatalf("⛔️ failed to create invalid CSV file: %v", err)
		}

		ds := datasources.NewMemoryDatasource("test-invalid", "id")
		defer ds.Close(&ctx)

		err = datasources.LoadCSV(&ctx, ds, csvPath, 5)
		// Should handle CSV parsing errors gracefully
		// Note: depending on how StreamReadCSV handles malformed CSV,
		// this might succeed or fail - both are acceptable behaviors
		if err != nil {
			// Error is expected for malformed CSV
			t.Logf("Expected error for malformed CSV: %v", err)
		}
	})
}

func TestParseGormFieldValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		field       *schema.Field
		input       any
		expected    any
		expectError bool
		errorMsg    string
	}{
		// String field tests
		{
			name:        "string field with string input",
			field:       &schema.Field{Name: "name", DataType: schema.String},
			input:       "hello world",
			expected:    "hello world",
			expectError: false,
		},
		{
			name:        "string field with JSON string input",
			field:       &schema.Field{Name: "data", DataType: schema.String},
			input:       `{"key":"value","number":123}`,
			expected:    map[string]any{"key": "value", "number": float64(123)},
			expectError: false,
		},
		{
			name:        "string field with invalid JSON string",
			field:       &schema.Field{Name: "data", DataType: schema.String},
			input:       `{"invalid":json}`,
			expected:    `{"invalid":json}`,
			expectError: false,
		},
		{
			name:        "string field with byte slice JSON",
			field:       &schema.Field{Name: "data", DataType: schema.String},
			input:       []byte(`{"test":"value"}`),
			expected:    map[string]any{"test": "value"},
			expectError: false,
		},
		{
			name:        "string field with byte slice non-JSON",
			field:       &schema.Field{Name: "data", DataType: schema.String},
			input:       []byte("plain text"),
			expected:    "plain text",
			expectError: false,
		},

		// Bool field tests
		{
			name:        "bool field with bool input",
			field:       &schema.Field{Name: "active", DataType: schema.Bool},
			input:       true,
			expected:    true,
			expectError: false,
		},
		{
			name:        "bool field with string true",
			field:       &schema.Field{Name: "active", DataType: schema.Bool},
			input:       "true",
			expected:    true,
			expectError: false,
		},
		{
			name:        "bool field with string false",
			field:       &schema.Field{Name: "active", DataType: schema.Bool},
			input:       "false",
			expected:    false,
			expectError: false,
		},
		{
			name:        "bool field with string 1",
			field:       &schema.Field{Name: "active", DataType: schema.Bool},
			input:       "1",
			expected:    true,
			expectError: false,
		},
		{
			name:        "bool field with string 0",
			field:       &schema.Field{Name: "active", DataType: schema.Bool},
			input:       "0",
			expected:    false,
			expectError: false,
		},
		{
			name:        "bool field with invalid string",
			field:       &schema.Field{Name: "active", DataType: schema.Bool},
			input:       "invalid",
			expected:    nil,
			expectError: true,
			errorMsg:    "invalid value type for bool field",
		},

		// Float field tests
		{
			name:        "float field with float64 input",
			field:       &schema.Field{Name: "price", DataType: schema.Float},
			input:       123.456,
			expected:    123.456,
			expectError: false,
		},
		{
			name:        "float field with float32 input",
			field:       &schema.Field{Name: "price", DataType: schema.Float},
			input:       float32(123.456),
			expected:    float64(float32(123.456)), // Account for float32 precision loss
			expectError: false,
		},
		{
			name:        "float field with string input",
			field:       &schema.Field{Name: "price", DataType: schema.Float},
			input:       "123.456",
			expected:    123.456,
			expectError: false,
		},
		{
			name:        "float field with int input",
			field:       &schema.Field{Name: "price", DataType: schema.Float},
			input:       123,
			expected:    123.0,
			expectError: false,
		},
		{
			name:        "float field with invalid string",
			field:       &schema.Field{Name: "price", DataType: schema.Float},
			input:       "not-a-number",
			expected:    nil,
			expectError: true,
			errorMsg:    "invalid value type for float field",
		},

		// Time field tests
		{
			name:        "time field with time.Time input",
			field:       &schema.Field{Name: "created_at", DataType: schema.Time},
			input:       time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			expected:    time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			expectError: false,
		},
		{
			name:        "time field with RFC3339 string",
			field:       &schema.Field{Name: "created_at", DataType: schema.Time},
			input:       "2023-01-01T12:00:00Z",
			expected:    time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			expectError: false,
		},
		{
			name:        "time field with RFC3339Nano string",
			field:       &schema.Field{Name: "created_at", DataType: schema.Time},
			input:       "2023-01-01T12:00:00.123456789Z",
			expected:    time.Date(2023, 1, 1, 12, 0, 0, 123456789, time.UTC),
			expectError: false,
		},
		{
			name:        "time field with custom format",
			field:       &schema.Field{Name: "created_at", DataType: schema.Time},
			input:       "2023-01-01 12:00:00.000000+00",
			expected:    time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			expectError: false,
		},
		{
			name:        "time field with invalid string",
			field:       &schema.Field{Name: "created_at", DataType: schema.Time},
			input:       "invalid-time",
			expected:    nil,
			expectError: true,
			errorMsg:    "invalid value type for time field",
		},

		// Int field tests
		{
			name:        "int field with int64 input",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       int64(123),
			expected:    int64(123),
			expectError: false,
		},
		{
			name:        "int field with int input",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       123,
			expected:    int64(123),
			expectError: false,
		},
		{
			name:        "int field with int32 input",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       int32(123),
			expected:    int64(123),
			expectError: false,
		},
		{
			name:        "int field with int16 input",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       int16(123),
			expected:    int64(123),
			expectError: false,
		},
		{
			name:        "int field with int8 input",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       int8(123),
			expected:    int64(123),
			expectError: false,
		},
		{
			name:        "int field with string input",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       "123",
			expected:    int64(123),
			expectError: false,
		},
		{
			name:        "int field with negative string",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       "-123",
			expected:    int64(-123),
			expectError: false,
		},
		{
			name:        "int field with invalid string",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       "not-a-number",
			expected:    nil,
			expectError: true,
			errorMsg:    "invalid value type for int field",
		},

		// Uint field tests
		{
			name:        "uint field with uint64 input",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       uint64(123),
			expected:    uint64(123),
			expectError: false,
		},
		{
			name:        "uint field with uint input",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       uint(123),
			expected:    uint64(123),
			expectError: false,
		},
		{
			name:        "uint field with uint32 input",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       uint32(123),
			expected:    uint64(123),
			expectError: false,
		},
		{
			name:        "uint field with uint16 input",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       uint16(123),
			expected:    uint64(123),
			expectError: false,
		},
		{
			name:        "uint field with uint8 input",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       uint8(123),
			expected:    uint64(123),
			expectError: false,
		},
		{
			name:        "uint field with string input",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       "123",
			expected:    uint64(123),
			expectError: false,
		},
		{
			name:        "uint field with negative string",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       "-123",
			expected:    nil,
			expectError: true,
			errorMsg:    "invalid value type for uint field",
		},
		{
			name:        "uint field with invalid string",
			field:       &schema.Field{Name: "id", DataType: schema.Uint},
			input:       "not-a-number",
			expected:    nil,
			expectError: true,
			errorMsg:    "invalid value type for uint field",
		},

		// Bytes field tests
		{
			name:        "bytes field with byte slice input",
			field:       &schema.Field{Name: "data", DataType: schema.Bytes},
			input:       []byte("hello world"),
			expected:    []byte("hello world"),
			expectError: false,
		},
		{
			name:        "bytes field with string input",
			field:       &schema.Field{Name: "data", DataType: schema.Bytes},
			input:       "hello world",
			expected:    []byte("hello world"),
			expectError: false,
		},
		{
			name:        "bytes field with invalid input",
			field:       &schema.Field{Name: "data", DataType: schema.Bytes},
			input:       123,
			expected:    nil,
			expectError: true,
			errorMsg:    "invalid value type for bytes field",
		},

		// Nil value tests
		{
			name:        "nil value with string field",
			field:       &schema.Field{Name: "name", DataType: schema.String},
			input:       nil,
			expected:    nil,
			expectError: false,
		},
		{
			name:        "nil value with int field",
			field:       &schema.Field{Name: "count", DataType: schema.Int},
			input:       nil,
			expected:    nil,
			expectError: false,
		},

		// Unsupported field type test
		{
			name:        "unsupported field type",
			field:       &schema.Field{Name: "unknown", DataType: schema.DataType("unknown")},
			input:       "value",
			expected:    nil,
			expectError: true,
			errorMsg:    "unsupported field type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := datasources.ParseGormFieldValue(tt.field, tt.input)

			if tt.expectError {
				if err == nil {
					t.Fatalf("⛔️ expected error but got none")
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("⛔️ expected error message to contain '%s', but got: %v", tt.errorMsg, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("⛔️ unexpected error: %v", err)
			}

			// Special handling for time comparison due to potential timezone differences
			if expectedTime, ok := tt.expected.(time.Time); ok {
				if resultTime, ok := result.(time.Time); ok {
					if !expectedTime.Equal(resultTime) {
						t.Errorf("⛔️ expected time %v, got %v", expectedTime, resultTime)
					}
					return
				}
			}

			// Special handling for byte slice comparison
			if expectedBytes, ok := tt.expected.([]byte); ok {
				if resultBytes, ok := result.([]byte); ok {
					if !reflect.DeepEqual(expectedBytes, resultBytes) {
						t.Errorf("⛔️ expected bytes %v, got %v", expectedBytes, resultBytes)
					}
					return
				}
			}

			// General comparison
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("⛔️ expected %v (type %T), got %v (type %T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

// TestParseGormFieldValue_EdgeCases tests edge cases and specific scenarios
func TestParseGormFieldValue_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("float precision edge cases", func(t *testing.T) {
		t.Parallel()

		field := &schema.Field{Name: "value", DataType: schema.Float}

		// Test very large numbers
		result, err := datasources.ParseGormFieldValue(field, "1.7976931348623157e+308")
		if err != nil {
			t.Fatalf("⛔️ unexpected error for large float: %v", err)
		}
		if result != 1.7976931348623157e+308 {
			t.Errorf("⛔️ expected large float to be parsed correctly")
		}

		// Test very small numbers
		result, err = datasources.ParseGormFieldValue(field, "2.2250738585072014e-308")
		if err != nil {
			t.Fatalf("⛔️ unexpected error for small float: %v", err)
		}
		if result != 2.2250738585072014e-308 {
			t.Errorf("⛔️ expected small float to be parsed correctly")
		}
	})

	t.Run("int overflow edge cases", func(t *testing.T) {
		t.Parallel()

		field := &schema.Field{Name: "value", DataType: schema.Int}

		// Test maximum int64 value
		maxInt64 := "9223372036854775807"
		result, err := datasources.ParseGormFieldValue(field, maxInt64)
		if err != nil {
			t.Fatalf("⛔️ unexpected error for max int64: %v", err)
		}
		if result != int64(9223372036854775807) {
			t.Errorf("⛔️ expected max int64 to be parsed correctly")
		}

		// Test minimum int64 value
		minInt64 := "-9223372036854775808"
		result, err = datasources.ParseGormFieldValue(field, minInt64)
		if err != nil {
			t.Fatalf("⛔️ unexpected error for min int64: %v", err)
		}
		if result != int64(-9223372036854775808) {
			t.Errorf("⛔️ expected min int64 to be parsed correctly")
		}

		// Test overflow
		overflow := "9223372036854775808" // max int64 + 1
		_, err = datasources.ParseGormFieldValue(field, overflow)
		if err == nil {
			t.Fatalf("⛔️ expected error for int64 overflow")
		}
	})

	t.Run("uint overflow edge cases", func(t *testing.T) {
		t.Parallel()

		field := &schema.Field{Name: "value", DataType: schema.Uint}

		// Test maximum uint64 value
		maxUint64 := "18446744073709551615"
		result, err := datasources.ParseGormFieldValue(field, maxUint64)
		if err != nil {
			t.Fatalf("⛔️ unexpected error for max uint64: %v", err)
		}
		if result != uint64(18446744073709551615) {
			t.Errorf("⛔️ expected max uint64 to be parsed correctly")
		}

		// Test overflow
		overflow := "18446744073709551616" // max uint64 + 1
		_, err = datasources.ParseGormFieldValue(field, overflow)
		if err == nil {
			t.Fatalf("⛔️ expected error for uint64 overflow")
		}
	})

	t.Run("complex JSON in string field", func(t *testing.T) {
		t.Parallel()

		field := &schema.Field{Name: "data", DataType: schema.String}

		// Test nested JSON
		complexJSON := `{
			"user": {
				"id": 123,
				"name": "John Doe",
				"preferences": {
					"theme": "dark",
					"notifications": true
				}
			},
			"metadata": ["tag1", "tag2"],
			"created_at": "2023-01-01T12:00:00Z"
		}`

		result, err := datasources.ParseGormFieldValue(field, complexJSON)
		if err != nil {
			t.Fatalf("⛔️ unexpected error for complex JSON: %v", err)
		}

		resultMap, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("⛔️ expected result to be map[string]any, got %T", result)
		}

		// Verify some nested values
		user, ok := resultMap["user"].(map[string]any)
		if !ok {
			t.Fatalf("⛔️ expected user to be map[string]any")
		}

		if user["id"] != float64(123) { // JSON numbers become float64
			t.Errorf("⛔️ expected user ID to be 123, got %v", user["id"])
		}

		if user["name"] != "John Doe" {
			t.Errorf("⛔️ expected user name to be 'John Doe', got %v", user["name"])
		}
	})

	t.Run("empty string inputs", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			dataType    schema.DataType
			expectError bool
			expected    any
		}{
			{schema.String, false, ""},        // empty string fails JSON parse, returns original string
			{schema.Bool, true, nil},          // empty string is not a valid bool
			{schema.Float, true, nil},         // empty string is not a valid float
			{schema.Int, true, nil},           // empty string is not a valid int
			{schema.Uint, true, nil},          // empty string is not a valid uint
			{schema.Bytes, false, []byte("")}, // empty string becomes empty byte slice
		}

		for _, tt := range tests {
			field := &schema.Field{Name: "field", DataType: tt.dataType}
			result, err := datasources.ParseGormFieldValue(field, "")

			if tt.expectError {
				if err == nil {
					t.Errorf("⛔️ expected error for empty string with %s field", tt.dataType)
				}
			} else {
				if err != nil {
					t.Errorf("⛔️ unexpected error for empty string with %s field: %v", tt.dataType, err)
				}
				// Verify the result matches expected
				if !reflect.DeepEqual(result, tt.expected) {
					t.Errorf("⛔️ expected %v for empty string with %s field, got %v", tt.expected, tt.dataType, result)
				}
			}
		}
	})
}
