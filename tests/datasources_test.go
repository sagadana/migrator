package tests

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
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
	}()

	return out
}

// --------
// Tests
// --------

// TestStreamChanges covers the main behaviors of StreamChanges:
// 1. immediate flush when batchSize is reached
// 2. periodic flush when batchWindowSeconds elapses
// 3. draining remaining events on context cancellation
// 4. no output when no events arrive and watcher closes
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
				// We know only ticker-based flushes will emit
				<-time.After(time.Duration(tc.request.BatchWindowSeconds+1) * time.Second)

				// Read exactly expected count
				for i := 0; i < len(tc.expectedBatches); i++ {
					select {
					case r := <-results:
						got = append(got, r.Docs)
					case <-time.After(2 * time.Second):
						t.Fatalf("⛔️ timed out waiting for batch %d", i)
					}
				}
			}

			// Ensure no extra batches
			select {
			case extra := <-results:
				if len(extra.Docs.Inserts)+len(extra.Docs.Updates)+len(extra.Docs.Deletes) > 0 {
					t.Errorf("❌ received unexpected extra batch %+v", extra)
				}
			default:
			}

			if !reflect.DeepEqual(got, tc.expectedBatches) {
				t.Errorf("❌ got batches %#v\nexpected %#v", got, tc.expectedBatches)
			}
		})
	}
}

// Test different datasource implementations
func TestDatasourceImplementations(t *testing.T) {
	t.Parallel()

	testCtx := context.Background()

	slog.SetDefault(helpers.CreateTextLogger(slog.LevelDebug)) // Default logger

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
				<-time.After(time.Duration(200 * time.Millisecond))

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

				// 1) Test watch: fire background pushes
				streamReq := &datasources.DatasourceStreamRequest{BatchSize: uint64(evCnt), BatchWindowSeconds: 1}
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
					case <-time.After(time.Duration((streamReq.BatchWindowSeconds*1000)+500) * time.Millisecond): // Wait for watch batch window
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

			fmt.Print("\n---------------------------------------------------------------------------------\n\n")
		})
	}
}
