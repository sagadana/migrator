package tests

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
	"go.mongodb.org/mongo-driver/mongo"
)

type TestDatasource struct {
	id     string
	source datasources.Datasource

	withFilter bool // Datasource supports filtering
	withSort   bool // Datasource supports sorting
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
			id:         id,
			withFilter: true,
			withSort:   true,
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
						data[datasources.MongoIDField] = data[TestIDField]
						return data, nil
					},
					OnInit: func(client *mongo.Client) error {
						return nil
					},
				},
			),
		}

	}()

	return out
}

// --------
// Tests
// --------

func TestDatasourceImplementations(t *testing.T) {
	testCtx := context.Background()

	slog.SetDefault(helpers.CreateTextLogger()) // Default logger

	instanceId := helpers.RandomString(6)

	for td := range getTestDatasources(&testCtx, instanceId) {

		fmt.Println("\n---------------------------------------------------------------------------------")
		t.Run(td.id, func(t *testing.T) {
			fmt.Println("---------------------------------------------------------------------------------")

			t.Parallel() // Run datasources tests in parallel

			ctx, cancel := context.WithTimeout(testCtx, time.Duration(5)*time.Minute)
			defer cancel()

			// Cleanup after
			t.Cleanup(func() {
				td.source.Clear(&ctx)
			})

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
						{TestIDField: "b", TestAField: "bar", TestBField: 123},
					},
				}
				cnt, err := td.source.Push(&ctx, pushReq)
				if err != nil {
					t.Fatalf("⛔️ Push(inserts) error: %v", err)
				}
				if cnt.Inserts != 2 || cnt.Updates != 0 || cnt.Deletes != 0 {
					t.Fatalf("⛔️ unexpected push count: %+v", cnt)
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
				if len(fetchRes.Docs) != 2 || fetchRes.Start != 0 || fetchRes.End != uint64(len(pushReq.Inserts))-1 {
					t.Errorf("❌ unexpected fetch result: %#v", fetchRes)
				}

				// 5) Test Fetch: by IDs
				fetchRes2 := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{IDs: []string{"b"}})
				if len(fetchRes2.Docs) != 1 || fmt.Sprintf("%v", fetchRes2.Docs[0][TestIDField]) != "b" {
					t.Errorf("❌ expected single doc with id 'b', got %#v", fetchRes2.Docs)
				}

				// 6) Test Push: updates only (existing + non-existing)
				pushUpd := &datasources.DatasourcePushRequest{
					Updates: map[string]map[string]any{
						"a": {TestAField: "fooood"},
						"x": {TestBField: "value"},
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
							Updates: map[string]map[string]any{
								fmt.Sprintf("concurrent-%d", i): {TestAField: fmt.Sprintf("updated-%d", i)},
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
				<-time.After(time.Duration(500 * time.Millisecond))

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

				// 1) Test watch: fire background pushes
				evCnt := 5
				streamReq := &datasources.DatasourceStreamRequest{BatchSize: uint64(evCnt), BatchWindowSeconds: 1}
				watchCtx, watchCancel := context.WithTimeout(ctx, time.Duration(5)*time.Second)
				watchCh := td.source.Watch(&watchCtx, streamReq)

				var expInsert, expUpdate, expDelete uint64

				wg := new(sync.WaitGroup)
				wg.Add(evCnt)
				go func() {
					random := helpers.RandomString(6)
					for i := range evCnt {
						defer wg.Done()
						r := &datasources.DatasourcePushRequest{
							Inserts: []map[string]any{{TestIDField: fmt.Sprintf("w%d", i)}},
						}
						// Update previous 1
						if i > 0 {
							r.Updates = map[string]map[string]any{fmt.Sprintf("w%d", i-1): {TestBField: fmt.Sprintf("%s-%d", random, i-1)}}
						}
						// Delete previous 2
						if i > 1 {
							r.Deletes = []string{fmt.Sprintf("w%d", i-2)}
						}

						c, err := td.source.Push(&ctx, r)
						if err != nil {
							t.Errorf("❌ background push error: %s", err.Error())
						}

						expInsert += c.Inserts
						expUpdate += c.Updates
						expDelete += c.Deletes
					}
				}()

				// Wait for background pushes to run
				rWg := new(sync.WaitGroup)
				rWg.Add(1)
				go func() {
					defer rWg.Done()
					wg.Wait()

					if expInsert != uint64(evCnt) {
						t.Errorf("❌ background inserts: expected %d, got %d", evCnt, expInsert)
					}
					if expUpdate != uint64(evCnt-1) {
						t.Errorf("❌ background updates: expected %d, got %d", evCnt-1, expUpdate)
					}
					if expDelete != uint64(evCnt-2) {
						t.Errorf("❌ background deletes: expected %d, got %d", evCnt-2, expDelete)
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
						watchCancel() // Stop watching
						break loop
					}
				}

				// Wait for background pushes to complete
				rWg.Wait()

				if gotInsert < expInsert {
					t.Errorf("❌ watch inserts: expected >=%d, got %d", expInsert, gotInsert)
				}
				if gotUpdate < expUpdate {
					t.Errorf("❌ watch updates: expected >=%d, got %d", expUpdate, gotUpdate)
				}
				if gotDelete < expDelete {
					t.Errorf("❌ watch deletes: expected >=%d, got %d", expDelete, gotDelete)
				}

			})

			t.Run("Test_Fetch", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Add more data
				data := datasources.DatasourcePushRequest{
					Inserts: []map[string]any{
						{TestIDField: "a", TestAField: "foo"},
						{TestIDField: "b", TestAField: "bar", TestBField: 123},
						{TestIDField: "c", TestAField: "bars", TestBField: 1234},
						{TestIDField: "d", TestAField: "barss", TestBField: 1235},
						{TestIDField: "e", TestAField: "barsss", TestBField: 1236},
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
						expectEnd:   0,
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
						expectEnd:   currentCount - 1,
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
						expectEnd:   currentCount - 1,
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

			t.Run("Test_Count", func(t *testing.T) {
				td.source.Clear(&ctx)

				// Insert test data
				data := datasources.DatasourcePushRequest{
					Inserts: []map[string]any{
						{TestIDField: "a", TestAField: "foo"},
						{TestIDField: "b", TestAField: "bar", TestBField: 123},
						{TestIDField: "c", TestAField: "baz", TestBField: 456},
						{TestIDField: "d", TestAField: "qux", TestBField: 789},
						{TestIDField: "e", TestAField: "quux", TestBField: 1011},
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
						if doc[TestSortAscField].(int32) != int32(expectedSort) {
							t.Errorf("❌ expected sort value %d at position %d, got %v", expectedSort, i, doc[TestSortAscField])
						}
					}
				})
			}

			fmt.Print("\n---------------------------------------------------------------------------------\n\n")
		})
	}
}
