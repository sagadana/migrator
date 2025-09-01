package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagadana/migrator/datasources"
)

type TestDatasource struct {
	id     string
	source datasources.Datasource
}

const (
	TestIDField string = "id"
	TestAField  string = "fieldA"
	TestBField  string = "fieldB"
)

// --------
// Utils
// --------

// Get base path for temp dir
func getDatasourcesTempBasePath() string {
	basePath := os.Getenv("RUNNER_TEMP")
	if basePath == "" {
		basePath = os.Getenv("TEMP_BASE_PATH")
		if basePath == "" {
			basePath = os.TempDir()
		}
	}
	return filepath.Join(basePath, "test-datasources")
}

// Retrieve Test Datasources
// TODO: Add more datasources here to test...
func getTestDatasources(_ *context.Context, basePath string) <-chan TestDatasource {
	out := make(chan TestDatasource)

	// Reusable vars
	var id string

	go func() {
		defer close(out)

		// -----------------------
		// 1. File
		// -----------------------
		id = "file-datasource"
		out <- TestDatasource{
			id:     id,
			source: datasources.NewFileDatasource(basePath, "test-"+id, TestIDField),
		}

	}()

	return out
}

// --------
// Tests
// --------

func TestDatasourceImplementations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()

	basePath := filepath.Join(getDatasourcesTempBasePath(), "datasources")
	os.RemoveAll(basePath) // Clean test path

	for td := range getTestDatasources(&ctx, basePath) {
		fmt.Println("\n---------------------------------------------------------------------------------")
		t.Run(td.id, func(t *testing.T) {
			fmt.Println("---------------------------------------------------------------------------------")

			td.source.Init()
			defer td.source.Clear(&ctx)

			// 1) Test Count on empty
			if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 0 {
				t.Fatalf("expected empty count=0, got %d", got)
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
				t.Fatalf("Push(inserts) error: %v", err)
			}
			if cnt.Inserts != 2 || cnt.Updates != 0 || cnt.Deletes != 0 {
				t.Fatalf("unexpected push count: %+v", cnt)
			}

			// 3) Count after inserts
			if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 2 {
				t.Errorf("expected count=%d, got %d", int64(len(pushReq.Inserts)), got)
			}

			// 4) Test Fetch: no filters
			fetchRes := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{Size: 0, Offset: 0})
			if fetchRes.Err != nil {
				t.Fatalf("Fetch error: %v", fetchRes.Err)
			}
			if len(fetchRes.Docs) != 2 || fetchRes.Start != 0 || fetchRes.End != int64(len(pushReq.Inserts))-1 {
				t.Errorf("unexpected fetch result: %#v", fetchRes)
			}

			// 5) Test Fetch: by IDs
			fetchRes2 := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{IDs: []string{"b"}})
			if len(fetchRes2.Docs) != 1 || fmt.Sprintf("%v", fetchRes2.Docs[0][TestIDField]) != "b" {
				t.Errorf("expected single doc with id 'b', got %#v", fetchRes2.Docs)
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
				t.Fatalf("Push(updates) error: %v", err)
			}
			if cntUpd.Updates != int64(len(pushUpd.Updates)) {
				t.Errorf("expected %d update, got %d", int64(len(pushUpd.Updates)), cntUpd.Updates)
			}

			// 7) Verify update applied
			fetchRes3 := td.source.Fetch(&ctx, &datasources.DatasourceFetchRequest{IDs: []string{"a"}})
			if fetchRes3.Docs[0][TestAField] != "fooood" {
				t.Errorf("expected %s='fooood', got %v", TestAField, fetchRes3.Docs[0][TestAField])
			}

			// 8) Test Push: deletes
			pushDel := &datasources.DatasourcePushRequest{Deletes: []string{"a", "x"}}
			cntDel, err := td.source.Push(&ctx, pushDel)
			if err != nil {
				t.Fatalf("Push(deletes) error: %v", err)
			}
			if cntDel.Deletes != int64(len(pushDel.Deletes)) {
				t.Errorf("expected %d delete, got %d", int64(len(pushDel.Deletes)), cntDel.Deletes)
			}
			if got := td.source.Count(&ctx, &datasources.DatasourceFetchRequest{}); got != 1 { // a,b,x - a,x = b
				t.Errorf("expected count=1 after delete, got %d", got)
			}

			// 9) Test watch: fire background pushes
			evCnt := 5
			streamReq := &datasources.DatasourceStreamRequest{BatchSize: int64(evCnt), BatchWindowSeconds: 1}
			watchCh := td.source.Watch(&ctx, streamReq)

			var expInsert, expUpdate, expDelete int64
			wg := new(sync.WaitGroup)
			wg.Add(evCnt)
			go func() {
				for i := range evCnt {
					defer wg.Done()
					r := &datasources.DatasourcePushRequest{
						Inserts: []map[string]any{{TestIDField: fmt.Sprintf("w%d", i)}},
						Updates: map[string]map[string]any{"b": {TestBField: i}},
						Deletes: []string{fmt.Sprintf("w%d", i)},
					}
					c, err := td.source.Push(&ctx, r)
					if err != nil {
						t.Errorf("background push error: %s", err.Error())
					}

					expInsert += c.Inserts
					expUpdate += c.Updates
					expDelete += c.Deletes
				}
			}()

			// Wait for background to run
			wg.Wait()

			if int64(evCnt) != expInsert {
				t.Errorf("background inserts: expected %d, got %d", evCnt, expInsert)
			}
			if int64(evCnt) != expUpdate {
				t.Errorf("background updates: expected %d, got %d", evCnt, expUpdate)
			}
			if int64(evCnt) != expDelete {
				t.Errorf("background deletes: expected %d, got %d", evCnt, expDelete)
			}

			// 10) Test watch: event listener
			mu := new(sync.Mutex)
			var gotInsert, gotUpdate, gotDelete int64

		loop:
			for {
				select {
				case evt := <-watchCh:
					// Our mock doesn't embed counts in Docs fields; aggregate manually
					mu.Lock()
					gotInsert += int64(len(evt.Docs.Inserts))
					gotUpdate += int64(len(evt.Docs.Updates))
					gotDelete += int64(len(evt.Docs.Deletes))
					mu.Unlock()
				case <-time.After(1 * time.Second):
					break loop
				}
			}

			if gotInsert != expInsert {
				t.Errorf("watch inserts: expected %d, got %d", expInsert, gotInsert)
			}
			if gotUpdate != expUpdate {
				t.Errorf("watch updates: expected %d, got %d", expUpdate, gotUpdate)
			}
			if gotDelete != expDelete {
				t.Errorf("watch deletes: expected %d, got %d", expDelete, gotDelete)
			}

			fmt.Print("\n---------------------------------------------------------------------------------\n\n")
		})
	}
}
