package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/sagadana/migrator/helpers"
	"github.com/sagadana/migrator/states"
)

type TestState struct {
	id    string
	store states.Store
}

// --------
// Utils
// --------

// statesEqual does a deep compare of all State fields.
func statesEqual(a, b states.State) bool {
	if a.MigrationStatus != b.MigrationStatus ||
		a.MigrationTotal.String() != b.MigrationTotal.String() ||
		a.MigrationOffset.String() != b.MigrationOffset.String() ||
		a.MigrationIssue != b.MigrationIssue ||
		!a.MigrationStartedAt.Equal(b.MigrationStartedAt) ||
		!a.MigrationStoppedAt.Equal(b.MigrationStoppedAt) ||
		a.ReplicationStatus != b.ReplicationStatus ||
		a.ReplicationIssue != b.ReplicationIssue ||
		!a.ReplicationStartedAt.Equal(b.ReplicationStartedAt) ||
		!a.ReplicationStoppededAt.Equal(b.ReplicationStoppededAt) {
		return false
	}
	return true
}

// Get base path for temp dir
func getStatesTempBasePath() string {
	return helpers.GetTempBasePath("test-states")
}

// Retrieve Test States
// TODO: Add more states here to test...
func getTestStates(_ *context.Context) <-chan TestState {
	out := make(chan TestState)

	// Reusable vars
	var id string

	go func() {
		defer close(out)

		// -----------------------
		// 1. Memory
		// -----------------------
		id = "memory-datasource"
		out <- TestState{
			id:    id,
			store: states.NewMemoryStateStore(),
		}

		// -----------------------
		// 2. File
		// -----------------------
		id = "file-datasource"
		out <- TestState{
			id:    id,
			store: states.NewFileStateStore(getStatesTempBasePath(), id),
		}

	}()

	return out
}

// --------
// Tests
// --------

func TestStateJSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	original := states.State{
		MigrationStatus:    states.MigrationStatusCompleted,
		MigrationTotal:     json.Number("500"),
		MigrationOffset:    json.Number("250"),
		MigrationIssue:     "issue found",
		MigrationStartedAt: now,
		MigrationStoppedAt: now.Add(2 * time.Hour),

		ReplicationStatus:      states.ReplicationStatusPaused,
		ReplicationIssue:       "replication halted",
		ReplicationStartedAt:   now.Add(-time.Hour),
		ReplicationStoppededAt: now,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded states.State
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if !statesEqual(original, decoded) {
		t.Errorf("❌ state mismatch after JSON round-trip:\nexpected %+v\ngot      %+v", original, decoded)
	}
}

func TestStateStoreLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(60)*time.Second)
	defer func() {
		time.Sleep(1 * time.Second) // Wait for logs
		cancel()
	}()
	slog.SetDefault(helpers.CreateTextLogger()) // Default logger

	for st := range getTestStates(&ctx) {

		fmt.Println("\n---------------------------------------------------------------------------------")
		t.Run(st.id, func(t *testing.T) {
			fmt.Println("---------------------------------------------------------------------------------")

			// 1) Loading a missing key should return ok=false
			t.Run("LoadMissingKey", func(t *testing.T) {
				if _, ok := st.store.Load(&ctx, "missing"); ok {
					t.Error("❌ expected Load to return ok=false for missing key")
				}
			})

			// 2) Store and Load a populated State
			t.Run("StoreAndLoad", func(t *testing.T) {
				now1 := time.Now().Truncate(time.Millisecond)
				now2 := now1.Add(time.Minute)
				original := states.State{
					MigrationStatus:    states.MigrationStatusInProgress,
					MigrationTotal:     json.Number("100"),
					MigrationOffset:    json.Number("42"),
					MigrationIssue:     "none",
					MigrationStartedAt: now1,
					MigrationStoppedAt: now2,

					ReplicationStatus:      states.ReplicationStatusStreaming,
					ReplicationIssue:       "replica error",
					ReplicationStartedAt:   now2,
					ReplicationStoppededAt: now1,
				}

				if err := st.store.Store(&ctx, "key1", original); err != nil {
					t.Fatalf("Store returned error: %v", err)
				}
				loaded, ok := st.store.Load(&ctx, "key1")
				if !ok {
					t.Fatal("⛔️ expected Load to find key1")
				}
				if !statesEqual(original, loaded) {
					t.Errorf("❌ loaded state mismatch:\nexpected %+v\ngot      %+v", original, loaded)
				}
			})

			// 3) Delete and ensure it’s gone
			t.Run("DeleteAndCheck", func(t *testing.T) {
				now1 := time.Now().Truncate(time.Millisecond)
				now2 := now1.Add(time.Minute)
				original := states.State{
					MigrationStatus:    states.MigrationStatusInProgress,
					MigrationTotal:     json.Number("100"),
					MigrationOffset:    json.Number("42"),
					MigrationIssue:     "none",
					MigrationStartedAt: now1,
					MigrationStoppedAt: now2,

					ReplicationStatus:      states.ReplicationStatusStreaming,
					ReplicationIssue:       "replica error",
					ReplicationStartedAt:   now2,
					ReplicationStoppededAt: now1,
				}
				st.store.Store(&ctx, "key1", original)
				if err := st.store.Delete(&ctx, "key1"); err != nil {
					t.Errorf("❌ Delete returned error: %v", err)
				}
				if _, ok := st.store.Load(&ctx, "key1"); ok {
					t.Error("❌ expected Load to return ok=false after Delete")
				}
			})

			// 4) Clear should wipe everything
			t.Run("ClearAll", func(t *testing.T) {
				now1 := time.Now().Truncate(time.Millisecond)
				now2 := now1.Add(time.Minute)
				original := states.State{
					MigrationStatus:    states.MigrationStatusInProgress,
					MigrationTotal:     json.Number("100"),
					MigrationOffset:    json.Number("42"),
					MigrationIssue:     "none",
					MigrationStartedAt: now1,
					MigrationStoppedAt: now2,

					ReplicationStatus:      states.ReplicationStatusStreaming,
					ReplicationIssue:       "replica error",
					ReplicationStartedAt:   now2,
					ReplicationStoppededAt: now1,
				}
				st.store.Store(&ctx, "a", original)
				st.store.Store(&ctx, "b", original)
				if err := st.store.Clear(&ctx); err != nil {
					t.Errorf("❌ Clear returned error: %v", err)
				}
				if _, ok := st.store.Load(&ctx, "a"); ok {
					t.Error("❌ expected Load to return ok=false after Clear")
				}
				if _, ok := st.store.Load(&ctx, "b"); ok {
					t.Error("❌ expected Load to return ok=false after Clear")
				}
			})

			// 5) Close and ensure further ops fail
			t.Run("CloseAndCheckOps", func(t *testing.T) {
				now1 := time.Now().Truncate(time.Millisecond)
				now2 := now1.Add(time.Minute)
				original := states.State{
					MigrationStatus:    states.MigrationStatusInProgress,
					MigrationTotal:     json.Number("100"),
					MigrationOffset:    json.Number("42"),
					MigrationIssue:     "none",
					MigrationStartedAt: now1,
					MigrationStoppedAt: now2,

					ReplicationStatus:      states.ReplicationStatusStreaming,
					ReplicationIssue:       "replica error",
					ReplicationStartedAt:   now2,
					ReplicationStoppededAt: now1,
				}
				st.store.Store(&ctx, "x", original)
				if err := st.store.Close(&ctx); err != nil {
					t.Errorf("❌ Close returned error: %v", err)
				}
				if err := st.store.Store(&ctx, "x", original); err != states.ErrClosed {
					t.Errorf("❌ expected Store after Close to return states.ErrClosed, got %v", err)
				}
				if _, ok := st.store.Load(&ctx, "a"); ok {
					t.Error("❌ expected Load after Close to return ok=false")
				}
				if err := st.store.Delete(&ctx, "b"); err != states.ErrClosed {
					t.Errorf("❌ expected Delete after Close to return states.ErrClosed, got %v", err)
				}
				if err := st.store.Clear(&ctx); err != states.ErrClosed {
					t.Errorf("❌ expected Clear after Close to return states.ErrClosed, got %v", err)
				}
			})
		})
	}
}
