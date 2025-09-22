package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
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
func getTestStates(ctx *context.Context) <-chan TestState {
	out := make(chan TestState)

	// Reusable vars
	var id string

	go func() {
		defer close(out)

		// -----------------------
		// 1. Memory
		// -----------------------
		id = "memory-store"
		out <- TestState{
			id:    id,
			store: states.NewMemoryStateStore(),
		}

		// -----------------------
		// 2. File
		// -----------------------
		id = "file-store"
		out <- TestState{
			id:    id,
			store: states.NewFileStateStore(getStatesTempBasePath(), id),
		}

		// -----------------------
		// 3. Redis
		// -----------------------
		redisAddr := os.Getenv("REDIS_ADDR")
		redisPass := os.Getenv("REDIS_PASS")
		redisDb := 0
		if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
			db, err := strconv.Atoi(dbStr)
			if err != nil {
				redisDb = db
			}
		}

		id = "redis-store"
		out <- TestState{
			id:    id,
			store: states.NewRedisStateStore(ctx, redisAddr, redisPass, redisDb, id),
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
		t.Errorf("❌ state mismatch after JSON round-trip:\nexpected %+v\ngot %+v", original, decoded)
	}
}

func TestStateStoreLifecycle(t *testing.T) {
	testCtx := context.Background()

	slog.SetDefault(helpers.CreateTextLogger()) // Default logger

	for st := range getTestStates(&testCtx) {

		fmt.Println("\n---------------------------------------------------------------------------------")
		t.Run(st.id, func(t *testing.T) {
			fmt.Println("---------------------------------------------------------------------------------")

			t.Parallel() // Run states tests in parallel

			ctx, cancel := context.WithTimeout(testCtx, time.Duration(5)*time.Minute)
			defer cancel()

			// Cleanup after
			t.Cleanup(func() {
				st.store.Clear(&testCtx)
				st.store.Close(&testCtx)
			})

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
					t.Errorf("❌ loaded state mismatch:\nexpected %+v\ngot %+v", original, loaded)
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

			// 5) Test concurrent operations
			t.Run("ConcurrentOperations", func(t *testing.T) {
				const numGoroutines = 10
				const numOperations = 50

				// Create channels to coordinate goroutines
				done := make(chan bool)
				errs := make(chan error, numGoroutines*numOperations)

				state := states.State{
					MigrationStatus:        states.MigrationStatusInProgress,
					MigrationTotal:         json.Number("100"),
					MigrationOffset:        json.Number("42"),
					MigrationIssue:         "none",
					MigrationStartedAt:     time.Now().Truncate(time.Millisecond),
					MigrationStoppedAt:     time.Now().Add(time.Minute).Truncate(time.Millisecond),
					ReplicationStatus:      states.ReplicationStatusStreaming,
					ReplicationIssue:       "replica error",
					ReplicationStartedAt:   time.Now().Add(time.Hour).Truncate(time.Millisecond),
					ReplicationStoppededAt: time.Now().Add(2 * time.Hour).Truncate(time.Millisecond),
				}

				// Launch goroutines that perform concurrent operations
				for i := range numGoroutines {
					go func(routineID int) {
						for j := range numOperations {
							key := fmt.Sprintf("concurrent-key-%d-%d", routineID, j)

							// Store
							if err := st.store.Store(&ctx, key, state); err != nil {
								errs <- fmt.Errorf("store error: %v", err)
								continue
							}

							// Load and verify
							loaded, ok := st.store.Load(&ctx, key)
							if !ok {
								errs <- fmt.Errorf("failed to load key %s", key)
								continue
							}
							if !statesEqual(state, loaded) {
								errs <- fmt.Errorf("state mismatch for key %s", key)
								continue
							}

							// Delete
							if err := st.store.Delete(&ctx, key); err != nil {
								errs <- fmt.Errorf("delete error: %v", err)
								continue
							}

							// Verify deletion
							if _, ok := st.store.Load(&ctx, key); ok {
								errs <- fmt.Errorf("key %s still exists after deletion", key)
							}
						}
						done <- true
					}(i)
				}

				// Wait for all goroutines to complete
				for range numGoroutines {
					<-done
				}
				close(errs)

				// Check for any errors
				for err := range errs {
					t.Errorf("❌ Concurrent operation error: %v", err)
				}
			})

			// 6) Close and ensure further ops fail
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
				if err := st.store.Store(&ctx, "x", original); err != states.ErrStoreClosed {
					t.Errorf("❌ expected Store after Close to return states.ErrStoreClosed, got %v", err)
				}
				if _, ok := st.store.Load(&ctx, "a"); ok {
					t.Error("❌ expected Load after Close to return ok=false")
				}
				if err := st.store.Delete(&ctx, "b"); err != states.ErrStoreClosed {
					t.Errorf("❌ expected Delete after Close to return states.ErrStoreClosed, got %v", err)
				}
				if err := st.store.Clear(&ctx); err != states.ErrStoreClosed {
					t.Errorf("❌ expected Clear after Close to return states.ErrStoreClosed, got %v", err)
				}
			})

		})
	}
}
