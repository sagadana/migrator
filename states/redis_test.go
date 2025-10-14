package states

import (
	"context"
	"testing"
)

func TestRedisStateStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	id := "memory-store"
	st := TestState{
		id:    id,
		store: NewMemoryStateStore(),
	}

	runStateStoreTests(ctx, t, st)
}
