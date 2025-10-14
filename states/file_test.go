package states

import (
	"context"
	"testing"
)

func TestFileStateStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	id := "file-store"
	st := TestState{
		id:    id,
		store: NewFileStateStore(getStatesTempBasePath(), id),
	}

	runStateStoreTests(ctx, t, st)
}
