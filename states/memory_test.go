package states

import (
	"context"
	"os"
	"strconv"
	"testing"
)

func TestMemoryStateStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	redisAddr := os.Getenv("REDIS_ADDR")
	redisPass := os.Getenv("REDIS_PASS")
	redisDb := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		db, err := strconv.Atoi(dbStr)
		if err != nil {
			redisDb = db
		}
	}

	id := "redis-store"
	st := TestState{
		id:    id,
		store: NewRedisStateStore(&ctx, redisAddr, redisPass, redisDb, id),
	}

	runStateStoreTests(ctx, t, st)
}
