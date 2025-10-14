package datasources

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/sagadana/migrator/helpers"
)

func TestRedisDatasource(t *testing.T) {
	t.Parallel()

	instanceId := helpers.RandomString(6)
	ctx := context.Background()

	redisAddr := os.Getenv("REDIS_ADDR")
	redisUser := os.Getenv("REDIS_USER")
	redisPass := os.Getenv("REDIS_PASS")
	redisDb := os.Getenv("REDIS_STATE_DB")

	redisURI := fmt.Sprintf("redis://%s/%s", redisAddr, redisDb)
	if len(redisUser) > 0 && len(redisPass) > 0 {
		redisURI = fmt.Sprintf("redis://%s:%s@%s/%s", redisUser, redisPass, redisAddr, redisDb)
	}

	// --------------- Redis Hash & Without Pefix --------------- //
	id := "redis-hash-datasource"
	td := TestDatasource{
		id:              id,
		withFilter:      false,
		withWatchFilter: false,
		withSort:        false,
		synchronous:     true, // Run synchronously to prevent overlap with "redis-json-datasource" test
		source: NewRedisDatasource(
			&ctx,
			RedisDatasourceConfigs{
				URI:      redisURI,
				IDField:  TestIDField,
				ScanSize: 10,
				OnInit: func(client *redis.Client) error {
					return nil
				},
			},
		),
	}
	runDatasourceTest(ctx, t, td)

	// --------------- Redis JSON & With Prefix --------------- //
	id = "redis-json-datasource"
	td = TestDatasource{
		id:              id,
		withFilter:      false,
		withWatchFilter: false,
		withSort:        false,
		source: NewRedisDatasource(
			&ctx,
			RedisDatasourceConfigs{
				URI:       redisURI,
				KeyPrefix: fmt.Sprintf("%s-%s:", id, instanceId),
				IDField:   TestIDField,
				ScanSize:  10,
				WithTransformer: func(data map[string]any) (RedisInputSchema, error) {
					return JSONToRedisJSONInputSchema(data), nil
				},
				OnInit: func(client *redis.Client) error {
					return nil
				},
			},
		),
	}
	runDatasourceTest(ctx, t, td)
}
