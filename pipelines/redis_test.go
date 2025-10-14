package pipelines

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
)

func Test_Redis_X_Memory_Pipeline(t *testing.T) {
	t.Parallel()

	instanceId := helpers.RandomString(6)
	ctx := context.Background()

	// Create datasource name
	getDsName := func(id string, name string) string {
		return fmt.Sprintf("%s-%s-%s", id, instanceId, name)
	}

	var id string
	var fromDs datasources.Datasource
	var toDs datasources.Datasource
	var tp TestPipeline

	redisAddr := os.Getenv("REDIS_ADDR")
	redisUser := os.Getenv("REDIS_USER")
	redisPass := os.Getenv("REDIS_PASS")
	redisDb := os.Getenv("REDIS_DB")

	redisURI := fmt.Sprintf("redis://%s/%s", redisAddr, redisDb)
	if len(redisUser) > 0 && len(redisPass) > 0 {
		redisURI = fmt.Sprintf("redis://%s:%s@%s/%s", redisUser, redisPass, redisAddr, redisDb)
	}

	// --------------- 1. Memory to Redis --------------- //
	id = "test-mem-to-redis-pipeline"
	fromDs = datasources.NewMemoryDatasource(getDsName(id, "source"), IDField)
	toDs = datasources.NewRedisDatasource(
		&ctx,
		datasources.RedisDatasourceConfigs{
			URI:       redisURI,
			KeyPrefix: fmt.Sprintf("%s:*", getDsName(id, "destination")),
			IDField:   IDField,
			ScanSize:  10,
			OnInit: func(client *redis.Client) error {
				return nil
			},
		},
	)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
	}
	runPipelineTest(ctx, t, tp)

	// --------------- 2. Redis to Memory --------------- //
	id = "test-redis-to-mem-pipeline"
	fromDs = datasources.NewRedisDatasource(
		&ctx,
		datasources.RedisDatasourceConfigs{
			URI:       redisURI,
			IDField:   IDField,
			KeyPrefix: fmt.Sprintf("%s:", getDsName(id, "source")),
			ScanSize:  10,
			OnInit: func(client *redis.Client) error {
				return nil
			},
		},
	)
	toDs = datasources.NewMemoryDatasource(getDsName(id, "destination"), IDField)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
	}
	runPipelineTest(ctx, t, tp)
}
