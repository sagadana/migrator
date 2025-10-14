package pipelines

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/helpers"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func Test_Mongo_X_Memory_Pipeline(t *testing.T) {
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

	mongoURI := os.Getenv("MONGO_URI")
	mongoDB := os.Getenv("MONGO_DB")
	// Transformer to convert map to Mongo format
	mongoTransformer := func(data map[string]any) (map[string]any, error) {
		// Use to transform to include default mongo ID
		if data != nil {
			data[datasources.MongoDefaultIDField] = data[IDField]
		}
		return data, nil
	}

	// --------------- 1. Memory to Mongo --------------- //
	id = "test-mem-to-mongo-pipeline"
	fromDs = datasources.NewMemoryDatasource(getDsName(id, "source"), IDField)
	toDs = datasources.NewMongoDatasource(
		&ctx,
		datasources.MongoDatasourceConfigs{
			URI:             mongoURI,
			DatabaseName:    mongoDB,
			CollectionName:  getDsName(id, "destination"),
			Filter:          map[string]any{},
			Sort:            map[string]any{},
			AccurateCount:   true,
			WithTransformer: mongoTransformer, // Use transformer when loading into source
			OnInit: func(client *mongo.Client) error {
				return nil
			},
		},
	)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
		transform:   mongoTransformer,
	}
	runPipelineTest(ctx, t, tp)

	// --------------- 2. Mongo to Memory --------------- //
	id = "test-mongo-to-mem-pipeline"
	fromDs = datasources.NewMongoDatasource(
		&ctx,
		datasources.MongoDatasourceConfigs{
			URI:             mongoURI,
			DatabaseName:    mongoDB,
			CollectionName:  getDsName(id, "source"),
			Filter:          map[string]any{},
			Sort:            map[string]any{},
			AccurateCount:   true,
			WithTransformer: mongoTransformer, // Use transformer when loading into source
			OnInit: func(client *mongo.Client) error {
				return nil
			},
		},
	)
	toDs = datasources.NewMemoryDatasource(getDsName(id, "destination"), IDField)
	tp = TestPipeline{
		id:          id,
		source:      fromDs,
		destination: toDs,
		transform:   mongoTransformer,
	}
	runPipelineTest(ctx, t, tp)
}
