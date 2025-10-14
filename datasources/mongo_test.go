package datasources

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/sagadana/migrator/helpers"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestMongoDatasource(t *testing.T) {
	t.Parallel()

	id := "mongo-datasource"
	instanceId := helpers.RandomString(6)
	ctx := context.Background()

	mongoURI := os.Getenv("MONGO_URI")
	mongoDB := os.Getenv("MONGO_DB")

	td := TestDatasource{
		id:              id,
		withFilter:      true,
		withWatchFilter: true,
		withSort:        true,
		source: NewMongoDatasource(
			&ctx,
			MongoDatasourceConfigs{
				URI:            mongoURI,
				DatabaseName:   mongoDB,
				CollectionName: fmt.Sprintf("%s-%s", id, instanceId),
				AccurateCount:  true,
				Filter: map[string]any{
					TestFilterOutField: map[string]any{"$in": []any{nil, false, ""}},
				},
				Sort: map[string]any{
					TestSortAscField: 1, // 1 = Ascending, -1 = Descending
				},
				WithTransformer: func(data map[string]any) (map[string]any, error) {
					if data != nil {
						data[MongoDefaultIDField] = data[TestIDField]
					}
					return data, nil
				},
				OnInit: func(client *mongo.Client) error {
					return nil
				},
			},
		),
	}

	runDatasourceTest(ctx, t, td)
}
