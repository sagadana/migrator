package datasources

import (
	"context"
	"fmt"
	"testing"

	"github.com/sagadana/migrator/helpers"
)

func TestMemoryDatasource(t *testing.T) {
	t.Parallel()

	id := "memory-datasource"
	instanceId := helpers.RandomString(6)
	ctx := context.Background()

	td := TestDatasource{
		id:     id,
		source: NewMemoryDatasource(fmt.Sprintf("%s-%s", id, instanceId), TestIDField),
	}

	runDatasourceTest(ctx, t, td)
}
