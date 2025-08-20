package datasources

import (
	"context"

	"github.com/sagadana/migrator/helpers"
)

type DatasourcePushRequest struct {
	Inserts []map[string]any
	Updates map[string]map[string]any
	Deletes []string
}
type DatasourceFetchRequest struct {
	// Number of items to fetch. 0 to fetch all
	Size int64
	// Offset to start fetching from. 0 to start from beginning
	Offset int64
}
type DatasourceStreamRequest struct {
	// Number of items to batch. 0 to disable batching
	BatchSize int64
	// How long to wait to accumulate batch
	BatchWindowSeconds int64
}
type DatasourceFetchResult struct {
	Err   error
	Docs  []map[string]any
	Start int64
	End   int64
}
type DatasourceStreamResult struct {
	Err  error
	Docs DatasourcePushRequest
}

type Datasource interface {
	// Initialize data source
	Init()
	// Get total count
	Count(ctx *context.Context, request *DatasourceFetchRequest) int64
	// Get data
	Fetch(ctx *context.Context, request *DatasourceFetchRequest) DatasourceFetchResult
	// Insert/Update/Delete data
	Push(ctx *context.Context, request *DatasourcePushRequest) error
	// Listen to Change Data Streams (CDC) if available
	Watch(ctx *context.Context, request *DatasourceStreamRequest) <-chan DatasourceStreamResult
	// Clear data source
	Clear(ctx *context.Context) error
}

// Load CSV data into datasource
func LoadCSV(ctx *context.Context,
	ds Datasource, path string, batchSize int64,
	// Nullable transformer
	transformer func(data map[string]any) (map[string]any, error),
) error {
	result, err := helpers.StreamCSV(path, batchSize)
	if err != nil {
		return err
	}

	for data := range result {
		if transformer != nil {
			var err error
			for i, item := range data {
				item, err = transformer(item)
				if err != nil {
					data[i] = item
				}
			}
		}
		err = ds.Push(ctx, &DatasourcePushRequest{
			Inserts: data,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
