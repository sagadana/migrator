# Migrator

High-performant, easy-to-use data replication tool. Replicate data from any source to any destination with ease.

## Features

- Schema transformation
- Parallel Loading: Break data into chunks and load in parallel
- Auto Resuming: Resume from last success position if failed
- Continuous Replication: Watch for new changes and replicate them

## Datasources

| Datasource | Status  | Notes                                                                  |
| ---------- | ------- | ---------------------------------------------------------------------- |
| `File`     | Done    |
| `MongoDB`  | Done    | _Continuous Replication only available for replica set / cluster mode_ |
| `Redis`    | WIP     |                                                                        |
| `Postgres` | Planned |                                                                        |
| `<More>`   | Soon    |                                                                        |

## State Stores

_Used for storing replication states_

| Datasource | Status |
| ---------- | ------ |
| `Memory`   | Done   |
| `File`     | Done   |
| `Redis`    | WIP    |
| `<More>`   | Soon   |

## Install

`go get github.com/sagadana/migrator/datasources`

## Usage

### Migration

```go
package main

import (
	"github.com/sagadana/migrator/datasources"
	"github.com/sagadana/migrator/pipelines"
	"github.com/sagadana/migrator/states"
)

func main() {

    ctx, cancel := context.WithCancel(context.TODO())
    defer cancel()

    // Create the `From` datasource _(File Data Source in this example)_
    fromDs := datasources.NewFileDatasource("./tests", "test-from", "_id")
    // Load data from a CSV if needed
    err := fromDs.LoadCSV(&ctx, "./tests/sample-100.csv", /*batch size*/ 10)
    if err != nil {
        panic(err)
    }

    // Create the `To` datasource _(File Data Source in this example)_
    toDs := datasources.NewFileDatasource("./tests", "test-to", "_id")

    // Initialize Pipeline
    pipeline := pipelines.Pipeline{
        ID:    "test-pipeline-1",
        From:  fromDs,
        To:    toDs,
        Store: states.NewFileStateStore("./tests", "state"),
    }

    // Start Migration + Replication
    err = pipeline.Start(&ctx, pipelines.PipelineConfig{
        MigrationParallelLoad:    5,
        MigrationBatchSize:       10,

        OnMigrationStart:       func() { /* Add your logic. E.g extra logs */ },
        OnMigrationProgress:    func(count pipelines.MigrateCount) { /* Add your logic. E.g extra logs */ },
        OnMigrationStopped:     func(state states.State) { /* Add your logic. E.g extra logs */ },
    })
    if err != nil {
        panic(err)
    }

}

```

### Migration + Continuous Replication

```go

func main() {

    // .... (init codes)

    // Start Migration + Replication
    err = pipeline.Start(&ctx, pipelines.PipelineConfig{
        MigrationParallelLoad:      5,
        MigrationBatchSize:         10,

        ContinuousReplication:      true,
        ReplicationBatchSize:       20,
        ReplicationBatchWindowSecs: 1,

        OnMigrationStart:       func() { /* Add your logic. E.g extra logs */ },
        OnMigrationProgress:    func(count pipelines.MigrateCount) { /* Add your logic. E.g extra logs */ },
        OnMigrationStopped:     func(state states.State) { /* Add your logic. E.g extra logs */ },

        OnReplicationStart:    func() { /* Add your logic. E.g extra logs */ },
        OnReplicationProgress: func(count pipelines.MigrateCount) { /* Add your logic. E.g extra logs */ },
        OnReplicationError:    func(state states.State) { /* Add your logic. E.g extra logs */ },
        OnReplicationStopped:  func(state states.State) { /* Add your logic. E.g extra logs */ },
    })
    if err != nil {
        panic(err)
    }
}

```

### Continuous Replication Only

```go

func main() {

    // .... (init codes)

    // Start Replication
    err = pipeline.Stream(&ctx, pipelines.PipelineConfig{
        ContinuousReplication:      true,
        ReplicationBatchSize:       20,
        ReplicationBatchWindowSecs: 1,

        OnReplicationStart:     func() { /* Add your logic. E.g extra logs */ },
        OnReplicationProgress:  func(count pipelines.MigrateCount) { /* Add your logic. E.g extra logs */ },
        OnReplicationError:     func(state states.State) { /* Add your logic. E.g extra logs */ },
        OnReplicationStopped:   func(state states.State) { /* Add your logic. E.g extra logs */ },
    })
    if err != nil {
        panic(err)
    }
}

```

## Test

```sh
docker compose --env-file ./tests/.env  up tester
```
