# Migrator

[![release](https://github.com/sagadana/migrator/actions/workflows/release.yml/badge.svg)](https://github.com/sagadana/migrator/actions/workflows/release.yml)
[![CodeQL](https://github.com/sagadana/migrator/actions/workflows/codeql.yml/badge.svg)](https://github.com/sagadana/migrator/actions/workflows/codeql.yml)

High-performant, easy-to-use data replication tool. Replicate data from any source to any
destination with ease.

## Features

- **Source transformation**: Modify, rename, delete fields
- **Auto Resuming**: Resume from the last successful position - if failed
- **Batch Processing**: Process migration in batches
- **Parallel Processing**: Break data into chunks and load in parallel
- **Continuous Replication**: Watch for new changes and replicate them

## Datasources

| Datasource | Status  | Read(R) / Write(W) | Continuous Replication                 |
| ---------- | ------- | ------------------ | -------------------------------------- |
| `Memory`   | ✅      | R/W                | ✅                                     |
| `MongoDB`  | ✅      | R/W                | ✅ (_with replica set / cluster mode_) |
| `Redis`    | WIP     | TBC                | TBC                                    |
| `Postgres` | Planned | TBC                | TBC                                    |
| `<More>`   | Soon    | TBC                | TBC                                    |

## State Stores

| Store    | Status |
| -------- | ------ |
| `Memory` | ✅     |
| `File`   | ✅     |
| `Redis`  | ✅     |

## Install

```bash
go get github.com/sagadana/migrator
```

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

    // Create the `From` datasource _(Memory Data Source in this example)_
    fromDs := datasources.NewMemoryDatasource("test-from", "id")
    // Load data from a CSV if needed
    err := datasources.LoadCSV(&ctx, fromDs, "./tests/sample-100.csv", /*batch size*/ 10)
    if err != nil {
        panic(err)
    }

    // Create the `To` datasource _(Memory Data Source in this example)_
    toDs := datasources.NewMemoryDatasource("test-to", "id")

    // Initialize Pipeline
    pipeline := pipelines.Pipeline{
        ID:    "test-pipeline-1",
        From:  fromDs,
        To:    toDs,
        Store: states.NewFileStateStore("./tests", "state"),
    }

    // Start Migration Only
    err = pipeline.Start(&ctx, pipelines.PipelineConfig{
        MigrationParallelWorkers:    5,
        MigrationBatchSize:          10,

        OnMigrationStart:       func(state states.State) { /* Add your logic. E.g extra logs */ },
        OnMigrationError:       func(state states.State, err error) { /* Add your logic. E.g extra logs */ },
        OnMigrationProgress:    func(state states.State, count pipelines.DatasourcePushCount) { /* Add your logic. E.g extra logs */ },
        OnMigrationStopped:     func(state states.State) { /* Add your logic. E.g extra logs */ },
    }, /*with replication*/ false)
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
        MigrationParallelWorkers:      5,
        MigrationBatchSize:            10,

        ReplicationBatchSize:       20,
        ReplicationBatchWindowSecs: 1,

        OnMigrationStart:       func(state states.State) { /* Add your logic. E.g extra logs */ },
        OnMigrationError:       func(state states.State, data datasources.DatasourcePushRequest, err error) { /* Add your logic. E.g extra logs */ },
        OnMigrationProgress:    func(state states.State, count pipelines.DatasourcePushCount) { /* Add your logic. E.g extra logs */ },
        OnMigrationStopped:     func(state states.State) { /* Add your logic. E.g extra logs */ },

        OnReplicationStart:     func(state states.State) { /* Add your logic. E.g extra logs */ },
        OnReplicationProgress:  func(state states.State, count pipelines.DatasourcePushCount) { /* Add your logic. E.g extra logs */ },
        OnReplicationError:     func(state states.State, data datasources.DatasourcePushRequest, err error) { /* Add your logic. E.g extra logs */ },
        OnReplicationStopped:   func(state states.State) { /* Add your logic. E.g extra logs */ },
    }, /*with replication*/ true)
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
        ReplicationBatchSize:       20,
        ReplicationBatchWindowSecs: 1,

        OnReplicationStart:     func(state states.State) { /* Add your logic. E.g extra logs */ },
        OnReplicationProgress:  func(state states.State, count pipelines.DatasourcePushCount) { /* Add your logic. E.g extra logs */ },
        OnReplicationError:     func(state states.State, data datasources.DatasourcePushRequest, err error) { /* Add your logic. E.g extra logs */ },
        OnReplicationStopped:   func(state states.State) { /* Add your logic. E.g extra logs */ },
    })
    if err != nil {
        panic(err)
    }
}

```

## Test Packages

### Test States

```sh
docker compose --env-file ./tests/.env.dev  up tester-state
```

### Test Datasources

```sh
docker compose --env-file ./tests/.env.dev  up tester-ds
```

### Test Pipelines

```sh
docker compose --env-file ./tests/.env.dev  up tester-pipe
```

### Test Helpers

```sh
docker compose --env-file ./tests/.env.dev  up tester-helper
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
