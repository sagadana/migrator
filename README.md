# Migrator

<div align="start">
 <span>
  <img src="https://img.shields.io/github/v/release/sagadana/migrator?display_name=tag&sort=semver&logo=github&label=version" alt="Release%20by%20tag" />
  <img src="https://img.shields.io/github/release-date/sagadana/migrator?display_name=tag&sort=semver&logo=github&label=date" alt="Release%20by%20date" />
  <a href="https://goreportcard.com/report/github.com/sagadana/migrator" target="_blank"><img src="https://goreportcard.com/badge/github.com/sagadana/migrator" alt="Report" /></a>
  <a href="https://app.codacy.com/gh/sagadana/migrator/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade" target="_blank"><img src="https://app.codacy.com/project/badge/Grade/d1b34caebf6e489da2da791249f38d73" alt="Code%20Quality" /></a>
  <a href="https://github.com/sagadana/migrator/actions/workflows/codeql.yml" target="_blank"><img src="https://github.com/sagadana/migrator/actions/workflows/codeql.yml/badge.svg" alt="CodeQL" /></a>
  <a href="https://github.com/sagadana/migrator/actions/workflows/release.yml" target="_blank"><img src="https://github.com/sagadana/migrator/actions/workflows/release.yml/badge.svg" alt="Tests" /></a>
 </span>
</div>

<br/>

<div align="start">
High-performant, easy-to-use data replication tool. Replicate data from any source to any destination with ease.
</div>

## Features

- **Source transformation**: Modify, rename, delete fields
- **Auto Resuming**: Resume from the last successful position - if failed
- **Batch Migration**: Process migration in batches
- **Parallel Migration**: Break data into chunks and load in parallel
- **Continuous Replication**: Watch for new changes and replicate them
- **Import/Export**: Import/Export data from/to CSV files

## Datasources

| Datasource | Status  | Read | Write | Migrate | Replicate                          |
| ---------- | ------- | ---- | ----- | ------- | ---------------------------------- |
| `Memory`   | ✅      | ✅   | ✅    | ✅      | ✅                                 |
| `MongoDB`  | ✅      | ✅   | ✅    | ✅      | ✅ (_with change streams_)         |
| `Redis`    | ✅      | ✅   | ✅    | ✅      | ✅ (_with keyspace notifications_) |
| `Postgres` | ✅      | ✅   | ✅    | ✅      | ✅ (_with logical replication_)    |
| `MySQL`    | Planned | TBC  | TBC   | TBC     | TBC                                |
| `<More>`   | Soon    | TBC  | TBC   | TBC     | TBC                                |

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
    err := fromDs.Import(&ctx, datasources.DatasourceImportRequest{
        Type:      datasources.DatasourceImportTypeCSV,
        Source:    datasources.DatasourceImportSourceFile,
        Location:  "./tests/sample-100.csv",
        BatchSize: 10,
    })
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

## Linting

Run this to catch lint issues

```sh
 docker compose --env-file ./tests/.env.dev  up lint
```

## Testing

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
