# Contributing to Migrator

Thank you for considering contributing to [Migrator](https://github.com/sagadana/migrator)! We welcome all
contributions, whether it's bug reports, feature suggestions, or code contributions. Please follow the guidelines below
to ensure a smooth collaboration.

## Getting Started

1. **Fork the Repository**  
   Fork this repository to your GitHub account and clone it to your local machine:

   ```bash
   git clone https://github.com/sagadana/migrator.git
   ```

2. **Set Up Your Environment** Ensure you have Go installed (minimum version: 1.23.x). You can download it from
   <https://go.dev/dl/>.

3. **Install Dependencies** Run the following command to install any required dependencies:

   ```bash
   go mod tidy
   ```

## Terminologies

- **State Store**: Used for storing migration & replication states
- **Datasource**: Connector to data origin. E.g., Database, File, Bucket
- **Pipeline**: Migration or Replication integration to transfer data from one datasource to another

## Run Tests

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

## How to Contribute

### Reporting Issues

- Search existing issues to avoid duplicates.
- Provide a clear and descriptive title.
- Include steps to reproduce the issue, if applicable.

### Suggesting Features

- Open a new issue and label it as a "Feature Request."
- Describe the feature and its benefits.

### Submitting Code Changes

- Create a Branch
- Use a descriptive branch name:

```bash
git checkout -b [feat/fix]/your-feature-name
```

### Write Clean Code

- Follow Go's coding conventions.
- Run `go fmt ./...` to format your code.

### Add Tests

Ensure your changes are covered by tests. Our test suite is structured to have dedicated test files for each component
(datasource, state, pipeline). Each module (`datasources`, `states`, `pipelines`) contains a `base_test.go` file that
provides a generic test runner. To add a new test, you typically create a new `_test.go` file and use the provided test
runner.

**Datasources:**

To add a test for a new datasource, follow these steps:

1. Create a new file named `new_datasource_name_test.go` in the `datasources/` directory.
2. Inside this file, define a new test function (e.g., `TestNewDatasource`).
3. Instantiate your new datasource implementation.
4. Create a `TestDatasource` struct, which is defined in `datasources/base_test.go`. This struct wraps your datasource
   instance and provides metadata about its capabilities (e.g., if it supports sorting or filtering).
5. Call the `runDatasourceTest` function, passing the test context, testing object (`t`), and the `TestDatasource`
   struct you created.

Here is an example based on the memory datasource test (`datasources/memory_test.go`):

```go
package datasources

import (
 "context"
 "fmt"
 "testing"

 "github.com/sagadana/migrator/helpers"
)

func TestMyNewDatasource(t *testing.T) {
 t.Parallel()

 id := "mynew-datasource"
 instanceId := helpers.RandomString(6)
 ctx := context.Background()

 td := TestDatasource{
  id:     id,
  source: NewMyNewDatasource(fmt.Sprintf("%s-%s", id, instanceId), "id_field"),
  withFilter:      true, // set capabilities
  withSort:        true,
 }

 runDatasourceTest(ctx, t, td)
}
```

**States:**

To test a new state store, create a `new_state_name_test.go` file in the `states/` directory and use the `runStateTest`
function from `states/base_test.go`.

**Pipelines:**

For new pipelines, create a `new_pipeline_name_test.go` file in the `pipelines/` directory and use the `runPipelineTest`
function from `pipelines/base_test.go`.

**Helpers:**

For helper utilities, add your test cases to `helpers/utils_test.go`.

### Commit Your Changes

Write clear and concise commit messages:

```bash
git commit -m "[feat/fix]: Brief description"
```

### Push Your Changes

Push your branch to your forked repository:

```bash
git push origin feature/your-feature-name
```

### Open a Pull Request

- Go to the original repository and open a pull request.
- Provide a detailed description of your changes.

## Code of Conduct

Please adhere to our Code of Conduct to maintain a welcoming and inclusive environment.

Thank you for your contributions! 🎉
