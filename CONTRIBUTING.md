# Contributing to Migrator

Thank you for considering contributing to [Migrator](https://github.com/sagadana/migrator)!
We welcome all contributions, whether it's bug reports, feature suggestions, or code contributions. Please follow the guidelines below to ensure a smooth collaboration.

## Getting Started

1. **Fork the Repository**  
   Fork this repository to your GitHub account and clone it to your local machine:

   ```bash
   git clone https://github.com/sagadana/migrator.git
   ```

2. **Set Up Your Environment**
   Ensure you have Go installed (minimum version: 1.23.x). You can download it from <https://go.dev/dl/>.

3. **Install Dependencies**
   Run the following command to install any required dependencies:

   ```bash
   mod tidy
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

Ensure your changes are covered by tests.

**States:**

- Add new state to `getTestStates` function in [states_test.go](tests/states_test.go)
- Add new state store to `getTestStores` function in [pipelines_test.go](tests/pipelines_test.go)

**Datasources:**

- Add new datasource to `getTestDatasources` function in [datasources_test.go](tests/datasources_test.go)

**Pipelines:**

- Add new pipeline to `getTestPipelines` function in [pipelines_test.go](tests/pipelines_test.go)

**Helpers:**

- Add new helpers tests to [helpers_test.go](tests/helpers_test.go)

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
