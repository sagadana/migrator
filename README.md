# Database Migration Scripts

## Description

Collection of Go scripts to migrate data from one database to another.
It connects to both databases, retrieves data from the source, transforms it, and then stores it in the destination.

## Script(s)

### [mongo-redis](./mongo-redis)

Migrate from Mongo to Redis

#### Features

- Schema transformation
- Parallel Loading: Break data into chunks and load in parallel
- Auto Resuming: Resume from last success position if failed
- Continuous Replication: Recursively check for new changes and replicate them (TODO: Use Change Stream)

#### Environment Variables

The following environment variables are required to run this program:

| Variable                 | Description                                                              |
| ------------------------ | ------------------------------------------------------------------------ |
| `MONGO_DATABASE_NAME`    | Name of the MongoDB database                                             |
| `MONGO_COLLECTION_NAME`  | Name of the MongoDB collection                                           |
| `MONGO_ADDR`             | Address of the MongoDB server                                            |
| `MONGO_USERNAME`         | Username for MongoDB authentication                                      |
| `MONGO_PASSWORD`         | Password for MongoDB authentication                                      |
| `MONGO_ID_FIELD`         | Optional: Field to use as the unique identifier. Default is `_id`        |
| `MONGO_EXCLUDE_ID_FIELD` | Optional: Exclude the `_id` field from the migration. Default is `false` |
| `REDIS_ADDR`             | Address of the Redis server                                              |
| `REDIS_USERNAME`         | Username for Redis authentication                                        |
| `REDIS_PASSWORD`         | Optional: Password for Redis authentication                              |
| `REDIS_DB`               | Redis database number                                                    |
| `REDIS_CLUSTER`          | Redis is a cluster                                                       |
| `PARRALLEL_LOAD`         | Number of parallel loads                                                 |
| `BATCH_SIZE`             | Number of documents to load in each batch                                |
| `MAX_SIZE`               | Optional: Maximum number of documents to load                            |
| `START_OFFSET`           | Optional: Minimum offset to start migration from                         |
| `CONTINOUS_REPLICATION`  | Optional: Enable continuous replication of new changes. `true/false`     |

#### Schema Transformation

- Update the `transform.go` file to modify the transformation logic as per your requirements.
- The `Transform` function is responsible for converting the MongoDB document into the format required by Redis.

## Set Up

1. Install `make`

```sh
# MAC
brew install make
# Linux
sudo apt-get install make
# Windows
choco install make
```

2. Install dependencies

```sh
make install
```

3. Modify the `.env` file in the `<script>` directory with the required environment variables.

## Run

To execute the program, run the following command in the project directory:

**Dev**

```sh
make <script>-dev
```

**Docker**

```sh
make <script>-docker
```

## Push (AWS ECR)

This will build the docker image and push it to an ECR repository.

First, sign in to the correct AWS Account

_Note: Ensure that the infrastructure used to run the migration task has the same [cpuArchitecture](https://repost.aws/knowledge-center/ecs-task-exec-format-error) as the your system used to build the image._

```sh
make <script>-push-aws
```
