# Database Migration Scripts

## Description

Collection of Go scripts to migrate data from one database to another.
It connects to both databases, retrieves data from the source, transforms it, and then stores it in the destination.

## Features

- Schema transformation
- Parallel Loading: Break data into chunks and load in parallel
- Auto Resuming: Resume from last success position if failed
- Continuous Replication: Recursively check for new changes and replicate them

## Scripts

The following environment variables are required to run this program:

| Script                       | Description                                 |
| ---------------------------- | ------------------------------------------- |
| [mongo-redis](./mongo-redis) | Migrate from DocumentDB(Mongo) to Redis |

## Environment Variables

The following environment variables are required to run this program:

| Variable                | Description                                                          |
| ----------------------- | -------------------------------------------------------------------- |
| `MONGO_DATABASE_NAME`   | Name of the MongoDB database                                         |
| `MONGO_COLLECTION_NAME` | Name of the MongoDB collection                                       |
| `MONGO_ADDR`            | Address of the MongoDB server                                        |
| `MONGO_USERNAME`        | Username for MongoDB authentication                                  |
| `MONGO_PASSWORD`        | Password for MongoDB authentication                                  |
| `REDIS_ADDR`            | Address of the Redis server                                          |
| `REDIS_USERNAME`        | Username for Redis authentication                                    |
| `REDIS_PASSWORD`        | Password for Redis authentication                                    |
| `REDIS_DB`              | Redis database number                                                |
| `REDIS_CLUSTER`         | Redis is a cluster                                                   |
| `PARRALLEL_LOAD`        | Number of parallel loads                                             |
| `BATCH_SIZE`            | Number of documents to load in each batch                            |
| `MAX_SIZE`              | Optional: Maximum number of documents to load                        |
| `START_OFFSET`          | Optional: Minimum offset to start migration from                     |
| `CONTINOUS_REPLICATION` | Optional: Enable continuous replication of new changes. `true/false` |

## Set Up

Install `make`

```sh
brew install make
```

Run install command for each migration script

```sh
make install-<script>
```

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

## Deploy

Sign in to the correct AWS Account via [Seek Auth](https://github.com/SEEK-Jobs/aws-auth-bash)

This will build the docker image and deploy it to an ECR repository.

_Note: Ensure that the infrastructure used to run the migration task has the same [cpuArchitecture](https://repost.aws/knowledge-center/ecs-task-exec-format-error) as the your system used to build the image._

**Staging**

```sh
make <script>-stag
```
