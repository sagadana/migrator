# Database Migration Scripts

## Description

Collection of Go scripts to migrate data from one database to another.
It connects to both databases, retrieves data from the source, transforms it, and then stores it in the destination.

## Features

- Schema transformation
- Parrallel Loading: Break data into chunks and load in parrallel
- Auto Resuming: Resume from last success postion if failed
- Continuous Replication: Recursively check for new changes and replicate them

## Scripts

The following environment variables are required to run this program:

| Script                          | Description                   |
| ------------------------------- | ----------------------------- |
| [mongo-redis](./mongo-redis.go) | Migrate from MongoDB to Redis |

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
| `REDIS_PASSWORD`        | Password for Redis authentication                                    |
| `REDIS_DB`              | Redis database number                                                |
| `PARRALLEL_LOAD`        | Number of parallel loads                                             |
| `BATCH_SIZE`            | Number of documents to load in each batch                            |
| `MAX_SIZE`              | Optional: Maximum number of documents to load                        |
| `CONTINOUS_REPLICATION` | Optional: Enable continuous replication of new changes. `true/false` |

## Set Up

Follow these steps to set up the Go application:

1. **Install Go:**
   Download and install Go from the official website: [https://golang.org/dl/](https://golang.org/dl/)

2. **Install the packages:**
   Navigate to the project directory and run the following command to install the required packages:
   ```sh
   go get ./...
   ```
3. **Set up the .env file:**
   Create a `.env` file in the project directory and populate it with the required environment variables as shown in the `.env.example` file.

## Run

To execute the program, run the following command in the project directory:

**Dev**

```sh
go run <script>.go
```

**Docker**

```sh
docker build  -f Dockerfile.<script>  -t migrate-<script> . && docker run -it migrate-<script> -e <env_nanme_1>=<env_value_2> -e <env_name_n..>=<env_value_2..>
```
