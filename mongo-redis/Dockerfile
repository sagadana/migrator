FROM golang:bullseye
WORKDIR /app

COPY . /app/

RUN go mod download

# Compile the application
RUN go build -o cnc-db-migrate-mongo-redis .

CMD ["./cnc-db-migrate-mongo-redis"]