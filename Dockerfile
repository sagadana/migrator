
FROM golang:1.24.5-alpine AS builder
WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN apk add --no-cache make build-base
RUN go env -w CGO_ENABLED=1