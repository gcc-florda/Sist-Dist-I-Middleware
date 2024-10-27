FROM golang:1.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./server/ ./server
COPY ./common/ ./common
COPY ./rabbitmq/ ./rabbitmq
COPY ./worker/schema/ ./worker/schema

RUN go build -o server.bin ./server

FROM busybox:latest

WORKDIR /app

COPY --from=builder /app/server.bin .

ENTRYPOINT ["./server.bin"]
