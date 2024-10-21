FROM golang:1.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./worker/ ./worker
COPY ./common/ ./common
COPY ./rabbitmq/ ./rabbitmq

RUN go build -o worker.bin ./worker

FROM busybox:latest

WORKDIR /app

COPY --from=builder /app/worker.bin .

ENTRYPOINT ["./worker.bin"]
