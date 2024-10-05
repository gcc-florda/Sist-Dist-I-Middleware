FROM golang:1.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o client.bin ./client

FROM busybox:latest

WORKDIR /app

COPY --from=builder /app/client.bin .

ENTRYPOINT ["./client.bin"]
