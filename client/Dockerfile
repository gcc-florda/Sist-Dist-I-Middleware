FROM golang:1.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./client ./client
COPY ./utils ./utils

WORKDIR /app/client

RUN go build -o client .
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/client middleware/client

CMD ["./client"]
