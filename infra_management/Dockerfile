FROM golang:1.23-alpine AS builder

RUN apk add --no-cache docker git

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY ./infra_management/ ./infra_management
COPY ./common/ ./common

RUN go build -o infra_management.bin ./infra_management

FROM busybox:latest

WORKDIR /app

COPY --from=builder /app/infra_management.bin .

ENTRYPOINT ["./infra_management.bin"]
