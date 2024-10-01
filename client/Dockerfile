FROM golang:1.23 AS builder

WORKDIR /build

COPY /go.mod /go.sum ./

RUN go mod download

COPY /*.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /build/bin/client /client

ENTRYPOINT ["/client"]
