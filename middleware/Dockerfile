FROM golang:1.23 AS builder

WORKDIR /app

COPY /go.mod /go.sum ./

RUN go mod download

COPY /*.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o middleware .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/middleware .

CMD ["./middleware"]
