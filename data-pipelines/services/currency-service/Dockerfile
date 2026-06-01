FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOWORK=off CGO_ENABLED=0 go build -o /currency-service ./cmd/currency-service

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /currency-service /currency-service
ENTRYPOINT ["/currency-service"]
