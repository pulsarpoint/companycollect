# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine AS builder
WORKDIR /app/scheduler
COPY scheduler/go.mod scheduler/go.sum ./
COPY countrydata/go.mod countrydata/go.sum ../countrydata/
RUN --mount=type=cache,target=/go/pkg/mod \
    GOWORK=off go mod download
COPY scheduler/ ./
COPY countrydata/ ../countrydata/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOWORK=off CGO_ENABLED=0 go build -o bin/worker ./cmd/worker

FROM alpine:3.21
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/scheduler/bin/worker .
CMD ["./worker"]
