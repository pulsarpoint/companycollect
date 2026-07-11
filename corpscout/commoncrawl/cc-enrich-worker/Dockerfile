# Build from the commoncrawl directory:
# docker build -f cc-enrich-worker/Dockerfile .
FROM golang:1.26 AS build
WORKDIR /src
COPY cc-raw ./cc-raw
COPY cc-enrich-worker ./cc-enrich-worker
WORKDIR /src/cc-enrich-worker
RUN go mod download
RUN CGO_ENABLED=0 go build -o /cc-enrich-worker ./cmd/cc-enrich-worker

FROM gcr.io/distroless/static-debian12
COPY --from=build /cc-enrich-worker /cc-enrich-worker
ENTRYPOINT ["/cc-enrich-worker"]
