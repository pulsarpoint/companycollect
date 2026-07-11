# Build from the commoncrawl directory:
# docker build -f cc-download-worker/Dockerfile .
FROM golang:1.26 AS build
WORKDIR /src
COPY cc-raw ./cc-raw
COPY cc-download-worker ./cc-download-worker
WORKDIR /src/cc-download-worker
RUN go mod download
RUN CGO_ENABLED=0 go build -o /cc-download-worker ./cmd/cc-download-worker

FROM python:3.13-slim
RUN pip install --no-cache-dir duckdb==1.5.4
COPY --from=build /cc-download-worker /cc-download-worker
ENTRYPOINT ["/cc-download-worker"]
