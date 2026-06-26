FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /cc-enrich-worker ./cmd/cc-enrich-worker

FROM gcr.io/distroless/static-debian12
COPY --from=build /cc-enrich-worker /cc-enrich-worker
ENTRYPOINT ["/cc-enrich-worker"]
