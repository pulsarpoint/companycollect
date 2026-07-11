# Common Crawl workers — build into bin/ (gitignored). Run from this directory.
ENRICH_BIN := bin/cc-enrich-worker
DOWNLOAD_BIN := bin/cc-download-worker

.PHONY: build enrich download arm test vet clean
build: enrich download

enrich:               ## static enrichment binary
	CGO_ENABLED=0 go build -o $(ENRICH_BIN) ./cmd/cc-enrich-worker

download:             ## static raw-download binary
	CGO_ENABLED=0 go build -o $(DOWNLOAD_BIN) ./cmd/cc-download-worker

arm:                  ## cross-compile both binaries for linux/arm64 (Graviton)
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o $(ENRICH_BIN) ./cmd/cc-enrich-worker
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o $(DOWNLOAD_BIN) ./cmd/cc-download-worker

test:                 ## go test ./...
	go test ./...

vet:                  ## go vet ./...
	go vet ./...

clean:                ## remove bin/
	rm -rf bin
