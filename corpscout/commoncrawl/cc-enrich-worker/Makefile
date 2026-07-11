# cc-enrich-worker — builds into bin/ (gitignored). Run from this directory.
BIN := bin/cc-enrich-worker
PKG := ./cmd/cc-enrich-worker

.PHONY: build arm test vet clean

build:                ## static enrichment binary
	CGO_ENABLED=0 go build -o $(BIN) $(PKG)

arm:                  ## cross-compile linux/arm64 (Graviton)
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o $(BIN) $(PKG)

test:                 ## go test ./...
	go test ./...

vet:                  ## go vet ./...
	go vet ./...

clean:                ## remove bin/
	rm -rf bin
