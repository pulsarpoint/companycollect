# cc-enrich-worker — builds into bin/ (gitignored). Run from this directory.
BIN ?= bin/cc-enrich-worker
PKG := ./cmd/cc-enrich-worker
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
CGO_ENABLED ?= 1
BUILD_FLAGS ?= -trimpath

.PHONY: build arm test vet clean

build:                ## enrichment binary (CGO is required by DuckDB)
	mkdir -p $(dir $(BIN))
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_FLAGS) -o $(BIN) $(PKG)

arm:                  ## cross-compile linux/arm64 (Graviton)
	$(MAKE) build GOOS=linux GOARCH=arm64

test:                 ## go test ./...
	go test ./...

vet:                  ## go vet ./...
	go vet ./...

clean:                ## remove bin/
	rm -rf bin
