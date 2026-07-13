BIN ?= bin/cc-crawl
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
CGO_ENABLED ?= 0
BUILD_FLAGS ?= -trimpath

.PHONY: build test vet fmt clean

build:
	mkdir -p $(dir $(BIN))
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_FLAGS) -o $(BIN) .

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -rf bin
