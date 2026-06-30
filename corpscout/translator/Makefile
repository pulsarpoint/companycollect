GO ?= go
CGO_ENABLED := 1
BINARY_DIR ?= bin
BINARY_NAME ?= translator-api
BINARY ?= $(BINARY_DIR)/$(BINARY_NAME)
CMD ?= ./cmd/translator-api

.PHONY: build test run clean

build:
	mkdir -p $(BINARY_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o $(BINARY) $(CMD)

test:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test ./...

run:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(CMD)

clean:
	rm -rf $(BINARY_DIR)
