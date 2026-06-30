GO ?= go
BINARY_DIR ?= bin
BINARY_NAME ?= translator-api
BINARY ?= $(BINARY_DIR)/$(BINARY_NAME)
CMD ?= ./cmd/translator-api

.PHONY: build test run clean

build:
	mkdir -p $(BINARY_DIR)
	$(GO) build -o $(BINARY) $(CMD)

test:
	$(GO) test ./...

run:
	$(GO) run $(CMD)

clean:
	rm -rf $(BINARY_DIR)
