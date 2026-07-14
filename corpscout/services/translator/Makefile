GO ?= go
CGO_ENABLED := 0
BINARY_DIR ?= bin
API_BINARY ?= $(BINARY_DIR)/translator-api
TRIGGER_BINARY ?= $(BINARY_DIR)/translator-trigger
API_CMD ?= ./cmd/translator-api
TRIGGER_CMD ?= ./cmd/translator-trigger

.PHONY: build build-api build-trigger test run trigger clean

build: build-api build-trigger

build-api:
	mkdir -p $(BINARY_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o $(API_BINARY) $(API_CMD)

build-trigger:
	mkdir -p $(BINARY_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o $(TRIGGER_BINARY) $(TRIGGER_CMD)

test:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test ./...

run:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(API_CMD)

trigger:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(TRIGGER_CMD)

clean:
	rm -rf $(BINARY_DIR)
