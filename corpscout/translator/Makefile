GO ?= go
CGO_ENABLED := 1
BINARY_DIR ?= bin
API_BINARY ?= $(BINARY_DIR)/translator-api
TRIGGER_BINARY ?= $(BINARY_DIR)/translator-trigger
API_CMD ?= ./cmd/translator-api
TRIGGER_CMD ?= ./cmd/translator-trigger
SOURCE ?= norway_brreg

.PHONY: build build-api build-trigger test run trigger-load-and-run trigger-run trigger-load-queue clean

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

trigger-load-and-run:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(TRIGGER_CMD) -source $(SOURCE) -action load-and-run

trigger-run:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(TRIGGER_CMD) -source $(SOURCE) -action run

trigger-load-queue:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(TRIGGER_CMD) -source $(SOURCE) -action load-queue

clean:
	rm -rf $(BINARY_DIR)
