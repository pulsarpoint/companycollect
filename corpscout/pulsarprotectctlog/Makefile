# pulsarprotectctlog — build & development tasks

BINARY     := ctlog
CMD        := ./cmd/ctlog
BIN_DIR    := bin
LINUX_BIN  := $(BIN_DIR)/$(BINARY)-linux-amd64

GO         ?= go
GOFLAGS    ?=

# ClickHouse schema migrations (golang-migrate in Docker). The connection URL is
# built from the CTLOG_CLICKHOUSE_* values in .env; set CLICKHOUSE_MIGRATE_URL
# directly to override (e.g. if the password needs URL-encoding). The migrate
# container resolves `companycollect` via --add-host COMPANYCOLLECT_HOST_IP.
CH_MIG_DIR     := clickhouse/migrations
MIGRATE_IMAGE  ?= migrate/migrate:v4.17.0
_CH_ADDR       := $(or $(shell sed -n 's/^CTLOG_CLICKHOUSE_ADDR=//p' .env 2>/dev/null | tail -n1),companycollect:9002)
_CH_USER       := $(or $(shell sed -n 's/^CTLOG_CLICKHOUSE_USER=//p' .env 2>/dev/null | tail -n1),default)
_CH_PASS       := $(shell sed -n 's/^CTLOG_CLICKHOUSE_PASSWORD=//p' .env 2>/dev/null | tail -n1)
_CH_DB         := $(or $(shell sed -n 's/^CTLOG_CLICKHOUSE_DATABASE=//p' .env 2>/dev/null | tail -n1),ctlogs)
CLICKHOUSE_MIGRATE_URL := $(or $(CLICKHOUSE_MIGRATE_URL),clickhouse://$(_CH_ADDR)?username=$(_CH_USER)&password=$(_CH_PASS)&database=$(_CH_DB)&x-multi-statement=true)
COMPANYCOLLECT_HOST_IP := $(or $(COMPANYCOLLECT_HOST_IP),$(shell sed -n 's/^COMPANYCOLLECT_HOST_IP=//p' .env 2>/dev/null | tail -n1),100.85.212.113)
CLICKHOUSE_MIGRATE_DOCKER_ARGS ?= --add-host companycollect:$(COMPANYCOLLECT_HOST_IP)
_MIGRATE_RUN    = docker run --rm $(CLICKHOUSE_MIGRATE_DOCKER_ARGS) -v "$(CURDIR)/$(CH_MIG_DIR):/clickhouse-migrations:ro" $(MIGRATE_IMAGE) -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)"

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the ctlog binary into bin/
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(BINARY) $(CMD)

.PHONY: build-linux
build-linux: ## Cross-compile a static linux/amd64 binary (for the server)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -o $(LINUX_BIN) $(CMD)

.PHONY: test
test: ## Run all tests
	$(GO) test ./...

.PHONY: test-race
test-race: ## Run tests with the race detector
	$(GO) test -race ./...

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format all code in place
	gofmt -w .

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-clean
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

.PHONY: lint
lint: ## Run golangci-lint (must be installed)
	golangci-lint run

.PHONY: tidy
tidy: ## Tidy go.mod / go.sum
	$(GO) mod tidy

.PHONY: clickhouse-migrate-up
clickhouse-migrate-up: ## Apply ClickHouse migrations (golang-migrate in Docker)
	@$(_MIGRATE_RUN) up

.PHONY: clickhouse-migrate-down
clickhouse-migrate-down: ## Roll back the last ClickHouse migration
	@$(_MIGRATE_RUN) down 1

.PHONY: clickhouse-migrate-force
clickhouse-migrate-force: ## Clear the dirty flag at a version after a failed run (VERSION=n)
	@$(_MIGRATE_RUN) force $(VERSION)

.PHONY: clickhouse-migrate-version
clickhouse-migrate-version: ## Print the current applied migration version
	@$(_MIGRATE_RUN) version

.PHONY: check
check: fmt-check vet test ## Run fmt-check, vet, and tests (CI gate)

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR)
