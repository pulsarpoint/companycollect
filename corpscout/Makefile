.PHONY: up down logs rebuild rebuild-local scheduler-local-bin migrate-up migrate-down migrate-test-up migrate-test-down clickhouse-migrate-up clickhouse-migrate-down clickhouse-migrate-force source-fixtures-existing-artifacts source-list sqlc-generate test test-db

DATABASE_URL := $(or $(DATABASE_URL),$(shell sed -n 's/^DATABASE_URL=//p' .env 2>/dev/null | tail -n 1),postgres://corpscout:change-me@companycollect:5432/corpscout?sslmode=disable)
CORPSCOUT_TEST_DATABASE_URL := $(or $(CORPSCOUT_TEST_DATABASE_URL),$(shell sed -n 's/^CORPSCOUT_TEST_DATABASE_URL=//p' .env 2>/dev/null | tail -n 1),$(shell sed -n 's/^DATABASE_URL=//p' .env 2>/dev/null | tail -n 1 | sed 's|/corpscout?|/corpscout_test?|'),postgres://corpscout:change-me@companycollect:5432/corpscout_test?sslmode=disable)
CLICKHOUSE_MIGRATE_URL := $(or $(CLICKHOUSE_MIGRATE_URL),$(shell sed -n 's/^CLICKHOUSE_MIGRATE_URL=//p' .env 2>/dev/null | tail -n 1),clickhouse://companycollect:9002?username=default&password=change-me&database=corpscout&x-multi-statement=true)
COMPANYCOLLECT_HOST_IP := $(or $(COMPANYCOLLECT_HOST_IP),$(shell sed -n 's/^COMPANYCOLLECT_HOST_IP=//p' .env 2>/dev/null | tail -n 1),100.85.212.113)
CLICKHOUSE_MIGRATE_DOCKER_ARGS ?= --add-host companycollect:$(COMPANYCOLLECT_HOST_IP)
POSTGRES_DOCKER_ARGS ?= --add-host companycollect:$(COMPANYCOLLECT_HOST_IP)
SCHEDULER_GOOS ?= linux
SCHEDULER_GOARCH ?= $(shell go env GOARCH)

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

rebuild:
	docker compose up -d --build scheduler

scheduler-local-bin:
	cd scheduler && GOWORK=off CGO_ENABLED=0 GOOS=$(SCHEDULER_GOOS) GOARCH=$(SCHEDULER_GOARCH) go build -o bin/worker ./cmd/worker

rebuild-local: scheduler-local-bin
	docker compose -f docker-compose.yml -f docker-compose.local-build.yml up -d --build scheduler

migrate-up:
	@docker compose run --rm migrate -path=/migrations -database="$(DATABASE_URL)" up

migrate-down:
	@docker compose run --rm migrate -path=/migrations -database="$(DATABASE_URL)" down 1

migrate-test-up:
	@docker compose run --rm migrate -path=/migrations -database="$(CORPSCOUT_TEST_DATABASE_URL)" up

migrate-test-down:
	@docker compose run --rm migrate -path=/migrations -database="$(CORPSCOUT_TEST_DATABASE_URL)" down 1

clickhouse-migrate-up:
	@docker run --rm $(CLICKHOUSE_MIGRATE_DOCKER_ARGS) -v "$(CURDIR)/clickhouse/migrations:/clickhouse-migrations:ro" migrate/migrate:v4.17.0 -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)" up

clickhouse-migrate-down:
	@docker run --rm $(CLICKHOUSE_MIGRATE_DOCKER_ARGS) -v "$(CURDIR)/clickhouse/migrations:/clickhouse-migrations:ro" migrate/migrate:v4.17.0 -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)" down 1

clickhouse-migrate-force:
	@docker run --rm $(CLICKHOUSE_MIGRATE_DOCKER_ARGS) -v "$(CURDIR)/clickhouse/migrations:/clickhouse-migrations:ro" migrate/migrate:v4.17.0 -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)" force $(VERSION)

source-fixtures-existing-artifacts:
	@docker run --rm $(POSTGRES_DOCKER_ARGS) -v "$(CURDIR)/database/fixtures:/fixtures:ro" postgres:16-alpine psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f /fixtures/company_source_existing_artifact_runs.sql

source-list:
	cd scheduler && GOWORK=off go run ./cmd/corpscout-source list-sources

sqlc-generate:
	cd scheduler && GOWORK=off sqlc generate -f ../database/sqlc.yaml

test:
	@cd scheduler && GOWORK=off go test ./...

test-db:
	@cd scheduler && CORPSCOUT_TEST_DATABASE_URL="$(CORPSCOUT_TEST_DATABASE_URL)" GOWORK=off go test ./...
