.PHONY: up down logs rebuild rebuild-local scheduler-local-bin migrate-up migrate-down migrate-test-up migrate-test-down clickhouse-migrate-up clickhouse-migrate-down clickhouse-generate-finland-prhytj clickhouse-import-finland-prhytj sqlc-generate test test-db

DATABASE_URL := $(or $(DATABASE_URL),$(shell sed -n 's/^DATABASE_URL=//p' .env 2>/dev/null | tail -n 1),postgres://corpscout:change-me@companycollect:5432/corpscout?sslmode=disable)
CORPSCOUT_TEST_DATABASE_URL := $(or $(CORPSCOUT_TEST_DATABASE_URL),$(shell sed -n 's/^CORPSCOUT_TEST_DATABASE_URL=//p' .env 2>/dev/null | tail -n 1),$(shell sed -n 's/^DATABASE_URL=//p' .env 2>/dev/null | tail -n 1 | sed 's|/corpscout?|/corpscout_test?|'),postgres://corpscout:change-me@companycollect:5432/corpscout_test?sslmode=disable)
CLICKHOUSE_MIGRATE_URL := $(or $(CLICKHOUSE_MIGRATE_URL),$(shell sed -n 's/^CLICKHOUSE_MIGRATE_URL=//p' .env 2>/dev/null | tail -n 1),clickhouse://clickhouse:9000?username=default&password=change-me&database=corpscout_sources&x-multi-statement=true)
FINLAND_PRHYTJ_EXPORT_DIR ?= ../companies/data/finland/countrydata/sources/prhytj/exports/20260607T205519Z-prhytj
FINLAND_PRHYTJ_SOURCE_EXPORT_ID ?= 00000000-0000-0000-0000-000000000000
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
	@docker compose run --rm clickhouse-migrate -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)" up

clickhouse-migrate-down:
	@docker compose run --rm clickhouse-migrate -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)" down 1

clickhouse-generate-finland-prhytj:
	@test -f clickhouse/sources/finland_prhytj.yaml || { echo "clickhouse-generate-finland-prhytj requires clickhouse/sources/finland_prhytj.yaml; run the Finland PRH YTJ ClickHouse source config task first." >&2; exit 1; }
	@test -d clickhouse/tools/parquetddl || { echo "clickhouse-generate-finland-prhytj requires clickhouse/tools/parquetddl; run the Parquet DDL generator task first." >&2; exit 1; }
	cd clickhouse && GOWORK=off go run ./tools/parquetddl \
		--source finland_prhytj \
		--database corpscout_sources \
		--export-dir "$(FINLAND_PRHYTJ_EXPORT_DIR)" \
		--config sources/finland_prhytj.yaml \
		--out migrations/000002_create_finland_prhytj_tables.up.sql \
		--down-out migrations/000002_create_finland_prhytj_tables.down.sql

clickhouse-import-finland-prhytj:
	@test -f clickhouse/sources/finland_prhytj.yaml || { echo "clickhouse-import-finland-prhytj requires clickhouse/sources/finland_prhytj.yaml; run the Finland PRH YTJ ClickHouse source config task first." >&2; exit 1; }
	@test -d clickhouse/tools/chimport || { echo "clickhouse-import-finland-prhytj requires clickhouse/tools/chimport; run the ClickHouse import tool task first." >&2; exit 1; }
	cd clickhouse && GOWORK=off go run ./tools/chimport \
		--database corpscout_sources \
		--source-export-id "$(FINLAND_PRHYTJ_SOURCE_EXPORT_ID)" \
		--export-dir "$(FINLAND_PRHYTJ_EXPORT_DIR)" \
		--config sources/finland_prhytj.yaml \
		--compose-file ../docker-compose.yml

sqlc-generate:
	cd scheduler && GOWORK=off sqlc generate -f ../database/sqlc.yaml

test:
	@cd scheduler && GOWORK=off go test ./...

test-db:
	@cd scheduler && CORPSCOUT_TEST_DATABASE_URL="$(CORPSCOUT_TEST_DATABASE_URL)" GOWORK=off go test ./...
