.PHONY: up down logs rebuild rebuild-local scheduler-local-bin migrate-up migrate-down migrate-test-up migrate-test-down sqlc-generate test test-db

DATABASE_URL := $(or $(DATABASE_URL),$(shell sed -n 's/^DATABASE_URL=//p' .env 2>/dev/null | tail -n 1),postgres://corpscout:change-me@companycollect:5432/corpscout?sslmode=disable)
CORPSCOUT_TEST_DATABASE_URL := $(or $(CORPSCOUT_TEST_DATABASE_URL),$(shell sed -n 's/^CORPSCOUT_TEST_DATABASE_URL=//p' .env 2>/dev/null | tail -n 1),$(shell sed -n 's/^DATABASE_URL=//p' .env 2>/dev/null | tail -n 1 | sed 's|/corpscout?|/corpscout_test?|'),postgres://corpscout:change-me@companycollect:5432/corpscout_test?sslmode=disable)
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

sqlc-generate:
	cd scheduler && GOWORK=off sqlc generate -f ../database/sqlc.yaml

test:
	@cd scheduler && GOWORK=off go test ./...

test-db:
	@cd scheduler && CORPSCOUT_TEST_DATABASE_URL="$(CORPSCOUT_TEST_DATABASE_URL)" GOWORK=off go test ./...
