.PHONY: up down logs rebuild migrate-up migrate-down sqlc-generate test

DATABASE_URL := $(or $(DATABASE_URL),$(shell sed -n 's/^DATABASE_URL=//p' .env 2>/dev/null | tail -n 1),postgres://corpscout:change-me@companycollect:5432/corpscout?sslmode=disable)

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

rebuild:
	docker compose up -d --build scheduler

migrate-up:
	docker compose run --rm migrate -path=/migrations -database="$(DATABASE_URL)" up

migrate-down:
	docker compose run --rm migrate -path=/migrations -database="$(DATABASE_URL)" down 1

sqlc-generate:
	cd scheduler && GOWORK=off sqlc generate -f ../database/sqlc.yaml

test:
	cd scheduler && GOWORK=off go test ./...
