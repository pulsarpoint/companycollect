# corpscout/scheduler/Makefile
.PHONY: build test sqlc-generate migrate-up migrate-down

build:
	GOWORK=off go build -o bin/worker ./cmd/worker

test:
	GOWORK=off go test ./... -v

sqlc-generate:
	GOWORK=off sqlc generate -f ../database/sqlc.yaml

migrate-up:
	migrate -path ../database/migrations -database "$$DATABASE_URL" up

migrate-down:
	migrate -path ../database/migrations -database "$$DATABASE_URL" down 1
