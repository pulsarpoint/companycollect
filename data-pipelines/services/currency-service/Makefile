.PHONY: build test run down logs

build:
	GOWORK=off go build -o bin/currency-service ./cmd/currency-service

test:
	GOWORK=off go test ./...

run:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f
