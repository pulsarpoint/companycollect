.PHONY: test build list-sources clean

BINARY ?= bin/companysource

test:
	GOWORK=off go test ./...

build:
	GOWORK=off go build -o $(BINARY) ./cmd/companysource

list-sources:
	GOWORK=off go run ./cmd/companysource list-sources

clean:
	rm -rf bin
