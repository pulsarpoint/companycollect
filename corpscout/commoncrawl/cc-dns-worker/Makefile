.PHONY: build test vet fmt
build:
	mkdir -p bin
	go build -o bin/cc-dns-worker ./cmd/cc-dns-worker
	go build -o bin/cc-axfr-worker ./cmd/cc-axfr-worker
test:
	go test ./...
vet:
	go vet ./...
fmt:
	go fmt ./...
