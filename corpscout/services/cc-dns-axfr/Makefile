.PHONY: build test test-race vet clean

BINARY := bin/cc-dns-axfr

build:
	mkdir -p bin
	go build -o $(BINARY) ./cmd/cc-dns-axfr

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

clean:
	rm -rf bin
