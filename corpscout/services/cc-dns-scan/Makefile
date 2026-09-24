.PHONY: build test vet fmt clean

build:
	mkdir -p bin
	go build -o bin/cc-dns-scan ./cmd/cc-dns-scan

test:
	go test ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

clean:
	rm -rf bin
