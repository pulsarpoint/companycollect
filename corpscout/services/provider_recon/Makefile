.PHONY: build test vet fmt live clean

build:
	mkdir -p bin
	go build -o bin/provider-recon ./cmd/provider-recon

test:
	go test ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

live:
	go test -tags live ./internal/feeds/ -run TestLive -v

clean:
	rm -rf bin
