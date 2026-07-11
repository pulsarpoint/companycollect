BIN := bin/cc-download-worker
PKG := ./cmd/cc-download-worker

.PHONY: build arm test vet clean

build:
	CGO_ENABLED=0 go build -o $(BIN) $(PKG)

arm:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o $(BIN) $(PKG)

test:
	go test ./...
	PYTHONDONTWRITEBYTECODE=1 uv run --with duckdb==1.5.4 python -m unittest discover -s internal/worklistbuilder -p '*_test.py'

vet:
	go vet ./...

clean:
	rm -rf bin
