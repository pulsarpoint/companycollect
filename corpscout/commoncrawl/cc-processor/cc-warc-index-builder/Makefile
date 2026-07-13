BIN := bin/cc-warc-index-builder
VENV_BIN := .venv/bin/cc-warc-index-builder

.PHONY: build test clean

build:
	uv sync --frozen --no-dev
	mkdir -p bin
	ln -sf ../$(VENV_BIN) $(BIN)

test:
	uv sync --frozen
	uv run --frozen pytest -q

clean:
	rm -rf bin .pytest_cache
