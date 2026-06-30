# Corpscout Translator Service

Standalone Go service for translation queue loading and translation runs.

This is the initial API scaffold only. The endpoints return accepted responses;
DuckDB queue loading, Temporal workflow starts, ClickHouse writes, and provider
calls will be added behind this API boundary later.

## Configuration

The service reads non-secret configuration from:

```text
config/translator.json
```

Override the config file path:

```bash
TRANSLATOR_CONFIG_FILE=/path/to/translator.json go run ./cmd/translator-api
```

The config file declares endpoint credential environment variable names. Secret
values are read from environment variables and are not stored in JSON.

Current environment variables:

```text
TRANSLATOR_CONFIG_FILE
TRANSLATOR_API_ADDR
CLICKHOUSE_NATIVE_URL
TRANSLATOR_INTEGRATION_TESTS
TRANSLATION_PROVIDER_LOCAL_BASE_URL
TRANSLATION_PROVIDER_LOCAL_MODEL
TRANSLATION_PROVIDER_LOCAL_API_KEY
```

The local LLM endpoint uses the same settings as the existing Python translator:

```text
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888/v1
TRANSLATION_PROVIDER_LOCAL_MODEL=qwen3:6b
TRANSLATION_PROVIDER_LOCAL_API_KEY=not-needed
max_tokens=32768
extra_body={"chat_template_kwargs":{"enable_thinking":false}}
```

The ClickHouse connection is read from `CLICKHOUSE_NATIVE_URL`. The JSON config
only stores the env var name, so credentials stay in `.env`.

## Run

```bash
go run ./cmd/translator-api
```

Override the listen address:

```bash
TRANSLATOR_API_ADDR=:8090 go run ./cmd/translator-api
```

## Endpoints

```text
GET  /healthz
POST /v1/sources/{source}/load-queue
POST /v1/sources/{source}/run
```

Examples:

```bash
curl -s http://localhost:8080/healthz
curl -s -X POST http://localhost:8080/v1/sources/norway_brreg/load-queue
curl -s -X POST http://localhost:8080/v1/sources/norway_brreg/run
```

## Build

```bash
go build ./cmd/translator-api
```

## Integration Test

The BRREG integration test hits the existing ClickHouse database, builds a real
temporary DuckDB queue, and may insert missing static legal-form translations
into `corpscout.text_translations`.

```bash
CLICKHOUSE_NATIVE_URL="$(grep '^CLICKHOUSE_NATIVE_URL=' ../.env | cut -d= -f2-)" \
TRANSLATOR_INTEGRATION_TESTS=true \
TRANSLATOR_CONFIG_FILE=../../config/translator.json \
go test ./internal/brreg -run TestInitializeTranslationWithExistingClickHouse -v
```
