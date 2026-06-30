# Corpscout Translator Service

Standalone Go service for translation queue loading and translation runs.

The HTTP API is still a scaffold: the endpoints return accepted responses while
BRREG queue loading, queue processing, and ClickHouse upload are wired in behind
the service boundary. The queue and local LLM translation packages already have
runtime behavior and focused tests.

## Packages

```text
cmd/translator-api        HTTP server entrypoint
internal/api              Minimal router and health endpoint
internal/config           JSON + environment config loading
internal/brreg            Norway BRREG queue creation and static translations
internal/queue            DuckDB queue runtime for existing source queue files
internal/translation      OpenAI-compatible local LLM translator
```

`internal/queue` opens one existing DuckDB queue file per source. It does not
create the file or schema. Source packages such as `internal/brreg` own queue
file creation because they know the ClickHouse table, source columns, static
translation rules, and language pair. Queue startup fails if the file is missing
or does not contain the expected `input_items` and `output_items` tables.

`GetBatch` reads untranslated rows by subtracting `output_items` from
`input_items` inside that same DuckDB file. `SaveBatch` writes translated rows
to `output_items`; no rows are deleted from `input_items`. `ProcessBatch` is a
small runtime helper that gets a batch, calls the injected translator, and saves
successful results with explicit provider/model metadata.

`internal/translation` defines the translator boundary:

```go
Translate(ctx context.Context, items []TranslationInput, timeoutSeconds int) ([]TranslationResult, error)
```

The production provider is OpenAI-compatible and posts to
`{base_url}/chat/completions`, matching the old Python translator behavior. Tests
use fake translators and `httptest`; they do not call the real local LLM.

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
