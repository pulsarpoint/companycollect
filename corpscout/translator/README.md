# Corpscout Translator Service

Standalone Go service for BRREG translation queue loading and translation runs.

The service owns the Norway BRREG DuckDB queue, registers the BRREG Temporal
workflow and activities, exposes HTTP triggers, and uses the configured local
LLM endpoint for dynamic translations.

## Packages

```text
cmd/translator-api        HTTP server entrypoint
cmd/translator-trigger    CLI trigger for the BRREG Temporal workflow
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
`{base_url}/chat/completions`. The prompt template is owned by the translation
package and compiled by `translation.Init`. Config only provides the source and
target language names used to fill that template. Tests use fake translators and
`httptest`; they do not call the real local LLM.

## Configuration

The service reads non-secret configuration from:

```text
config/translator.json
```

Local environment variables can be started from:

```bash
cp .env.example .env
set -a
. ./.env
set +a
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
TEMPORAL_ADDRESS
CLICKHOUSE_HOST
CLICKHOUSE_NATIVE_PORT
CLICKHOUSE_HTTP_PORT
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE
CLICKHOUSE_SECURE
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
prompt_data.source_language=Norwegian
prompt_data.target_language=English
extra_body={"chat_template_kwargs":{"enable_thinking":false}}
```

The ClickHouse connection is resolved from the same `CLICKHOUSE_*` environment
variables used by `dagster_v3`. The Go service uses `CLICKHOUSE_NATIVE_PORT`
because `clickhouse-go` connects over the native protocol.

## Run

```bash
make run
```

The service expects ClickHouse and Temporal to be reachable at startup. It reads
configuration from `config/translator.json` plus the environment variables
listed above.

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

## Direct Temporal Trigger

Use the Go trigger command when you want to start the BRREG workflow directly
through Temporal instead of the HTTP API:

```bash
make trigger-brreg-load-queue
make trigger-brreg-run
```

Or run it directly:

```bash
go run ./cmd/translator-trigger -action load-queue
go run ./cmd/translator-trigger -action run
```

The command uses Temporal `SignalWithStartWorkflow` with:

```text
workflow_id=translator/norway_brreg
workflow_type=NorwayBRREGWorkflow
task_queue=translator-norway-brreg
signal_name=source-action
```

The `run` action processes batches until the input queue is empty, then uploads
the output queue to ClickHouse. The `load-queue` action reloads new BRREG input
from ClickHouse into the DuckDB queue.

If the Temporal CLI is installed, `scripts/trigger-brreg-workflow.sh` can also
send the same `source-action` signal through `temporal workflow
signal-with-start`.

## Build

DuckDB is linked through `github.com/marcboeker/go-duckdb`, which requires cgo.
The Makefile sets `CGO_ENABLED=1` for `build`, `test`, and `run`.

On Debian/Ubuntu Linux, install a C/C++ toolchain before building:

```bash
apt-get update
apt-get install -y build-essential
```

If cgo is disabled, Go will fail inside `go-duckdb` with errors such as
`undefined: bindings.Type` or `undefined: bindings.State`.

```bash
make build
```

This writes the binary to:

```text
bin/translator-api
bin/translator-trigger
```

Run the compiled binary from the `translator` directory:

```bash
./bin/translator-api
```

Trigger BRREG through the compiled CLI:

```bash
./bin/translator-trigger -action run
./bin/translator-trigger -action load-queue
```

Clean build outputs:

```bash
make clean
```

Run the package test suite:

```bash
make test
```

## Integration Test

The BRREG integration test hits the existing ClickHouse database, builds a real
temporary DuckDB queue, and may insert missing static legal-form translations
into `corpscout.text_translations`.

```bash
set -a
. ./.env
set +a
TRANSLATOR_INTEGRATION_TESTS=true go test ./internal/brreg -run TestCreateInputQueueWithExistingClickHouseProducesNorwayBRREGDuckDBEntries -v
```
