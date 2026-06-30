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
internal/brreg            Norway BRREG workflow, queue loading, and translations
internal/orchestration    Central Temporal registration and workflow starts
internal/queue            DuckDB queue runtime for existing source queue files
internal/translation      Translator provider and shared batch translation logic
```

`internal/queue` opens one existing DuckDB queue file per source. It does not
create the file or schema. Source packages such as `internal/brreg` own queue
file creation because they know the ClickHouse table, source columns, static
translation rules, and language pair. Queue startup fails if the file is missing
or does not contain the expected `input_items`, `output_items`, and
`failed_items` tables.

`GetBatch` reads pending rows by subtracting `output_items` and `failed_items`
from `input_items` inside that same DuckDB file. `SaveBatch` writes translated
rows to `output_items`; `SaveFailed` writes irreducible failed rows to
`failed_items`; no rows are deleted from `input_items`. The queue package is
storage-only: it does not call the translator, know provider/model metadata, or
own batch processing policy.

Source runtimes such as BRREG are responsible for getting a batch from the
queue and saving successful or failed results back to the queue. The
source-independent translation policy lives in `internal/translation`
`TranslateItems`, so new source packages do not need to duplicate the same LLM
recovery behavior.

The BRREG runtime is owned by the BRREG Temporal worker/workflow path. Queue
access is serialized by that workflow: it loads input, processes one batch
activity at a time, continues as new when needed, and uploads output only after
the queue is empty. The runtime itself intentionally has no actor loop, request
channel, mutex, lease system, or second serialization layer. If this service is
changed to run multiple workers for the same source queue, that ownership model
must be changed explicitly instead of hidden inside `Runtime`.

`internal/translation.TranslateItems` handles deterministic LLM output failures
inside the activity. A normal provider/network error returns to Temporal and is
retried by the activity retry policy. A model-output error such as invalid JSON,
missing IDs, duplicate IDs, unexpected IDs, or empty translated text is handled
locally: first retry the same items in a deterministic shuffled order, then
split the item list in half after repeated model-output failure. If a single
item still cannot produce a valid translation, `TranslateItems` returns it as a
failed item so the source runtime can write it to `failed_items` and continue
with the rest of the queue.

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
TRANSLATOR_BATCHES_PER_RUN
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
POST /v1/sources/{source}/load-and-run
POST /v1/sources/{source}/run
```

Examples:

```bash
curl -s http://localhost:8080/healthz
curl -s -X POST http://localhost:8080/v1/sources/norway_brreg/load-and-run
curl -s -X POST http://localhost:8080/v1/sources/norway_brreg/load-queue
curl -s -X POST http://localhost:8080/v1/sources/norway_brreg/run
```

## Direct Temporal Trigger

Use the Go trigger command when you want to start the BRREG workflow directly
through Temporal instead of the HTTP API:

```bash
make trigger-brreg-load-and-run
make trigger-brreg-load-queue
make trigger-brreg-run
```

Or run it directly:

```bash
go run ./cmd/translator-trigger -action load-and-run
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

The `load-and-run` action first reloads new BRREG input from ClickHouse into the
DuckDB queue, then processes batches until the queue is empty and uploads the
output queue to ClickHouse. The `run` action only processes the existing DuckDB
queue; it does not query ClickHouse. The `load-queue` action only reloads the
input queue.

Queue runs use Temporal Continue-As-New after
`batches_per_run` / `TRANSLATOR_BATCHES_PER_RUN` processed batches. The next run
resumes with `run` against the same DuckDB queue, so progress is kept in
`output_items` and workflow history stays bounded.

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
./bin/translator-trigger -action load-and-run
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
