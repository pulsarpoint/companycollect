# Corpscout Translator Service

Standalone Go service that owns one shared translation queue and one
Temporal workflow. It has zero per-source configuration: sources exist only
as loader scripts (dagster assets in `corpscout/dagster_v3`) that POST batches
of untranslated text to this service's HTTP API. The service upserts the
batch into a DuckDB queue, drives translation through a configured LLM
endpoint (or accepts static-map translations written directly by loaders),
and periodically flushes finished rows into ClickHouse
`corpscout.text_translations`.

Extraction (deciding which rows need translation, scanning ClickHouse,
computing hashes) is a loader concern, not a translator concern. The
translator only ever sees `(table, column, text, hash, lang pair)` tuples on
the wire; it does not know what source produced them.

## Packages

```text
cmd/translator-api        HTTP server entrypoint: queue API, Temporal worker,
                           boot-resume
cmd/translator-trigger    CLI that signals (or starts) the single translation
                           workflow
internal/api              Router: /healthz, /v1/queue/items, /v1/queue/stats,
                           /v1/queue/process
internal/config           JSON + environment config loading
internal/engine           Shared-queue engine: enqueue/stats, ClickHouse I/O,
                           workflow/activities
internal/orchestration    Temporal registration and workflow starter
                           (signal-with-start)
internal/queue            DuckDB queue runtime: GetBatch, SaveBatch, SaveFailed
internal/translation      Translator provider and shared batch translation logic
```

`internal/engine` owns one DuckDB queue file (`input_items`, `output_items`,
`failed_items`) shared by every source. It creates the file and schema on
first run; there is no per-source file and no source registry. `GetBatch`
selects up to `BatchSize` pending rows from a single language pair — the pair
holding the oldest pending item — because a translation prompt requires a
homogeneous batch. `SaveBatch` writes translated rows to `output_items`;
`SaveFailed` writes irreducible failed rows to `failed_items`.

`internal/translation.TranslateItems` handles deterministic LLM output
failures inside the activity. A normal provider/network error returns to
Temporal and is retried by the activity retry policy. A model-output error
such as invalid JSON, missing IDs, duplicate IDs, unexpected IDs, or empty
translated text is handled locally: first retry the same items in a
deterministic shuffled order, then split the item list in half after
repeated model-output failure. If a single item still cannot produce a valid
translation, `TranslateItems` returns it as a failed item so the workflow
activity can write it to `failed_items` and continue with the rest of the
batch.

`internal/translation` defines the translator boundary:

```go
Translate(ctx context.Context, items []TranslationInput, timeoutSeconds int,
          promptData PromptData) ([]TranslationResult, error)
```

`promptData` (source/target language display names, e.g. `"Norwegian"` /
`"English"`) is supplied per call, not baked into the provider at
construction, so one provider instance serves every language pair in the
queue. The production provider is OpenAI-compatible and posts to
`{base_url}/chat/completions`. The prompt template is owned by the
translation package and compiled by `translation.Init`. Tests use fake
translators and `httptest`; they do not call the real local LLM.

## Service Contract

The translator exposes three endpoints plus health. There is no
per-source routing — every loader talks to the same shared queue.

```text
GET  /healthz
POST /v1/queue/items
GET  /v1/queue/stats
POST /v1/queue/process
```

### `POST /v1/queue/items`

Language pair and human-readable language names are declared once per
request (a loader naturally loads one pair); items carry only per-text
fields:

```json
{
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "items": [
    {
      "source_table": "corpscout.no_companies",
      "source_column": "activity_text_original",
      "source_text": "Utvikling av programvare",
      "source_text_hash": "1234567890123456789"
    }
  ]
}
```

Rules:

- `source_text_hash` is a **decimal string** (uint64 exceeds JSON's safe
  integer range). Loaders compute it in ClickHouse SQL (`cityHash64(col)`) so
  hashes always agree with the loader's anti-join; the translator parses and
  stores it as uint64 and never recomputes it.
- Validation (reject the whole request with `400` and a per-field error):
  request-level fields non-empty; at most 10,000 items per request; every
  item's four fields non-empty; hash parses as a uint64.
- Behavior: upsert into `input_items` keyed by
  `(source_table, source_column, source_text_hash, source_lang, target_lang)`
  — `ON CONFLICT DO NOTHING`. Re-running a loader is free; **loaders own
  dedup** via their anti-join scan, and the upsert makes a re-POST of
  already-queued rows a no-op rather than an error.
- After a successful upsert of at least one item, the handler
  signal-with-starts the processing workflow. Enqueue succeeds even if the
  signal fails (the queue write is durable; boot-resume and the manual kick
  below cover it) — the response then carries a `warning` field instead of
  workflow IDs.

Response `202`:

```json
{
  "received": 1,
  "inserted": 1,
  "workflow_id": "translator/process",
  "run_id": "c3d4e5f6-..."
}
```

`inserted < received` means duplicates were skipped by the upsert — normal
on a re-run.

```bash
curl -s -X POST http://localhost:8080/v1/queue/items \
  -H 'Content-Type: application/json' \
  -d '{
        "source_lang": "no",
        "target_lang": "en",
        "source_language_name": "Norwegian",
        "target_language_name": "English",
        "items": [
          {
            "source_table": "corpscout.no_companies",
            "source_column": "activity_text_original",
            "source_text": "Utvikling av programvare",
            "source_text_hash": "1234567890123456789"
          }
        ]
      }'
```

### `GET /v1/queue/stats`

`200`:

```json
{"input": 42, "pending": 10, "output": 30, "failed": 2}
```

Same counting queries the workflow's batch loop uses. Dagster asset checks
and humans use this to confirm the queue is reachable and to watch progress.

```bash
curl -s http://localhost:8080/v1/queue/stats
```

### `POST /v1/queue/process`

Manual kick: signal-with-start the workflow with no enqueue. Useful for
nudging a worker after a config change or to confirm the workflow is
running without loading data.

```bash
curl -s -X POST http://localhost:8080/v1/queue/process
```

`202`:

```json
{"workflow_id": "translator/process", "run_id": "c3d4e5f6-...", "status": "accepted"}
```

There is no `POST /v1/sources/{source}/{action}` family any more — sources
do not exist inside the translator.

## Loader Pattern

Loaders live in `corpscout/dagster_v3` (one dagster asset per source), not in
this repository. Each loader:

1. Runs an anti-join scan in ClickHouse to find distinct untranslated text
   for one `(table, column)`, computing the hash in SQL so it matches exactly
   what the translator stores.
2. Chunks the result to at most 10,000 items and POSTs each chunk to
   `/v1/queue/items` with the language pair for that source.
3. For statically-mapped columns (e.g. legal-form codes), skips the queue
   entirely and inserts translated rows straight into
   `corpscout.text_translations` with `provider = 'static'`.

**The canonical anti-join scan** (the exact SQL shape the deleted Go scan
templates generated, reproduced here as the template for every future
loader) — LLM columns:

```sql
SELECT DISTINCT
    'corpscout.no_companies' AS source_table,
    'activity_text_original' AS source_column,
    c.activity_text_original AS source_text,
    cityHash64(c.activity_text_original) AS source_text_hash,
    'no' AS source_lang,
    'en' AS target_lang
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.activity_text_original)
WHERE c.activity_text_original <> ''
```

Static (code-mapped) columns use the same shape, selecting the key column
instead of the language pair — the key is joined against an in-loader
code→text map before writing directly to `text_translations`:

```sql
SELECT DISTINCT
    c.legal_form_description_original AS source_text,
    cityHash64(c.legal_form_description_original) AS source_text_hash,
    c.legal_form_code AS legal_form_code
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'legal_form_description_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.legal_form_description_original)
WHERE c.legal_form_description_original <> ''
```

**Dedup ownership:** the translator's `input_items` upsert only prevents
duplicate rows *within the queue*; it does not know whether a text was
already translated and flushed. The anti-join against `text_translations` is
what stops a loader from re-enqueueing already-translated text — **loaders
own dedup**, not the translator.

**Trust boundary:** loader-authored table and column names are interpolated
into this SQL, and flow into `text_translations` metadata, without escaping
— loaders are trusted, developer-authored code (same as the deleted
definition files were) and must never be built from untrusted input.

## Processing Workflow

One workflow, fixed identity: workflow ID `translator/process`, task queue
`translator-process`, workflow type `TranslationWorkflow`. There are no
per-source workflow IDs or task queues.

Behavior:

1. Started, or signal-with-started, with
   `{BatchSize, TimeoutSeconds, BatchesPerRun, FlushEveryBatches}`, defaulted
   from config to `50 / 120 / 500 / 10`.
2. Loop up to `BatchesPerRun` times: `ProcessOneBatch` translates one batch
   (a single language pair — the batch loop just processes whatever pair has
   the oldest pending item), saving output/failed rows.
3. **Flush semantics:** every `FlushEveryBatches` batches, and whenever the
   pending count reaches zero, the `FlushOutput` activity inserts
   `output_items` into `corpscout.text_translations`, and — only after that
   insert succeeds — deletes the matching `input_items` rows and all of
   `output_items` from the DuckDB queue. `failed_items` is never
   auto-deleted; an operator inspects and clears it. The `FlushEveryBatches`
   counter only counts batches that actually translated at least one item —
   a batch that finds nothing pending means the queue is already empty,
   which flushes immediately under the "pending count reaches zero" rule
   above, so it never needs to also advance the every-N counter.
4. When the queue drains and there is nothing left to flush, the workflow
   checks for a buffered enqueue signal (an enqueue that raced the drain) and
   loops once more if one arrived; otherwise it completes.
5. Once `BatchesPerRun` is exhausted, the workflow continues-as-new with the
   same input, keeping workflow history bounded across a queue that never
   fully empties.

**Crash-window duplicate note:** a crash between the ClickHouse insert and
the DuckDB delete re-flushes the same rows on resume, producing duplicate
`text_translations` rows with identical content. Readers already pick the
latest version per key, so this is the same idempotent-read property the
rest of the pipeline relies on — documented, accepted, not treated as a bug.

**Boot resume:** `translator-api`'s `main`, after starting the Temporal
worker, checks the queue's pending count and signal-with-starts the workflow
if it is greater than zero. A restart with a half-full queue is never
stranded — you do not have to remember to re-trigger it.

## Deploy Migration

Deploying this shared-queue engine over an existing installation requires
terminating three legacy Temporal workflows **before** restarting the
service, so nothing signals a stale run that the new worker cannot process:

- `translator/norway_brreg` — the old per-source Go workflow (predates this
  split; task queue `translator-norway-brreg`).
- `build-queue-norway_brreg` — legacy Python loader workflow. That Python
  package (`corpscout/dagster_v3/translator/`) has been removed in this
  branch; its Temporal workflows still need cleaning up at deploy.
- `translate-norway_brreg` — legacy Python translation workflow.

```bash
temporal workflow terminate --workflow-id "translator/norway_brreg" --reason "translator shared-queue cutover"
temporal workflow terminate --workflow-id "build-queue-norway_brreg" --reason "translator shared-queue cutover"
temporal workflow terminate --workflow-id "translate-norway_brreg" --reason "translator shared-queue cutover"
```

Then simply restart `translator-api`. Nothing re-creates the old
identities, and the new `translator/process` workflow does not need to be
started manually: the first successful loader enqueue signal-with-starts it,
and boot-resume covers the case where the queue was already non-empty at
restart.

**Stop the legacy Python worker too.** Any already-deployed instance of the
old `uv run translator-worker` process (task queues `translation-build` /
`translation-llm`) must be stopped and removed as part of this migration —
its Dockerfile and package (`corpscout/dagster_v3/translator/`) were deleted
in this branch, so it can no longer be rebuilt, and a still-running old
container/process will otherwise keep polling those task queues forever with
no work arriving.

**The old per-source queue file is abandoned.** `data/translator/norway_brreg.duckdb`
(the pre-split, per-source queue used by the legacy Go and Python workflows)
is no longer read by anything and can be deleted. Any text it still held
that was never translated is not lost: the loaders' anti-join against
`corpscout.text_translations` re-discovers untranslated text on the next
scan and re-enqueues it into the new shared `data/translator/queue.duckdb`.

## Configuration

The service reads non-secret configuration from:

```text
config/translator.json
```

```json
{
  "server": {"listen_address": ":8080"},
  "clickhouse": {"host": "...", "native_port": 9002, "user": "default", "database": "corpscout", "secure": false},
  "temporal": {"address": "localhost:7233", "namespace": "default", "batch_size": 50, "timeout_seconds": 120, "batches_per_run": 500},
  "endpoints": {"local_llm": {"model": "...", "model_env": "...", "base_url": "...", "base_url_env": "...", "api_key_env": "...", "max_tokens": 32768, "extra_body": {}}},
  "queue": {"path": "data/translator/queue.duckdb", "flush_every_batches": 10},
  "endpoint_id": "local_llm"
}
```

The `queue` block is the entire source configuration surface: a path to the
single shared DuckDB file and the flush cadence. There is no `sources` map
and no `definition_path` any more.

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

The config file declares endpoint credential environment variable names
(`model_env`, `base_url_env`, `api_key_env`); secret values are read from
those environment variables and are not stored in JSON.

Environment variables the Go service actually reads:

```text
TRANSLATOR_CONFIG_FILE
TRANSLATOR_API_ADDR
TEMPORAL_ADDRESS
TRANSLATOR_BATCHES_PER_RUN
CLICKHOUSE_HOST
CLICKHOUSE_NATIVE_PORT
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE
CLICKHOUSE_SECURE
TRANSLATOR_INTEGRATION_TESTS
TRANSLATION_PROVIDER_LOCAL_BASE_URL
TRANSLATION_PROVIDER_LOCAL_MODEL
TRANSLATION_PROVIDER_LOCAL_API_KEY
```

(`.env.example` also carries `CLICKHOUSE_HTTP_PORT` for parity with
`dagster_v3`'s environment file — the Go service does not read it; ClickHouse
access here is always over the native protocol.)

The local LLM endpoint uses the same settings as the retired Python
translator:

```text
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888/v1
TRANSLATION_PROVIDER_LOCAL_MODEL=qwen3:6b
TRANSLATION_PROVIDER_LOCAL_API_KEY=not-needed
max_tokens=32768
extra_body={"chat_template_kwargs":{"enable_thinking":false}}
```

Source and target language display names (`"Norwegian"` / `"English"`) come
from each enqueue request, not from any config file or definition — the same
endpoint serves every loader's language pair.

The ClickHouse connection is resolved from the same `CLICKHOUSE_*`
environment variables used by `dagster_v3`. The Go service uses
`CLICKHOUSE_NATIVE_PORT` because `clickhouse-go` connects over the native
protocol.

## Run

```bash
make run
```

The service expects ClickHouse and Temporal to be reachable at startup. It
reads configuration from `config/translator.json` plus the environment
variables listed above, opens (or creates) the shared DuckDB queue, starts
the Temporal worker, and performs boot-resume before serving HTTP.

## Direct Temporal Trigger

Use the Go trigger command when you want to signal (or start) the shared
translation workflow directly through Temporal instead of the HTTP API. It
takes no source or action argument — there is only one workflow:

```bash
make trigger
```

Or run it directly:

```bash
go run ./cmd/translator-trigger
go run ./cmd/translator-trigger -config /path/to/translator.json
```

The command uses Temporal `SignalWithStartWorkflow` with:

```text
workflow_id=translator/process
workflow_type=TranslationWorkflow
task_queue=translator-process
signal_name=new-items
```

If the Temporal CLI is installed, `scripts/trigger-translator-workflow.sh`
sends the same signal through `temporal workflow signal-with-start`. It also
takes no action argument — running it with any positional argument is an
error:

```bash
scripts/trigger-translator-workflow.sh
```

```text
Environment overrides:
  TEMPORAL_ADDRESS                 default: localhost:7233
  TEMPORAL_NAMESPACE               default: default
  TEMPORAL_CLI                     default: temporal
  TRANSLATOR_BATCH_SIZE            default: 50
  TRANSLATOR_TIMEOUT_SECONDS       default: 120
  TRANSLATOR_BATCHES_PER_RUN       default: 500
  TRANSLATOR_FLUSH_EVERY_BATCHES   default: 10
  TRANSLATOR_WORKFLOW_TYPE         default: TranslationWorkflow
```

In normal operation you should not need either trigger path: enqueueing
through `/v1/queue/items` starts the workflow automatically, and boot-resume
covers restarts with a non-empty queue. These exist for a manual nudge.

## Build

DuckDB is linked through `github.com/marcboeker/go-duckdb`, which requires
cgo. The Makefile sets `CGO_ENABLED=1` for `build`, `test`, `run`, and
`trigger`.

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

This writes the binaries to:

```text
bin/translator-api
bin/translator-trigger
```

Run the compiled binaries from the `translator` directory:

```bash
./bin/translator-api
./bin/translator-trigger
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

`internal/engine/integration_test.go` exercises the whole engine loop
against a real ClickHouse: enqueue two synthetic items, process them with a
fake translator, flush the output, and confirm the rows land in
`corpscout.text_translations` while the DuckDB queue empties (plus a second
test that exercises `InsertTextTranslations` directly). Both tests are
skipped unless `TRANSLATOR_INTEGRATION_TESTS=true`.

`go test` runs a package's tests with that package's directory as the
working directory, so the default config path
(`config/translator.json`, relative) will not resolve when the tests run
from `internal/engine/`. Point `TRANSLATOR_CONFIG_FILE` at an absolute path,
and make sure the ClickHouse credentials in your environment are real (a
copied `.env.example` password is a placeholder, not a working credential):

```bash
TRANSLATOR_INTEGRATION_TESTS=true \
TRANSLATOR_CONFIG_FILE="$(pwd)/config/translator.json" \
CLICKHOUSE_PASSWORD=<real clickhouse password> \
go test ./internal/engine/ -run WithExistingClickHouse -v -count=1
```

If you keep a `.env` in this directory with an absolute
`TRANSLATOR_CONFIG_FILE` and the real `CLICKHOUSE_PASSWORD`, sourcing it
first is equivalent:

```bash
set -a
. ./.env
set +a
TRANSLATOR_INTEGRATION_TESTS=true go test ./internal/engine/ -run WithExistingClickHouse -v -count=1
```
