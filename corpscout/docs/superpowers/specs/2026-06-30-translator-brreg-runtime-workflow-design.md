# Translator BRREG Runtime And Workflow Design

Date: 2026-06-30

## Summary

The translator service will run as exactly one service instance for now. Because
DuckDB allows only one writer, the Norway BRREG translation queue must have one
in-process owner. The owner is a BRREG runtime object that holds the DuckDB
connection, ClickHouse client, and translation provider. API handlers and
Temporal activities do not open the DuckDB file directly; they call methods on
that runtime.

The runtime exposes three source-specific actions:

- `LoadNewInput(ctx)` reloads new ClickHouse text into DuckDB `input_items` and
  writes static legal-form translations directly to ClickHouse.
- `ProcessOneBatch(ctx, batchSize)` takes untranslated rows from the DuckDB
  queue, sends them to the translator, and saves successful results to
  `output_items`.
- `UploadOutput(ctx)` uploads completed output rows to
  `corpscout.text_translations`.

Temporal owns orchestration and retry. The BRREG runtime owns DuckDB access.

## Goals

- Keep exactly one DuckDB owner inside the translator service.
- Avoid adding distributed locks while the service runs as one instance.
- Preserve source-specific BRREG SQL and static translation logic inside the
  BRREG package.
- Let Temporal retry LLM failures without corrupting the queue.
- Allow a manual/API trigger to load new source text into the existing queue.
- Keep normal queue processing as a repeated batch workflow.
- Keep `input_items` immutable and use `output_items` as durable completion
  state.
- Upload completed dynamic translations to ClickHouse after queue processing
  reaches an empty batch.

## Non-Goals

- Do not build a generic source framework before a second source needs it.
- Do not add DuckDB file locks for multi-process deployment yet.
- Do not add lease tables, failed tables, manifest tables, or batch attempt
  tables.
- Do not let API handlers touch DuckDB directly.
- Do not let Temporal activities open the queue file independently.
- Do not delete rows from `input_items`.

## Package Ownership

The BRREG package owns orchestration around the BRREG source:

```text
translator/internal/brreg
  runtime.go        # single owner of BRREG DuckDB + source clients
  translation.go    # BRREG ClickHouse -> queue load and static translations
  clickhouse.go     # ClickHouse source and text_translations writer
```

The existing `internal/queue` package can remain as low-level queue table
mechanics, but runtime usage should not call `queue.Init(path, translator)`.
The runtime opens the DuckDB file once and passes the owned connection into
queue helpers. If this creates awkward abstractions, the queue mechanics should
be folded into `internal/brreg` instead of made more generic.

The translation package remains the provider adapter:

```text
translator/internal/translation
  provider.go       # OpenAI-compatible provider and package-owned prompt
  types.go          # TranslationInput / TranslationResult / Translator
```

The API package stays thin. It starts/signals Temporal workflows and never opens
ClickHouse or DuckDB directly.

## BRREG Runtime

The runtime is an in-process actor. It owns a goroutine and serializes all
DuckDB-touching work through a request channel.

Conceptual shape:

```go
type Runtime struct {
    requests chan request
}

func NewRuntime(config RuntimeConfig) (*Runtime, error)
func (r *Runtime) Start(ctx context.Context) error
func (r *Runtime) Close(ctx context.Context) error

func (r *Runtime) LoadNewInput(ctx context.Context) (LoadResult, error)
func (r *Runtime) ProcessOneBatch(ctx context.Context, input ProcessInput) (ProcessResult, error)
func (r *Runtime) UploadOutput(ctx context.Context) (UploadResult, error)
```

`RuntimeConfig` contains explicit BRREG dependencies:

```go
type RuntimeConfig struct {
    QueuePath string
    ClickHouseNativeURL string
    Translator translation.Translator
    ProviderName string
    Model string
}
```

The actor goroutine is the only code path that holds the DuckDB connection. It
also owns the ClickHouse client and translator instance for the BRREG source.
Method calls enqueue requests and wait for a typed response.

## DuckDB Access Rule

Only the BRREG runtime opens:

```text
data/translator/norway_brreg.duckdb
```

This means these rules must hold:

- `brreg.InitializeTranslation` is refactored so it can run using the runtime's
  owned DuckDB connection.
- Queue processing uses the runtime's owned DuckDB connection.
- Upload reads from the runtime's owned DuckDB connection.
- API handlers never call `duckdb.Open`, `queue.Init`, or source queue load
  functions directly.
- Temporal activities call runtime methods only.

Because there is one service instance, this in-process serialization is enough.
If deployment later moves to multiple service instances, add a process-level
file lock or move DuckDB access behind a single dedicated service. Do not add
that now.

## Load-New-Input Action

`LoadNewInput(ctx)` reloads new translation input from ClickHouse using the same
BRREG SQL already used by `InitializeTranslation`.

Dynamic fields:

- `articles_purpose_original`
- `activity_text_original`

Static field:

- `legal_form_description_original`

Dynamic rows are upserted into DuckDB `input_items`.

Static legal-form rows bypass the LLM queue and are inserted directly into
`corpscout.text_translations` with `provider='static'` and `model='static'`.

The action is idempotent:

- Existing `input_items` rows are ignored by primary key.
- Already-translated rows are excluded by the ClickHouse anti-join against
  `corpscout.text_translations`.
- Existing static translations are safe because ClickHouse uses
  `ReplacingMergeTree(version)`.

## Process-One-Batch Action

`ProcessOneBatch(ctx, batchSize)` performs exactly one batch of dynamic
translation work.

Steps:

1. Read up to `batchSize` rows from `input_items` where no matching
   `output_items` row exists.
2. If no rows are available, return `translated_count=0`.
3. Convert queue rows into `translation.TranslationInput`.
4. Call the configured translator.
5. Validate every result:
   - item ID exists in the requested batch
   - no duplicate item IDs
   - no missing item IDs
   - translated text is not empty
6. Save successful rows into `output_items`.
7. Return `translated_count`.

If the translator returns an error, the action returns an error and writes
nothing. Temporal retries the activity.

If saving succeeds but Temporal retries because of a later transport problem,
the retry is safe: `output_items` already contains the rows, and the next batch
read skips them.

## Upload-Output Action

`UploadOutput(ctx)` reads completed dynamic translations from `output_items` and
inserts them into `corpscout.text_translations`.

ClickHouse table shape:

```sql
ReplacingMergeTree(version)
ORDER BY (source_table, source_column, source_text_hash)
```

The upload can be append-based. Repeating upload inserts duplicate logical rows
with a version, and the translated views use `argMax(translated_text, version)`.
That keeps the action idempotent enough for the first implementation.

Do not delete from `output_items` after upload. Keeping local output rows makes
the queue durable and lets future queue loads skip completed local work.

If repeated uploads become operationally noisy, add an `uploaded_items` table
later. Do not add it in this implementation.

## Temporal Workflows

Use one workflow ID for Norway BRREG:

```text
translator/norway_brreg
```

The API starts or signals this workflow.

Endpoints:

```text
POST /v1/sources/norway_brreg/load-queue
POST /v1/sources/norway_brreg/run
```

`load-queue` sends a `load_new_input` signal.

`run` sends a `process` signal.

Workflow behavior:

```text
on load_new_input:
  activity LoadNewInput

on process:
  loop:
    activity ProcessOneBatch(batch_size)
    if translated_count > 0:
      continue
    if translated_count == 0:
      activity UploadOutput
      stop or idle
```

If `load_new_input` arrives while processing is active, it should be queued by
the workflow and run after the current activity completes. Do not run load and
process concurrently.

## API Behavior

API handlers do not do work directly. They validate source/action and send a
Temporal signal/start request.

Accepted actions for now:

- `norway_brreg/load-queue`
- `norway_brreg/run`

The response should include enough information for operators:

```json
{
  "source": "norway_brreg",
  "action": "run",
  "workflow_id": "translator/norway_brreg",
  "status": "accepted"
}
```

No generic source dispatch is needed beyond routing `norway_brreg` to the BRREG
workflow.

## Error Handling

LLM/provider failure:

- `ProcessOneBatch` returns an error.
- Temporal retries up to 10 times.
- No `output_items` rows are saved unless a complete, validated result set is
  available.

DuckDB failure:

- Runtime request returns an error.
- Workflow activity fails and retries if the error is transient.
- Because one runtime goroutine owns DuckDB, write conflicts inside the process
  should not occur.

ClickHouse load failure:

- `LoadNewInput` returns an error.
- Temporal retries the activity.
- Upserts make partial DuckDB input inserts safe.

ClickHouse upload failure:

- `UploadOutput` returns an error.
- Temporal retries the activity.
- Repeated insert is acceptable because `text_translations` is versioned.

## Testing

Unit tests:

- Runtime serializes requests to the owned queue connection.
- `ProcessOneBatch` reads only untranslated rows and writes `output_items`.
- Translator failures do not write output rows.
- `LoadNewInput` reuses the existing BRREG load SQL path.

Integration tests:

- Real ClickHouse BRREG queue load creates expected `input_items` rows.
- Real local LLM translates a 10-row Norwegian to English batch.
- Upload inserts `output_items` rows into `corpscout.text_translations`.

Concurrency test:

- Issue concurrent `LoadNewInput` and `ProcessOneBatch` calls against the
  runtime and assert they execute serially without DuckDB writer conflict.

## Open Deployment Constraint

This design depends on exactly one translator service instance. That is the
current plan.

If the deployment changes to multiple instances, this design must be revisited
before scaling. The next design should add either:

- an OS file lock around the queue file, or
- a dedicated queue-owner service process, or
- a different queue database with multi-writer support.
