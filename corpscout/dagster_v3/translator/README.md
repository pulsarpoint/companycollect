# Translator Package

This package contains the current company text translation pipeline used by
Corpscout. The implementation is centered on Norway BRREG today, even though
some modules still expose generic-looking names such as `SourceConfig`,
`FieldConfig`, and `build_scan_sql`.

At a high level the package does four things:

1. It scans source text from ClickHouse and determines which texts are not yet
   present in `corpscout.text_translations`.
2. It writes untranslated dynamic text into a local DuckDB queue.
3. It drains that queue through a local OpenAI-compatible LLM provider.
4. It writes completed translations back to ClickHouse.

Static, closed-enum fields are handled without the LLM. For example, Norway
legal-form descriptions are translated from a local authoritative code map and
flushed directly to ClickHouse.

The current live source is Norway BRREG. The important ClickHouse input table is
`corpscout.no_companies`, and the translation cache table is
`corpscout.text_translations`.

## Current Scope

The package is not a mature generic translation framework. It currently behaves
as a Norway BRREG translation subsystem with shared queue, provider, worker, and
flush utilities around it.

Current translated Norway fields:

- `articles_purpose_original`: dynamic Norwegian text translated by the LLM.
- `activity_text_original`: dynamic Norwegian text translated by the LLM.
- `legal_form_description_original`: static legal-form text translated through
  `legal_form_code` and `LEGAL_FORM_DESCRIPTION_EN_BY_CODE`.

Current source metadata:

- Source slug: `norway_brreg`
- Source language: `no`
- Target language: `en`
- Source ClickHouse table: `corpscout.no_companies`
- Translation cache table: `corpscout.text_translations`
- Queue file used by the Dagster trigger: `data/translator/norway_brreg.duckdb`

## Package Layout

```text
translator/
  __init__.py
  README.md
  Dockerfile
  clickhouse.py
  config.py
  errors.py
  flush.py
  import_legacy.py
  llm_batch.py
  provider.py
  queue.py
  static_maps.py
  task_queues.py
  types.py
  norway_brreg/
    __init__.py
    config.py
    dump.py
    seed.py
    workflows.py
```

### `queue.py`

`queue.py` owns the local DuckDB queue. This is the central persistence layer
between ClickHouse scanning and LLM translation.

Main classes and records:

- `TranslationQueue`: object wrapping a DuckDB file path and queue operations.
- `TranslationQueueItem`: input record used when enqueuing text.
- `ClaimedTranslationItem`: record returned to workers after a batch is leased.
- `TranslationQueueSummary`: aggregate counts for operational reporting.
- `CompletedTranslationQueueResult`: completed queue row with source location
  metadata.
- `FlushTranslationRow`: minimal row shape used when flushing translations to
  ClickHouse.

Main tables created inside DuckDB:

- `translation_items`
- `translation_locations`
- `translation_results`
- `translation_batch_attempts`

The schema deliberately separates unique text items from source locations:

- `translation_items` stores each unique `source_text` and `target_language`
  pair once.
- `translation_locations` stores where that text came from: source table,
  source field, source primary key, and source path.
- `translation_results` stores one completed translation per item.
- `translation_batch_attempts` stores success and failure metadata for each
  translation batch.

That split matters because the same source text can appear in multiple rows or
fields. The LLM should translate the text once, while the flush step still needs
to know every source field where the result applies.

Queue item identity:

- `source_text_hash` is `sha256(source_text)`.
- `item_id` is `sha256(source_text_hash + "|" + target_language)`.
- `location_id` is `sha256(source_duckdb_path, source_table, source_pk,
  source_field, source_text_hash, target_language)`.

The queue statuses are:

- `pending`: item is ready to be claimed.
- `leased`: item is currently owned by a worker batch.
- `completed`: item has a translation result.
- `failed_retryable`: item failed due to a transient category and can be claimed
  again.
- `failed`: item failed due to deterministic bad output and is terminal.

Retry rules:

- Retryable categories are `connection_refused`, `timeout`, and
  `provider_error`.
- Retryable failures remain retryable indefinitely. The queue intentionally does
  not permanently discard work because a provider was temporarily down.
- Non-retryable failures, such as invalid JSON or invalid response shape, become
  terminal `failed` immediately.

Important queue operations:

- `initialize()`: creates the DuckDB file, creates tables, and applies a legacy
  migration if an older queue schema is detected.
- `enqueue_items(items)`: inserts unique items and locations idempotently.
- `claim_batch(limit, worker_id)`: leases pending items first, then retryable
  failed items.
- `release_stale_leases(older_than_seconds)`: returns old leased items to
  pending.
- `complete_batch(...)`: writes results, marks items completed, and records a
  successful batch attempt.
- `fail_batch(...)`: marks items as retryable or terminal failed and records a
  failed batch attempt.
- `summary()`: returns operational counts.
- `completed_results_for_flush()`: returns completed rows in the format expected
  by the ClickHouse flush layer.

### `provider.py`

`provider.py` contains the OpenAI-compatible translation provider.

The provider is designed for local OpenAI-compatible endpoints rather than the
public OpenAI API specifically. It uses the `openai` Python client, but the
caller supplies `base_url`, `model`, `api_key`, `max_tokens`, and optional
`extra_body`.

Important constants:

- `DEFAULT_MAX_TOKENS = 32768`
- `DEFAULT_EXTRA_BODY = {"chat_template_kwargs": {"enable_thinking": False}}`

Main functions:

- `build_translation_prompt(items)`: builds the JSON-only translation prompt.
- `parse_translation_response(response_text, expected_item_ids=...)`: parses
  and validates provider output.
- `_parse_extra_body(value)`: converts a JSON string from config/env into a dict
  or `None`.
- `_strip_json_fence(response_text)`: accepts plain JSON or fenced JSON.

Main class:

- `LocalOpenAICompatibleTranslationProvider`

Prompt contract:

- Source language is Norwegian.
- Target language is English.
- The provider receives synthetic item IDs.
- The response must be valid JSON.
- The response shape must be:

```json
{"translations":[{"item_id":"...","translated_text":"..."}]}
```

Validation rules:

- The top-level `translations` value must be a list.
- Every row must be an object.
- Every returned `item_id` must be expected.
- Duplicate item IDs are rejected.
- Empty `translated_text` values are rejected.
- Missing item IDs are rejected.

The provider does not decide queue retry behavior. It raises on bad responses or
provider errors, and `workflows.py` categorizes those exceptions.

### `llm_batch.py`

`llm_batch.py` adapts queue items to provider items.

The queue uses stable item IDs derived from text hashes. The provider receives
short positional IDs instead:

- `batch-item-000`
- `batch-item-001`
- `batch-item-002`

After the provider returns results, `translate_batch(...)` maps positional IDs
back to queue item IDs.

This protects queue internals from the prompt and keeps provider response
validation simple. It also lets the provider see compact IDs that are easy to
copy exactly.

Main function:

- `translate_batch(items, provider, timeout) -> list[TranslationResult]`

Behavior:

- Empty input returns an empty list.
- Provider errors propagate to the caller.
- Returned results use queue item IDs, not provider positional IDs.

### `errors.py`

`errors.py` maps provider and parsing exceptions into queue failure categories.

Main function:

- `_categorize_exception(exc) -> str`

Categories:

- `connection_refused`
- `timeout`
- `invalid_json`
- `missing_item_ids`
- `invalid_response`
- `provider_error`

The function looks at the exception message, chained exception messages, and
exception class names. The category is then passed to `TranslationQueue.fail_batch`.

Queue retryability is controlled by `queue._is_retryable(...)`, not by
`errors.py`.

### `flush.py`

`flush.py` writes translated rows to ClickHouse table
`corpscout.text_translations`.

Main functions:

- `_staging_table_name(run_id)`: creates a safe temporary Memory table name.
- `build_flush_select_sql(staging_table)`: builds the `INSERT INTO
  corpscout.text_translations ... SELECT ...` statement.
- `flush_translations(client, source_config, rows, provider, model, version,
  run_id)`: performs the flush.

Flush process:

1. Drop rows whose `translated_text` is an empty string.
2. Create a ClickHouse Memory staging table.
3. Insert `source_column`, `source_text`, and `translated_text` into staging.
4. Insert into `corpscout.text_translations`.
5. Compute `source_text_hash` in ClickHouse with `cityHash64(source_text)`.
6. Drop the staging table in a `finally` block.

`flush_translations` still accepts `SourceConfig`, because the current
implementation passes source table and source language through that object. For
Norway, those values are currently:

- `source_config.ch_table = "corpscout.no_companies"`
- `source_config.source_lang = "no"`

### `clickhouse.py`

`clickhouse.py` contains ClickHouse client construction and the current generic
scan SQL builder.

Main functions:

- `clickhouse_client_from_env()`
- `build_scan_sql(source_config, field)`
- `scan_untranslated_terms(client, source_config)`
- `query_arrow(client, sql, parameters=None)`

`clickhouse_client_from_env()` reads:

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT` with default `8123`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_SECURE`

The current scan SQL uses ClickHouse `LEFT ANTI JOIN` against
`corpscout.text_translations` to find texts whose `cityHash64(c.<column>)` is not
already present for the same source table and source column.

The use of `LEFT ANTI JOIN` is important. The tests explicitly reject the older
`LEFT JOIN ... WHERE t.hash IS NULL` pattern because ClickHouse defaults can make
that pattern silently return zero rows.

Current design pressure:

- This module currently exposes abstractions that look source-generic.
- Today the only live source wired into the translator workflows is Norway
  BRREG.
- For that reason, parts of this module are candidates for simplification or
  movement into `translator/norway_brreg/`.

### `config.py`

`config.py` defines the generic-looking source and field dataclasses used by the
current implementation.

Types:

- `FieldConfig`
- `SourceConfig`

`FieldConfig` contains:

- `original_col`
- `static_map`
- `static_key_col`

`SourceConfig` contains:

- `source_slug`
- `source_lang`
- `ch_table`
- `fields`

This model is useful only insofar as it feeds the current generic scan and flush
functions. Since there is currently one live translation source, this is one of
the clearest places where the package has more abstraction than it needs.

### `static_maps.py`

`static_maps.py` contains closed-enum translations that should not go through
the LLM.

Current map:

- `LEGAL_FORM_DESCRIPTION_EN_BY_CODE`

This maps Norway legal-form codes such as `AS`, `ENK`, `ANS`, `ASA`, `NUF`, and
others to English descriptions. The code map is used for
`legal_form_description_original`, keyed by `legal_form_code`.

Static-map behavior:

- Known code produces a `FlushTranslationRow`.
- Unknown code produces no row.
- Empty translation produces no row.
- Static translations are written directly to ClickHouse with provider/model
  labels set to `static`.

### `task_queues.py`

`task_queues.py` defines the Temporal task queue names:

- `BUILD_TASK_QUEUE = "translation-build"`
- `LLM_TASK_QUEUE = "translation-llm"`

The split is operationally important:

- The build queue runs workflows and non-LLM activities.
- The LLM queue runs only `translate_loop_activity`.
- The LLM queue has bounded concurrency and acts as the global LLM gate.

### `types.py`

`types.py` contains tiny provider-facing dataclasses:

- `TranslationInput`
- `TranslationResult`

These are deliberately smaller than queue records. They represent only what the
provider needs to see and return.

### `worker.py`

`worker.py` starts the Temporal worker fleet.

It creates two workers in one process:

1. Build worker:
   - Task queue: `translation-build`
   - Workflows: `BuildQueueWorkflow`, `TranslateWorkflow`
   - Activities: `build_queue_activity`, `start_translate_workflow_activity`,
     `dump_activity`, `summarize_queue_activity`
   - Max activities: `BUILD_MAX_WORKERS = 8`

2. LLM worker:
   - Task queue: `translation-llm`
   - Workflows: none
   - Activities: `translate_loop_activity`
   - Max activities: `TRANSLATOR_LLM_CONCURRENCY`, default `2`

The activity executors are `ThreadPoolExecutor` instances. This is intentional:
the sync Temporal activities heartbeat from worker threads, and the code relies
on the SDK's thread-safe heartbeat path.

CLI entry point:

```bash
uv run translator-worker --env-file .env
```

The script is registered in `pyproject.toml` as:

```toml
[project.scripts]
translator-worker = "translator.worker:worker_main"
```

Logging:

- `worker_main` calls `logging.basicConfig`.
- Log level is read from `TRANSLATOR_LOG_LEVEL`, default `INFO`.
- Format is `%(asctime)s %(levelname)s %(name)s | %(message)s`.

### `import_legacy.py`

`import_legacy.py` is a one-time CLI for importing completed translations from
an older DuckDB queue into ClickHouse.

CLI entry point:

```bash
uv run translator-import-legacy-queue \
  --duckdb data/norway_brreg_translation_queue.duckdb \
  --source norway_brreg \
  --env-file .env
```

Optional flags:

- `--provider`, default `legacy-import`
- `--model`, default `legacy`
- `--batch-size`, default `50000`
- `--dry-run`

Behavior:

1. Load environment variables from `.env` without overriding real env vars.
2. Open the queue DuckDB file.
3. Resolve source config by slug. Currently only `norway_brreg` is supported.
4. Read completed queue results.
5. Import only dynamic columns.
6. Skip static fields, because static translations are authoritative and should
   be regenerated from the static map instead.
7. Skip unknown fields.
8. Flush rows to ClickHouse in batches.

### `Dockerfile`

The translator Dockerfile is minimal:

```dockerfile
FROM python:3.14-slim

RUN pip install --no-cache-dir uv
WORKDIR /app
COPY dagster_v3 /app/dagster_v3
WORKDIR /app/dagster_v3
RUN uv sync --no-dev
CMD ["uv", "run", "translator-worker"]
```

It copies the `dagster_v3` project, installs dependencies with `uv`, and starts
the translator worker.

## Norway BRREG Subpackage

The `norway_brreg` subpackage contains the source-specific workflow, seed, dump,
and config code for Norway.

### `norway_brreg/config.py`

This file currently returns the Norway `SourceConfig`.

Current config:

```python
SourceConfig(
    source_slug="norway_brreg",
    source_lang="no",
    ch_table="corpscout.no_companies",
    fields=(
        FieldConfig(original_col="articles_purpose_original"),
        FieldConfig(original_col="activity_text_original"),
        FieldConfig(
            original_col="legal_form_description_original",
            static_map=tuple(LEGAL_FORM_DESCRIPTION_EN_BY_CODE.items()),
            static_key_col="legal_form_code",
        ),
    ),
)
```

The function `get_config()` returns that object.

Design note: this file is one of the current abstraction hotspots. Since the
translation package is only wired for Norway today, a future cleanup may replace
this with explicit Norway constants and field-specific scan functions.

### `norway_brreg/seed.py`

`seed.py` seeds the DuckDB queue from ClickHouse.

Conceptual responsibilities:

1. Initialize the DuckDB queue.
2. For each dynamic field, scan ClickHouse for untranslated source texts.
3. Bulk insert dynamic texts into `translation_items`.
4. Bulk insert source locations into `translation_locations`.
5. For each static field, resolve local static-map translations.
6. Flush static translations directly to `corpscout.text_translations`.
7. Heartbeat after each field when run as a Temporal activity.
8. Return `SeedResult(dynamic_enqueued, static_flushed)`.

Dynamic fields currently go through:

- `build_scan_sql(config, field)`
- `query_arrow(ch_client, sql, params)`
- DuckDB `register("_scan_result", arrow_table)`
- DuckDB `INSERT INTO translation_items ... ON CONFLICT DO NOTHING`
- DuckDB `INSERT INTO translation_locations ... ON CONFLICT DO NOTHING`

Static field flow:

- Convert the Arrow table to a Python dict.
- Read `source_text`.
- Read `static_key`.
- Look up the translation in `field.static_map_dict()`.
- Build `FlushTranslationRow` values.
- Call `flush_translations(..., provider="static", model="static")`.

Important idempotency behavior:

- Queue insertion uses conflict handling.
- Re-running seed should not duplicate existing dynamic queue items.
- Static flush writes through the ClickHouse translation table path.

Known cleanup target:

- The function still receives a generic `SourceConfig`.
- The scan SQL still receives generic table/column parameters.
- This does not match the fact that this path is Norway-specific.
- A cleaner version should make the Norway table, Norway columns, and Norway
  static field behavior visible directly in this module.

### `norway_brreg/dump.py`

`dump.py` flushes completed queue results from DuckDB back into ClickHouse.

Main function:

- `dump_to_clickhouse(queue_duckdb_path, ch_client, config, provider, model,
  batch_size=50000, heartbeat_fn=None) -> int`

Behavior:

1. Read completed queue results through
   `TranslationQueue.completed_results_for_flush()`.
2. Return `0` if there are no completed rows.
3. Create a single integer `version` for the dump run.
4. Chunk rows by `batch_size`.
5. Call `flush_translations` for each chunk.
6. Heartbeat after each chunk when a heartbeat function is provided.
7. Return total rows written.

The dump path labels LLM-generated rows with provider `local-llm` and model from
`TRANSLATION_PROVIDER_LOCAL_MODEL`.

### `norway_brreg/workflows.py`

`workflows.py` defines the Temporal workflows and activities.

Dataclasses:

- `BuildQueueActivityInput`
- `StartTranslateWorkflowInput`
- `TranslateLoopActivityInput`
- `TranslateLoopResult`
- `DumpActivityInput`
- `BuildQueueWorkflowInput`
- `BuildQueueWorkflowOutput`
- `TranslateWorkflowInput`
- `TranslateWorkflowOutput`

Activity implementation helpers:

- `build_queue_once(params)`
- `translate_loop_once(params)`
- `dump_once(params)`
- `summarize_queue_once(queue_duckdb_path)`

Temporal activities:

- `build_queue_activity`
- `start_translate_workflow_activity`
- `translate_loop_activity`
- `dump_activity`
- `summarize_queue_activity`

Temporal workflows:

- `BuildQueueWorkflow`
- `TranslateWorkflow`

Workflow timeouts:

- `HEARTBEAT_TIMEOUT = 150 seconds`
- `START_TO_CLOSE_TIMEOUT = 24 hours`
- `SHORT_TIMEOUT = 60 seconds`
- `RETRY_POLICY = RetryPolicy(maximum_attempts=3)`

Build workflow:

1. Run `build_queue_activity`.
2. Start or reuse `TranslateWorkflow` through
   `start_translate_workflow_activity`.
3. Return the seed result counts.

Translate workflow:

1. Run `translate_loop_activity` on the gated LLM task queue.
2. Run `dump_activity` on the build task queue.
3. Run `summarize_queue_activity`.
4. Return completed counts, failed counts, flushed row count, and batch counts.

The workflow uses `WorkflowIDConflictPolicy.USE_EXISTING` when starting the
translate workflow. That makes the handoff idempotent at the workflow ID level.

## End-to-End Runtime Flow

The normal end-to-end path starts from the Dagster Norway translation trigger
asset in:

```text
dagster_v3/src/dagster_v3/defs/norway_brreg/assets/translation.py
```

The flow is:

```text
Dagster asset materializes
  -> starts BuildQueueWorkflow on translation-build
     -> build_queue_activity scans ClickHouse and seeds DuckDB
     -> start_translate_workflow_activity starts TranslateWorkflow
        -> translate_loop_activity runs on translation-llm
           -> claim DuckDB batch
           -> call local OpenAI-compatible provider
           -> complete or fail queue batch
           -> repeat until no claimable items remain
        -> dump_activity writes completed translations to ClickHouse
        -> summarize_queue_activity reads final queue counts
```

The Dagster trigger is fire-and-forget. It starts the Temporal workflow and
returns metadata; it does not wait for all translation work to finish.

Default Dagster trigger configuration:

- `batch_size = 50`
- `max_tokens = 32768`
- `extra_body_json = '{"chat_template_kwargs": {"enable_thinking": false}}'`

Default workflow IDs:

- Build workflow ID: `build-queue-norway_brreg`
- Translate workflow ID: `translate-norway_brreg`

Default queue path:

- `data/translator/norway_brreg.duckdb`

## ClickHouse Translation Cache

The package writes translations to:

```text
corpscout.text_translations
```

The flush SQL inserts:

- `source_table`
- `source_column`
- `source_text_hash`
- `source_lang`
- `target_lang`
- `translated_text`
- `provider`
- `model`
- `version`

The package assumes the ClickHouse table already exists. Table creation and
schema migration are outside this package.

`source_text_hash` is computed with ClickHouse `cityHash64(source_text)` during
flush. This is separate from the DuckDB queue's SHA-256 identity. The queue uses
SHA-256 for local deterministic queue IDs; the ClickHouse translation cache uses
the existing ClickHouse hash convention.

## Dynamic Versus Static Translation

The package has two translation modes.

### Dynamic LLM Translation

Dynamic fields contain free text and must go through the LLM.

Current dynamic fields:

- `articles_purpose_original`
- `activity_text_original`

Dynamic flow:

1. Scan source ClickHouse table.
2. Remove values already present in `corpscout.text_translations`.
3. Write remaining distinct values to the DuckDB queue.
4. Translate through the LLM provider.
5. Dump completed results back to ClickHouse.

### Static Map Translation

Static fields contain values from an official finite code set.

Current static field:

- `legal_form_description_original`, keyed by `legal_form_code`

Static flow:

1. Scan source ClickHouse table.
2. Read `source_text` and `static_key`.
3. Resolve `static_key` through `LEGAL_FORM_DESCRIPTION_EN_BY_CODE`.
4. Write directly to ClickHouse with provider/model `static`.

Static translations do not enter the DuckDB queue, because there is no LLM work
to schedule, retry, or lease.

## Environment Variables

ClickHouse:

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_HTTP_PORT`, default `8123`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_SECURE`, truthy values are `1`, `true`, `yes`

Temporal:

- `TEMPORAL_ADDRESS`, default `companycollect:7233`

Translator worker:

- `TRANSLATOR_LOG_LEVEL`, default `INFO`
- `TRANSLATOR_LLM_CONCURRENCY`, default `2`

LLM provider:

- `TRANSLATION_PROVIDER_LOCAL_BASE_URL`
- `TRANSLATION_PROVIDER_LOCAL_MODEL`
- `TRANSLATION_PROVIDER_LOCAL_API_KEY`, default `not-needed`

Dagster translation config:

- `batch_size`
- `max_tokens`
- `extra_body_json`

The worker CLI loads `.env` with `override=False`, so environment variables
already present in the shell or container win over values in the `.env` file.

## Running The Worker Locally

From the `dagster_v3` project directory:

```bash
uv run translator-worker --env-file .env
```

Override Temporal address:

```bash
uv run translator-worker --env-file .env --temporal-address localhost:7233
```

Control log level:

```bash
TRANSLATOR_LOG_LEVEL=DEBUG uv run translator-worker --env-file .env
```

Control global LLM concurrency:

```bash
TRANSLATOR_LLM_CONCURRENCY=1 uv run translator-worker --env-file .env
```

## Running Tests

Focused translator tests:

```bash
uv run pytest tests/test_translator_queue.py
uv run pytest tests/test_translator_flush.py
uv run pytest tests/test_translator_llm_batch.py
uv run pytest tests/test_translator_worker.py
uv run pytest tests/test_norway_brreg_seed.py
uv run pytest tests/test_norway_brreg_workflows.py
```

All translator-related tests:

```bash
uv run pytest tests/test_translator_*.py tests/test_norway_brreg_seed.py tests/test_norway_brreg_dump.py tests/test_norway_brreg_workflows.py
```

The tests use real temporary DuckDB files and focused fake clients. They do not
connect to a real ClickHouse instance or a real LLM provider for unit coverage.

## Operational Invariants

The following invariants are important to preserve:

- Queue initialization must be idempotent.
- Seeding must be idempotent.
- Re-seeding must not duplicate queue items.
- The same source text for the same target language should be translated once.
- Source locations must be retained separately from unique text items.
- Static translations should not be sent to the LLM.
- Empty translations should not be flushed to ClickHouse.
- Retryable provider failures must not permanently lose work.
- Invalid deterministic provider output should become terminal failed work.
- `translate_loop_activity` must run only on `translation-llm`.
- Build, dump, summarize, and handoff activities must run on
  `translation-build`.
- Sync activities that heartbeat should keep using a `ThreadPoolExecutor`.
- ClickHouse scans should use `LEFT ANTI JOIN`, not `LEFT JOIN ... IS NULL`.

## Current Design Debt

The package currently mixes two ideas:

1. Real shared infrastructure:
   - DuckDB queue
   - LLM provider adapter
   - batch translation adapter
   - Temporal worker split
   - ClickHouse flush helper
   - legacy import CLI

2. Generic source abstractions that are not currently justified:
   - `SourceConfig`
   - `FieldConfig`
   - `build_scan_sql(source_config, field)`
   - source/table/column parameter passing in the Norway seed path

The first group is useful because it owns real boundaries and operational
behavior. The second group hides Norway-specific decisions behind a generic API,
even though the translator is only wired for Norway BRREG today.

A cleaner Norway-focused design would likely:

- Keep `TranslationQueue`.
- Keep `LocalOpenAICompatibleTranslationProvider`.
- Keep `translate_batch`.
- Keep Temporal worker separation.
- Keep ClickHouse flush mechanics, possibly with simpler Norway constants.
- Move Norway scan SQL into `translator/norway_brreg/seed.py`.
- Replace generic source config in the seed path with explicit Norway constants.
- Make the three Norway translated fields visible directly at the call site.

This README documents the package as it exists now. It should be updated when
the Norway seed path is simplified.

## Glossary

- Dynamic field: free text that must be translated by the LLM.
- Static field: finite coded value that can be translated through a local map.
- Source text: original text from ClickHouse.
- Translated text: English output stored in `corpscout.text_translations`.
- Source location: where a source text appeared, such as table and column.
- Queue item: unique text and target language pair.
- Lease: temporary ownership of queue items by a worker batch.
- Retryable failure: transient failure that should be claimed again.
- Terminal failure: deterministic bad output that should not be retried forever.
- Build worker: Temporal worker for workflow control and non-LLM activities.
- LLM worker: Temporal worker for bounded provider calls.
