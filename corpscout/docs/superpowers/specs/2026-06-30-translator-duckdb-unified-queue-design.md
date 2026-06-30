# Translator DuckDB Unified Queue Design

Date: 2026-06-30

## Summary

Replace the current split BuildQueue/Translate workflow shape with a simpler
source-owned translation workflow backed by one DuckDB queue database per source.

For Norway BRREG the queue file will be:

```text
data/translator/norway_brreg.duckdb
```

The DuckDB file will contain exactly two queue tables:

```text
input_items
output_items
```

`input_items` is the immutable set of source texts that need translation.
`output_items` is the idempotent set of completed translations. Work is never
removed from `input_items`; completion is proven by matching rows in
`output_items`.

The workflow should:

1. Ensure the source queue database exists.
2. If `input_items` is missing or empty, build it from ClickHouse with explicit
   source-specific SQL.
3. Repeatedly get untranslated batches from `input_items` by anti-joining
   `output_items`.
4. Send each batch to the configured translation endpoint.
5. Save successful translations to `output_items`.
6. When no untranslated input remains, flush `output_items` to
   `corpscout.text_translations`.

This design intentionally removes the current generic source abstraction from
the hot path. The source package owns source-specific SQL and static behavior.
The central translator package owns queue mechanics, provider invocation, and
workflow control.

## Motivation

The current translator package has useful shared infrastructure but weak source
abstractions.

Useful boundaries:

- DuckDB as durable local translation state.
- OpenAI-compatible provider adapter.
- Temporal worker split and bounded LLM concurrency.
- ClickHouse flush through staging inserts.
- Static map path for closed-enum fields.

Problematic boundaries:

- `SourceConfig`
- `FieldConfig`
- `get_config()`
- `build_scan_sql(source_config, field)`
- table and column passed through parameter dictionaries
- separate BuildQueue and Translate workflows for one logical translation run

Those abstractions hide the domain facts that matter. Norway BRREG translation
has known table names, known source columns, known source language, known target
language, and known static fields. The implementation should make those facts
visible where the work happens.

## Goals

- Use one DuckDB queue database per translation source.
- Use a unified queue across translated columns for that source.
- Keep only `input_items` and `output_items` tables.
- Store `source_table`, `source_column`, `source_lang`, and `target_lang` on
  every queue row.
- Use `cityHash64` calculated by ClickHouse as `source_text_hash`.
- Avoid mutating or deleting `input_items`.
- Treat `output_items` as the durable proof of completed work.
- Let Temporal retry transient translation failures.
- Preserve idempotency for queue creation, batch saving, and ClickHouse flush.
- Keep source-specific SQL inside the source package, starting with
  `translator/norway_brreg`.
- Keep provider endpoint definitions centralized in the translator package.
- Use real ClickHouse integration tests for source-specific queue creation.

## Non-Goals

- Do not build a generic country/source framework before at least one more real
  source proves the same shape.
- Do not keep lease tables.
- Do not keep queue manifest tables.
- Do not keep failed item tables in the first implementation.
- Do not keep batch attempt tables in the first implementation.
- Do not use mutable Parquet files as the queue.
- Do not create separate DuckDB files per source column.
- Do not trigger ClickHouse queue dumps automatically on worker startup.
- Do not send static closed-enum translations through the LLM queue.

## Architecture

The new package shape should separate real shared mechanics from source-specific
behavior.

```text
translator/
  endpoints.py
  queue.py
  provider.py
  llm_batch.py
  worker.py

  norway_brreg/
    queue_build.py
    queue_flush.py
    static_translations.py
    workflows.py
```

The exact filenames can change during implementation, but the ownership should
not:

- Central translator package:
  - endpoint configuration
  - DuckDB queue operations
  - provider calls
  - Temporal worker setup
  - generic workflow loop mechanics

- Norway BRREG package:
  - source table names
  - source columns
  - ClickHouse SQL used to create queue rows
  - static legal-form translation logic
  - final ClickHouse flush for Norway queue rows

## Queue Database

There is one DuckDB file per source:

```text
data/translator/norway_brreg.duckdb
```

That file contains all dynamic Norway BRREG translation rows. Multiple source
columns are represented in the same queue by storing `source_column` on every
row.

There are no per-column queue files:

```text
data/translator/norway_brreg/articles_purpose_original.duckdb
data/translator/norway_brreg/activity_text_original.duckdb
```

Those paths should not exist in the new design.

## Queue Tables

### `input_items`

`input_items` stores source texts that need dynamic LLM translation.

```sql
create table if not exists input_items (
  source_table text not null,
  source_column text not null,
  source_text text not null,
  source_text_hash ubigint not null,
  source_lang text not null,
  target_lang text not null,
  created_at timestamp not null,

  primary key (
    source_table,
    source_column,
    source_text_hash,
    source_lang,
    target_lang
  )
);
```

Notes:

- `source_text_hash` is ClickHouse `cityHash64(source_text)` stored in DuckDB
  as `ubigint`.
- `source_text` is stored because the provider needs the original text.
- `source_table` and `source_column` are stored because final ClickHouse flush
  needs them.
- `source_lang` and `target_lang` are stored so the row is self-describing and
  flush-safe.
- `created_at` is for traceability only.

### `output_items`

`output_items` stores completed translations.

```sql
create table if not exists output_items (
  source_table text not null,
  source_column text not null,
  source_text text not null,
  source_text_hash ubigint not null,
  source_lang text not null,
  target_lang text not null,
  translated_text text not null,
  provider text not null,
  model text not null,
  completed_at timestamp not null,

  primary key (
    source_table,
    source_column,
    source_text_hash,
    source_lang,
    target_lang
  )
);
```

Notes:

- The primary key matches `input_items`.
- `insert ... on conflict do nothing` makes `save_bulk` idempotent.
- `translated_text` must be non-empty before saving.
- `provider` and `model` are stored because ClickHouse
  `corpscout.text_translations` needs those labels.

## Queue API

The central queue package should expose a small API around one DuckDB file.

Conceptual API:

```python
class TranslationQueue:
    def initialize(self) -> None: ...
    def input_is_empty(self) -> bool: ...
    def get_bulk(self, batch_size: int) -> list[TranslationInputRow]: ...
    def save_bulk(self, rows: list[TranslationOutputRow]) -> int: ...
    def output_rows(self) -> list[TranslationOutputRow]: ...
```

The API should remain intentionally small. It should not expose leases, failed
states, batch attempts, or status transitions in the first implementation.

### `get_bulk(batch_size)`

`get_bulk` returns rows that exist in `input_items` but do not yet exist in
`output_items`.

```sql
select
  i.source_table,
  i.source_column,
  i.source_text,
  i.source_text_hash,
  i.source_lang,
  i.target_lang
from input_items i
anti join output_items o
  on o.source_table = i.source_table
 and o.source_column = i.source_column
 and o.source_text_hash = i.source_text_hash
 and o.source_lang = i.source_lang
 and o.target_lang = i.target_lang
order by
  i.source_table,
  i.source_column,
  i.source_text_hash
limit ?;
```

If `get_bulk` returns an empty list, all current input rows have matching output
rows and translation is complete.

### `save_bulk(rows)`

`save_bulk` inserts completed translations into `output_items`.

```sql
insert into output_items (
  source_table,
  source_column,
  source_text,
  source_text_hash,
  source_lang,
  target_lang,
  translated_text,
  provider,
  model,
  completed_at
)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
on conflict (
  source_table,
  source_column,
  source_text_hash,
  source_lang,
  target_lang
) do nothing;
```

The method should skip or reject rows with empty `translated_text`.

## Norway BRREG Queue Creation

Norway BRREG creates one unified queue database:

```text
data/translator/norway_brreg.duckdb
```

The source-specific queue build function should insert dynamic fields into
`input_items`.

Dynamic fields:

- `articles_purpose_original`
- `activity_text_original`

Static fields:

- `legal_form_description_original`, keyed by `legal_form_code`

Static legal-form translations should not be inserted into `input_items`.
They should be flushed directly to `corpscout.text_translations` through a
source-specific static translation path.

### Articles Purpose Input

Conceptual ClickHouse query:

```sql
select distinct
  'corpscout.no_companies' as source_table,
  'articles_purpose_original' as source_column,
  articles_purpose_original as source_text,
  cityHash64(articles_purpose_original) as source_text_hash,
  'no' as source_lang,
  'en' as target_lang
from corpscout.no_companies
left anti join (
  select source_text_hash
  from corpscout.text_translations
  where source_table = 'corpscout.no_companies'
    and source_column = 'articles_purpose_original'
    and source_lang = 'no'
    and target_lang = 'en'
  group by source_text_hash
) t on t.source_text_hash = cityHash64(articles_purpose_original)
where articles_purpose_original != '';
```

### Activity Text Input

Conceptual ClickHouse query:

```sql
select distinct
  'corpscout.no_companies' as source_table,
  'activity_text_original' as source_column,
  activity_text_original as source_text,
  cityHash64(activity_text_original) as source_text_hash,
  'no' as source_lang,
  'en' as target_lang
from corpscout.no_companies
left anti join (
  select source_text_hash
  from corpscout.text_translations
  where source_table = 'corpscout.no_companies'
    and source_column = 'activity_text_original'
    and source_lang = 'no'
    and target_lang = 'en'
  group by source_text_hash
) t on t.source_text_hash = cityHash64(activity_text_original)
where activity_text_original != '';
```

The implementation can stream each query through Arrow and bulk insert into
DuckDB.

## Workflow Model

Use one source translation workflow instead of separate BuildQueue and
Translate workflows.

For Norway BRREG:

```text
NorwayBrregTranslationWorkflow
```

Workflow input:

```python
@dataclass(frozen=True)
class NorwayBrregTranslationWorkflowInput:
    queue_duckdb_path: str
    batch_size: int
    max_tokens: int
    translation_endpoint_id: str
```

The workflow should execute activities in this order:

1. `ensure_norway_brreg_queue_activity`
2. repeated `translate_queue_batch_activity`
3. `flush_norway_brreg_output_activity`

Conceptual workflow:

```text
ensure queue exists and input_items is populated if needed

loop:
  run translate batch activity
  if activity says no rows:
    break

flush output_items to ClickHouse
return final counts
```

## Queue Initialization

Queue initialization happens when the source workflow starts, not when the
translator worker starts.

Startup should register code and endpoint configuration only. It should not dump
ClickHouse data into queue files.

For Norway BRREG:

1. Create DuckDB file if missing.
2. Create `input_items` and `output_items` if missing.
3. If `input_items` has rows, do not rebuild it.
4. If `input_items` is empty, run the explicit Norway ClickHouse queries and
   insert rows into `input_items`.
5. Flush static legal-form translations directly to ClickHouse.

This makes queue creation idempotent and avoids surprising worker startup side
effects.

## Batch Translation

The batch worker activity receives:

- queue path
- batch size
- translation endpoint ID
- max tokens

It does:

1. Open the DuckDB queue.
2. Call `get_bulk(batch_size)`.
3. If no rows are returned, report `empty=True`.
4. Convert rows into provider inputs.
5. Call the configured translation endpoint.
6. Validate provider response.
7. Save translations through `save_bulk`.
8. Report saved count.

The provider should receive stable batch-local IDs, not raw source hashes, so
response validation remains simple. The activity maps provider IDs back to queue
rows before saving.

## Error Handling And Temporal Retry

The first implementation should lean on Temporal retries.

Rules:

- Transient provider failure should raise from the activity.
- Invalid JSON or invalid response shape should raise from the activity.
- Temporal should retry the activity up to 10 attempts.
- If all attempts fail, the workflow should fail visibly.

There is no `failed_items` table in this design. That is acceptable because the
agreed invariant is one active workflow per queue and Temporal owns retry
history.

If poison batches become a real operational problem, add an explicit error table
later. Do not add it preemptively.

## Endpoint Configuration

Translation endpoints should be configured centrally in the translator package.

Conceptual shape:

```python
TRANSLATION_ENDPOINTS = {
    "local_qwen": TranslationEndpoint(
        unique_name="local_qwen",
        model="...",
        base_url="...",
        api_key_env="TRANSLATION_PROVIDER_LOCAL_API_KEY",
        default_max_tokens=32768,
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
}
```

The source package refers to an endpoint by ID:

```python
TRANSLATION_ENDPOINT_ID = "local_qwen"
```

This keeps source packages explicit while avoiding provider connection details
inside source-specific SQL/build code.

## ClickHouse Flush

When `get_bulk(batch_size)` returns an empty list, the workflow flushes
`output_items` to ClickHouse.

Conceptual ClickHouse insert:

```sql
insert into corpscout.text_translations (
  source_table,
  source_column,
  source_text_hash,
  source_lang,
  target_lang,
  translated_text,
  provider,
  model,
  version
)
select
  source_table,
  source_column,
  source_text_hash,
  source_lang,
  target_lang,
  translated_text,
  provider,
  model,
  {version}
from output_items;
```

Implementation detail:

- The rows may still be staged through a ClickHouse Memory table if that remains
  the simplest reliable way to bulk insert from Python.
- The flush should be idempotent with the ClickHouse table's uniqueness model.
- Empty translated text must not be flushed.

## Static Translation Path

Static closed-enum fields should bypass the LLM queue.

For Norway BRREG:

- Use `legal_form_code`.
- Resolve through `LEGAL_FORM_DESCRIPTION_EN_BY_CODE`.
- Write rows directly to `corpscout.text_translations`.
- Label rows with provider/model `static`.

This can happen during queue initialization because it is deterministic and does
not need Temporal batch retry semantics.

## Concurrency Model

The design assumes one active Temporal workflow per queue database.

For Norway:

```text
workflow_id = "translate-norway_brreg"
```

Starting the workflow should use Temporal's existing-workflow policy so duplicate
triggers do not run concurrent queue processors for the same DuckDB file.

Because only one workflow writes to `output_items`, the queue does not need:

- leases
- leased timestamps
- worker ownership
- stale lease release
- row status transitions

Idempotent `save_bulk` still protects against activity retries after partial
success.

## Dagster Trigger

Dagster should start the single source translation workflow after the source
ClickHouse table is refreshed.

For Norway:

```text
norway_brreg_translation_trigger
  -> starts NorwayBrregTranslationWorkflow
```

The trigger should not start a separate build queue workflow. It should pass
only operator-tunable runtime values:

- queue path
- batch size
- max tokens
- translation endpoint ID

Defaults belong at the Dagster config boundary. Runtime workflow inputs should
be explicit.

## Testing Strategy

### Queue Tests

Queue tests should use local temporary DuckDB files.

They should verify:

- `initialize` creates `input_items` and `output_items`.
- `get_bulk` returns input rows not present in output.
- `get_bulk` returns an empty list after matching output rows are saved.
- `save_bulk` is idempotent.
- empty translated text is rejected or skipped.
- unified rows from multiple source columns can live in one input table.

These tests do not need ClickHouse or a real provider.

### Provider And Batch Tests

Provider and batch tests should use fake provider responses.

They should verify:

- batch-local IDs map back to queue rows.
- missing IDs fail validation.
- duplicate IDs fail validation.
- invalid JSON fails validation.
- provider exceptions propagate for Temporal retry.

These tests do not need ClickHouse.

### Norway Queue Creation Tests

Norway source queue creation should be tested against a real ClickHouse test
database.

The tests should verify:

- source fixtures are inserted into `corpscout.no_companies`.
- already-translated rows are inserted into `corpscout.text_translations`.
- queue creation writes exactly the expected `input_items` rows.
- both dynamic columns enter the same DuckDB queue file.
- `source_table`, `source_column`, `source_lang`, `target_lang`, and
  `source_text_hash` are correct.
- `source_text_hash` matches ClickHouse `cityHash64`.
- static legal-form translations are flushed directly and do not enter
  `input_items`.

No mocking should be used for this source-specific ClickHouse behavior.

### Workflow Tests

Workflow tests should verify:

- the workflow initializes queue before translation.
- repeated batch activities run until no rows remain.
- flush runs only after queue completion.
- activity failure triggers Temporal retry policy.
- duplicate workflow starts do not process the same queue concurrently.

## Migration Plan

1. Keep current translator code in place while adding the new queue API and
   tests.
2. Add the new DuckDB `input_items`/`output_items` queue implementation.
3. Add Norway-specific queue build SQL that writes unified rows.
4. Add Norway-specific static translation flush.
5. Add batch translation activity using the new queue API.
6. Add single Norway translation workflow.
7. Update Dagster trigger to start the new workflow.
8. Keep old BuildQueue/Translate workflow code temporarily only until tests and
   runtime validation pass.
9. Remove old generic source config, generic scan SQL, and old workflow split.

## Acceptance Criteria

- `data/translator/norway_brreg.duckdb` is the only dynamic queue file for
  Norway BRREG.
- The queue database contains only `input_items` and `output_items` queue tables.
- `input_items` contains rows for both `articles_purpose_original` and
  `activity_text_original`.
- No static legal-form rows enter `input_items`.
- `get_bulk` skips rows already present in `output_items`.
- `save_bulk` can be safely retried.
- The workflow has one source-level translation run, not separate build and
  translate workflows.
- ClickHouse flush writes rows with the original `source_table`,
  `source_column`, `source_text_hash`, `source_lang`, and `target_lang`.
- Norway queue creation tests use real ClickHouse and assert exact queue
  contents.
- Queue mechanics tests use local DuckDB files and do not mock DuckDB.

## Decisions Made

- Use DuckDB instead of mutable Parquet queue files.
- Use one DuckDB file per source, not per source column.
- Use a unified source queue with metadata columns.
- Keep exactly two queue tables in the first implementation.
- Do not delete from `input_items`.
- Use `output_items` as completion state.
- Let Temporal own retry attempts.
- Do not add failure tables until a concrete operational need appears.
- Do not populate queues on worker startup.
- Keep static translations outside the LLM queue.

## Open Questions

There are no blocking open questions for the first implementation.

Potential later decisions:

- Whether to add a poison-batch table if deterministic provider failures block
  real runs.
- Whether to export completed queues to Parquet for long-term audit.
- Whether to introduce a generic source registration model after a second source
  needs the same workflow shape.
