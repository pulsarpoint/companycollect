# Translator Generic Engine — Design

**Date:** 2026-07-03
**Status:** Approved design, pending implementation plan
**Scope:** `corpscout/translator`

## Problem

The translator service can only translate Norway BRREG data. Everything specific to
Norway lives hardcoded in Go (`internal/brreg`): the per-column scan queries, the
legal-form static translation map, the workflow/task-queue identity, and the
source/target language (which today sits on the *endpoint* config as `prompt_data`,
even though language is a property of the data source, not the LLM endpoint).

The rest of the pipeline — DuckDB queue (`input_items`/`output_items`/`failed_items`),
batch translation loop, `text_translations` insert into ClickHouse — is already fully
generic. Adding a new country or a new column should be a config change, not a Go
change.

## Goal

Translate **any column in any table** by adding a per-source definition file. A new
source requires: one JSON definition file, one entry in `translator.json`, worker
restart. No rebuild, no new Go code.

## Design

### 1. Per-source definition file

One JSON file per source under `config/sources/`:

```json
{
  "source": "norway_brreg",
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "columns": [
    { "table": "corpscout.no_companies", "column": "articles_purpose_original" },
    { "table": "corpscout.no_companies", "column": "activity_text_original" },
    {
      "table": "corpscout.no_companies",
      "column": "legal_form_description_original",
      "static": {
        "key_column": "legal_form_code",
        "values": {
          "AS": "Private limited company",
          "ASA": "Public limited company"
        }
      }
    }
  ]
}
```

Field semantics:

- `source` — source name; must match the key in `translator.json` `sources`.
- `source_lang` / `target_lang` — ISO codes written into queue rows and
  `text_translations` (`source_lang`, `target_lang` columns).
- `source_language_name` / `target_language_name` — human-readable names used to
  frame the LLM prompt. These **replace** the endpoint-level `prompt_data`, which is
  removed. One endpoint can then serve sources in any language pair.
- `columns[]` — what to translate. Each entry:
  - `table`, `column` — required. Fully qualified table name and column name.
  - `static` — optional. When present the column is translated from the `values`
    map keyed by `key_column` (another column on the same table) instead of the LLM.
    Static translations are flushed directly to ClickHouse with
    `provider = "static"`, `model = "static"`, exactly like today's legal-form flow.
  - `custom_sql_file` — optional. Path to a `.sql` file, resolved **relative to the
    definition file's directory**. Overrides the generated scan query for this
    column. There is no inline SQL field; the file is the single escape hatch.

### 2. Generated scan SQL

For columns without `custom_sql_file`, the engine renders a Go `text/template`
embedded in the engine package (it is behavior, not per-source config). The output is
byte-for-byte equivalent to today's brreg constants:

LLM column template (data: Table, Column, SourceLang, TargetLang):

```sql
SELECT DISTINCT
    '{{.Table}}' AS source_table,
    '{{.Column}}' AS source_column,
    c.{{.Column}} AS source_text,
    cityHash64(c.{{.Column}}) AS source_text_hash,
    '{{.SourceLang}}' AS source_lang,
    '{{.TargetLang}}' AS target_lang
FROM {{.Table}} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{{.Table}}' AND source_column = '{{.Column}}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{{.Column}})
WHERE c.{{.Column}} <> ''
```

Static column template (data: Table, Column, KeyColumn) selects
`(source_text, source_text_hash, key)` with the same anti-join, matching today's
`legalFormDescriptionScanSQL`. Because both shapes are generated, **Norway needs no
`.sql` file at all** — its definition is fully declarative.

Custom SQL contract: an LLM column's custom query must return the same six columns
as the generated LLM query; a static column's custom query must return the same
three columns as the generated static query.

### 3. Config wiring and loading

`translator.json` `sources` keeps deployment concerns and points at the definition:

```json
"sources": {
  "norway_brreg": {
    "queue_path": "data/translator/norway_brreg.duckdb",
    "endpoint_id": "local_llm",
    "definition_path": "config/sources/norway_brreg.json"
  }
}
```

Startup flow:

1. `config.Load` parses `translator.json` as today.
2. For each source, `engine.LoadDefinition(definition_path)` parses the definition
   JSON and `os.ReadFile`s every `custom_sql_file` relative to the definition file's
   directory. The SQL string is stored on the in-memory column spec; downstream code
   never knows whether a query was generated or file-loaded.
3. Validation is fail-fast at startup: definition name matches the `sources` key,
   langs and language names non-empty, columns non-empty, `table`/`column` non-empty,
   static `values` non-empty when `static` present, referenced `.sql` files exist and
   are non-empty. A worker with a broken definition refuses to boot rather than
   failing mid-workflow.

Definitions are read once at boot; changing one means restarting the worker, same as
any config change today. `go:embed` was rejected: it would turn "add a source" back
into "rebuild and redeploy".

### 4. Engine package (`internal/engine`)

`internal/brreg` is generalized into `internal/engine` and then deleted. The engine
contains:

- `Definition` / `ColumnSpec` types + `LoadDefinition` + validation.
- Scan SQL generation (the two templates above).
- The existing `Runtime` (queue lifecycle, `LoadNewInput`, `ProcessOneBatch`,
  `UploadOutput`), parameterized by a `Definition` instead of hardcoded constants.
  `LoadNewInput` iterates the definition's columns: LLM columns → scan → DuckDB queue
  upsert; static columns → scan → map lookup → direct `text_translations` flush.
- The existing ClickHouse adapter and queue table DDL, unchanged.
- One generic Temporal workflow (today's `NorwayBRREGWorkflow` body, renamed), with
  per-source identity derived from the source name:
  - `WorkflowID = "translator/<source>"`
  - `TaskQueue  = "translator-<source>"`
  - Activity names `"<source>.LoadNewInput"`, `"<source>.ProcessOneBatch"`,
    `"<source>.UploadOutput"`

The worker builds one runtime + one registration per configured source; each source
runs independently on its own task queue with its own DuckDB queue file. The
translation provider receives the source's language names for prompt framing instead
of reading endpoint `prompt_data`.

The API/trigger surface (`translator-trigger`, HTTP router, trigger script) is
parameterized by source name and derives workflow ID / task queue via the same
helpers, instead of importing brreg constants.

### 5. Migration and compatibility

- **Queue files:** DuckDB schema is unchanged; existing `norway_brreg.duckdb` keeps
  working.
- **ClickHouse:** `text_translations` insert is unchanged.
- **Temporal identity:** workflow ID and task queue for Norway are unchanged
  (`translator/norway_brreg`, `translator-norway-brreg`). Activity names change from
  `brreg.*` to `norway_brreg.*`; any waiting/running brreg workflow must be
  terminated and re-signaled after deploy. This is acceptable — the workflow is
  signal-driven and idempotent (anti-join + queue upsert dedupe re-scans).
- **Norway legal-form map:** moves verbatim from Go into
  `config/sources/norway_brreg.json` `static.values`.
- **Endpoint `prompt_data`:** removed from `EndpointConfig` and `translator.json`.

## Error handling

- All definition problems fail at startup with the definition path in the error.
- A scan query failure during `LoadNewInput` fails the activity (Temporal retries as
  today).
- A static column whose scanned key has no entry in `values` is skipped, matching
  current behavior (`legalFormDescriptionENByCode` miss → skip).

## Testing

- **Golden SQL tests:** generated queries for the Norway definition compared
  byte-for-byte against the current brreg constants — proves the refactor is
  behavior-preserving.
- **Definition loading:** table-driven tests for validation failures, relative
  `.sql` resolution, missing/empty files.
- **Static flush:** map hit / miss / empty-text cases (port of existing tests).
- **Integration tests:** existing ClickHouse + DuckDB integration tests re-pointed
  at a definition-driven runtime, executed against real services per project
  convention.

## Out of scope

- Hot-reloading definitions without restart.
- Multiple target languages per source (one definition = one language pair; a second
  pair is a second definition file).
- Backfilling or rewriting existing `text_translations` rows.
