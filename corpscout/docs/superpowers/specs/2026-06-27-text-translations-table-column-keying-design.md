# text_translations: key by physical table + column (self-describing cache)

**Date:** 2026-06-27
**Status:** Design approved, pending implementation
**Area:** `corpscout/dagster_v3/translator/` + `corpscout/clickhouse/migrations/`

## Goal

Re-key the `corpscout.text_translations` cache on the **physical table and column** the
source text comes from, instead of the abstract `source_slug` + `field`. A cache row must be
legible on its own — no code or registry lookup required to know where a translation belongs —
while preserving the two properties the current design already has: **no text duplication** and
**no re-translation across `no_companies` rebuilds**.

## Motivation / problems with the current design

`text_translations` is currently keyed by `(source_slug, field, source_text_hash)`:

1. **Not self-describing.** A row `norway_brreg / company_description / <hash>` does not say which
   table or column it maps to. You must open `translator/registry.py` and the view SQL to decode it.
2. **Mapping duplicated in code.** `source_slug → table` and `field → column` live in *two* hand-synced
   places — `registry.py` (`ch_table`, `field`/`original_col`) and the view SQL. Renaming a table means
   editing both, from memory.

Rejected alternatives:
- **Materialize `_en` onto `no_companies`** (store the English as real columns): stores every
  translation twice (the `_en` column *and* the cache) and must be re-stitched after every
  wipe-and-replace export. Duplication — rejected.
- **Drop the cache, only keep `_en` on the table**: every `EXCHANGE TABLES` rebuild wipes `_en` →
  re-translate everything. Defeats the purpose — rejected.

## Decision

Keep the **cache + view** shape (translation stored once, `_en` computed on read), but replace the
abstract key with the physical one:

- `source_slug` → **`source_table`** — the fully-qualified table, e.g. `corpscout.no_companies`.
- `field` → **`source_column`** — the **exact, literal column name** whose text was translated. No
  suffix convention: whatever the column is named is what is stored (for `no_companies` today that is
  `company_description_original` etc.; a source whose column is named `description` would store
  `description`).

`source_table` is unique per source (each source owns its table), so it fully subsumes `source_slug`.
`source_slug` survives **only** as the registry key and Temporal workflow id
(`translate-norway_brreg`) — internal processing addressing, never a column in the data.

### No text duplication (unchanged, restated)

| Where | Holds | Copies |
|---|---|---|
| `no_companies.<source_column>` | the source-language text | 1 |
| `text_translations.translated_text` | the English text | 1 |
| `text_translations.source_text_hash` | `cityHash64` of the source text — a link, not a copy | — |
| view `no_companies_translated` | `*_en` computed on read (a join), never stored | — |

## Schema

```sql
corpscout.text_translations
    source_table      LowCardinality(String)   -- 'corpscout.no_companies'
    source_column     LowCardinality(String)   -- literal column name, e.g. 'company_description_original'
    source_text_hash  UInt64                    -- cityHash64(text in that column)
    source_lang       LowCardinality(String)
    target_lang       LowCardinality(String)
    translated_text   String
    provider          LowCardinality(String)   -- model name | 'static' | 'legacy-import'
    model             LowCardinality(String)
    version           UInt64
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_table, source_column, source_text_hash)
```

`source_table`/`source_column` are `LowCardinality(String)` (few distinct values) and non-nullable, so
they are valid in `ORDER BY`. The join key is still `cityHash64(<text>)`, computed **only in ClickHouse**
on both write (flush/static) and read (view) — one definition, no drift.

## View (`no_companies_translated`) — re-pointed

Unchanged shape; each `_en` join now filters on the physical key:

```sql
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'company_description_original'
    GROUP BY source_text_hash
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original)
```
…one join per translated column (articles_purpose, activity_text, company_description,
legal_form_description), each producing the corresponding `*_en` alias. The view spells out each
`_original`-column → `_en`-alias pairing explicitly (no convention in code).

## Code changes (write side)

- **`translator/registry.py`**
  - `FieldConfig`: **remove `field`.** Keep `original_col` (this is the `source_column`), `static_map`,
    `static_key_col`.
  - `SourceConfig`: `ch_table` is the `source_table`; keep `source_lang`, `fields`, and `source_slug`
    (registry/workflow key only).
- **`translator/clickhouse.py`** (scan): `SELECT DISTINCT c.<original_col> [, c.<static_key_col>]` with the
  anti-join `WHERE source_table = {ch_table} AND source_column = {original_col}`.
- **`translator/flush.py`**: `INSERT … (source_table, source_column, source_text_hash, …) SELECT
  {table}, {column}, cityHash64(source_text), …`.
- **`translator/workflow.py`** (static resolution): flush `legal_form_description_original` results with
  `source_table`/`source_column` and `provider='static'`.
- **`translator/queue.py`**: the DuckDB queue already records `source_field` = the column name; carry it as
  `source_column` through `completed_results_for_flush`.
- **`translator/import_legacy.py`**: filter the old queue by the registry's `original_col`s (the queue's
  `source_field` already equals the column name — direct match, no logical-field remap needed) and write
  `source_column` = that column. Skip static + unknown columns. Batched flush as today.

## Migration `000063`

`text_translations` is empty (0 rows), so reshape is destructive-safe:
```sql
-- up
CREATE DATABASE IF NOT EXISTS corpscout;
DROP TABLE IF EXISTS corpscout.text_translations;
CREATE TABLE corpscout.text_translations ( …new schema… ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (source_table, source_column, source_text_hash);
CREATE OR REPLACE VIEW corpscout.no_companies_translated AS …new joins… ;
```
Down reverts to the `(source_slug, field, source_text_hash)` table + the `field`-keyed view. Append
`"000063_corpscout_text_translations_table_column"` to `EXPECTED_MIGRATIONS`.

## Documented rename procedure

When a source table is renamed, the cache moves with it:
```sql
ALTER TABLE corpscout.text_translations UPDATE source_table = 'corpscout.<new>'
    WHERE source_table = 'corpscout.<old>';
```
plus edit the view's `FROM`/`WHERE` and the registry's `ch_table`.

## Non-goals

- No materializing `_en` onto `no_companies` (duplication).
- No renaming the `no_companies` base columns (store literal names as-is).
- No change to `no_companies`'s own schema.
- `source_slug` is not removed from the registry/workflow — only from the cache table.

## Testing

- `test_translator_registry.py`: `FieldConfig` has no `field`; `ch_table`/`original_col`/static fields present.
- `test_translator_scan.py`: scan SQL filters `source_table`/`source_column`; static field selects the key col.
- `test_translator_flush.py`: flush writes `source_table`/`source_column` + `cityHash64(source_text)`.
- static-resolution unit test: known legal-form code → English, unknown → '' (skipped).
- `test_translator_import_legacy.py`: imports by `original_col`, writes `source_column`, skips static/unknown.
- `test_text_translations_schema.py` / `test_clickhouse_migrations.py`: 000063 table + view contract; `EXPECTED_MIGRATIONS` entry.
- Full suite + `dg check defs` green.

## Rollout

1. Implement (subagent-driven, reviewed).
2. Deploy + `make clickhouse-migrate-up` on the server (applies 000063 — table empty, safe).
3. Run the 2.1M legacy import (now writing `source_table`/`source_column`).
4. Start the worker / fire the trigger; verify `no_companies_translated`.

## Docs to update

`dagster_v3/README.md` "# Translation" (schema + view + monitoring queries), and
`docs/data-source-guidelines.md` §8 + the design-doc template (cache keyed by table+column).
