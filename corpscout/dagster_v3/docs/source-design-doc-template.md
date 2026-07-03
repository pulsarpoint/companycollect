# <Source> design doc

> Copy to `defs/<source>/docs/<source>-design.md` and fill in. Records *decisions*, not code.
> Follows `docs/data-source-guidelines.md`; call out and justify every deviation from it.

## 1. Source overview
- **Country / registry**: <e.g. Estonia — e-Business Register (Äriregister)>
- **Module**: `defs/<source>/` · DuckDB file `data/<source>_source.duckdb` · pool `<source>_duckdb`
- **ClickHouse tables**: `corpscout.<table>` (migrations `0000XX…`)
- **Datasets used** (URL, format, size, update cadence, licensing/credentials):
  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
- **Entity key**: <reg_code / business_id / org_number> · **record count**: <~N>

## 2. Ingest mode (§2 of guidelines) — and *why*
- Chosen: <bulk file full-refresh | partitioned API incremental | single-request full-refresh>
- Why this over the alternatives: <e.g. bulk CSV snapshot exists → no need to page the API; preferred>
- Format choice: <CSV/JSON/Parquet> because <…>. Quirks: <delimiter, encoding/BOM, date format,
  quoting, zipped, rotating filenames, …>
- If **partitioned**: partition key + granularity, backfill policy, incremental cursor, and why.

## 3. Loading (§3)
- Reader: <DuckDB read_csv/read_json/read_parquet | read_csv_duckdb(pyarrow) | narrow dlt row-resource>
- Why (and why *not* row-by-row Python): <…>
- Staging shape: raw tables <names>; raw provenance kept (`raw_*`, `source_payload_hash`) in DuckDB only.
- Checkpoints / per-file split: <…> · filename resolution: <static | resolved from index>

## 4. Transform (§5)
- Mechanism: <set-based DuckDB SQL | dbt> — and why (if dbt: which earns-its-keep reason).
- Shape: <register row-map | wide pivot | EAV conditional-aggregate pivot | …>. Key SQL ideas: <…>

## 5. ClickHouse schema — and DDL deviations
- Tables + grain: <ee_companies (1/company), ee_financial_statements (1/report), …>
- `ORDER BY`: <…> · engine: ReplacingMergeTree
- **Deviations from the norm** (and why): <e.g. `report_category_*` instead of `statement_type_*`
  because the source has no per-report statement type; `first_entry_date Nullable(Date)` parsed from
  DD.MM.YYYY; element X has no native field → column stays NULL; …>
- Export subset: `<TABLE>_EXPORT_COLUMNS` drops `raw_*`/`source_payload_hash`.

## 6. Translation (§8) — loader in `src/dagster_v3/defs/translator_load/assets.py`
| label | source_column (= original_col) | mechanism | static_map / notes |
|---|---|---|---|
| legal form | `legal_form_original` | static dict | `<CC>_LEGAL_FORM_EN_BY_CODE`, `static_key_col=legal_form_code` |
| status | `status_original` | static dict | |
| description (if any) | `<field>_original` | LLM | free text |
- `_en` is served by the `<source>_translated` join view from the `text_translations` cache — **not**
  base-table columns (the base table carries only `<field>_original`). The cache key is
  `(source_table, source_column, source_text_hash)` where `source_column` = the `original_col` value above.
- A loader asset scans for untranslated text after the ClickHouse export and enqueues it to the
  standalone Go translator service (`corpscout/translator`); static-map fields are direct-inserted (no sensor).
- Fields deliberately **not** translated (proper nouns): <name, address>.

## 6b. Contacts (§8b) — MANDATORY to assess
- Contact data found? <yes / no — if no, say why none is available>. Source dataset: <e.g. `yldandmed`
  `sidevahendid`>. Types present: <Website/Email/Phone/Mobile/Fax/…>.
- Stored as `<cc>_company_contacts` (1 row per contact). Website coverage: <~N / partial>.

## 7. Currency (§7)
- Native currency: <EUR | …>; legacy/unmapped currencies: <e.g. LVL — native-only, `_usd` NULL>.
- Metrics carry `<metric>_amount_original` + `<metric>_amount_usd` + `fx_rate_to_usd/_date/_source`,
  keyed on `period_end_date`. Conversion is the separate `apply_<source>_usd_conversion` step.

## 8. Scheduling (§9)
- Jobs + cadence: <register daily HH:MM; financials monthly Nth>; cron stagger vs other sources: <…>
- Special orchestration: <translation loader asset downstream of the ClickHouse export>

## 9. Issues found during processing
> The most valuable section — what bit us, the symptom, and the fix, so the next source avoids it.
- <e.g. report-general column names contain a literal `?` → collides with DuckDB `?` params → inline
  the values instead.>
- <e.g. ChunkedEncodingError mid-stream on the 140 MB file → whole-download retry loop.>
- <e.g. `+leaf` CLI selection only resolved 1 hop → launch the explicit asset list / use `.upstream()`.>

## 10. Verification
- Tests: `tests/test_<source>_*.py` (mapping, pivot, export/migration match, schedule registration).
- Live: migrate → materialize (register then financials) → spot-check ClickHouse (counts, `_en` and
  `_usd` populated, a known row's `original × rate`). Run `scripts/dagster-health-check.py` after.
