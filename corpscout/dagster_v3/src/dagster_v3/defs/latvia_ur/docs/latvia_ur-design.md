# Latvia UR / Latvia financial design doc

> Per `docs/source-design-doc-template.md` / `docs/data-source-guidelines.md`.

## 1. Source overview
- **Country / registry**: Latvia — Uzņēmumu Reģistrs (UR), open data on `data.gov.lv`.
- **Modules**: `defs/latvia_ur/` for register/company data, `defs/latvia_financial/` for
  financial statements/metrics · DuckDB `data/latvia_ur_source.duckdb` · pool `latvia_ur_duckdb`
- **ClickHouse**: `corpscout.lv_companies` (000015), `lv_financial_statements` (000016),
  `lv_financial_metrics` (000019); repair 000020; provenance drop 000021.
- **Datasets** (free bulk CSV, no auth): `register.csv` (register) + 4 financial CSVs linked by
  `statement_id` — `financial_statements`, `balance_sheets`, `income_statements`,
  `cash_flow_statements`. Plain (unzipped) `;`-delimited CSV; the financials file is ~140 MB.
- **Entity key**: `regcode` · **counts**: 485,380 companies; 1,971,834 statements/metrics.

## 2. Ingest mode — bulk file, non-partitioned full-refresh
- **Why**: full-snapshot CSVs published openly → simplest path. Register URL stable; financial CSV
  URLs are stable `data.gov.lv` resource links.

## 3. Loading
- Register: narrow **dlt row-resource** (`iter_latvia_ur_entity_rows`) — applies the static legal-form
  translation + status derivation per row, over 485 k rows.
- Address enrichment: three VZD State Address Register CSVs are loaded as reference tables in the
  same DuckDB database:
  `AW_EKA.CSV` -> `latvia_address_buildings_duckdb`,
  `AW_PILSETA.CSV` -> `latvia_address_cities_duckdb`,
  `AW_NOVADS.CSV` -> `latvia_address_municipalities_duckdb`.
- Financials: one Dagster **multi-asset** downloads the four full CSV files and writes four DuckDB
  raw staging tables with `read_csv(all_varchar=true)`. Each output is visible as its own raw asset:
  `latvia_financial_statements_raw_duckdb`, `latvia_balance_sheets_raw_duckdb`,
  `latvia_income_statements_raw_duckdb`, `latvia_cash_flow_statements_raw_duckdb`.
- `raw_entity`/`raw_financial_record` + `source_payload_hash` kept in DuckDB only.

## 4. Transform
- **Register → tier 1 (enriched copy)**: `entities` + company activity + VZD address reference
  tables → `lv_companies`.
  The ClickHouse export joins UR `address_id`, `city_code`/`region_code`, and `atvk_code` against
  VZD address objects to populate `vzd_address_text`, `vzd_address_postal_code`,
  `address_city_name`, `address_municipality_name`, `address_latitude`, and `address_longitude`.
- **Financials → tier 2 (set-based SQL)**: pivot the 4 raw tables on `statement_id` into the wide
  `financial_statements` (16 balance + 26 income + 35 cashflow numeric columns); native metrics
  build (scaled by `rounded_to_nearest`); separate USD step.

## 5. ClickHouse schema — deviations
- `lv_companies` 1/company (`ORDER BY regcode`); statements/metrics 1/statement
  (`ORDER BY (regcode, statement_id)`).
- **Deviations**: `source_type LowCardinality(String)` (see issue below); **`rounded_to_nearest`
  scaling** (`ONES`/`THOUSANDS`/`MILLIONS`) applied before FX — Latvia reports are scaled, unlike
  Estonia's full EUR. `raw_*`/`source_payload_hash` created in the original DDL then **dropped**
  (000021) — new sources omit them from the start.

## 6. Translation
- Legal form → **static map** `LATVIA_LEGAL_FORM_DESCRIPTION_EN_BY_CODE` (`legal_form_text` +
  `legal_form_description_en`). **No LLM.** Names/addresses not translated.

## 7. Currency
- **EUR + legacy LVL**: ~388 k pre-2014 statements are in **LVL**, which is **not in the ECB set** →
  they keep native-only (`*_usd` NULL); EUR rows convert. Documented; deriving LVL→USD via the fixed
  pre-euro peg is a possible future enhancement. Metrics carry `_original`/`_usd` + fx, keyed on
  `period_end_date`, via the batched `apply_latvia_ur_usd_conversion`.

## 8. Scheduling
- `latvia_ur_register_job` daily **04:30**.
- `latvia_financials_job` weekly Monday **05:00**. It is a full-refresh chain because Latvia
  publishes complete financial CSV snapshots and re-pulling them every 7 days is acceptable.
  Staggered; default STOPPED.

## 9. Issues found during processing
- **`source_type` NULL → `'NoneType' has no attribute 'encode'`** on insert: ~685 k pre-euro filings
  have no `source_type`, but the column is non-nullable `LowCardinality(String)` → **coalesce to `''`**
  in the wide build. (A comprehensive scan confirmed it was the *only* non-nullable string with NULLs;
  this is now a general CLAUDE.md rule.)
- **40-minute metrics build**: the native-metrics asset was a Python `fetchall()` + per-row `Decimal`
  loop over 2 M rows → **rewritten as one DuckDB `CREATE TABLE AS SELECT`** (~0.9 s, ~2,600×). It was
  also the step that held the single-writer pool and starved everything behind it.
- **USD step hit ClickHouse code 572** (`TOO_MANY_QUERY_PLAN_OPTIMIZATIONS`): 1,077 (currency,date)
  pairs → the client built a 1,077-branch `UNION ALL`. Fixed at the root (**array-param query** in
  `ExchangeRateClient`) + batched `_load_rates` (≤50/call).
- **`ChunkedEncodingError`/`IncompleteRead` mid-stream** on the 140 MB balance-sheets download →
  whole-download **retry loop** re-truncating the temp file + `Content-Length` check
  (`_download_to_path`).
- **DuckDB "Ambiguous reference"** when the file stem == dataset name → file `latvia_ur_source.duckdb`,
  dataset `latvia_ur`.
- Financials depend on `corpscout.exchange_rates` being populated first (the USD step no-ops safely if
  empty, then can be re-run).

## 10. Verification
- Tests `tests/test_latvia_ur_*.py`; live: `lv_companies` 485,380; statements/metrics 1,971,834;
  EUR rows USD-filled (sample `42103111054/2023`: 27788 × 1.105 = 30705.74), LVL native-only.
