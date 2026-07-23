# IMF World Economic Outlook design

## 1. Source overview

- **Source**: International Monetary Fund — World Economic Outlook (WEO)
- **Module**: `defs/imf_weo/` · DuckDB file `data/imf_weo_source.duckdb` · pool `imf_weo_duckdb`
- **ClickHouse tables**: `corpscout.imf_weo_vintages`, `corpscout.imf_weo_series`, and
  `corpscout.imf_weo_observations` (migration `000158_corpscout_imf_weo`)
- **Dataset**: the current “WEO Entire Dataset in Excel” workbook, discovered from
  `https://data.imf.org/en/datasets/IMF.RES%3AWEO`; about 5.3 MB for April 2026, updated for
  the April and October WEO releases and occasionally corrected between releases; no credentials
- **Coverage**: every country and indicator in the workbook. The April 2026 live validation found
  197 countries, 44 indicators, 8,668 country-indicator series, and 238,397 non-empty observations
  for 2000–2031.
- **Keys**: release `vintage_date`; series `(vintage_date, country_iso3, indicator_code)`;
  observation `(vintage_date, country_iso3, indicator_code, year)`

## 2. Ingest mode — and why

- **Chosen**: non-partitioned bulk-file full refresh with retained publication vintages.
- The official workbook contains all countries and indicators in one small file. Country or year
  partitions would add orchestration state without reducing the download.
- The initial load starts at 2000 and includes every forecast year present. It does not synthesize
  historical publication vintages. New April/October releases are retained; a corrected workbook
  for an already-seen `vintage_date` replaces that vintage atomically in DuckDB.
- The raw asset stores the complete XLSX under its SHA-256 in S3 and writes the run manifest last.
  This gives a durable checkpoint before parsing and avoids hard-coded country lists.

## 3. Loading

- HTTP uses the repository-standard dlt requests client for request-level retries plus a separate
  whole-file retry around streaming. This does **not** use a dlt pipeline or dlt normalization.
- DuckDB's `excel` extension reads the `Countries` worksheet directly with `read_xlsx(...,
  all_varchar=true)`. No workbook rows or normalized observations are materialized as Python
  dictionaries.
- This is an intentional deviation from the normal dlt-owned raw-table boundary: S3 owns the raw,
  immutable workbook checkpoint, while DuckDB is the native reader needed for the wide XLSX.
- The S3 download and hash check finish first. The Excel extension is installed through an
  ephemeral in-memory connection. Only then does the asset open the persistent single-writer
  DuckDB file, so network activity never holds its lock.

## 4. Transform

- Set-based DuckDB SQL validates the workbook metadata, creates one series row per country and
  indicator, and unpivots all year columns from 2000 onward into observations.
- `value` preserves the workbook number. `value_base` applies the declared scale (`Billions`,
  `Millions`, `Thousands`, or `Units`) without changing the indicator's unit.
- `LATEST_ACTUAL_ANNUAL_DATA` classifies later years as estimates. A missing/zero latest-actual
  year means the entire series is estimated, matching the IMF WEO metadata guidance.
- The full workbook and workbook-level metadata sheets remain in the S3 source object. DuckDB and
  ClickHouse contain normalized country series and observations only.

## 5. ClickHouse schema

- `imf_weo_vintages`: one row per WEO publication release with source-object lineage.
- `imf_weo_series`: one metadata row per release, country, and indicator.
- `imf_weo_observations`: one numeric row per release, country, indicator, and year.
- Migration `000158` owns all ClickHouse DDL. The exporter validates the DuckDB contracts and uses
  stage tables plus `EXCHANGE TABLES` to replace the complete retained-vintage snapshot.
- `source_payload_hash` is kept only once per vintage, where it identifies the source workbook;
  it is not repeated on every series or observation.

## 6. Translation, contacts, and industry

- Not applicable. This is an English macroeconomic dataset, not a company registry. It has no
  company contact records, company industry assignments, or registry identifiers.

## 7. Currency

- WEO indicators have source-defined units and scales. Both are retained on the series, and
  `value_base` removes only the declared magnitude scale.
- No ECB conversion step is applied: converting national-currency or ratio indicators would alter
  their meaning. Consumers select an official USD-denominated WEO indicator when USD is required.

## 8. Scheduling

- `imf_weo_refresh_job` selects the complete three-asset chain with `.upstream()`.
- `imf_weo_weekly_schedule` runs Sundays at 05:00 Europe/Belgrade. Weekly polling captures
  corrections between the twice-yearly WEO releases; content-addressing avoids duplicate S3 data.
- The schedule is default-running because the source path and current workbook were live-validated
  before enablement, as explicitly agreed for this pipeline.

## 9. Issues found during processing

- IMF's CDN rejected the short `/Datasets/WEO` route and generic script/browser user agents from
  the validation environment. The pipeline uses the canonical localized dataset route and an
  identified curl-compatible user agent.
- IMF serves the XLSX with gzip content encoding. `requests` reports decoded bytes to the caller,
  while `Content-Length` describes wire bytes, so exact length validation applies only to identity
  responses. XLSX ZIP structure, required sheets, non-zero size, and SHA-256 still validate every
  completed download.
- The workbook is wide and revision-aware. Direct DuckDB unpivoting avoids an in-memory Python
  normalization step, and same-vintage delete/insert inside one transaction handles corrections.

## 10. Verification

- `tests/test_imf_weo.py` covers workbook discovery and storage, encoded downloads, DuckDB
  unpivoting/scaling/estimate flags, vintage retention and correction replacement, migration column
  order, Dagster graph/schedule registration, and atomic ClickHouse publishing.
- `tests/test_clickhouse_migrations.py` registers migration `000158` and enforces the normal
  migration invariants.
- Live validation downloads and normalizes the current IMF workbook with the production source and
  transform functions. Repository validation uses `uv run dg check defs`.
