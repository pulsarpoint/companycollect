# UN Comtrade annual totals source design

## Scope

- Module: `defs/un_comtrade/`
- Raw bucket: `source-un-comtrade`
- DuckDB file: `data/un_comtrade_source.duckdb`
- DuckDB pool: `un_comtrade_duckdb`
- ClickHouse migration: `000160_corpscout_un_comtrade`
- Historical window: fixed at 2015 through the latest completed calendar year
- Credentials: none

This source loads annual merchandise-trade totals for every reporter returned by
UN Comtrade. It does not contain a configured country list. The API request
omits `reporterCode`, so newly available reporting countries and areas enter the
dataset without a code change.

The fixed source query is:

- product type `C` (goods);
- annual frequency `A`;
- classification search code `HS` (each reporter's original HS edition);
- commodity `TOTAL`;
- partner and second partner `0` (World);
- flows `M,X` (imports and exports);
- customs code `C00`; and
- mode of transport `0`.

This is deliberately separate from a future detailed product/partner source.

## Ingest decision

UN Comtrade's anonymous public preview API supports filtered final-data queries
but is capped at 500 rows per request. Total/world trade contains only up to two
rows per reporter and year and remains below that cap, so the raw asset makes
one anonymous preview request per year and batches anonymous data-availability
requests at the source's 12-period limit. It rejects a response at the
500-record cap rather than publishing potentially truncated data.

The Dagster assets are not partitioned. API responses and S3 objects are
physically separated by year, but a refresh is one atomic snapshot. This keeps
monthly refreshes to roughly a dozen API calls, gives each annual response an
auditable raw object, and avoids dynamic country partitions or a maintained
country registry.

The raw downloader uses dlt's retrying HTTP client, honors `Retry-After`, and
paces anonymous requests at one request per second to respect UN Comtrade's
fair-usage policy.

## Asset chain

1. `un_comtrade_snapshot_s3`
   downloads batched public `getDA` availability CSVs first, then one anonymous
   final-data preview CSV for each available year. If the previous calendar
   year has not been released yet, the asset still refreshes every available
   historical year and records both the requested and actual latest year. It
   validates CSV contracts and the preview cap, writes content-addressed
   objects, and writes the run manifest only after every request succeeds.
2. `un_comtrade_annual_totals_duckdb`
   downloads and hash-verifies all snapshot objects before opening the
   persistent DuckDB file. DuckDB's CSV reader performs all casts,
   normalization, filtering checks, and transactional table replacement.
3. `un_comtrade_annual_totals_clickhouse`
   validates both DuckDB contracts and atomically replaces the two
   migration-owned ClickHouse tables.

There is no Python row normalization and no network wait while DuckDB is open.

## Normalized model

- `un_comtrade_annual_availability`: one UN Comtrade dataset release per
  reporter, year, and original classification, including first/last release
  timestamps, record count, checksum, extended-dimension flags, and raw-object
  lineage.
- `un_comtrade_annual_totals`: one reporter/year/import-or-export observation,
  with `primaryValue` in current USD, import CIF/export FOB values where
  supplied, classification edition, reporting/aggregation flags, and raw-object
  lineage.

ClickHouse holds the latest normalized snapshot. Content-addressed S3 objects
retain each monthly source version for audit and future revision-history work.

## Validation

Publication fails when:

- raw files or manifest hashes/counts disagree;
- any row falls outside the fixed goods/annual/total/World query;
- requested years are missing;
- annual totals have duplicate reporter/year/flow keys;
- release metadata has duplicate dataset codes;
- reporter identifiers, release dates, or numeric trade values are malformed;
- a total lacks matching data-availability metadata;
- a historical year outside the two-year publication-lag window has fewer
  than 150 reporters; or
- either normalized table is empty.

The two most recent years are allowed to have fewer reporters because annual
releases arrive gradually and are revised throughout the year. The July 2026
availability catalog contains 139 reporters for 2024 and 90 for 2025, versus
167 or more for 2015-2023.

## Automation

`un_comtrade_refresh_job` selects the complete three-asset chain through
`AssetSelection.assets(...).upstream()`.

`un_comtrade_monthly_schedule` is registered for the 10th at 06:10
`Europe/Belgrade`. It remains stopped until the first production materialization
and ClickHouse validation succeed.

## Issues found during processing

- Public preview is capped at 500 records. The total/world request currently
  returns fewer than 400 records per year, so anonymous access is sufficient
  and the pipeline fails closed if the cap is ever reached.
- Country/year requests improve fault isolation but exceed the free daily quota.
  Year-level requests preserve automatic reporter discovery with far fewer
  calls.
- UN Comtrade total rows can be reported directly
  (`isReported=true`, `isAggregate=false`) or constructed from detailed rows
  (`isReported=false`, `isAggregate=true`). Both are official total/world
  observations, so the pipeline preserves both flags without filtering on
  either one.
