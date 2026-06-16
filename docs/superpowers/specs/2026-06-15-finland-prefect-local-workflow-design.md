# Finland Prefect YTJ Base Ingest Design

## Goal

Create the first narrow Prefect task in `companycollect/processor`: download the
Finland PRH YTJ `/all_companies` JSON into S3/RustFS once as the base file, and
create a Parquet base list for downstream processing.

## Scope

Included:

- PRH YTJ `/all_companies` JSON to S3 as `base.json`.
- `base.parquet` derived from `base.json`, with business ID, registration date,
  status fields, lifecycle status, active flag, and legal name.
- Idempotent base-object checks: existing `base.json` means the workflow does not
  redownload or rebuild it unless `refresh=True`.
- A Prefect flow that can run manually.
- S3-compatible storage using existing `CORPSCOUT_S3_*` environment variables.

Excluded:

- Local Parquet outputs.
- Structured/canonical processing.
- dbt integration.
- ClickHouse ingestion.
- XBRL download.
- Dagster assets or Dagster schedules.

## Architecture

The implementation lives in `companycollect/processor/finland_raw_ingest.py`.

The workflow uses plain Python helper functions for API and S3 work, with a small
Prefect flow around them:

- `download_ytj_base_to_s3`: materializes YTJ `/all_companies` as `base.json`,
  then ensures `base.parquet` exists.
- `finland_raw_ingest_flow`: runs the YTJ base task and creates a Prefect Markdown
  summary artifact.

## Storage Layout

YTJ bucket:

```text
source-finland-prhytj/
  base/base.json
  base/base.parquet
  base/manifest.json
```

## Parameters

- `refresh`: if `False`, reuse existing `base.json` and skip the full download. If
  `True`, redownload and rebuild `base.json` and `base.parquet`.

## Error Handling

HTTP calls retry transient `429`, `5xx`, timeout, and connection errors. Persistent
errors fail the flow. The `base.json` object is checked before the API call so the
base workflow is naturally run-once unless explicitly refreshed.

Manifests record object keys, company count, Parquet key, and skip/download
outcomes. Prefect records run state and exposes a Markdown summary artifact.

## Acceptance Criteria

- The workflow writes YTJ `/all_companies` JSON as `base.json` to
  `source-finland-prhytj`.
- The workflow writes `base.parquet` to `source-finland-prhytj`.
- Existing `base.json` skips the full download when `refresh=False`.
- `refresh=True` forces redownload.
- The flow can run once from Python.
