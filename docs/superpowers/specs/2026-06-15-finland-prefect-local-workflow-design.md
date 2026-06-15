# Finland Prefect YTJ Base Ingest Design

## Goal

Create the first narrow Prefect task in `companycollect/processor`: download the
full Finland PRH YTJ companies JSON into S3/RustFS, filter it into a smaller base
file by registration date, and store that base file for downstream processing.

## Scope

Included:

- PRH YTJ company API full JSON to S3.
- Filtered `base.json` containing companies with
  `registrationDate >= start_date` and `registrationDate < today`.
- Idempotent full-object checks: existing full JSON is reused unless
  `refresh=True`.
- A Prefect flow that can run manually or be served on a cron.
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

- `download_ytj_full_and_base_to_s3`: materializes full YTJ JSON and filtered
  `base.json`.
- `finland_raw_ingest_flow`: runs the YTJ base task and creates a Prefect Markdown
  summary artifact.
- `serve_finland_raw_ingest`: serves the flow with a cron schedule.

## Storage Layout

YTJ bucket:

```text
source-finland-prhytj/
  full/date=<today>/companies.json
  base/start_date=<start_date>/end_date=<today>/base.json
  base/start_date=<start_date>/end_date=<today>/manifest.json
```

## Parameters

- `start_date`: inclusive registration-date lower bound. Defaults to `2024-01-01`.
- `today`: exclusive registration-date upper bound. Defaults to current UTC date.
- `refresh`: if `False`, reuse existing full JSON. If `True`, redownload and
  overwrite the full JSON before rebuilding `base.json`.

## Error Handling

HTTP calls retry transient `429`, `5xx`, timeout, and connection errors. Persistent
errors fail the flow. The full JSON object is checked before the API call so scheduled
reruns can rebuild the base file without redownloading the full source unless needed.

Manifests record object keys, date bounds, full/base counts, and skip/download outcomes.
Prefect records run state and exposes a Markdown summary artifact.

## Acceptance Criteria

- The workflow writes YTJ full JSON to `source-finland-prhytj`.
- The workflow writes filtered `base.json` to `source-finland-prhytj`.
- Existing full JSON is reused when `refresh=False`.
- `refresh=True` forces redownload.
- The flow can run once from Python and can be served with a cron expression.
