# Finland Prefect Raw Ingest Design

## Goal

Create a narrow Prefect workflow in `companycollect/processor` that downloads raw
Finland PRH data from public APIs into S3/RustFS. This phase is only raw ingest:
no Parquet conversion, no canonical tables, no dbt, and no ClickHouse.

## Scope

Included:

- PRH YTJ company API to S3 as NDJSON.
- PRH XBRL discovery JSON/listing to S3.
- PRH XBRL XML documents to S3.
- Idempotent object checks: existing objects are skipped unless `refresh=True`.
- A Prefect flow that can run manually or be served on a cron.
- S3-compatible storage using existing `CORPSCOUT_S3_*` environment variables.

Excluded:

- Local Parquet outputs.
- Structured/canonical processing.
- dbt integration.
- ClickHouse ingestion.
- Dagster assets or Dagster schedules.

## Architecture

The implementation lives in `companycollect/processor/finland_raw_ingest.py`.

The workflow uses plain Python helper functions for API and S3 work, with a small
Prefect flow around them:

- `download_ytj_snapshot_to_s3`: materializes one YTJ snapshot.
- `download_xbrl_window_to_s3`: materializes one XBRL registration-date window.
- `finland_raw_ingest_flow`: runs both downloads and creates a Prefect Markdown
  summary artifact.
- `serve_finland_raw_ingest`: serves the flow with a cron schedule.

## Storage Layout

YTJ bucket:

```text
source-finland-prhytj/
  snapshots/<YYYY-MM-DD>/source.ndjson
  snapshots/<YYYY-MM-DD>/manifest.json
```

XBRL bucket:

```text
source-finland-prh-xbrl/
  windows/<start>_<end>/listing.json
  windows/<start>_<end>/manifest.json
  companies/<business_id>/<financial_date>.xml
```

The XBRL XML object path is stable across windows so repeated runs can skip already
downloaded documents.

## Parameters

- `snapshot_date`: YTJ snapshot date. Defaults to current UTC date.
- `max_companies`: optional cap for experimental YTJ downloads. Defaults to `200`.
- `xbrl_start`: XBRL registered-date window start. Defaults to `2025-01-01`.
- `xbrl_end`: XBRL registered-date window end. Defaults to `2025-01-03`.
- `refresh`: if `False`, skip existing S3 objects. If `True`, redownload and
  overwrite objects.

## Error Handling

HTTP calls retry transient `429`, `5xx`, timeout, and connection errors. Persistent
errors fail the flow. Existing S3 objects are checked before API calls so scheduled
reruns do not duplicate download work by default.

Manifests record object keys, source windows, counts, and skip/download outcomes.
Prefect records run state and exposes a Markdown summary artifact.

## Acceptance Criteria

- The workflow writes YTJ raw NDJSON to `source-finland-prhytj`.
- The workflow writes XBRL listing JSON and XML files to `source-finland-prh-xbrl`.
- Existing S3 objects are skipped when `refresh=False`.
- `refresh=True` forces redownload.
- The flow can run once from Python and can be served with a cron expression.
