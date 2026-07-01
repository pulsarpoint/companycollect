# Norway Financial Bootstrap

Standalone Temporal application for the one-time historical Norway BRREG
financial report bootstrap.

This package is intentionally independent of Dagster. It reads candidate
companies from the fixed ClickHouse table `corpscout.no_companies`, discovers
financial reports from BRREG by organization and year, and writes raw report
JSON objects to S3. Dagster daily finance jobs use the same fixed raw report
paths to decide which reports already exist.

## Fixed Storage Contract

The bucket and object keys are not configurable.

Bucket:

```text
source-norway-brreg
```

Candidate batch files written by the bootstrap starter:

```text
norway_brreg/finance/bootstrap_runs/run=norway-brreg-finance-historical-bootstrap/attempt={attempt_id}/candidates/batch={batch_index}.parquet
```

Raw financial reports written by workers:

```text
norway_brreg/finance/raw_reports/org={org_number}/year={year}/type={report_type}/id={report_id}.json
```

The raw report path is the completion marker. If that exact report object
already exists, the worker skips it and does not call it completed again.

## What It Fetches

The starter reads active companies with a non-empty
`last_submitted_accounts_year` from ClickHouse:

```sql
select
    org_number,
    name,
    primary_website_url,
    last_submitted_accounts_year
from corpscout.no_companies
where is_active = true
  and last_submitted_accounts_year is not null
order by org_number
```

For each candidate, workers call BRREG with the company and year:

```text
GET https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}?år={year}
```

Each returned report is stored as a separate JSON object under the fixed raw
report key. The bootstrap does not create ClickHouse tables, parse financial
metrics, run FX conversion, or call Dagster.

## Local Setup

From this directory:

```bash
uv sync
cp .env.example .env
```

Required environment variables:

```bash
export CORPSCOUT_S3_ACCESS_KEY=...
export CORPSCOUT_S3_SECRET_KEY=...
export CLICKHOUSE_HOST=clickhouse-host
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=...
```

Optional ClickHouse environment variables:

```bash
export CLICKHOUSE_HTTP_PORT=8123
export CLICKHOUSE_SECURE=false
```

The ClickHouse database and table are fixed:

```text
corpscout.no_companies
```

Set the S3-compatible endpoint either as an environment variable or a CLI
argument:

```bash
export CORPSCOUT_S3_ENDPOINT=http://s3-host:9000
```

Temporal address can also be supplied as an environment variable or CLI
argument:

```bash
export TEMPORAL_ADDRESS=temporal-host:7233
```

The fixed Temporal task queue is:

```text
norway-financial-bootstrap
```

The fixed workflow id is:

```text
norway-brreg-finance-historical-bootstrap
```

## Run

`norway-financial-bootstrap-worker` is the long-running Temporal worker. It
connects to Temporal, registers the workflow/activity implementation, and waits
for work on the fixed `norway-financial-bootstrap` task queue.

Start one or more workers and leave them running:

```bash
uv run norway-financial-bootstrap-worker
```

`norway-financial-bootstrap` is the starter command. It reads candidate
companies from ClickHouse, writes candidate batch parquet files to S3, and
starts the fixed `norway-brreg-finance-historical-bootstrap` workflow.

Start the workflow once from another terminal:

```bash
uv run norway-financial-bootstrap
```

The starter prints the workflow id after Temporal accepts the workflow.

Both commands read `.env`. `--temporal-address` and `--s3-endpoint` are only
one-off overrides when you do not want to change `.env`.

## Resume And Rerun Behavior

The application is safe to rerun against the same S3 bucket.

On every activity batch, the worker lists existing raw report objects under:

```text
norway_brreg/finance/raw_reports/
```

For each BRREG report returned by `org + year`, it checks the exact
`org/year/type/id` key. Existing report objects are skipped. Missing report
objects are written.

The starter may write new candidate batch files under a new `attempt=...`
folder for the fixed workflow id. Those batch files are not the source of truth;
the raw report JSON objects are.

## Failure Behavior

Retryable BRREG failures are not written as raw report objects.

The worker retries transient network, `429`, and `5xx` failures using finite
backoff. If the retry budget is exhausted for any candidate in an activity
batch, the activity raises after processing the batch and Temporal retries the
activity according to the workflow retry policy.

Non-retryable `404` results are counted as `not_found` and do not create raw
report objects.

Invalid payloads are counted as `invalid_payload` and fail the batch.

## Verification

Run tests:

```bash
uv run pytest -q
```

Run lint:

```bash
uv run ruff check .
```
