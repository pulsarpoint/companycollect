# Norway Financial Bootstrap

Standalone Temporal application for the one-time Norway BRREG financial report
bootstrap.

This package is intentionally independent of Dagster. Startup code creates or
reuses a frozen ClickHouse candidate table from `corpscout.no_companies`.
Temporal workers then process those candidates by slot, write raw BRREG
financial JSON objects to S3, and write colocated S3 status markers.

## Fixed Storage Contract

The bucket and object keys are fixed because Dagster jobs read from the same
layout.

Bucket:

```text
source-norway-brreg
```

Raw financial reports:

```text
norway_brreg/finance/raw_reports/org=<org_number>/year=<year>/type=<report_type>/id=<report_id>.json
```

Status markers:

```text
norway_brreg/finance/raw_reports/org=<org_number>/status/done.json
norway_brreg/finance/raw_reports/org=<org_number>/status/failed.json
```

There are no S3 candidate batch files in this workflow.

## Candidate Table

The starter creates or reuses this ClickHouse table:

```text
corpscout.norway_financial_bootstrap_candidates
```

Candidates are deterministic:

```text
run_id + slot + slot_index -> org_number
```

The starter reads active companies with a non-empty
`last_submitted_accounts_year` from ClickHouse:

```sql
select
    toString(org_number) as org_number
from corpscout.no_companies
where is_active = true
  and last_submitted_accounts_year is not null
order by org_number
```

`last_submitted_accounts_year` is only an eligibility filter. BRREG's structured
financial endpoint behaves as a latest-report endpoint, so workers call it
without an `år` query parameter:

```text
GET https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}
```

## Runtime Model

The starter launches four Temporal slot workflows by default:

```text
norway-brreg-finance-historical-bootstrap-slot-0
norway-brreg-finance-historical-bootstrap-slot-1
norway-brreg-finance-historical-bootstrap-slot-2
norway-brreg-finance-historical-bootstrap-slot-3
```

Each workflow run processes one organization, then continues as new with the
next slot index. Restarting from index `0` is safe because the workflow checks
S3 status markers before calling BRREG.

The bootstrap does not create final ClickHouse financial tables, parse financial
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

The fixed run id prefix is:

```text
norway-brreg-finance-historical-bootstrap
```

## Run

`norway-financial-bootstrap-worker` is the long-running Temporal worker. It
connects to Temporal, registers the slot workflow and activity implementation,
and waits for work on the fixed `norway-financial-bootstrap` task queue.

Start one or more workers and leave them running:

```bash
uv run norway-financial-bootstrap-worker
```

`norway-financial-bootstrap` is the starter command. It prepares the ClickHouse
candidate table and starts one Temporal workflow per slot.

Start the workflows from another terminal:

```bash
uv run norway-financial-bootstrap
```

The starter prints the slot workflow ids after Temporal accepts the workflows.

Both commands read `.env`. `--temporal-address` and `--s3-endpoint` are one-off
overrides when you do not want to change `.env`.

## Check Status

`norway-financial-bootstrap-status` is a read-only status script. It checks the
four slot workflows, Temporal task queue pollers, S3 raw report counts, latest
S3 object, and failed marker samples.

Run a point-in-time status check:

```bash
uv run norway-financial-bootstrap-status
```

Check whether S3 counts move over one minute:

```bash
uv run norway-financial-bootstrap-status --compare-after-seconds 60
```

Print JSON for automation:

```bash
uv run norway-financial-bootstrap-status --json
```

## Resume And Rerun Behavior

The application is safe to rerun against the same S3 bucket.

If a slot workflow restarts from `slot_index = 0`, it reads the deterministic
candidate table and checks each organization's status markers:

```text
done marker exists   -> skip organization
failed marker exists -> skip organization
marker missing       -> fetch organization
```

Already completed organizations do not trigger BRREG calls.

## Failure Behavior

Retryable BRREG failures are not written as done markers.

The worker retries transient network, `429`, and `5xx` failures using finite
backoff. If Temporal exhausts retries for an organization, the workflow writes:

```text
norway_brreg/finance/raw_reports/org=<org_number>/status/failed.json
```

Non-retryable `404` results are terminal source outcomes and write a done marker
with `report_count = 0`.

Invalid payloads fail the fetch activity and become failed markers after
Temporal retry exhaustion.

## Verification

Run tests:

```bash
uv run pytest -q
```

Run lint:

```bash
uv run ruff check .
```
