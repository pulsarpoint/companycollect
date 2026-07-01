# Norway Financial Bootstrap Temporal Slot Design

## Summary

The Norway BRREG financial bootstrap should stop using S3 candidate batch shards. Candidate queue state belongs in a local DuckDB database, while raw financial reports remain stored on S3.

Temporal should run a fixed number of independent slot workflow chains. Each workflow run processes one claimed organization/accounting-year candidate, then continues as new. This keeps workflow history small, gives stable retry inputs, and avoids batch/chunk orchestration.

## Goals

- Use only the fields needed for financial fetch candidates: `org_number` and `accounts_year`.
- Avoid storing candidate batches on S3.
- Keep exactly `N` concurrent financial fetches, with default `N = 4`.
- Make each Temporal retry process the same claimed candidate.
- Keep state resumable through DuckDB queue tables.
- Store raw BRREG financial report JSON on S3 using deterministic object keys.
- Mark done and failed candidates in DuckDB so reruns do not repeat completed work.

## Non-Goals

- Do not implement daily BRREG update processing in this change.
- Do not parse raw reports into final ClickHouse tables in this bootstrap tool.
- Do not store company names or websites in the financial bootstrap queue.
- Do not build S3 candidate shards or Temporal payloads containing candidate lists.

## Candidate Query

The bootstrap candidate source is ClickHouse `corpscout.no_companies`.

Use this query shape:

```sql
SELECT
  toString(org_number) AS org_number,
  toString(last_submitted_accounts_year) AS accounts_year
FROM corpscout.no_companies
WHERE is_active = true
  AND last_submitted_accounts_year IS NOT NULL
ORDER BY org_number
```

`name` and `website_url` are not needed for BRREG financial fetching and should not be part of the bootstrap candidate model.

## DuckDB Queue

Use a single DuckDB database for bootstrap queue state. The queue has three logical states:

- `input`: all candidate `org_number + accounts_year` rows loaded from ClickHouse.
- `output`: candidates successfully processed or terminally completed.
- `failed`: candidates that failed after Temporal exhausted retries.

Suggested schema:

```sql
CREATE TABLE IF NOT EXISTS input (
  org_number TEXT NOT NULL,
  accounts_year TEXT NOT NULL,
  claimed_by TEXT,
  claimed_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (org_number, accounts_year)
);

CREATE TABLE IF NOT EXISTS output (
  org_number TEXT NOT NULL,
  accounts_year TEXT NOT NULL,
  fetch_status TEXT NOT NULL,
  report_count BIGINT NOT NULL,
  raw_report_keys TEXT NOT NULL,
  completed_at TIMESTAMP NOT NULL,
  PRIMARY KEY (org_number, accounts_year)
);

CREATE TABLE IF NOT EXISTS failed (
  org_number TEXT NOT NULL,
  accounts_year TEXT NOT NULL,
  error_type TEXT NOT NULL,
  error_message TEXT NOT NULL,
  failed_at TIMESTAMP NOT NULL,
  PRIMARY KEY (org_number, accounts_year)
);
```

All DuckDB write operations should be protected by a file mutex because DuckDB supports one writer at a time.

## Temporal Model

Start a fixed number of workflow chains:

```text
norway-financial-bootstrap-slot-0
norway-financial-bootstrap-slot-1
norway-financial-bootstrap-slot-2
norway-financial-bootstrap-slot-3
```

Each workflow run processes at most one candidate:

```text
claim_next_candidate
  -> returns org/year or none

if none:
  complete this slot chain

fetch_and_store_candidate(org/year)
  -> BRREG HTTP call
  -> write raw report JSON to S3
  -> retried by Temporal up to 3 times with the same org/year

mark_done(org/year, result)
  -> insert into DuckDB output

continue-as-new(slot_id)
```

If `fetch_and_store_candidate` fails after retries:

```text
mark_failed(org/year, error)
continue-as-new(slot_id)
```

The workflow decides when to continue as new. Activities must not start workflows.

## Activities

Use small, concrete activities:

- `prepare_queue`: create DuckDB tables and load ClickHouse candidates into `input`.
- `claim_next_candidate`: under mutex, claim one row not present in `output` or `failed`.
- `fetch_and_store_candidate`: fetch BRREG financial report data for one org/year and upload raw reports to S3.
- `mark_candidate_done`: under mutex, insert completed result into `output`.
- `mark_candidate_failed`: under mutex, insert failed result into `failed`.

The retryable activity is `fetch_and_store_candidate`, and its input must be the fixed candidate returned by `claim_next_candidate`.

## S3 Output

Raw financial reports remain on S3 with deterministic keys:

```text
norway_brreg/finance/raw_reports/org=<org_number>/year=<accounts_year>/type=<report_type>/id=<report_id>.json
```

Candidate queue files should not be written to S3.

Old candidate batch leftovers can be removed from:

```text
norway_brreg/finance/bootstrap_runs/run=norway-brreg-finance-historical-bootstrap/
```

Do not delete:

```text
norway_brreg/finance/raw_reports/
```

## Idempotency

Candidate completion is decided from DuckDB:

```text
input row exists
and no output row exists
and no failed row exists
and not currently claimed
```

Raw report object keys are also deterministic, so re-uploading the same report is safe if a crash happens between S3 upload and `mark_candidate_done`.

The final durable completion signal is the `output` row.

## Failure Handling

- BRREG network, timeout, 429, 5xx, and invalid payload errors should fail `fetch_and_store_candidate`.
- Temporal retries `fetch_and_store_candidate` up to 3 attempts.
- After retry exhaustion, the workflow calls `mark_candidate_failed`.
- `claim_next_candidate`, `mark_candidate_done`, and `mark_candidate_failed` should be short DuckDB operations and protected by the mutex.
- Stale claims should be reclaimable by `claim_next_candidate` after a configured timeout, because a workflow slot can die after claiming but before marking done or failed.

## Operational Flow

1. Operator runs the bootstrap starter.
2. Starter prepares the DuckDB queue from ClickHouse.
3. Starter starts 4 slot workflows if they are not already running.
4. Each slot workflow repeatedly processes one candidate per run and continues as new.
5. When no candidates remain, each slot workflow completes.
6. Dagster can later process raw report JSON from S3 into parquet and ClickHouse.

## Testing

Add focused tests for:

- ClickHouse query selects only `org_number` and `accounts_year`.
- Queue preparation creates `input`, `output`, and `failed`.
- Claiming skips candidates already in `output` or `failed`.
- Claiming uses a mutex-protected DuckDB write path.
- Fetch activity receives one fixed candidate.
- Fetch retry does not claim a different candidate.
- Successful fetch writes raw report JSON to deterministic S3 keys.
- Successful fetch inserts `output`.
- Retry exhaustion inserts `failed`.
- Slot workflow continues as new after done or failed.
- Slot workflow completes when claim returns no candidate.

## Migration From Current Design

Remove the S3 candidate batch path from the bootstrap runtime:

```text
norway_brreg/finance/bootstrap_runs/.../candidates/batch=*.parquet
```

Replace it with DuckDB queue preparation and Temporal slot workflows.

The existing raw report S3 layout can stay unchanged.
