# Norway Financial Bootstrap Temporal Slot Design

## Summary

The Norway BRREG financial bootstrap should stop using S3 candidate batch shards and should not use a mutable DuckDB queue.

The bootstrap should freeze the candidate list in a persistent ClickHouse table, split it deterministically across slot workflows, and use S3 terminal markers to record done or failed organizations. Temporal should run a fixed number of independent slot workflow chains. Each workflow run processes at most one organization, then continues as new with `slot_index + 1`.

This keeps workflow history small, avoids queue claiming and mutex logic, gives stable retry inputs, and makes restart simple: start each slot again from index `0`; already completed or failed organizations are skipped by checking S3 markers.

## Goals

- Use only the field needed for financial fetch candidates: `org_number`.
- Avoid storing candidate batches on S3.
- Avoid a mutable DuckDB queue, queue claims, and stale claim recovery.
- Keep exactly `N` concurrent financial fetches, with default `N = 4`.
- Make each Temporal retry process the same organization.
- Store raw BRREG financial report JSON on S3 using deterministic object keys.
- Store terminal done/failed markers on S3 so reruns do not repeat completed work.
- Keep the candidate list deterministic and frozen for the bootstrap run.

## Non-Goals

- Do not implement daily BRREG update processing in this change.
- Do not parse raw reports into final ClickHouse tables in this bootstrap tool.
- Do not store company names or websites in the financial bootstrap candidate table.
- Do not build S3 candidate shards or Temporal payloads containing candidate lists.

## Candidate Source

The bootstrap candidate source is ClickHouse `corpscout.no_companies`.

Use this query shape:

```sql
SELECT
  toString(org_number) AS org_number
FROM corpscout.no_companies
WHERE is_active = true
  AND last_submitted_accounts_year IS NOT NULL
ORDER BY org_number
```

`last_submitted_accounts_year` is only a source-side eligibility filter. The BRREG structured financial endpoint behaves like a latest-report endpoint even when `år` is supplied, so the bootstrap should call it without a year selector.

`name`, `website_url`, and `last_submitted_accounts_year` are not needed for BRREG financial fetching and should not be part of the bootstrap candidate model.

## Frozen Candidate Table

Create a persistent, run-scoped ClickHouse table for the frozen candidate list. Do not use a temporary table because the bootstrap can run for days and workflow/activity workers run in different processes.

Suggested table:

```sql
CREATE TABLE IF NOT EXISTS corpscout.norway_financial_bootstrap_candidates
(
  run_id String,
  slot UInt8,
  slot_index UInt64,
  org_number String,
  created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (run_id, slot, slot_index);
```

Prepare candidates once for the fixed bootstrap `run_id`. If rows already exist for that `run_id`, reuse them.

Use deterministic round-robin slot assignment:

```sql
INSERT INTO corpscout.norway_financial_bootstrap_candidates
  (run_id, slot, slot_index, org_number)
SELECT
  {run_id} AS run_id,
  rn % {slot_count} AS slot,
  intDiv(rn, {slot_count}) AS slot_index,
  org_number
FROM (
  SELECT
    row_number() OVER (ORDER BY org_number) - 1 AS rn,
    toString(org_number) AS org_number
  FROM corpscout.no_companies
  WHERE is_active = true
    AND last_submitted_accounts_year IS NOT NULL
)
ORDER BY slot, slot_index;
```

Round-robin assignment gives balanced slots:

```text
slot=0 index=0 -> org0
slot=1 index=0 -> org1
slot=2 index=0 -> org2
slot=3 index=0 -> org3
slot=0 index=1 -> org4
slot=1 index=1 -> org5
```

## Temporal Model

Start a fixed number of workflow chains:

```text
norway-financial-bootstrap-slot-0
norway-financial-bootstrap-slot-1
norway-financial-bootstrap-slot-2
norway-financial-bootstrap-slot-3
```

Each workflow input contains:

```text
run_id
slot_id
slot_index
slot_count
```

Each workflow run processes at most one candidate:

```text
get_candidate(run_id, slot_id, slot_index)
  -> returns org or none

if none:
  complete this slot chain

candidate_marker_status(org)
  -> returns done, failed, or missing

if done or failed:
  continue-as-new(slot_index + 1)

fetch_and_store_candidate(org)
  -> BRREG HTTP call without år query parameter
  -> write raw report JSON to S3
  -> retried by Temporal up to 3 times with the same org

write_done_marker(org, result)

continue-as-new(slot_index + 1)
```

If `fetch_and_store_candidate` fails after Temporal retries:

```text
write_failed_marker(org, error)
continue-as-new(slot_index + 1)
```

The workflow decides when to continue as new. Activities must not start workflows.

## Activities

Use small, concrete activities:

- `prepare_candidate_table`: create the ClickHouse candidate table and insert deterministic candidates for the run if missing.
- `get_candidate`: read one organization from ClickHouse by `run_id + slot_id + slot_index`.
- `candidate_marker_status`: check S3 for terminal done/failed markers for that organization.
- `fetch_and_store_candidate`: fetch BRREG financial report data for one organization and upload raw reports to S3.
- `write_done_marker`: write a terminal S3 done marker for successful or terminal empty/not-found source outcomes.
- `write_failed_marker`: write a terminal S3 failed marker after Temporal retry exhaustion.

The retryable activity is `fetch_and_store_candidate`, and its input must be the fixed organization returned by `get_candidate`.

## S3 Output

Raw financial reports remain on S3 with deterministic keys:

```text
norway_brreg/finance/raw_reports/org=<org_number>/year=<report_year_from_response>/type=<report_type>/id=<report_id>.json
```

The `year` path segment must be derived from the returned report, normally from `regnskapsperiode`, not from `no_companies.last_submitted_accounts_year`.

Terminal markers live on S3:

```text
norway_brreg/finance/bootstrap_status/done/org=<org_number>.json
norway_brreg/finance/bootstrap_status/failed/org=<org_number>.json
```

Done marker example:

```json
{
  "org_number": "923609016",
  "fetch_status": "success",
  "report_count": 1,
  "raw_report_keys": [
    "norway_brreg/finance/raw_reports/org=923609016/year=2024/type=SELSKAP/id=5667197.json"
  ],
  "completed_at": "2026-07-01T18:00:00Z"
}
```

A terminal not-found or empty source response should also write a done marker with `report_count = 0`. Retryable failures should not write a done marker.

Candidate queue files should not be written to S3.

Old candidate batch leftovers can be removed from:

```text
norway_brreg/finance/bootstrap_runs/run=norway-brreg-finance-historical-bootstrap/
```

Do not delete:

```text
norway_brreg/finance/raw_reports/
norway_brreg/finance/bootstrap_status/
```

## Idempotency

Candidate order is fixed by the ClickHouse candidate table:

```text
run_id + slot_id + slot_index -> org_number
```

Candidate completion is decided from S3 terminal markers:

```text
done marker exists   -> skip organization
failed marker exists -> skip organization
marker missing       -> fetch organization
```

Restart behavior is intentionally simple. If every slot restarts from `slot_index = 0`, each workflow walks forward through the frozen candidate table and skips organizations that already have done or failed markers. The restart cost is S3 marker checks only; it does not call BRREG for completed organizations.

Raw report object keys are also deterministic, so re-uploading the same report is safe if a crash happens between raw report upload and done marker write.

The final durable completion signal is the S3 done or failed marker.

## Failure Handling

- BRREG network, timeout, 429, 5xx, and invalid payload errors should fail `fetch_and_store_candidate`.
- Temporal retries `fetch_and_store_candidate` up to 3 attempts with the same organization.
- After retry exhaustion, the workflow calls `write_failed_marker`.
- 404/not-found and empty responses are terminal source outcomes, not retryable infrastructure failures; they should write a done marker with `report_count = 0`.
- If a workflow crashes after uploading raw reports but before writing the done marker, a later restart may re-fetch and re-upload those deterministic raw report objects, then write the done marker.

## Operational Flow

1. Operator runs the bootstrap starter.
2. Starter prepares the frozen ClickHouse candidate table for the fixed `run_id`.
3. Starter starts 4 slot workflows from `slot_index = 0`.
4. Each slot workflow reads exactly one candidate by `slot_id + slot_index`.
5. If the candidate already has a done or failed marker on S3, the slot continues as new with `slot_index + 1`.
6. If the candidate has no terminal marker, the slot fetches BRREG financial data, stores raw reports on S3, writes a done or failed marker, and continues as new.
7. When `get_candidate` returns no row for a slot, that slot workflow completes.
8. Dagster can later process raw report JSON from S3 into parquet and ClickHouse.

## Testing

Add focused tests for:

- ClickHouse candidate query selects only `org_number`.
- Candidate table preparation creates deterministic `slot` and `slot_index` assignments.
- Candidate table preparation reuses existing rows for the same `run_id`.
- `get_candidate` returns the expected org for `run_id + slot_id + slot_index`.
- Slot workflow completes when `get_candidate` returns no candidate.
- Slot workflow skips done markers and continues as new with `slot_index + 1`.
- Slot workflow skips failed markers and continues as new with `slot_index + 1`.
- Fetch activity receives one fixed organization.
- Fetch retry does not read or select a different organization.
- Successful fetch writes raw report JSON to deterministic S3 keys.
- Successful fetch writes a done marker.
- Terminal empty/not-found writes a done marker with `report_count = 0`.
- Retry exhaustion writes a failed marker.
- Restarting from `slot_index = 0` does not call BRREG for organizations with terminal markers.

## Migration From Current Design

Remove the S3 candidate batch path from the bootstrap runtime:

```text
norway_brreg/finance/bootstrap_runs/.../candidates/batch=*.parquet
```

Replace it with ClickHouse candidate table preparation and Temporal slot workflows.

Remove any DuckDB queue plan from this bootstrap. The candidate table is ClickHouse; progress is S3 terminal markers.

The existing raw report S3 layout can stay unchanged.
