# Norway Data Temporal Tool Design

## Problem

The current Norway BRREG implementation mixes responsibilities across Dagster
and the external bootstrap tool:

- Dagster assets still call BRREG for company updates and daily financial
  fetches.
- The standalone `norway_financial_bootstrap` package only handles historical
  financial raw reports.
- The financial bootstrap reads candidates from ClickHouse, which makes an
  external source download depend on a downstream serving database.
- Some raw/intermediate outputs are parquet, even when the source handoff should
  be raw JSON.
- Long-running external API work can run for hours or days, which is a poor fit
  for normal Dagster materialization runs.

The target architecture is:

```text
Temporal app -> BRREG APIs -> raw JSON/JSONL on S3 -> manifests
Dagster      -> completed S3 manifests -> parquet -> FX -> ClickHouse
ClickHouse   -> queryable product tables only
```

Dagster should not communicate with BRREG after this migration.

## Goals

Build a standalone Temporal application named `norway_data` that owns all
Norway BRREG source communication:

- One-time full company snapshot download.
- One-time full financial report download, driven by the full company snapshot.
- Daily company update download.
- Daily financial report update download, driven by relevant company update
  events.
- Durable JSON/JSONL raw data and manifest files on S3.
- Idempotent full-load behavior: if the full-load outputs already exist on S3,
  the workflow exits without downloading again.
- Daily update behavior: daily partitions can be rerun and overwrite that day's
  manifest/output, while raw financial report files remain immutable by report
  id.

Keep the Temporal tool independent from Dagster:

- Separate `uv` project.
- No imports from `dagster_v3`.
- No Dagster resources.
- No Dagster instance dependency.
- Runtime values limited to Temporal address, S3 endpoint, and credentials.

## Non-Goals

- Do not redesign final ClickHouse schemas in this work.
- Do not keep BRREG API calls inside Dagster assets.
- Do not use ClickHouse as the normal input for full financial candidate
  selection.
- Do not store source raw payloads as parquet in the Temporal tool.
- Do not create a scheduler for the full load. It is a manual, one-time
  workflow.
- Do not preserve `norway_financial_bootstrap` as a second maintained package.

## Package Rename

Current package:

```text
corpscout/norway_financial_bootstrap
python package: norway_financial_bootstrap
scripts:
  norway-financial-bootstrap
  norway-financial-bootstrap-worker
```

Target package:

```text
corpscout/norway_data
python package: norway_data
scripts:
  norway-data
  norway-data-worker
```

The final implementation should remove the old import namespace. During the
single migration commit series, script aliases can exist only to keep local
tests executable, but the final public CLI names are `norway-data` and
`norway-data-worker`.

## Source APIs

Company full snapshot:

```text
GET https://data.brreg.no/enhetsregisteret/api/enheter/lastned
```

Company daily updates:

```text
GET https://data.brreg.no/enhetsregisteret/api/oppdateringer/enheter
  ?dato={start}
  &updatedBefore={end}
  &size={page_size}
  &page={page}
  &sort=id,ASC
  &includeChanges=true
```

Financial reports for one organization/year:

```text
GET https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}?år={year}
```

The company update API can return HTTP 400 for a one-day window when the result
set exceeds the API limit. The Temporal company update workflow must split the
requested day into smaller time windows and retry the same endpoint with the
smaller windows. The default split strategy is:

```text
1 day -> 12 hours -> 6 hours -> 3 hours -> 1 hour
```

If a one-hour window still exceeds the API limit, the workflow fails the date
partition and writes no `_SUCCESS.json`.

## Fixed Configuration

These values are part of the source contract and should not be operator inputs:

```text
S3 bucket: source-norway-brreg
Temporal task queue: norway-data
Full workflow id: norway-brreg-full-load
Company daily workflow id prefix: norway-brreg-company-update-
Finance daily workflow id prefix: norway-brreg-finance-update-
```

Runtime inputs:

```text
--temporal-address
--s3-endpoint
```

Environment variables:

```text
CORPSCOUT_S3_ACCESS_KEY
CORPSCOUT_S3_SECRET_KEY
CORPSCOUT_S3_REGION
```

Default `CORPSCOUT_S3_REGION` is `us-east-1`.

The tool should accept test-only options in test helpers, not in the production
CLI, for limiting candidate count or overriding source endpoints.

## S3 Data Contract

All Temporal outputs are source-owned JSON or JSONL files plus manifests. Large
collections are gzip-compressed JSONL so they can be streamed and resumed.

### Company Full Snapshot

```text
norway_brreg/company/raw/snapshot/run=norway-brreg-full-load/entities.jsonl.gz
norway_brreg/company/manifests/snapshot/run=norway-brreg-full-load/manifest.json
norway_brreg/company/manifests/snapshot/run=norway-brreg-full-load/_SUCCESS.json
```

Each `entities.jsonl.gz` line is one raw BRREG company entity JSON object with
an envelope:

```json
{
  "source": "norway_brreg",
  "dataset": "company_snapshot",
  "run_id": "norway-brreg-full-load",
  "record_number": 1,
  "fetched_at": "2026-07-01T10:00:00.000Z",
  "entity": {}
}
```

The `entity` field stores the BRREG source object without normalization.

### Company Daily Updates

```text
norway_brreg/company/raw/updates/date=YYYY-MM-DD/entities.jsonl.gz
norway_brreg/company/manifests/updates/date=YYYY-MM-DD/manifest.json
norway_brreg/company/manifests/updates/date=YYYY-MM-DD/_SUCCESS.json
```

Each update line is one raw BRREG update object with an envelope:

```json
{
  "source": "norway_brreg",
  "dataset": "company_update",
  "partition_date": "2026-07-01",
  "window_start": "2026-07-01T00:00:00.000Z",
  "window_end": "2026-07-01T23:59:59.999Z",
  "record_number": 1,
  "fetched_at": "2026-07-02T02:00:00.000Z",
  "update": {}
}
```

The `update` field stores the BRREG source update object, including
`endringer` when the API returns it.

### Financial Full Candidate Shards

```text
norway_brreg/finance/candidates/full/run=norway-brreg-full-load/shard=000000.jsonl.gz
norway_brreg/finance/candidates/full/run=norway-brreg-full-load/manifest.json
```

Each candidate line:

```json
{
  "source": "company_snapshot",
  "run_id": "norway-brreg-full-load",
  "shard_number": 0,
  "record_number": 1,
  "org_number": "980345106",
  "legal_name": "EXAMPLE AS",
  "is_active": true,
  "accounts_year": "2025"
}
```

Candidate extraction rule for full financial load:

```text
active company AND non-empty sisteInnsendteAarsregnskap
```

There is no website filter.

### Financial Daily Candidate Shards

```text
norway_brreg/finance/candidates/updates/date=YYYY-MM-DD/shard=000000.jsonl.gz
norway_brreg/finance/candidates/updates/date=YYYY-MM-DD/manifest.json
```

Each candidate line:

```json
{
  "source": "company_update",
  "partition_date": "2026-07-01",
  "update_id": "24577000",
  "update_published_at": "2026-07-01T18:00:23.412Z",
  "org_number": "980345106",
  "accounts_year": "2025"
}
```

Candidate extraction rule for daily financial update:

```text
company update event contains endringer[].path == "/sisteInnsendteAarsregnskap"
```

Only those organizations/years are fetched. Daily finance must not fetch reports
for every company updated that day.

### Raw Financial Reports

```text
norway_brreg/finance/raw_reports/org={org_number}/year={year}/type={report_type}/id={report_id}.json
```

The raw report object is exactly one BRREG financial report JSON object wrapped
with a metadata envelope:

```json
{
  "source": "norway_brreg",
  "dataset": "financial_report",
  "org_number": "980345106",
  "accounts_year": "2025",
  "report_type": "SELSKAP",
  "report_id": "6697842",
  "journal_number": "2026428651",
  "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/980345106?%C3%A5r=2025",
  "fetched_at": "2026-07-01T10:00:00.000Z",
  "report": {}
}
```

The `report` field stores the BRREG source financial report object without
normalization.

The raw financial report key is immutable. If the exact key exists, the
workflow records `skipped_existing` in the manifest and does not overwrite it.

### Financial Full Manifest

```text
norway_brreg/finance/manifests/full/run=norway-brreg-full-load/reports.jsonl.gz
norway_brreg/finance/manifests/full/run=norway-brreg-full-load/manifest.json
norway_brreg/finance/manifests/full/run=norway-brreg-full-load/_SUCCESS.json
```

### Financial Daily Manifest

```text
norway_brreg/finance/manifests/updates/date=YYYY-MM-DD/reports.jsonl.gz
norway_brreg/finance/manifests/updates/date=YYYY-MM-DD/manifest.json
norway_brreg/finance/manifests/updates/date=YYYY-MM-DD/_SUCCESS.json
```

Each `reports.jsonl.gz` row describes one report outcome:

```json
{
  "partition_key": "2026-07-01",
  "org_number": "980345106",
  "accounts_year": "2025",
  "report_id": "6697842",
  "journal_number": "2026428651",
  "report_type": "SELSKAP",
  "period_start_date": "2025-01-01",
  "period_end_date": "2025-12-31",
  "raw_report_key": "norway_brreg/finance/raw_reports/org=980345106/year=2025/type=SELSKAP/id=6697842.json",
  "status": "fetched",
  "fetched_at": "2026-07-01T10:00:00.000Z",
  "attempt_count": 1,
  "status_code": 200,
  "error_type": "",
  "error_message": ""
}
```

Allowed report row statuses:

```text
fetched
skipped_existing
not_found
invalid_payload
failed
```

`failed` is terminal for the workflow. A workflow that has a `failed` report row
must not write `_SUCCESS.json`.

### Top-Level Manifest

All `manifest.json` files use this common shape:

```json
{
  "source": "norway_brreg",
  "manifest_schema_version": 1,
  "workflow_name": "norway_brreg_full_load",
  "workflow_id": "norway-brreg-full-load",
  "run_id": "norway-brreg-full-load",
  "partition_key": "snapshot",
  "status": "complete",
  "started_at": "2026-07-01T08:00:00.000Z",
  "completed_at": "2026-07-01T12:00:00.000Z",
  "raw_object_count": 1,
  "record_count": 1000000,
  "candidate_count": 900000,
  "fetched_count": 850000,
  "skipped_existing_count": 50000,
  "not_found_count": 10000,
  "failed_count": 0,
  "payload_sha256": "hex",
  "objects": [
    {
      "bucket": "source-norway-brreg",
      "key": "norway_brreg/company/raw/snapshot/run=norway-brreg-full-load/entities.jsonl.gz",
      "content_type": "application/gzip",
      "record_count": 1000000,
      "sha256": "hex"
    }
  ]
}
```

Complete statuses:

```text
complete
complete_with_not_found
complete_empty
```

Incomplete statuses:

```text
running
failed
```

`_SUCCESS.json` is written last and contains:

```json
{
  "manifest_key": "norway_brreg/company/manifests/snapshot/run=norway-brreg-full-load/manifest.json",
  "completed_at": "2026-07-01T12:00:00.000Z",
  "status": "complete"
}
```

Dagster sensors should watch `_SUCCESS.json`, then read `manifest.json`.

## Temporal Workflows

### `NorwayBrregFullLoadWorkflow`

Purpose:

- Run the complete one-time Norway source load.
- Pull full company data first.
- Derive full financial candidates from the stored company snapshot.
- Pull all missing raw financial reports for those candidates.

Input:

```json
{
  "force": false
}
```

Behavior:

1. Check for both success markers:

```text
norway_brreg/company/manifests/snapshot/run=norway-brreg-full-load/_SUCCESS.json
norway_brreg/finance/manifests/full/run=norway-brreg-full-load/_SUCCESS.json
```

2. If both exist and `force=false`, return success immediately without BRREG
   calls.
3. If the company marker is missing, execute
   `NorwayBrregCompanyFullLoadWorkflow`.
4. If the finance marker is missing, execute
   `NorwayBrregFinanceFullLoadWorkflow`.
5. Write:

```text
norway_brreg/manifests/full/run=norway-brreg-full-load/manifest.json
norway_brreg/manifests/full/run=norway-brreg-full-load/_SUCCESS.json
```

### `NorwayBrregCompanyFullLoadWorkflow`

Purpose:

- Download the full current company snapshot exactly once.

Behavior:

1. If company snapshot `_SUCCESS.json` exists and `force=false`, return reused
   success.
2. Download the full snapshot endpoint.
3. Stream each source entity into `entities.jsonl.gz`.
4. Fail if the payload is not parseable or contains zero entities.
5. Write manifest.
6. Write `_SUCCESS.json` last.

The workflow does not write parquet.

### `NorwayBrregFinanceFullLoadWorkflow`

Purpose:

- Fetch financial reports for every active company in the full company snapshot
  that has a last submitted accounts year.

Behavior:

1. Require company snapshot `_SUCCESS.json`; fail if missing.
2. Read `entities.jsonl.gz` from S3.
3. Extract full financial candidates into JSONL gzip shards.
4. For each candidate, call:

```text
GET https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}?år={year}
```

5. For each report returned, derive `(org_number, year, report_type, report_id)`.
6. If the raw report key exists, skip the write and emit a
   `skipped_existing` report manifest row.
7. If the raw report key does not exist, write the raw report JSON envelope and
   emit a `fetched` report manifest row.
8. Treat `404` as `not_found` and continue.
9. Treat retry-exhausted network errors, HTTP 429, HTTP 5xx, and invalid
   payloads as workflow failures.
10. Write financial full report manifest.
11. Write financial full `_SUCCESS.json` last.

The workflow must not read candidates from ClickHouse. ClickHouse is only a
downstream serving database.

### `NorwayBrregCompanyDailyUpdateWorkflow`

Purpose:

- Fetch one day of company update events from BRREG.
- Store the raw update event JSON.
- Trigger finance update for the same date.

Input:

```json
{
  "partition_date": "2026-07-01"
}
```

Behavior:

1. Fetch the requested UTC day with `includeChanges=true`.
2. If BRREG rejects the day window because it is too broad, split the window
   using the configured split strategy.
3. Page each window with stable sorting by `id,ASC`.
4. Deduplicate update IDs across split windows.
5. Write `entities.jsonl.gz` for the date.
6. Write manifest with window count, page count, update count, duplicate count,
   payload hash, and object key.
7. Write `_SUCCESS.json`.
8. Execute `NorwayBrregFinanceDailyUpdateWorkflow` for the same date.

Rerunning the same date replaces that day's company update raw object and
manifest only after the new run succeeds.

### `NorwayBrregFinanceDailyUpdateWorkflow`

Purpose:

- Fetch financial reports only for companies whose daily company update events
  indicate a changed last submitted annual account.

Input:

```json
{
  "partition_date": "2026-07-01"
}
```

Behavior:

1. Require company update `_SUCCESS.json` for the same date.
2. Read the company update JSONL.
3. Keep only update events where at least one change has:

```text
path == "/sisteInnsendteAarsregnskap"
```

4. Extract `org_number` and `accounts_year`.
5. Write daily financial candidate shards.
6. Fetch reports for each `(org_number, accounts_year)`.
7. Store only missing raw report IDs on S3.
8. Write daily financial report manifest and `_SUCCESS.json`.

If no financial changes exist for the date, write a `complete_empty` manifest
and `_SUCCESS.json` with `candidate_count=0` and `fetched_count=0`.

## Temporal Activities

The implementation should keep activities concrete and source-specific. Do not
hide registration behind generic registries.

Activity groups:

```text
company_download_full_snapshot
company_download_update_window
company_write_snapshot_jsonl
company_write_update_jsonl
company_write_manifest

finance_build_full_candidates
finance_build_update_candidates
finance_fetch_candidate_batch
finance_write_report_manifest

s3_object_exists
s3_read_jsonl_gzip
s3_write_jsonl_gzip
s3_write_json
```

Financial fetch activities should operate in batches. Default production
settings:

```text
candidate shard size: 1000
activity batch size: 100
max concurrent finance batch activities: 4
HTTP attempts: 5
retry delays: 30s, 60s, 120s, 240s
```

HTTP `Retry-After` should be respected when present.

## Scheduling

Temporal owns source schedules.

Full load:

```text
Manual only:
NorwayBrregFullLoadWorkflow(workflow_id="norway-brreg-full-load", force=false)
```

Daily update:

```text
Temporal schedule: norway_brreg_company_daily_update_schedule
Workflow: NorwayBrregCompanyDailyUpdateWorkflow(partition_date=YYYY-MM-DD)
Child workflow: NorwayBrregFinanceDailyUpdateWorkflow(partition_date=YYYY-MM-DD)
```

There should not be a normal independent daily finance schedule. Finance daily
is a child of company daily so operators cannot accidentally trigger two
separate source downloads for the same logical update.

Manual repair commands can start `NorwayBrregFinanceDailyUpdateWorkflow` for a
specific date when the company update success marker already exists.

## Idempotency And Resume

Full load:

- If company full `_SUCCESS.json` exists, skip company full download.
- If finance full `_SUCCESS.json` exists, skip finance full download.
- If both exist, the top-level full load exits without BRREG calls.
- Candidate shards can be reused when the finance manifest is missing.
- Existing raw financial report keys are not overwritten.
- Failed workflows never write `_SUCCESS.json`.

Daily company update:

- The date-level raw update file can be replaced on rerun.
- The date-level manifest can be replaced on rerun.
- Replacement is write-temp-then-rename/copy, or write-new-then-promote, so
  Dagster never sees `_SUCCESS.json` for a partial write.

Daily finance update:

- Existing raw financial report keys are skipped.
- The date-level report manifest can be replaced on rerun.
- Empty candidate sets are successful and produce `complete_empty`.

## Failure Semantics

Hard failures:

- Source payload cannot be parsed.
- Full company snapshot has zero records.
- Company update time window still fails after one-hour split.
- Retry budget exhausted for HTTP 429, HTTP 5xx, timeout, or connection errors.
- Financial report payload is not the expected list/object structure.
- Manifest write fails.

Soft outcomes:

- Financial endpoint returns 404 for an organization/year. Record `not_found`
  and continue.
- Financial report raw key already exists. Record `skipped_existing` and
  continue.
- Company daily update has zero records. Write `complete_empty` and continue to
  finance daily, which should also complete empty if no finance candidates are
  present.

## Dagster Handoff

Dagster should consume S3 completion markers. It should not call BRREG.

Manifest assets/sensors:

```text
norway_brreg_company_snapshot_manifest
norway_brreg_company_update_manifest[date]
norway_brreg_finance_full_manifest
norway_brreg_finance_update_manifest[date]
```

Processing assets:

```text
norway_brreg_company_snapshot_normalized_parquets
norway_brreg_company_update_normalized_parquets[date]
norway_brreg_company_snapshot_clickhouse
norway_brreg_company_update_clickhouse[date]

norway_brreg_finance_statements_snapshot_parquet
norway_brreg_finance_statements_updates_parquet[date]
norway_brreg_finance_statements_snapshot_usd_parquet
norway_brreg_finance_statements_updates_usd_parquet[date]
norway_brreg_finance_statements_snapshot_clickhouse
norway_brreg_finance_statements_updates_clickhouse[date]
```

Assets to remove or rewrite:

- `norway_brreg_entities_snapshot_s3`: replace BRREG download with manifest
  consumption from Temporal S3 output.
- `norway_brreg_entity_updates_s3`: replace BRREG update download with manifest
  consumption from Temporal S3 output.
- `norway_brreg_financial_fetches_updates_parquet`: remove BRREG financial
  endpoint calls; replace with manifest/raw report consumption.
- `norway_brreg_financial_fetches_snapshot_parquet`: keep only as a temporary
  compatibility inventory if required during migration, then remove once
  statement parsing reads raw report manifests directly.

Dagster jobs after migration:

```text
norway_brreg_company_snapshot_publish_job
  - company snapshot manifest
  - company snapshot normalized parquets
  - company snapshot ClickHouse

norway_brreg_company_update_publish_job
  - company update manifest
  - company update normalized parquets
  - company update ClickHouse

norway_brreg_finance_snapshot_publish_job
  - finance full manifest
  - finance statements snapshot parquet
  - finance statements snapshot USD parquet
  - finance statements snapshot ClickHouse

norway_brreg_finance_update_publish_job
  - finance update manifest
  - finance statements update parquet
  - finance statements update USD parquet
  - finance statements update ClickHouse
```

Dagster sensors should launch these jobs from `_SUCCESS.json` detection. Dagster
schedules should not trigger BRREG source downloads.

## CLI

Worker:

```bash
uv run norway-data-worker \
  --temporal-address companycollect:7233 \
  --s3-endpoint http://companycollect:9000
```

Start full load:

```bash
uv run norway-data full-load \
  --temporal-address companycollect:7233 \
  --s3-endpoint http://companycollect:9000
```

Start company daily update:

```bash
uv run norway-data company-update \
  --date 2026-07-01 \
  --temporal-address companycollect:7233 \
  --s3-endpoint http://companycollect:9000
```

Manual finance daily repair:

```bash
uv run norway-data finance-update \
  --date 2026-07-01 \
  --temporal-address companycollect:7233 \
  --s3-endpoint http://companycollect:9000
```

Inspect S3 state:

```bash
uv run norway-data status \
  --s3-endpoint http://companycollect:9000
```

`status` should print whether full company, full finance, and recent daily
partitions have `_SUCCESS.json`.

## Target File Layout

```text
corpscout/norway_data/
  README.md
  pyproject.toml
  norway_data/
    __init__.py
    cli.py
    worker.py
    config.py
    jsonl.py
    s3_storage.py
    manifest.py
    company_api.py
    company_records.py
    company_storage.py
    company_workflows.py
    finance_api.py
    finance_candidates.py
    finance_storage.py
    finance_workflows.py
    full_load_workflow.py
  tests/
    test_company_api.py
    test_company_update_windows.py
    test_company_workflows.py
    test_finance_candidates.py
    test_finance_storage.py
    test_finance_workflows.py
    test_full_load_workflow.py
    test_package_independence.py
```

Responsibilities:

- `config.py`: fixed constants and environment loading.
- `jsonl.py`: streaming gzip JSONL read/write helpers.
- `s3_storage.py`: concrete S3 client wrapper and object existence helpers.
- `manifest.py`: manifest data classes and success marker helpers.
- `company_api.py`: BRREG company HTTP client.
- `company_records.py`: source JSON field extraction for org number, active
  status, and last submitted accounts year.
- `company_storage.py`: company S3 object keys and read/write methods.
- `company_workflows.py`: company full and daily workflows/activities.
- `finance_api.py`: BRREG financial HTTP client.
- `finance_candidates.py`: full and daily candidate extraction.
- `finance_storage.py`: finance S3 object keys and read/write methods.
- `finance_workflows.py`: finance full and daily workflows/activities.
- `full_load_workflow.py`: top-level company-then-finance orchestration.

## Migration Steps

1. Rename package directory:

```text
corpscout/norway_financial_bootstrap -> corpscout/norway_data
```

2. Rename Python package:

```text
norway_financial_bootstrap -> norway_data
```

3. Update `pyproject.toml`:

```text
project.name = "norway-data"
scripts:
  norway-data = "norway_data.cli:main"
  norway-data-worker = "norway_data.worker:worker_main"
```

4. Split current finance-only code into the target file layout.
5. Replace parquet candidate batches with JSONL gzip candidate shards.
6. Remove ClickHouse candidate selection from the Temporal tool.
7. Add company full snapshot workflow and S3 storage.
8. Add company daily update workflow with split-window handling.
9. Add finance daily update workflow driven by
   `/sisteInnsendteAarsregnskap` changes.
10. Add top-level full-load workflow.
11. Add manifests and `_SUCCESS.json` for every workflow output.
12. Update README with exact run commands, S3 contract, and resume behavior.
13. Update Dagster assets to consume Temporal S3 manifests.
14. Remove BRREG API resource use from Norway Dagster definitions.

## Testing Plan

Temporal package tests:

- Full load exits without BRREG calls when both full success markers exist.
- Full load runs company full before finance full when no markers exist.
- Company full writes JSONL gzip, manifest, and `_SUCCESS.json`.
- Company full fails on zero records and does not write `_SUCCESS.json`.
- Company daily splits a rejected day window into smaller windows.
- Company daily deduplicates update IDs across split windows.
- Company daily writes `complete_empty` for zero updates.
- Finance full reads company snapshot JSONL from S3, not ClickHouse.
- Finance full writes candidate shards from active companies with accounts year.
- Finance full skips existing raw report keys.
- Finance daily filters only `/sisteInnsendteAarsregnskap` changes.
- Finance daily writes `complete_empty` for no financial changes.
- Retry-exhausted 429/5xx/network errors fail the workflow and do not write
  `_SUCCESS.json`.
- Package independence test verifies no `dagster` or `dagster_v3` imports.

Dagster tests:

- Manifest assets read `_SUCCESS.json` then `manifest.json`.
- Company snapshot normalization reads Temporal JSONL, not BRREG.
- Company update normalization reads Temporal JSONL, not BRREG.
- Financial statement parsing reads raw report manifest rows and raw report
  JSON, not financial fetch parquet generated by Dagster.
- Norway Dagster definitions do not require `NorwayBrregApiResource` after
  migration.

Smoke tests:

- Run `norway-data full-load` against fake BRREG HTTP fixtures and local
  S3-compatible storage.
- Run `norway-data company-update --date YYYY-MM-DD` against fake update
  fixtures that include and exclude `/sisteInnsendteAarsregnskap`.
- Run `norway-data status` against local S3-compatible storage.

## Acceptance Criteria

- `corpscout/norway_data` replaces `corpscout/norway_financial_bootstrap`.
- The Temporal package has its own `uv` project and no Dagster dependency.
- Full company data is written to S3 as JSONL gzip with manifest and
  `_SUCCESS.json`.
- Full finance data is driven by the full company S3 snapshot.
- Full load can be rerun after success without downloading company or finance
  data again.
- Daily company update writes raw JSONL gzip with manifest and `_SUCCESS.json`.
- Daily finance update is triggered after daily company update and fetches only
  organizations with `/sisteInnsendteAarsregnskap` changes.
- Raw financial report JSON objects are immutable and skipped when already
  present.
- Dagster consumes S3 manifests and raw JSON only.
- No Norway Dagster asset calls BRREG directly after the migration.
- Operators can see completion state from S3 manifests and the `norway-data
  status` command.
