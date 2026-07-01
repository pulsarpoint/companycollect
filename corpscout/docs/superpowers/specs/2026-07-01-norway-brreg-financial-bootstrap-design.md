# Norway BRREG financial bootstrap via Temporal

**Date:** 2026-07-01
**Status:** Design approved, pending implementation plan
**Area:** `corpscout` Norway BRREG financial ingestion

## Goal

Move the one-time Norway BRREG historical financial-account crawl out of a normal Dagster asset
materialization and into a small Temporal-backed Python package. The bootstrap package will crawl the
current eligible Norway company snapshot once, persist resumable intermediate results while it runs, and
produce one dated financial fetch snapshot parquet. After that cutover, Dagster should process only the
final snapshot parquet plus normal daily update partitions.

## Motivation

The current `norway_brreg_financial_fetches_snapshot_parquet` asset is doing a multi-day external API
crawl inside one Dagster run. The current snapshot has roughly 430K financial candidates and more than
400K missing raw financial fetches, so the asset performs hundreds of thousands of serial BRREG HTTP
requests plus one S3 cache check per candidate. That is acceptable as a one-time data collection job, but
it is a poor fit for a single unpartitioned Dagster materialization.

The bootstrap should be operationally boring:

- a restart should not lose completed company fetches;
- the final output should be a stable dated parquet file;
- daily Norway financial processing should stay in Dagster and should not depend on rerunning the
  historical crawl;
- the implementation should be concrete and source-specific, not a generic ingestion framework.

## Decisions

- Create a separate Python package for the one-time bootstrap crawl instead of expanding the Dagster
  asset.
- Use Temporal for orchestration, retry, visibility, and bounded worker concurrency.
- Use 2 to 4 worker activities in parallel by default. This is enough to cut wall-clock time without
  hammering BRREG.
- Do not append to one large parquet during the crawl. Parquet is the final compacted format, not the
  row-by-row write format.
- Store intermediate per-company or per-shard JSON/JSONL outputs during the crawl so the workflow can
  resume by skipping already completed org/year results.
- Preserve existing Norway financial storage. If
  `norway_brreg/financial/raw_fetches/org=<org_number>/year=<last_submitted_accounts_year>/financial_fetch.parquet`
  already exists, the bootstrap must not call BRREG again for that org/year. It should reuse that file as
  completed historical work and include it in the final snapshot compaction.
- At workflow completion, compact intermediate outputs into one dated parquet file.
- Dagster consumes the dated final parquet as the historical starting point. Daily update assets remain
  the only recurring financial ingestion path.

## Package Shape

The package should be small and source-specific:

```text
norway_financial_bootstrap/
  __init__.py
  workflows.py      # Temporal workflow: plan work, run bounded fetch activities, compact final output
  activities.py     # Fetch batches, write intermediate outputs, compact outputs
  brreg_client.py   # GET /regnskapsregisteret/regnskap/{org_number}
  storage.py        # S3/object-store keys and JSON/JSONL/parquet IO
  candidates.py     # Read no_companies parquet and build org/year candidates
  schemas.py        # Final fetch row schema shared with Dagster where practical
  cli.py            # Start workflow, inspect status, optionally compact only
```

This is intentionally not a shared abstraction for all countries. If another source needs the same shape
later, extract only the pieces that are demonstrably shared.

## Data Flow

Input:

```text
norway_brreg/entities/normalized/snapshot/no_companies.parquet
```

Candidate rule:

```text
is_active = true
and last_submitted_accounts_year is not null
```

Fetch endpoint:

```text
https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}
```

Intermediate output options:

```text
norway_brreg/financial/bootstrap/date=2026-07-01/raw/org=811685852/year=2024.json
norway_brreg/financial/bootstrap/date=2026-07-01/shards/shard=0000.jsonl
```

The implementation can use either per-org JSON files or worker/shard JSONL files. The important contract
is that completed org/year work is externally visible and can be skipped on retry. If using JSONL shards,
the package must maintain a completed-key index or write idempotent batch manifests so reruns do not
duplicate rows.

Existing raw fetch parquet files produced by the current Dagster path are also completed work:

```text
norway_brreg/financial/raw_fetches/org=<org_number>/year=<last_submitted_accounts_year>/financial_fetch.parquet
```

The bootstrap should build a completed-org/year index from those keys before planning remote fetches. The
final compaction must read and include those existing parquet rows so already-collected data is preserved
and no company/year is fetched twice.

Final output:

```text
norway_brreg/financial/snapshots/date=2026-07-01/financial_fetches.parquet
```

The final parquet should match the existing financial fetch row contract as closely as possible:

- `org_number`
- `legal_name`
- `website`
- `last_submitted_accounts_year`
- `source_url`
- `fetch_status`
- `http_status`
- `error_type`
- `error_message`
- `attempt_count`
- `fetched_at`
- `raw_response`
- existing source/provenance columns used downstream

## Temporal Workflow

Workflow name:

```text
NorwayBrregInitialFinancialSnapshotWorkflow
```

Input:

```text
snapshot_date: "2026-07-01"
source_no_companies_key: "norway_brreg/entities/normalized/snapshot/no_companies.parquet"
output_key: "norway_brreg/financial/snapshots/date=2026-07-01/financial_fetches.parquet"
worker_count: 2 | 4
batch_size: e.g. 100 or 500 candidates per activity
resume: true
```

Workflow steps:

1. Load candidate org/year records from the normalized `no_companies` parquet.
2. Build a completed-org/year index from existing `financial_fetch.parquet` raw fetch files and bootstrap
   intermediate outputs.
3. Partition only missing candidates into deterministic batches.
4. Execute fetch activities with bounded concurrency.
5. Each fetch activity skips already-completed org/year results when `resume=true`.
6. Each fetch activity writes intermediate output and heartbeats progress.
7. After all batches complete, compact existing raw fetch parquets plus bootstrap intermediate outputs into
   the final dated parquet.
8. Return counts: candidates, existing_raw_fetches, fetched, skipped, success, not_found, retryable
   failures, output key.

## Fetch Semantics

Terminal source outcomes:

- `success`
- `not_found`
- `gone`
- `empty`

Retryable/failing outcomes:

- network errors
- timeout
- invalid payload
- `429`
- `5xx`

The package should retry transient failures with bounded backoff. Persistent retryable failures should be
written to intermediate diagnostics and surfaced in the final workflow result. The final compaction can
include failure rows, but the workflow should clearly report non-terminal counts so the operator decides
whether to rerun failed batches before declaring the bootstrap complete.

## Dagster Cutover

The historical Dagster snapshot fetch asset should stop crawling BRREG directly. It should become a
lightweight asset that verifies and records the final dated bootstrap parquet:

```text
norway_brreg_financial_fetches_snapshot_parquet
```

Expected behavior:

- read or validate `norway_brreg/financial/snapshots/date=<snapshot_date>/financial_fetches.parquet`;
- emit metadata: snapshot date, row count, status counts, S3 key;
- fail hard if the configured snapshot key is missing;
- never start the multi-day historical crawl.

Downstream assets keep their current shape:

```text
norway_brreg_financial_statements_snapshot_parquet
norway_brreg_financial_statements_snapshot_usd_parquet
norway_brreg_financial_statements_snapshot_clickhouse
```

Daily update assets remain Dagster partitioned assets:

```text
norway_brreg_financial_fetches_updates_parquet
norway_brreg_financial_statements_updates_parquet
norway_brreg_financial_statements_updates_usd_parquet
norway_brreg_financial_statements_updates_clickhouse
```

## Operational Notes

- Run this bootstrap once for the chosen snapshot date.
- Use a stable workflow id such as `norway-brreg-financial-bootstrap-2026-07-01`.
- Run with conservative concurrency first: 2 workers. Increase to 4 only if BRREG latency and error rate
  are healthy.
- Do not delete intermediate bootstrap files until the final parquet has been compacted, validated, and
  consumed successfully by Dagster.
- After successful cutover, the historical crawl code in Dagster can be removed or guarded so it cannot be
  accidentally started again.

## Testing

- Unit test candidate selection from `no_companies` parquet.
- Unit test BRREG response mapping for success, 404, invalid payload, network error, `429`, and `5xx`.
- Unit test storage idempotency: existing org/year output is skipped when `resume=true`.
- Unit test compaction from intermediate JSON/JSONL into the final parquet schema.
- Temporal workflow test with fake activities: batching, bounded concurrency intent, resume behavior, and
  final compaction call.
- Dagster test: historical snapshot fetch asset fails when the configured final parquet is missing and
  succeeds with row/status metadata when it exists.

## Non-goals

- Do not make a generic multi-country bootstrap framework.
- Do not append directly to one parquet file during the crawl.
- Do not use ClickHouse as the bootstrap checkpoint store.
- Do not change Norway daily update behavior except to keep it independent from the historical bootstrap.
- Do not remove daily Dagster financial assets.
