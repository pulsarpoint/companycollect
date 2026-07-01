# Norway BRREG financial bootstrap via Temporal

**Date:** 2026-07-01
**Status:** Design approved, pending implementation plan
**Area:** `corpscout` Norway BRREG financial ingestion

## Goal

Move the one-time Norway BRREG historical financial-account crawl out of a normal Dagster asset
materialization and into a small Temporal-backed Python package. The bootstrap package will crawl the
current eligible Norway company snapshot once and upload one raw financial fetch parquet object per
company/year to S3. Dagster will then discover those historical raw fetch objects and convert them into:

```text
norway_brreg/financial/statements/snapshot/financial_statements.parquet
```

After this starting snapshot exists, recurring Norway financial processing is only daily updates.

## Motivation

The current `norway_brreg_financial_fetches_snapshot_parquet` asset is doing a multi-day external API
crawl inside one Dagster run. The current snapshot has roughly 430K financial candidates and more than
400K missing raw financial fetches, so the asset performs hundreds of thousands of serial BRREG HTTP
requests plus one S3 cache check per candidate. That is acceptable as a one-time collection campaign, but
it is a poor fit for a single unpartitioned Dagster materialization.

The bootstrap should be operationally boring:

- a restart should not lose completed company fetches;
- existing `financial_fetch.parquet` objects must be preserved and skipped;
- Temporal should only fetch and upload raw report objects;
- Dagster should own conversion from raw fetch objects into normalized statement parquet;
- daily Norway financial processing should not depend on rerunning the historical crawl;
- the implementation should be concrete and source-specific, not a generic ingestion framework.

## Decisions

- Create a separate Python package for the one-time bootstrap crawl instead of expanding the Dagster
  asset.
- Use Temporal for orchestration, retry, visibility, and bounded worker concurrency.
- Use 2 to 4 worker activities in parallel by default. This is enough to cut wall-clock time without
  hammering BRREG.
- Temporal does not create an aggregate fetch parquet and does not create statement parquet.
- Temporal writes one raw fetch parquet per org/year using the existing storage key contract:

```text
norway_brreg/financial/raw_fetches/org=<org_number>/year=<last_submitted_accounts_year>/financial_fetch.parquet
```

- If that key already exists, Temporal must not call BRREG again for that org/year.
- Dagster gets a historical financial resource that lists and reads the raw fetch parquet objects.
- Dagster converts the historical raw fetch objects into the snapshot statements parquet, then continues
  with USD conversion and ClickHouse publishing.
- Daily update assets remain the only recurring financial ingestion path.

## Package Shape

The package should be small and source-specific:

```text
norway_financial_bootstrap/
  __init__.py
  workflows.py      # Temporal workflow: plan work and run bounded fetch activities
  activities.py     # Fetch batches and upload raw fetch parquet objects
  brreg_client.py   # GET /regnskapsregisteret/regnskap/{org_number}
  storage.py        # S3/object-store keys and raw fetch parquet IO
  candidates.py     # Read no_companies parquet and build org/year candidates
  cli.py            # Start workflow / inspect status
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

Bootstrap output:

```text
norway_brreg/financial/raw_fetches/org=<org_number>/year=<last_submitted_accounts_year>/financial_fetch.parquet
```

Each raw fetch parquet should match the existing financial fetch row contract:

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

Dagster output:

```text
norway_brreg/financial/statements/snapshot/financial_statements.parquet
```

## Temporal Workflow

Workflow name:

```text
NorwayBrregInitialFinancialRawFetchWorkflow
```

Input:

```text
snapshot_date: "2026-07-01"
source_no_companies_key: "norway_brreg/entities/normalized/snapshot/no_companies.parquet"
worker_count: 2 | 4
batch_size: e.g. 100 or 500 candidates per activity
resume: true
```

Workflow steps:

1. Load candidate org/year records from the normalized `no_companies` parquet.
2. Build a completed-org/year index from existing `financial_fetch.parquet` raw fetch files.
3. Partition only missing candidates into deterministic batches.
4. Execute fetch activities with bounded concurrency.
5. Each fetch activity skips already-completed org/year results when `resume=true`.
6. Each fetch activity writes one raw fetch parquet object per fetched company/year and heartbeats
   progress.
7. Return counts: candidates, existing_raw_fetches, fetched, skipped, success, not_found, retryable
   failures.

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
written as raw fetch parquet diagnostics and surfaced in the workflow result so the operator decides
whether to rerun failed batches before declaring the bootstrap complete.

## Dagster Cutover

The historical Dagster path should stop crawling BRREG directly. Dagster gets a historical financial
resource, named around the concept `historical_financial`, that can list and read raw fetch parquet
objects.

Expected resource behavior:

- list `norway_brreg/financial/raw_fetches/**/financial_fetch.parquet`;
- read those raw fetch parquet objects as the historical source;
- expose counts and status summaries for observability;
- never call the BRREG financial API.

The snapshot statements asset converts historical raw fetch objects into:

```text
norway_brreg/financial/statements/snapshot/financial_statements.parquet
```

Downstream assets keep their current shape:

```text
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
- Use a stable workflow id such as `norway-brreg-financial-raw-fetch-2026-07-01`.
- Run with conservative concurrency first: 2 workers. Increase to 4 only if BRREG latency and error rate
  are healthy.
- Do not delete raw fetch files. They are the historical source of truth for Dagster conversion.
- After successful cutover, the historical crawl code in Dagster can be removed or guarded so it cannot be
  accidentally started again.

## Testing

- Unit test candidate selection from `no_companies` parquet.
- Unit test BRREG response mapping for success, 404, invalid payload, network error, `429`, and `5xx`.
- Unit test storage idempotency: existing org/year raw fetch output is skipped when `resume=true`.
- Unit test raw fetch parquet writing.
- Temporal workflow test with fake activities: batching, bounded concurrency intent, and resume behavior.
- Dagster test: historical financial resource lists/reads raw fetch parquet and the snapshot statements
  asset creates `financial_statements.parquet` without calling BRREG.

## Non-goals

- Do not make a generic multi-country bootstrap framework.
- Do not produce a final aggregate parquet in Temporal.
- Do not produce normalized statement parquet in Temporal.
- Do not use ClickHouse as the bootstrap checkpoint store.
- Do not change Norway daily update behavior except to keep it independent from the historical bootstrap.
- Do not remove daily Dagster financial assets.
