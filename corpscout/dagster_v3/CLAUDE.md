# dagster_v3 — agent guide & best practices

Dagster pipelines that ingest company open-data per country into DuckDB → ClickHouse.
Standard per-source shape: **dlt download → per-source DuckDB file → (dbt transforms) → ClickHouse export.**
When adding a source, mirror the nearest existing module: `finland_ytj` (register spine), `norway_brreg`
(registry + financials), `latvia_ur` (bulk CSV + financials + EUR/USD metrics).

## Commands
- Always use **`uv run`** for `dg`/`pytest`/`dagster` (e.g. `uv run dg check defs`, `uv run pytest tests/...`).
- Start the dev instance with **`./scripts/dagster-dev.sh`** — it exports `DAGSTER_HOME`, `DAGSTER_PG_URL`
  (from `.env`), and the connection-pool overflow. Don't run bare `uv run dg dev` (misses the env).
- `dg` is **project-aware** (finds defs + instance automatically). The raw **`dagster`** CLI is not — it needs
  `DAGSTER_HOME` and `DAGSTER_PG_URL` exported and `-m dagster_v3.definitions`. Prefer `dg` for dev commands.
- Validate before done: **`uv run dg check defs`** and the relevant `uv run pytest tests/...`.

## DuckDB (per-source file)
- **One DuckDB file per source**, single-writer. Put a concurrency **`pool="..."`** on *every* asset that writes
  that file (dlt load, dbt, ClickHouse export). The instance defaults every pool to limit 1
  (`dagster.yaml` `concurrency.pools.default_limit: 1`), so just declaring the pool serializes writes.
- **The DuckDB file stem must differ from the dlt dataset name** (e.g. file `latvia_ur_source.duckdb`, dataset
  `latvia_ur`). If they match, DuckDB's binder can't resolve `<dataset>.<table>` (`Ambiguous reference`).
- dlt's `pipelines_dir` is a global singleton keyed only on `pipeline_name` — pass a per-checkout
  `pipelines_dir` in tests/CLI helpers to avoid cross-worktree `LoadPackageNotFound`.

## Assets / Dagster gotchas
- **No `from __future__ import annotations` in modules defining `@dlt_assets`/`@dg.asset`/`@dbt_assets`.** It
  stringizes the `context: AssetExecutionContext` hint and breaks Dagster's op context-type validation.
- **ClickHouse export**: the **migration owns the schema**. Code asserts the table exists
  (`assert_clickhouse_tables_exist`) then atomically replaces it (stage table + `EXCHANGE TABLES`). Never
  duplicate DDL in Python. Pin the export column order with a contract test that greps the migration file.
- **Refuse to replace on empty input** — `raise ValueError` when a download yields zero rows, so a bad fetch
  can't blank a populated table.
- **A non-nullable ClickHouse `String`/`LowCardinality(String)` column must get `''`, never `NULL`.** The native
  driver calls `.encode()` per value and dies on `None` (`'NoneType' object has no attribute 'encode'`). Coalesce
  string columns to `''` in the producing SQL (e.g. `coalesce(spine.source_type,'')`); only make the migration
  column `Nullable(...)` if NULL is semantically meaningful. Numeric/date NULLs are fine **iff** the column is
  `Nullable(...)`. A small unit test won't catch this — it only fires when real data has a NULL in that column.
- **Don't export `raw_*` JSON or `source_payload_hash` to ClickHouse.** The full source JSON (`raw_entity`,
  `raw_financial_record`) is the biggest column, and the per-row SHA256 hash is *incompressible* (one unique
  value/row); nothing queries them — together ~60% of table size (dropping both shrank `lv_companies` 119→43 MiB).
  Keep them in the **DuckDB staging** tables; define a `*_EXPORT_COLUMNS` subset (full tuple minus
  `CLICKHOUSE_EXCLUDED_COLUMNS`) for the exporter. Keep the cheap lineage columns
  (`source_url`/`source_run_id`/`source_record_id`). `source_url` is *constant* → compresses to ~nothing, so
  it's fine. Only reinstate `source_payload_hash` if a table moves to hash-based incremental/SCD2.

## Downloads (HTTP)
- Use **`dlt.sources.helpers.requests`** (`Session`/`Client`) as the HTTP session — built-in retry/backoff on
  connection errors and 429/5xx. **Don't use plain `requests`** (Latvia regressed on exactly this).
- For large **streaming** downloads, *also* wrap the stream in a whole-download retry loop: request-level retry
  does **not** cover a mid-stream `ChunkedEncodingError`/`IncompleteRead`. Re-truncate the temp file each
  attempt and verify `Content-Length`. See `latvia_ur/resources.py:_download_to_path`.
- Split big multi-file downloads into **one raw-load asset per file** (checkpoints), so a failure in one file
  doesn't re-download the others. Pivot/join downstream.
- Parse CSV with a real `csv` reader (handles quoted delimiters and doubled quotes) and `restkey`/`restval` so a
  malformed row can't crash the load. Never `split(';')`.

## Partitions & backfills
- **Don't partition at all if the whole dataset comes back in one request.** `exchange_rates_v2` pulls every
  ECB reference currency's full multi-year history in a single ~1 MB call, so it's a *non-partitioned*
  full-refresh asset on a daily schedule — partitions only added event-log churn and backfill ceremony for
  nothing. Partition only when per-period fetching/bookkeeping is actually needed (large per-period source data).
- For reference/time-series data that *does* need partitioning use **`MonthlyPartitionsDefinition`**, not daily.
  Dagster writes one materialization event per partition to the Postgres event log; daily-over-years = thousands
  of events → connection storms. `end_offset=1` keeps the in-progress current month valid for a refresh schedule.
- Backfill policy: **`BackfillPolicy.multi_run(max_partitions_per_run=1)` + a per-source op pool.** A UI/daemon
  backfill then runs partitions throttled, one small run each — no connection spike. **Do NOT use
  `single_run()`** (one giant run = event-log connection storm); it only exists to enable
  `dg launch --partition-range`, which we avoid.
- Cap in-flight runs with **`concurrency.runs.max_concurrent_runs`** in `dagster.yaml`.
- **Launch backfills from the UI (or the running daemon)** — NOT `dagster job backfill -m dagster_v3.definitions`
  (it tags runs with code-location `dagster_v3.definitions`, which mismatches `dg dev`'s location name
  `dagster_v3` → orphaned runs the daemon can't launch). Don't mix `dg` and `dagster` CLIs for runs.

## ClickHouse migrations
- Schema in `clickhouse/migrations/` (golang-migrate); add each migration name to `EXPECTED_MIGRATIONS` in
  `tests/test_clickhouse_migrations.py`. Only the `corpscout` database; never the old `reference` DB.
- **`ORDER BY` cannot contain `Nullable` columns** (`allow_nullable_key` is off) — keep sort keys non-nullable.
- The ledger is **forward-only**: if a migration failed-then-was-fixed and the prod ledger advanced past it, add
  a forward **repair** migration (`CREATE TABLE IF NOT EXISTS …`) rather than rewinding the shared ledger.

## Currency / financials
- Store native-currency `*_original` values faithfully. Do **USD conversion as a SEPARATE step** via the shared
  `ExchangeRateClient` (EUR-based, keyed on the report `period_end_date`), filling `*_usd` +
  `fx_rate_to_usd`/`fx_rate_date`/`fx_source`. Mirror `norway_brreg/financial_normalize.py`.
- Apply `rounded_to_nearest` scaling **before** FX. Keep values signed. Catch `LookupError` for the per-request
  rate fallback (don't swallow real connection errors).
- **`ExchangeRateClient.usd_rates()` takes an arbitrarily large request set in one call** — it binds the requested
  `(currency, date)` pairs as ClickHouse `Array(String)` params expanded with `arrayJoin(arrayZip(...))`, a single
  O(1)-plan query. (It previously built one `SELECT ... UNION ALL` branch per pair, so a source's full date span —
  Latvia ≈ 1k distinct `period_end_date`s — overflowed the query-plan optimizer → `code 572
  TOO_MANY_QUERY_PLAN_OPTIMIZATIONS`. No longer chunk requests for plan size.) `usd_rates` still raises `LookupError`
  if *any* requested rate is missing, so `latvia_ur/metrics.py:_load_rates` batches (≈50/call) purely to bound the
  per-request fallback when a source has a currency absent from the rate table (e.g. Latvia's pre-euro LVL) — a
  missing rate then degrades one batch, not the whole set.

## Connections & scaling (target: 100+ sources)
- The Dagster Postgres (`companycollect`) is **shared with Temporal** — connection pressure is the main scaling
  bottleneck. The durable fix is **PgBouncer**: see `docs/superpowers/plans/2026-06-20-dagster-connection-scaling-pgbouncer.md`.
  Connect Dagster as the **least-privilege `dagster` user**, never the superuser.
- Dev pool overflow is set in `scripts/dagster-dev.sh` (`DAGSTER_DB_POOL_MAX_OVERFLOW`). Too low starves the
  daemon's own pool (`QueuePool limit reached`); too high on a small server cap starves the shared DB.

## Troubleshooting
- **`dg dev` hangs at "Launching Dagster services…"** → usually a stale dbt manifest lock from an orphaned
  code-server: `pkill -f "dagster api grpc"` then
  `find src/dagster_v3/defs -name '*.concurrent-update-lock' -delete`.
- **"remaining connection slots are reserved for SUPERUSER" / "QueuePool limit … reached"** → connection
  exhaustion. Inspect with
  `docker exec ppoint-postgres psql -U corpscout -c "SELECT usename,state,count(*) FROM pg_stat_activity GROUP BY 1,2 ORDER BY 3 DESC;"`.
  PgBouncer is the real fix.
- `DAGSTER_HOME=$PWD` writes daemon/sensor artifacts into the repo — keep `dagster_v3/storage/` gitignored.
- **To materialize a multi-level chain, `dg launch --assets` the FULL explicit asset list — don't rely on `+leaf`.**
  Despite the docs saying `+expr` = "all upstream", this `dg` build resolved `+asset` (and `+a,+b`) as **one hop**
  only: the planned set was the leaves + their direct deps, so deeper steps (e.g. the raw downloads, the
  intermediate build) silently never ran and the chain failed when their inputs didn't exist. List every asset
  key in the chain (e.g. the 8 raw loaders + pivot + metrics + usd + 2 exports), or verify the planned set with
  `select count(distinct step_key) ... where dagster_event_type='ASSET_MATERIALIZATION_PLANNED'` before trusting it.
- **A run stuck `QUEUED` forever while the daemon keeps cycling = a leaked op-concurrency pool slot.** When a run
  crashes ungracefully (`RUN_EXCEPTION`/`PIPELINE_FAILURE`, not a clean step failure) Dagster can fail to release
  its pool slot; the limit-1 pool then blocks every later run whose root step needs it. Diagnose:
  `els.get_concurrency_info("<pool>")` (via `DagsterInstance.get()`) — a `claimed_slots`/`pending_steps` entry
  whose `run_id` is a long-finished/FAILED run is the leak. Free it: `instance.event_log_storage`
  `.free_concurrency_slots_for_run("<dead_run_id>")`; the QueuedRunCoordinator dequeues the waiter within a cycle.
  **`uv run python scripts/dagster-health-check.py`** detects all of this (leaked slots + stuck-QUEUED +
  stale-STARTED runs) and exits non-zero for cron alerting; add `--fix` to free leaked slots automatically.
- **Cancel in-flight backfills BEFORE changing an asset's `partitions_def`** (e.g. de-partitioning). A queued
  partition run that starts after the partitions are gone fails with `RUN_EXCEPTION` and can leak its pool slot
  (see above). Check `bulk_actions` / `run_tags key='dagster/backfill'` for stragglers first.

## Workflow
- Mirror the nearest existing source module. TDD where practical; `uv run dg check defs` before finishing.
- **Commit by explicit path** — the working tree often carries unrelated in-flight WIP; never `git add -A`.
