# dagster_v3 deployment runbook

How to stand up, operate, and recover the corpscout Dagster instance. This documents the
**current host-mode deployment** (one machine running `dg dev`); containerization (image build,
compose topology, `DAGSTER_RUN_IMAGE`) is planned but **not built yet** — `DAGSTER_RUN_IMAGE` in
`.env` is currently consumed by nothing.

## 1. Topology

```
                 ┌──────────────────────────────┐
                 │ dagster_v3 host               │
                 │  dg dev (webserver + daemon)  │
                 │  data/*.duckdb  (staging)     │
                 │  DUCKDB_TEMP_DIRECTORY (spill)│
                 └──────┬───────────┬───────────┘
   Dagster metadata     │           │ exports
┌───────────────────┐   │           │        ┌─────────────────────────┐
│ Postgres          │◄──┘           └───────►│ ClickHouse `corpscout`  │
│ dagster_v3 DB     │  (shared with Temporal)│ (native 9002 / http 8123)│
└───────────────────┘                        └─────────────────────────┘
        plus: MinIO/S3 (raw snapshots), Temporal (translator), translator service,
        vLLM endpoints (translation/embedding), MaxMind GeoLite2 directory.
```

External services a host must reach (credentials via `.env`, see §2):

| Service | Address (dev) | Used for |
|---|---|---|
| Postgres | `DAGSTER_PG_URL` | Dagster run/event/schedule storage (shared DB with Temporal — connection pressure is the scaling bottleneck; see CLAUDE.md) |
| ClickHouse | `CLICKHOUSE_HOST` native `CLICKHOUSE_NATIVE_PORT` | all `corpscout.*` exports |
| MinIO/S3 | `CORPSCOUT_S3_ENDPOINT` | raw snapshot buckets (Norway parquet, Finland XBRL XML, Sweden archives, …) |
| Temporal | `TEMPORAL_ADDRESS` | translator workflow |
| Translator service | `TRANSLATOR_API_URL` (default `http://localhost:8080`) | translation queue |
| vLLM endpoints | `TRANSLATION_PROVIDER_LOCAL_*`, `COMMONCRAWL_EMBED_*`, `COMMONCRAWL_LLM_*` | translation + NACE classification |
| MaxMind dir | `MAXMIND_DATABASE_DIRECTORY` | commoncrawl_geoip |
| Companies House API | `COMPANY_HOUSE` key | UK financials |
| Alert webhook | `ALERT_WEBHOOK_URL` | run-failure alerts (Slack incoming-webhook payload) |

## 2. Environment contract

`.env` in the repo root (gitignored) is the single env source; `.env.example` documents every
variable with placeholders. `scripts/dagster-dev.sh` and `scripts/dagster-health-check.py`
bootstrap `DAGSTER_HOME`/`DAGSTER_PG_URL` from it. Anything secret (ClickHouse password, S3
keys, HF token, API keys) lives only in `.env` — never in code or migrations.

Gotchas:
- **DuckDB paths are CWD-relative** (`data/...`), so every process must start with the repo root
  as its working directory (the scripts guarantee this). This is the main container blocker.
- `DUCKDB_TEMP_DIRECTORY` must point at a large disk (Brazil RFB spills >100 GiB;
  `DUCKDB_MAX_TEMP_DIRECTORY_SIZE` caps it).
- `DAGSTER_HOME` defaults to the repo checkout in dev. Run state under `storage/` and `logs/`
  is gitignored; on a fresh host these directories are created on first run.

## 3. Disk layout

| Path | Contents | Class |
|---|---|---|
| `data/*.duckdb` (+ per-source subdirs) | per-source staging databases | **rebuildable cache** |
| `$DUCKDB_TEMP_DIRECTORY` | DuckDB spill | scratch, safe to wipe when idle |
| `storage/`, `logs/` (under `DAGSTER_HOME`) | compute logs, IO artifacts | scratch |
| MinIO buckets (`source-*`) | raw source snapshots (per-company API fetches, XBRL XML, …) | **expensive-to-rebuild cache** |
| Postgres `dagster_v3` DB | run history, schedules, event log | **backup** |
| ClickHouse `corpscout` DB | all published tables + `text_translations` / `text_classifications` caches | **backup** |

## 4. Backup scope (decision, 2026-07-12)

**Back up only Postgres and ClickHouse.**

- Every DuckDB file under `data/` re-derives from source downloads by re-running the source's
  full-refresh job — they are staging, not a system of record. Do not back them up; after disk
  loss, re-materialize each source chain (register jobs first, then financials).
- The MinIO raw-snapshot buckets are also re-derivable, but some were expensive to build (the
  Norway per-company financial fetches took days of throttled API calls). Treat them as
  *expensive-to-rebuild cache*: no scheduled backup required, but do not delete casually, and
  prefer bucket versioning if storage is cheap.
- ClickHouse matters beyond the exports: `text_translations` / `text_classifications` are
  accumulated LLM output that survives every wipe-and-replace export and would cost real GPU
  time to regenerate.
- Postgres holds run history, schedule state, and concurrency bookkeeping; losing it loses
  operational history and in-flight backfill state (the pipelines themselves would still rerun).

## 5. ClickHouse migrations (always before code that needs them)

Schema is owned by golang-migrate files in `corpscout/clickhouse/migrations/`; Dagster code only
asserts tables exist (`assert_clickhouse_tables_exist`) and refuses to run against a missing
table. Apply from the `corpscout/` directory:

```bash
cd ..   # corpscout/
make clickhouse-migrate-up        # migrate/migrate v4.17.0 against CLICKHOUSE_MIGRATE_URL
```

Rules (see CLAUDE.md "ClickHouse migrations"):
- The ledger is **forward-only**. Never rewind a shared ledger; ship a repair migration instead.
- Every migration is registered in `EXPECTED_MIGRATIONS`
  (`tests/test_clickhouse_migrations.py`) and has a matching `.down.sql`.
- Ordering on deploy: **migrate → deploy code → materialize.** New export columns must exist in
  ClickHouse before the exporting asset runs (the contract tests pin column order to the
  migration files).

## 6. Startup sequence (fresh host or restart)

1. `.env` present with all §2 variables; large disk mounted at `DUCKDB_TEMP_DIRECTORY`.
2. Postgres + ClickHouse + MinIO + Temporal reachable; ClickHouse migrations applied (§5).
3. `uv sync --frozen` (Python 3.14; `uv.lock` is authoritative).
4. `uv run dg check defs && uv run pytest tests -q -m "not integration"` — a broken definition
   load takes the whole code location down.
5. `./scripts/dagster-dev.sh` — starts webserver + daemon with `DAGSTER_HOME`, `DAGSTER_PG_URL`,
   and the DB pool overflow set. **A restart is required after any `dagster.yaml` change**
   (run_retries, run_monitoring, retention, tag concurrency limits are daemon-side).
6. Health-check cron (installed via `crontab -l | grep dagster-health-check`; reinstall on a new
   host):
   ```
   */10 * * * * cd <repo> && <uv> run python scripts/dagster-health-check.py --fix >> <repo>/logs/health-check.log 2>&1
   ```
7. Set `ALERT_WEBHOOK_URL` so `run_failure_alert_sensor` (default RUNNING) can deliver failures.
8. Verify in the UI: schedules RUNNING for the sources you expect (schedules are
   default-STOPPED until validated per the data-source guidelines), daemon heartbeats healthy.

## 7. Operations

- **Failure signal**: `run_failure_alert_sensor` posts every failed run to the webhook and logs
  it. Freshness/row-count asset checks on the ClickHouse leaves catch silently-stopped
  schedules and empty tables.
- **Wedged queue** (run stuck QUEUED, leaked pool slot): `run_monitoring` +
  `free_slots_after_run_end_seconds` in `dagster.yaml` should prevent it; the cron'd
  `dagster-health-check.py --fix` is the backstop. Manual diagnosis: CLAUDE.md
  "Troubleshooting".
- **Connection pressure**: inspect with the `pg_stat_activity` one-liner in CLAUDE.md; the
  durable fix is PgBouncer in front of the shared Postgres.
- **Heavy bulk jobs** are capped at 2 concurrent via the `corpscout/workload=heavy-bulk` run
  tag (`defs/common/tags.py` + `dagster.yaml`); tag any new multi-GB snapshot job the same way.
- **Backfills**: launch from the UI only, one-partition-per-run (enforced by the repo-level
  backfill-policy contract test). Cancel in-flight backfills before changing a
  `partitions_def`.

## 8. Not built yet (deliberate gaps)

- **No container image / compose topology.** `DAGSTER_RUN_IMAGE` is unused. When built, the
  image must bundle `uv`-synced deps + dbt projects, mount `data/`, the DuckDB temp disk and
  the MaxMind dir, point `DAGSTER_HOME` at a volume (not the checkout), and replace the
  CWD-relative `data/` paths with a configurable base dir.
- **No CI** — tests/lint/contract tests run locally only.
- **Compute logs are local** (`storage/` under `DAGSTER_HOME`); an S3 compute-log manager is
  the fix once containerized.
