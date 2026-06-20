# Retire exchange_rates v1 (make v2 canonical) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. Checkbox steps track progress.

**Goal:** Make `exchange_rates_v2` the single pipeline that populates `corpscout.exchange_rates`, and remove the v1 Dagster module — **without losing v1's daily schedule** (so daily rate updates keep flowing).

**Why:** v1 (`defs/exchange_rates/`) and v2 (`defs/exchange_rates_v2/`) both write the same `corpscout.exchange_rates` table (`ReplacingMergeTree(pulled_at)`). Two pipelines writing one table = double-writes and last-`pulled_at`-wins races. v2 is the intended successor (raw-payload provenance, dbt parsing, DuckDB schema contract). Columns are **identical** between v1 and v2 (verified), so v2 is a drop-in.

## CRITICAL distinctions (do not get these wrong)

- **KEEP** the top-level `exchange_rates/` **client library** (`exchange_rates/client.py`, `models.py`) — this is `from exchange_rates import ExchangeRateClient`, imported by Latvia and Norway. It is NOT v1 and must stay untouched.
- **REMOVE** only the v1 **Dagster module** `src/dagster_v3/defs/exchange_rates/` (`__init__.py`, `assets.py`, `source.py`, `tables.py`).
- **KEEP** migration `000002_reference_exchange_rates` — it defines the shared `corpscout.exchange_rates` table that v2 keeps writing. No migration changes.

## Pre-known facts (verified 2026-06-20)

- v1 schedule: `@dg.schedule(name="exchange_rates_daily_schedule", cron_schedule="30 18 * * 1-5", execution_timezone="Europe/Belgrade", job=exchange_rates_daily_job)` → `RunRequest(partition_key=<scheduled date>)`.
- v2 has **no** schedule (manual/backfill only) — this is the gap to close first.
- v1 jobs: `exchange_rates_backfill_job`, `exchange_rates_daily_job`.
- Column parity: `exchange_rates.tables.EXCHANGE_RATES_COLUMNS == exchange_rates_v2.tables.EXCHANGE_RATES_V2_COLUMNS` (True).
- Cross-dependency: `tests/test_clickhouse_migrations.py` imports `from dagster_v3.defs.exchange_rates import tables as exchange_rate_tables` and uses `exchange_rate_tables.EXCHANGE_RATES_COLUMNS` to assert migration 000002's schema.
- v1 tests live in `tests/test_exchange_rates_assets.py` (removed with the module).

---

### Task 1: Parity check before removing anything

**Files:** none (operational verification).

- [ ] **Step 1: Confirm v2 is populating the table.** After running the v2 backfill (`dg launch --assets "+exchange_rates_v2_clickhouse" --partition-range "2023-01-01...<yesterday>"`), verify rows exist:

```sql
SELECT source, count(), min(rate_date), max(rate_date), countDistinct(quote_currency)
FROM corpscout.exchange_rates GROUP BY source;
```
Expected: rows with `source IN ('ECB EXR','identity')` covering the range, and the currencies the client needs (USD always; NOK/GBP/SEK/DKK per config).

- [ ] **Step 2: Confirm the client reads correctly from v2 output.** A quick `ExchangeRateClient.from_env().usd_rates([...])` for a known EUR date returns a rate. (This proves v2's rows satisfy the same client v1 fed.)

If parity holds, proceed. If not, STOP — do not remove v1 until v2 output is correct.

---

### Task 2: Add a daily schedule to v2 (close the gap FIRST)

**Files:**
- Modify: `src/dagster_v3/defs/exchange_rates_v2/assets.py`
- Test: `tests/test_exchange_rates_v2_dbt.py`

- [ ] **Step 1: Add the schedule** (replicates v1 exactly). Near the job definition in `assets.py`:

```python
from datetime import date  # already imported

@dg.schedule(
    name="exchange_rates_v2_daily_schedule",
    cron_schedule="30 18 * * 1-5",  # weekdays 18:30, after ECB's ~16:00 CET publish
    execution_timezone="Europe/Belgrade",
    job=exchange_rates_v2_job,
)
def exchange_rates_v2_daily_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest:
    scheduled_time = context.scheduled_execution_time
    partition_key = (
        scheduled_time.date().isoformat()
        if scheduled_time is not None
        else date.today().isoformat()
    )
    return dg.RunRequest(partition_key=partition_key)
```

- [ ] **Step 2: Register it** in the module `defs`:

```python
defs = dg.Definitions(
    assets=[...],
    jobs=[exchange_rates_v2_job],
    schedules=[exchange_rates_v2_daily_schedule],
    resources={...},
)
```

- [ ] **Step 3: Test it’s registered + targets the partitioned job.** Add to `tests/test_exchange_rates_v2_dbt.py` (extend the existing registration test):

```python
def test_exchange_rates_v2_daily_schedule_registered() -> None:
    repository = load_project_defs().get_repository_def()
    sched = repository.get_schedule_def("exchange_rates_v2_daily_schedule")
    assert sched.cron_schedule == "30 18 * * 1-5"
    assert sched.job.name == "exchange_rates_v2_job"
```

- [ ] **Step 4: Verify.** `uv run pytest tests/test_exchange_rates_v2_dbt.py -q` and `uv run dg check defs`. Confirm `uv run dg list defs | grep exchange_rates_v2_daily_schedule`.

- [ ] **Step 5: Commit.** `feat(exchange_rates_v2): add daily schedule (replaces v1's)`

---

### Task 3: Repoint the migration test off v1

**Files:**
- Modify: `tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Switch the import** from v1 to v2 (columns are identical):

```python
# was: from dagster_v3.defs.exchange_rates import tables as exchange_rate_tables
from dagster_v3.defs.exchange_rates_v2 import tables as exchange_rate_tables
```

- [ ] **Step 2: Fix the column reference.** Wherever the test uses `exchange_rate_tables.EXCHANGE_RATES_COLUMNS`, change to `exchange_rate_tables.EXCHANGE_RATES_V2_COLUMNS` (same 11 columns). The migration `000002` schema assertion is unchanged.

- [ ] **Step 3: Verify.** `uv run pytest tests/test_clickhouse_migrations.py -q` → still green.

- [ ] **Step 4: Commit.** `test(clickhouse): read exchange-rate columns from v2 tables`

---

### Task 4: Remove the v1 module

**Files:**
- Delete: `src/dagster_v3/defs/exchange_rates/__init__.py`, `assets.py`, `source.py`, `tables.py`
- Delete: `tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Pre-flight — confirm no remaining importers.** Run:

```bash
rg -rn "defs\.exchange_rates\b|defs\.exchange_rates import|from dagster_v3\.defs\.exchange_rates\b" src tests | rg -v "exchange_rates_v2"
```
Expected after Task 3: only matches inside `defs/exchange_rates/` itself (the files being deleted). If anything else references it, resolve first.

- [ ] **Step 2: Delete the module and its tests.**

```bash
git rm -r src/dagster_v3/defs/exchange_rates
git rm tests/test_exchange_rates_assets.py
```

- [ ] **Step 3: Verify.** `uv run dg check defs` (clean). Confirm v1 is gone and v2 schedule remains:

```bash
uv run dg list defs | grep -E "exchange_rates(_daily_schedule|_backfill_job|_daily_job)\b" || echo "v1 gone (expected)"
uv run dg list defs | grep exchange_rates_v2_daily_schedule
```

- [ ] **Step 4: Full suite.** `uv run pytest -q` → green (the only removed tests are v1's).

- [ ] **Step 5: Commit.** `refactor: retire exchange_rates v1 module; v2 is canonical`

---

## Self-Review

- **Coverage:** parity gate (T1) → schedule gap closed BEFORE removal (T2) → cross-dep repointed (T3) → module + tests deleted (T4). Daily updates never stop because the v2 schedule lands before v1 is removed.
- **Do-not-touch invariants:** the `exchange_rates/` client library and migration `000002` are untouched; `corpscout.exchange_rates` keeps the same schema and writer cadence.
- **Type consistency:** v1/v2 column tuples are identical, so the migration test repoint is a pure rename of the symbol.

## Open follow-ups (not in this plan)
- The heavy `ALTER TABLE … DELETE` window mutation in the v2 export may be redundant given `ReplacingMergeTree(pulled_at)` — evaluate whether the client reads with `FINAL`/`argMax` and, if so, drop the mutation. (Tier-2 item from the package analysis.)
- Unbounded `append` growth of the `ecb_raw_payloads` staging table — add retention if it matters.
