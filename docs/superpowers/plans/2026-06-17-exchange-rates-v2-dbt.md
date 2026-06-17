# Exchange Rates V2 dbt Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a parallel `exchange_rates_v2` Dagster section that extracts ECB raw payloads with dlt and performs DuckDB transformations with dbt models.

**Architecture:** Keep the existing `exchange_rates` flow unchanged. Add `exchange_rates_v2` with a raw dlt asset writing to `data/exchange_rates_v2_source.duckdb`, dbt models that materialize `exchange_rates_v2_stage.ecb_rates` and `exchange_rates_v2_stage.identity_rates`, and a final Dagster asset that exports dbt-produced rows to ClickHouse. The v2 flow is experimental and isolated by asset names, DuckDB path, dbt project, and dbt target schema.

**Tech Stack:** Dagster, dagster-dlt, dagster-dbt, dbt-duckdb, DuckDB, dlt, pytest.

---

## File Structure

- Create `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/` package with `assets.py`, `source.py`, `tables.py`, and a local dbt project under `dbt/`.
- Modify `corpscout/dagster_v3/pyproject.toml` and `uv.lock` to add `dagster-dbt` and `dbt-duckdb` if compatible with the current Python runtime.
- Create `corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py` with source, dbt model, and export tests.
- Add `docs/superpowers/plans/2026-06-17-exchange-rates-v2-dbt.md`.

### Task 1: Dependencies and dbt Project Skeleton

**Files:**
- Modify: `corpscout/dagster_v3/pyproject.toml`
- Modify: `corpscout/dagster_v3/uv.lock`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/dbt/dbt_project.yml`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/dbt/profiles.yml`

- [x] **Step 1: Add dbt dependencies**

Run:

```bash
cd corpscout/dagster_v3
uv add dagster-dbt dbt-duckdb
```

Expected: dependencies install or report a clear Python-version incompatibility.

- [x] **Step 2: Create dbt project files**

Create a minimal dbt project named `exchange_rates_v2` with profile `exchange_rates_v2`, DuckDB database path read from `EXCHANGE_RATES_V2_DUCKDB_PATH`, defaulting to `data/exchange_rates_v2_source.duckdb`, and target schema `exchange_rates_v2_stage`.

### Task 2: V2 Source and dbt Models

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/source.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/tables.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/dbt/models/sources.yml`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/dbt/models/ecb_rates.sql`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/dbt/models/identity_rates.sql`
- Test: `corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py`

- [x] **Step 1: Copy raw ECB source behavior into v2**

Use the same API request and raw payload table shape as v1, but rename constants to v2 and use `exchange_rates_v2_raw` / `exchange_rates_v2_stage` names.

- [x] **Step 2: Add dbt source declaration**

Declare `exchange_rates_v2_stage.ecb_raw_payloads` as a dbt source and link it to the Dagster raw asset through source metadata if needed.

- [x] **Step 3: Add `ecb_rates` dbt model**

The model should parse ECB JSON payloads from `source_payload_json`, explode each currency/date observation, cast `rate_date` to `date`, `rate` to `decimal(38,12)`, `pulled_at` to `timestamp`, and produce the same column contract as v1.

- [x] **Step 4: Add `identity_rates` dbt model**

The model should read distinct `rate_date` values from `ref('ecb_rates')` and generate EUR/EUR identity rows using the same column contract as v1.

### Task 3: Dagster Assets and Tests

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/assets.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/__init__.py`
- Test: `corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py`

- [x] **Step 1: Add raw dlt asset**

Define `exchange_rates_v2_raw_duckdb` with `@dlt_assets`, using an inline DuckDB dlt pipeline and v2 source.

- [x] **Step 2: Add dbt assets**

Define `exchange_rates_v2_dbt_assets` with `@dbt_assets`, a `DbtProject`, and `DbtCliResource`. Pass partition date vars to dbt so dbt models update the current date window.

- [x] **Step 3: Add ClickHouse export asset**

Define `exchange_rates_v2_clickhouse` depending on the dbt `ecb_rates` and `identity_rates` models. For the experiment, export to the same `reference.exchange_rates` table with source values `ECB EXR` and `identity`, using the same date-window delete/insert behavior as v1.

- [x] **Step 4: Add tests**

Tests should verify:
- v2 raw source yields the same raw payload structure as v1.
- dbt models compile and run against a temporary DuckDB database with a fake raw payload.
- dbt output rows match v1 expected rows for ECB and identity rates.
- v2 assets are registered in Dagster.

### Task 4: Verification and Commit

**Files:**
- Commit all v2 files and dependency changes if verification passes.

- [x] **Step 1: Run v2 tests**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_v2_dbt.py -q
```

Expected: pass.

- [x] **Step 2: Run exchange-rate related tests**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py tests/test_exchange_rates_v2_dbt.py tests/test_duckdb_schema_contract.py tests/test_clickhouse_migrations.py -q
```

Expected: pass.

- [x] **Step 3: Run Dagster definition check**

```bash
cd corpscout/dagster_v3
uv run dg check defs
```

Expected: definitions load successfully.

- [ ] **Step 4: Commit**

```bash
git add corpscout/dagster_v3/pyproject.toml corpscout/dagster_v3/uv.lock corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2 corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py docs/superpowers/plans/2026-06-17-exchange-rates-v2-dbt.md
git commit -m "feat: add dbt exchange rates v2 experiment"
```
