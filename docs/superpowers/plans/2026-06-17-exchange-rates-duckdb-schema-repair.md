# Exchange Rates Disposable DuckDB Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make exchange-rate DuckDB derived tables disposable so stale local schemas cannot break materialization.

**Architecture:** Keep the raw dlt table `exchange_rates_stage.ecb_raw_payloads` as the input snapshot table. Treat `exchange_rates_stage.ecb_rates`, `exchange_rates_stage.identity_rates`, and `exchange_rates_stage.clickhouse_exchange_rates` as per-run derived tables: drop and recreate them from schema contracts whenever their asset materializes. The ClickHouse asset validates the derived input tables instead of silently creating empty tables.

**Tech Stack:** Python, pytest, DuckDB, Dagster asset functions, existing `DuckDBTableContract` helper.

---

## File Structure

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`: reset derived DuckDB tables on materialization and validate them before ClickHouse export.
- Modify `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`: add regression coverage for stale `VARCHAR` derived tables and for missing derived tables during export.
- Add `docs/superpowers/plans/2026-06-17-exchange-rates-duckdb-schema-repair.md`: this plan.

### Task 1: Stale Derived Table Regression

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`

- [x] **Step 1: Add a failing test for stale `ecb_rates`**

Create a temp DuckDB database with the normal raw payload table plus an old `exchange_rates_stage.ecb_rates` table where all columns are `varchar`. Call `normalize_exchange_rates_ecb_duckdb(...)`. Assert it returns four rows and `information_schema.columns` reports `DATE`, `DECIMAL(38,12)`, and `TIMESTAMP` for `rate_date`, `rate`, and `pulled_at`.

- [x] **Step 2: Run the test and verify it fails**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_normalize_exchange_rates_ecb_duckdb_recreates_disposable_stale_table -q
```

Expected: fail with the same contract mismatch seen in Dagster materialization.

- [x] **Step 3: Reset `ecb_rates` before normalization**

In `normalize_exchange_rates_ecb_duckdb(...)`, call a local `_reset_exchange_rates_duckdb_table(connection, ECB_RATES_TABLE)` before deleting/inserting rows. The reset helper should drop only that derived table and recreate it from `EXCHANGE_RATES_DUCKDB_CONTRACT`.

### Task 2: Identity and Export Behavior

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`

- [x] **Step 1: Add identity-table stale schema coverage**

Create a temp DuckDB database with a typed `ecb_rates` table plus an old `identity_rates` table where all columns are `varchar`. Call `generate_exchange_rates_identity_duckdb(...)` and assert the recreated identity table has typed `DATE`, `DECIMAL(38,12)`, and `TIMESTAMP` columns.

- [x] **Step 2: Reset `identity_rates` before generation**

In `generate_exchange_rates_identity_duckdb(...)`, call `_reset_exchange_rates_duckdb_table(connection, IDENTITY_RATES_TABLE)` before querying source dates and inserting identity rows.

- [x] **Step 3: Validate input tables before ClickHouse export**

Replace `_ensure_exchange_rates_duckdb_schema(connection)` inside `export_exchange_rates_clickhouse(...)` with `_validate_exchange_rates_duckdb_table(...)` calls for `ecb_rates` and `identity_rates`. Export should not create missing upstream-derived tables.

### Task 3: Verification

**Files:**
- Commit only exchange-rate disposable-table files plus this plan if requested.

- [x] **Step 1: Run focused regression tests**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_normalize_exchange_rates_ecb_duckdb_recreates_disposable_stale_table tests/test_exchange_rates_assets.py::test_generate_exchange_rates_identity_duckdb_recreates_disposable_stale_table -q
```

Expected: pass.

- [x] **Step 2: Run relevant exchange-rate suite**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py tests/test_duckdb_schema_contract.py tests/test_clickhouse_migrations.py -q
```

Expected: pass.

- [x] **Step 3: Run Dagster definition check**

```bash
cd corpscout/dagster_v3
uv run dg check defs
```

Expected: definitions load successfully.
