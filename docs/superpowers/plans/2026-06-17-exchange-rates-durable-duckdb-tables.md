# Exchange Rates Durable DuckDB Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep exchange-rate DuckDB derived tables durable and update only the current materialization window.

**Architecture:** `ecb_rates` and `identity_rates` are asset output tables, so they must preserve rows outside the current Dagster partition/range. A materialization validates the table schema, deletes only rows in its date window, and inserts replacement rows for that window. If a local table has an obsolete schema, the asset should fail with the existing schema-contract error; cleanup/migration should be an explicit operator action, not hidden inside partition materialization.

**Tech Stack:** Dagster asset functions, DuckDB, pytest, existing DuckDB schema contracts.

---

## File Structure

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`: remove disposable-table reset behavior and return to durable schema validation plus window delete/insert.
- Modify `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`: replace stale-table recreation tests with durable-table tests proving rows outside the current date window survive and stale schemas fail explicitly.
- Add `docs/superpowers/plans/2026-06-17-exchange-rates-durable-duckdb-tables.md`: this plan.

### Task 1: Durable Window Tests

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`

- [x] **Step 1: Replace disposable-table tests**

Remove tests expecting stale `VARCHAR` tables to be dropped/recreated. Add tests that seed older rows outside the current date window into typed `ecb_rates` and `identity_rates`, run the relevant transform, and assert older rows remain.

- [x] **Step 2: Keep stale-schema failure test**

Add or keep one test showing an old `VARCHAR` schema fails with `does not match contract`. This documents that schema cleanup is explicit, not automatic.

- [x] **Step 3: Run targeted tests and see current disposable behavior fail**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_normalize_exchange_rates_ecb_duckdb_preserves_rows_outside_window tests/test_exchange_rates_assets.py::test_generate_exchange_rates_identity_duckdb_preserves_rows_outside_window -q
```

Expected: fail while reset behavior drops the whole table.

### Task 2: Durable Implementation

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`

- [x] **Step 1: Remove table reset helper usage**

Remove `_reset_exchange_rates_duckdb_table(...)` calls from `normalize_exchange_rates_ecb_duckdb(...)` and `generate_exchange_rates_identity_duckdb(...)`.

- [x] **Step 2: Restore schema creation/validation for durable tables**

Make `_ensure_exchange_rates_duckdb_schema(...)` create/validate both `ecb_rates` and `identity_rates` with `create_duckdb_table_from_contract(...)`. Keep `_validate_exchange_rates_duckdb_table(...)` only if it is still useful for export validation.

- [x] **Step 3: Verify transform SQL already deletes only the current date window**

`normalize_exchange_rates_ecb_duckdb(...)` and `generate_exchange_rates_identity_duckdb(...)` already delete by `rate_date >= start_date and rate_date <= end_date`; keep that behavior.

### Task 3: Verification and Commit

**Files:**
- Commit only this durable-table correction and tests.

- [x] **Step 1: Run targeted tests**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_normalize_exchange_rates_ecb_duckdb_preserves_rows_outside_window tests/test_exchange_rates_assets.py::test_generate_exchange_rates_identity_duckdb_preserves_rows_outside_window tests/test_exchange_rates_assets.py::test_normalize_exchange_rates_ecb_duckdb_fails_on_stale_schema -q
```

Expected: pass.

- [x] **Step 2: Run relevant suite**

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

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-06-17-exchange-rates-durable-duckdb-tables.md corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py corpscout/dagster_v3/tests/test_exchange_rates_assets.py
git commit -m "fix: keep exchange rate duckdb tables durable"
```
