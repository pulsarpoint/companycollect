# Staged NACE DuckDB Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the direct NACE dlt-to-ClickHouse asset with a three-stage flow: raw SPARQL payloads in DuckDB, normalized typed NACE categories in DuckDB, and final ClickHouse export.

**Architecture:** dlt is only responsible for extracting official SPARQL CSV payloads into a DuckDB raw table. A regular Dagster asset parses those raw payloads into a schema-contracted DuckDB table matching `reference.nace_categories`. A final Dagster asset publishes the typed DuckDB table into the migrated ClickHouse table with the shared ClickHouse replacement helper, without creating or truncating final tables inside the asset.

**Tech Stack:** Dagster assets/jobs/schedules, dagster-dlt, dlt DuckDB destination, DuckDB, dagster-clickhouse, existing DuckDB schema contract helpers.

---

## File Structure

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/nace/source.py`: expose raw SPARQL payload fetch/resource functions and remove direct ClickHouse dlt pipeline wiring.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/nace/tables.py`: add normalized DuckDB table contract for `nace_stage.nace_categories`.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/nace/assets.py`: define `nace_raw_duckdb`, `nace_categories_duckdb`, and `nace_categories_clickhouse` assets plus jobs/schedule.
- Delete `corpscout/dagster_v3/src/dagster_v3/defs/nace/clickhouse.py`: stale final-table DDL/truncate helper.
- Modify `corpscout/dagster_v3/tests/test_nace_categories.py`: update tests for the staged flow, DuckDB normalization, ClickHouse export, and repository registration.

### Task 1: Source and Schema Contracts

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/nace/source.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/nace/tables.py`
- Test: `corpscout/dagster_v3/tests/test_nace_categories.py`

- [x] **Step 1: Write failing tests for raw payload source and DuckDB contract**

Tests assert that `nace_raw_source()` yields one raw CSV payload row per official scheme, that `fetch_nace_scheme_payload()` hashes the raw response body, and that `tables.NACE_CATEGORIES_DUCKDB_CONTRACT.column_names` equals `tables.NACE_CATEGORIES_COLUMNS`.

- [x] **Step 2: Run targeted tests and confirm they fail before implementation**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_nace_categories.py -q
```

- [x] **Step 3: Implement raw payload source directly**

`source.py` now defines `NACE_DUCKDB_PIPELINE_NAME`, `NACE_DUCKDB_DATASET_NAME`, `NACE_RAW_DLT_TABLE`, `fetch_nace_scheme_payload()`, and `nace_raw_source()`. `fetch_nace_scheme_rows()` remains as a parser convenience around `fetch_nace_scheme_payload()`.

- [x] **Step 4: Implement normalized DuckDB contract**

`tables.py` now defines `NACE_CATEGORIES_DUCKDB_CONTRACT` with the same columns as the final ClickHouse table and DuckDB-native types.

### Task 2: Staged Assets and Export

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/nace/assets.py`
- Delete: `corpscout/dagster_v3/src/dagster_v3/defs/nace/clickhouse.py`
- Test: `corpscout/dagster_v3/tests/test_nace_categories.py`

- [x] **Step 1: Write tests for normalization and export behavior**

Tests create a temporary DuckDB raw table, call `normalize_nace_categories_duckdb()`, and assert parsed category rows. A fake ClickHouse connection test verifies `export_nace_categories_clickhouse()` uses the shared stage/exchange replacement helper and inserts normalized rows.

- [x] **Step 2: Implement `nace_raw_duckdb` as `@dlt_assets`**

Definition-time source uses `nace_raw_source(schemes=())`; runtime materialization supplies the real schemes and Dagster run id.

- [x] **Step 3: Implement `nace_categories_duckdb`**

The asset depends on `nace_raw_duckdb`, creates/validates `nace_stage.nace_categories`, clears prior rows, parses raw CSV payloads, builds normalized rows, and inserts `_dlt_load_id`/`_dlt_id` values.

- [x] **Step 4: Implement `nace_categories_clickhouse`**

The asset depends on `nace_categories_duckdb` and calls `replace_duckdb_tables_in_clickhouse()` for `reference.nace_categories`. It does not create, truncate, or migrate ClickHouse tables.

- [x] **Step 5: Wire job and schedule**

`nace_refresh_job` selects `nace_categories_clickhouse` plus upstream assets, and `nace_weekly_schedule` runs it weekly.

### Task 3: Verification and Commit

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_nace_categories.py`
- Commit only NACE files and this plan document.

- [x] **Step 1: Run targeted NACE tests**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_nace_categories.py -q
```

Expected: pass.

- [x] **Step 2: Run adjacent schema and migration tests**

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py tests/test_duckdb_schema_contract.py -q
```

Expected: pass.

- [x] **Step 3: Run project definition check**

```bash
cd corpscout/dagster_v3
uv run dg check defs
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-06-17-nace-staged-duckdb-flow.md corpscout/dagster_v3/src/dagster_v3/defs/nace corpscout/dagster_v3/tests/test_nace_categories.py
git commit -m "feat: stage nace reference flow through duckdb"
```
