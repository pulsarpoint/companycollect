# DuckDB Schema Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reusable DuckDB schema contracts and use them to create and validate exchange-rate DuckDB asset tables.

**Architecture:** Define table contracts as first-class Python values. Shared helpers create DuckDB tables from contracts, validate existing DuckDB table schemas, and expose Dagster `TableSchema` metadata for asset UI. Exchange-rate normalized DuckDB tables use those helpers instead of building SQL types manually.

**Tech Stack:** Dagster assets, DuckDB, pytest.

---

## Files

- Create `src/dagster_v3/defs/duckdb/__init__.py`
  - Make the shared DuckDB helper package importable.
- Create `src/dagster_v3/defs/duckdb/schema_contract.py`
  - Add `DuckDBColumnContract` and `DuckDBTableContract`.
  - Add `create_duckdb_table_from_contract`.
  - Add `validate_duckdb_table_contract`.
  - Add `dagster_table_schema_from_contract`.
- Create `tests/test_duckdb_schema_contract.py`
  - Test create-table SQL behavior with real DuckDB.
  - Test validation failure on type mismatch.
  - Test Dagster metadata conversion.
- Modify `src/dagster_v3/defs/exchange_rates/tables.py`
  - Replace the dict-only type contract with `EXCHANGE_RATES_DUCKDB_CONTRACT`.
  - Keep `EXCHANGE_RATES_DUCKDB_COLUMN_TYPES` as a derived compatibility mapping for existing tests and callers.
- Modify `src/dagster_v3/defs/exchange_rates/assets.py`
  - Add Dagster table schema metadata to the normalized DuckDB assets.
  - Use `create_duckdb_table_from_contract` and `validate_duckdb_table_contract` in `_ensure_exchange_rates_duckdb_schema`.
- Modify `tests/test_exchange_rates_assets.py`
  - Assert the contract object exists and drives the compatibility mapping.

## Tasks

- [x] Add failing tests for the shared DuckDB schema contract helper.
- [x] Implement the shared DuckDB schema contract helper.
- [x] Add failing exchange-rate tests for the contract object and asset metadata.
- [x] Wire exchange-rate table creation/validation to the contract helper.
- [x] Run focused DuckDB contract and exchange-rate tests.
- [x] Run `dg check defs`.
- [ ] Commit only schema-contract changes, preserving unrelated working-tree edits.

## Verification Commands

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_duckdb_schema_contract.py tests/test_exchange_rates_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```
