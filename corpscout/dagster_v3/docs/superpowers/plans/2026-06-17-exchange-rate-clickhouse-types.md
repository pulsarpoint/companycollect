# Exchange Rate ClickHouse Types Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure exchange-rate rows exported from DuckDB to ClickHouse use Python values compatible with the ClickHouse native driver.

**Architecture:** Define the normalized DuckDB staging table schema in the exchange-rate table contract. `ecb_rates`, `identity_rates`, and the final ClickHouse export table use those typed DuckDB columns, so `export_duckdb_table_to_clickhouse` fetches native Python `date`, `Decimal`, and `datetime` values.

**Tech Stack:** Dagster, DuckDB, ClickHouse native driver, pytest.

---

## Files

- Modify `tests/test_exchange_rates_assets.py`
  - Assert the DuckDB table type contract.
  - Assert inserted ClickHouse rows contain `date`, `Decimal`, and `datetime` values.
- Modify `src/dagster_v3/defs/exchange_rates/tables.py`
  - Add DuckDB column types for normalized exchange-rate tables.
- Modify `src/dagster_v3/defs/exchange_rates/assets.py`
  - Create normalized DuckDB tables with the typed schema.
  - Export typed columns directly from the normalized DuckDB tables.

## Tasks

- [x] Update tests to assert the DuckDB schema contract and ClickHouse-compatible Python types.
- [x] Run the export test and confirm it fails with string rows.
- [x] Define typed DuckDB schemas and use them for normalized exchange-rate tables.
- [x] Run the export test.
- [x] Run focused tests and `dg check defs`.
- [ ] Commit only this type fix, preserving unrelated working-tree changes.

## Verification Commands

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_export_exchange_rates_clickhouse_reads_duckdb_and_inserts_union -q
uv run pytest tests/test_exchange_rates_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```
