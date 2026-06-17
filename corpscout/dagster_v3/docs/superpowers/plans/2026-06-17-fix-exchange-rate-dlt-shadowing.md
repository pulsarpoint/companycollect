# Fix Exchange Rate Dlt Shadowing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `exchange_rates_raw_duckdb` so runtime dlt pipeline construction calls the dlt module, not the Dagster dlt resource parameter.

**Architecture:** Keep the `dlt` Dagster resource parameter named `dlt` because Dagster binds resources by parameter name. Alias the imported dlt library as `dlt_lib` in `assets.py`, and use that alias for `dlt_lib.pipeline(...)` and `dlt_lib.destinations.duckdb(...)`.

**Tech Stack:** Dagster, dagster-dlt, dlt, pytest.

---

## Files

- Modify `src/dagster_v3/defs/exchange_rates/assets.py`
  - Change `import dlt` to `import dlt as dlt_lib`.
  - Change decorator and runtime dlt pipeline construction to use `dlt_lib`.
  - Leave asset `kinds` changes already present in the working tree untouched.
- Modify `tests/test_exchange_rates_assets.py`
  - Add a materialization regression test using a fake `dlt` resource.

## Tasks

- [x] Add a failing materialization test for `exchange_rates_raw_duckdb_asset`.
- [x] Run that single test and confirm it fails with `AttributeError: 'FakeDagsterDltResource' object has no attribute 'pipeline'`.
- [x] Alias the dlt module as `dlt_lib` and update pipeline construction.
- [x] Run the regression test and focused exchange-rate tests.
- [x] Run `dg check defs`.
- [ ] Commit only the bug fix and regression test, preserving unrelated user changes.

## Verification Commands

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rates_raw_duckdb_materialization_uses_dlt_module_pipeline -q
uv run pytest tests/test_exchange_rates_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```
