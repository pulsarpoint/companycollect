# Remove Exchange Rate Pipeline Wrapper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the thin `exchange_rates_duckdb_pipeline` helper so the dlt DuckDB pipeline is defined directly where Dagster assets use it.

**Architecture:** Keep the exchange-rate staged flow unchanged. The raw dlt source remains in `source.py`; the concrete dlt pipeline belongs in `assets.py` because it is asset wiring, not source extraction logic.

**Tech Stack:** Dagster, dagster-dlt, dlt, DuckDB, pytest.

---

## Files

- Modify `src/dagster_v3/defs/exchange_rates/source.py`
  - Remove the `Pipeline` import and `exchange_rates_duckdb_pipeline` function.
- Modify `src/dagster_v3/defs/exchange_rates/assets.py`
  - Import `dlt`.
  - Inline `dlt.pipeline(...)` in the `@dlt_assets` decorator and in `dlt.run(...)`.
- Modify `tests/test_exchange_rates_assets.py`
  - Remove tests that only verify the deleted wrapper.
  - Use direct `dlt.destinations.duckdb(...)` in translator tests.

## Tasks

- [x] Remove wrapper-specific test assertions.
- [x] Inline dlt pipeline construction in the asset decorator and runtime `dlt.run`.
- [x] Remove the wrapper from `source.py`.
- [x] Run focused exchange-rate tests.
- [x] Run `dg check defs`.
- [ ] Commit.

## Verification Commands

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```
