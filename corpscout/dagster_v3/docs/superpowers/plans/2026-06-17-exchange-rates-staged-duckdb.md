# Exchange Rates Staged DuckDB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework exchange rates into raw DuckDB extraction, two DuckDB transform assets, and a final ClickHouse publish asset.

**Architecture:** Use one daily-partitioned graph beginning at 2023-01-01. dlt extracts raw ECB payload metadata into DuckDB. Regular Dagster assets transform raw payloads into ECB rows and identity rows. A final regular Dagster asset deletes/reinserts the selected partition range into the migrated ClickHouse table.

**Tech Stack:** Dagster assets, dagster-dlt, DuckDB, ClickHouse native resource, pytest.

---

## Files

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/source.py`
  - Replace final-row dlt resources with raw ECB payload resource.
  - Add DuckDB dlt pipeline.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`
  - Replace direct ClickHouse dlt assets with staged assets.
  - Add transform/export helpers.
- Modify `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
  - Update source tests and add DuckDB transformation/export tests.

## Tasks

- [x] Add failing tests for raw source, DuckDB transforms, and final ClickHouse export.
- [x] Implement raw ECB dlt resource and DuckDB pipeline.
- [x] Implement `normalize_exchange_rates_ecb_duckdb`.
- [x] Implement `generate_exchange_rates_identity_duckdb`.
- [x] Implement `export_exchange_rates_clickhouse`.
- [x] Replace Dagster asset definitions and schedule selection.
- [x] Run focused tests and `dg check defs`.
- [ ] Commit.

## Verification Commands

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py tests/test_clickhouse_migrations.py -q
uv run dg check defs
```
