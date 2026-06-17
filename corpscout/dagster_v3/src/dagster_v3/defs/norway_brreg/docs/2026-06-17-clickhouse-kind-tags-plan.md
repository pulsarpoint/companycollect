# Norway Brreg ClickHouse Kind Tags Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove misleading `python`/`duckdb` kind tags from Norway Brreg ClickHouse output assets.

**Architecture:** Keep implementation unchanged. Update only Dagster asset metadata so `norway_brreg_clickhouse_companies` and `norway_brreg_clickhouse_financial_statements` show as ClickHouse assets in the UI.

**Tech Stack:** Dagster asset metadata, pytest, dg definitions validation.

---

## Tasks

### Task 1: Update Kind Tags

- [ ] In `src/dagster_v3/defs/norway_brreg/assets.py`, change both ClickHouse asset decorators:

```python
kinds={"clickhouse"}
```

### Task 2: Verify

- [ ] Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
```

- [ ] Run:

```bash
uv run dg check defs
```

## Self-Review

- The ClickHouse output assets no longer advertise Python as a UI kind.
- No execution behavior changes.
