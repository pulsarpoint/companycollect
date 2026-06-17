# Norway Brreg Financial Statements Asset Key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the accidental duplicate Dagster node `norway_brreg_financial_statements_duckdb_asset` and make downstream ClickHouse financial export depend on the real normalized DuckDB financial statements asset.

**Architecture:** Keep the Python function name `norway_brreg_financial_statements_duckdb_asset`, but set the Dagster asset key explicitly to `norway_brreg_financial_statements_duckdb`. This matches the naming style already used by the dlt assets and avoids exposing implementation suffixes in the graph.

**Tech Stack:** Dagster asset key naming, pytest, dg definitions validation.

---

## Tasks

### Task 1: Fix the Asset Key

- [ ] In `src/dagster_v3/defs/norway_brreg/assets.py`, update the decorator for `norway_brreg_financial_statements_duckdb_asset`:

```python
@dg.asset(
    name="norway_brreg_financial_statements_duckdb",
    deps=[dg.AssetKey("norway_brreg_financial_fetches_duckdb")],
    ...
)
```

### Task 2: Add Graph Assertion

- [ ] In `tests/test_norway_brreg_assets.py`, assert the bad suffix node is absent:

```python
assert "norway_brreg_financial_statements_duckdb_asset" not in asset_names
```

- [ ] Keep the ClickHouse financial dependency assertion:

```python
assert {key.path[-1] for key in clickhouse_financial_node.parent_keys} == {
    "norway_brreg_financial_statements_duckdb",
}
```

### Task 3: Verify

- [ ] Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
```

- [ ] Run:

```bash
uv run dg check defs
```

- [ ] Print graph edges and verify:

```bash
uv run python - <<'PY'
from dagster_v3.definitions import defs as load_project_defs
repo = load_project_defs().get_repository_def()
for key in sorted(repo.asset_graph.get_all_asset_keys(), key=lambda k: k.to_user_string()):
    name = key.to_user_string()
    if 'norway_brreg_financial' in name or 'norway_brreg_clickhouse_financial' in name:
        node = repo.asset_graph.get(key)
        parents = sorted(parent.to_user_string() for parent in node.parent_keys)
        print(f'{name} <- {parents}')
PY
```

Expected:

```text
norway_brreg_clickhouse_financial_statements <- ['norway_brreg_financial_statements_duckdb']
norway_brreg_financial_fetches_duckdb <- ['norway_brreg_entities_duckdb']
norway_brreg_financial_statements_duckdb <- ['norway_brreg_financial_fetches_duckdb']
```

## Self-Review

- No `_asset` implementation suffix remains in the Dagster graph.
- No placeholder `norway_brreg_financial_statements_duckdb` node remains.
- ClickHouse financial export depends on the real normalized financial statements asset.
