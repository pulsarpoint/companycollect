# Norway Brreg ClickHouse Export Dependency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the false dependency from `norway_brreg_clickhouse_financial_statements` to `norway_brreg_translations_applied`.

**Architecture:** Replace the current combined ClickHouse multi-asset with two normal Dagster assets. Company export depends on the translated company staging table. Financial statement export depends only on normalized financial statements. Keep any convenience “export both” helper as a direct wrapper for tests/manual use, but asset lineage must be modeled as two independent assets.

**Tech Stack:** Dagster assets, DuckDB staging tables, dagster-clickhouse resource, pytest.

---

## File Structure

- Modify `src/dagster_v3/defs/norway_brreg/clickhouse.py`
  - Add `prepare_norway_brreg_clickhouse_companies_table`.
  - Add `prepare_norway_brreg_clickhouse_financial_statements_table`.
  - Keep `prepare_norway_brreg_clickhouse_tables` as a direct combined helper.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Remove the `@dg.multi_asset` ClickHouse export.
  - Add `@dg.asset(name="norway_brreg_clickhouse_companies")`.
  - Add `@dg.asset(name="norway_brreg_clickhouse_financial_statements")`.
  - Add per-table export functions.
  - Keep `export_norway_brreg_clickhouse_tables` as a direct wrapper.
- Modify `src/dagster_v3/defs/norway_brreg/definitions.py`
  - Register the two new ClickHouse asset functions instead of `norway_brreg_clickhouse_tables`.
- Modify `tests/test_norway_brreg_assets.py`
  - Update graph expectations.
  - Add focused tests proving companies export does not truncate/insert financial table.
  - Add focused tests proving financial export does not truncate/insert companies table.

## Tasks

### Task 1: Split ClickHouse Preparation

- [ ] Add focused preparation functions in `clickhouse.py`:

```python
def prepare_norway_brreg_clickhouse_companies_table(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NORWAY_BRREG_DATABASE}")
        client.execute(tables.COMPANIES_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_COMPANIES_TABLE}")


def prepare_norway_brreg_clickhouse_financial_statements_table(
    clickhouse: ClickhouseResource,
) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NORWAY_BRREG_DATABASE}")
        client.execute(tables.FINANCIAL_STATEMENTS_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE}")
```

- [ ] Keep the combined helper direct:

```python
def prepare_norway_brreg_clickhouse_tables(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NORWAY_BRREG_DATABASE}")
        client.execute(tables.COMPANIES_DDL.strip())
        client.execute(tables.FINANCIAL_STATEMENTS_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_COMPANIES_TABLE}")
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE}")
```

### Task 2: Add Per-Table Export Functions

- [ ] Add `export_norway_brreg_clickhouse_companies(...) -> int` in `assets.py`.

- [ ] Add `export_norway_brreg_clickhouse_financial_statements(...) -> int` in `assets.py`.

- [ ] Keep `export_norway_brreg_clickhouse_tables(...) -> dict[str, int]` as:

```python
companies = export_norway_brreg_clickhouse_companies(...)
financial_statements = export_norway_brreg_clickhouse_financial_statements(...)
return {"companies": companies, "financial_statements": financial_statements}
```

### Task 3: Replace Multi-Asset With Two Assets

- [ ] Add:

```python
@dg.asset(
    deps=[dg.AssetKey("norway_brreg_translations_applied")],
    name="norway_brreg_clickhouse_companies",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
)
def norway_brreg_clickhouse_companies(...)
```

- [ ] Add:

```python
@dg.asset(
    deps=[dg.AssetKey("norway_brreg_financial_statements_duckdb")],
    name="norway_brreg_clickhouse_financial_statements",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
)
def norway_brreg_clickhouse_financial_statements(...)
```

- [ ] Remove `norway_brreg_clickhouse_tables` as an asset function.

### Task 4: Update Definitions and Tests

- [ ] Register both new assets in `definitions.py`.

- [ ] Assert graph parents:

```python
assert {key.path[-1] for key in clickhouse_companies_node.parent_keys} == {
    "norway_brreg_translations_applied",
}
assert {key.path[-1] for key in clickhouse_financial_node.parent_keys} == {
    "norway_brreg_financial_statements_duckdb",
}
```

- [ ] Assert `"norway_brreg_clickhouse_tables" not in asset_names`.

### Task 5: Verify

- [ ] Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
```

- [ ] Run:

```bash
uv run dg check defs
```

- [ ] Run:

```bash
uv run pytest -q
```

## Self-Review

- `norway_brreg_clickhouse_financial_statements` no longer waits for company translation.
- `norway_brreg_clickhouse_companies` no longer waits for financial normalization.
- No financial LLM translation is introduced.
- The graph now matches the actual data each asset reads.
