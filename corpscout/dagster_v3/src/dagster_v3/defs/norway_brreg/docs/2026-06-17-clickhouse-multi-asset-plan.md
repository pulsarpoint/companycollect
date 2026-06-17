# Norway BRREG ClickHouse Multi-Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Model the two Norway BRREG ClickHouse destination tables as two Dagster assets produced by one export operation.

**Architecture:** Keep the native `dagster_clickhouse.ClickhouseResource` export path. Replace the single `@dg.asset` with one `@dg.multi_asset` that yields one `MaterializeResult` for `norway_brreg_clickhouse_companies` and one for `norway_brreg_clickhouse_financial_statements`.

**Tech Stack:** Dagster `@multi_asset`, `AssetSpec`, `MaterializeResult`, `dagster_clickhouse.ClickhouseResource`, DuckDB staging.

---

### Task 1: Pin ClickHouse Outputs As Two Assets

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py`

- [ ] **Step 1: Write the failing test**

Update `test_norway_entity_asset_is_registered` so it asserts:

```python
assert "norway_brreg_clickhouse_tables" not in asset_names
assert "norway_brreg_clickhouse_companies" in asset_names
assert "norway_brreg_clickhouse_financial_statements" in asset_names
```

Then assert both ClickHouse asset nodes have upstream dependencies on `norway_brreg_translations_applied` and `norway_brreg_financial_statements_duckdb`.

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_entity_asset_is_registered -q
```

Expected: FAIL because the code still defines `norway_brreg_clickhouse_tables` as a single asset.

- [ ] **Step 3: Convert the asset**

Replace `@dg.asset def norway_brreg_clickhouse_tables(...)` with:

```python
@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            "norway_brreg_clickhouse_companies",
            deps=[
                dg.AssetKey("norway_brreg_translations_applied"),
                dg.AssetKey("norway_brreg_financial_statements_duckdb"),
            ],
            group_name=GROUP_NAME,
            kinds={"python", "duckdb", "clickhouse"},
            description="Norway Brreg final companies table exported to ClickHouse.",
            metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
        ),
        dg.AssetSpec(
            "norway_brreg_clickhouse_financial_statements",
            deps=[
                dg.AssetKey("norway_brreg_translations_applied"),
                dg.AssetKey("norway_brreg_financial_statements_duckdb"),
            ],
            group_name=GROUP_NAME,
            kinds={"python", "duckdb", "clickhouse"},
            description="Norway Brreg final financial statements table exported to ClickHouse.",
            metadata={"table": tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE},
        ),
    ],
)
def norway_brreg_clickhouse_tables(...):
    counts = export_norway_brreg_clickhouse_tables(...)
    yield dg.MaterializeResult(asset_key="norway_brreg_clickhouse_companies", metadata={...})
    yield dg.MaterializeResult(asset_key="norway_brreg_clickhouse_financial_statements", metadata={...})
```

Keep the function name `norway_brreg_clickhouse_tables` because it names the operation, not the output asset key.

- [ ] **Step 4: Keep definitions registration stable**

Leave `norway_brreg_clickhouse_tables` in the `assets=[...]` list in `definitions.py`; it is now an `AssetsDefinition` containing two asset specs.

- [ ] **Step 5: Verify**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
uv run dg check defs
uv run pytest -q
```

Expected: all pass.
