# Norway ClickHouse Native Insert Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix Norway ClickHouse exports so they use the native `dagster_clickhouse` client API instead of the incompatible `clickhouse-connect` `insert(...)` API.

**Architecture:** Use the existing shared `export_duckdb_table_to_clickhouse(...)` helper, which reads DuckDB rows and inserts them through `client.execute("INSERT ... VALUES", rows)`. Keep Norway table preparation logic unchanged and apply the same fix to both company and financial-statement exports because both currently call `client.insert(...)`.

**Tech Stack:** Dagster, dagster-clickhouse, ClickHouse native driver, DuckDB, pytest.

---

### Task 1: Make Tests Use the Native ClickHouse Client Shape

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Replace fake `insert(...)` API with `execute(..., params)`**

Change `FakeClickHouseClient` so it has:

```python
self.insert_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

def execute(
    self,
    sql: str,
    params: list[tuple[Any, ...]] | None = None,
) -> None:
    if params is None:
        self.statements.append(sql)
    else:
        self.insert_calls.append((sql, params))
```

and remove `insert(...)`.

- [ ] **Step 2: Update Norway export assertions**

Replace references to `client.inserts` with `client.insert_calls`. Assert SQL starts with native `INSERT INTO \`norway_brreg\`.\`...\`` and reconstruct row dictionaries from `insert_calls[*][1][0]`.

- [ ] **Step 3: Run one export test red**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_assets.py::test_export_norway_brreg_clickhouse_financials_touches_only_financial_table -q
```

Expected: FAIL with `AttributeError: 'FakeClickHouseClient' object has no attribute 'insert'`.

### Task 2: Use Shared Native ClickHouse Export Helper

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py`

- [ ] **Step 1: Import the helper**

Add:

```python
from dagster_v3.defs.clickhouse.resolved import export_duckdb_table_to_clickhouse
```

- [ ] **Step 2: Replace company export insert path**

After `prepare_norway_brreg_clickhouse_companies_table(clickhouse)`, open the official resource connection and call:

```python
rows = export_duckdb_table_to_clickhouse(
    duckdb_path=database_path,
    clickhouse_client=client,
    duckdb_schema=DLT_DATASET_NAME,
    duckdb_table=ENTITIES_TABLE,
    clickhouse_database=tables.NORWAY_BRREG_DATABASE,
    clickhouse_table=tables.COMPANIES_TABLE,
    columns=tables.COMPANIES_COLUMNS,
    truncate=False,
)
```

Return `rows`.

- [ ] **Step 3: Replace financial export insert path**

After `prepare_norway_brreg_clickhouse_financial_statements_table(clickhouse)`, call the same helper with `duckdb_table=FINANCIAL_STATEMENTS_TABLE`, `clickhouse_table=tables.FINANCIAL_STATEMENTS_TABLE`, and `columns=tables.FINANCIAL_STATEMENTS_COLUMNS`.

- [ ] **Step 4: Run red test green**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_assets.py::test_export_norway_brreg_clickhouse_financials_touches_only_financial_table -q
```

Expected: PASS.

### Task 3: Verify and Commit

**Files:**
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py`
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_assets.py -q
```

Expected: PASS.

- [ ] **Step 2: Validate Dagster definitions**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
git ls-files --others --exclude-standard -z corpscout/dagster_v3/storage | xargs -0 rm -f
git add docs/superpowers/plans/2026-06-18-fix-norway-clickhouse-native-insert.md \
  corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py \
  corpscout/dagster_v3/tests/test_norway_brreg_assets.py
git commit -m "fix: use native clickhouse insert for norway exports"
```

Expected: Commit succeeds on `main`.

## Self-Review

Spec coverage: the plan fixes the exact `Client has no attribute insert` failure and covers the parallel company export bug.

Placeholder scan: no placeholders remain.

Type consistency: the fake ClickHouse client now matches the native driver pattern used by `dagster_clickhouse`.
