# Norway Brreg Resumable Financial Fetches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `norway_brreg_financial_fetches_duckdb` resume from already fetched organizations after interruption instead of starting from the first candidate again.

**Architecture:** Replace the financial-fetch dlt asset with a regular Dagster asset that writes fetch outcomes directly to `norway_brreg.financial_fetches` in DuckDB in small durable batches. Keep dlt for the entity bulk load, where replace semantics are acceptable; do not use dlt for the long per-organization BRREG financial API crawl because dlt extraction does not provide a per-request durable checkpoint.

**Tech Stack:** Dagster assets, DuckDB, Python requests/dlt HTTP client, pytest.

---

### Task 1: Add Durable Financial Fetch Storage Helpers

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_fetches.py`
- Test: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_fetches.py`

- [x] **Step 1: Write interruption persistence test**

Add a test that seeds four candidate entities, runs the fetch loop with `commit_every_rows=1`, makes the second HTTP call raise `KeyboardInterrupt`, and verifies `norway_brreg.financial_fetches` already contains the first fetched row.

- [x] **Step 2: Write resume skip test**

Add a test that pre-inserts one `financial_fetches` row, runs the fetch loop again, and verifies the client is not called for the already fetched organization.

- [x] **Step 3: Implement table creation and row upsert helpers**

Add direct DuckDB helpers that create `norway_brreg.financial_fetches` from `BRREG_FINANCIAL_FETCHES_COLUMNS`, delete rows for the same `org_number`, and insert the latest fetch row. Use explicit column order from `BRREG_FINANCIAL_FETCHES_COLUMNS`.

- [x] **Step 4: Implement resumable fetch function**

Add `run_brreg_financial_statement_fetches(...) -> dict[str, int]` that:

- reads candidates from `norway_brreg.entities`
- ensures `norway_brreg.financial_fetches` exists
- reads existing `org_number` values from `norway_brreg.financial_fetches`
- skips existing organizations
- fetches only missing organizations
- upserts results every `commit_every_rows`
- logs total candidates, skipped existing rows, fetched rows, and status counts

- [x] **Step 5: Keep iterator behavior for tests that need row shaping**

Keep `iter_brreg_financial_statement_fetch_rows(...)` for pure row-shaping tests, but do not use it as the production Dagster materialization path for the long crawl.

### Task 2: Convert Financial Fetch Asset Away From dlt

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/resources.py`
- Test: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [x] **Step 1: Replace `@dlt_assets` financial fetch asset**

Change `norway_brreg_financial_fetches_duckdb_asset` from `@dlt_assets` to a regular `@dg.asset`:

```python
@dg.asset(
    name="norway_brreg_financial_fetches_duckdb",
    deps=[dg.AssetKey("norway_brreg_entities_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Resumable Norway Brreg annual-account fetch outcomes stored in DuckDB.",
)
def norway_brreg_financial_fetches_duckdb_asset(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    ...
```

- [x] **Step 2: Call the resumable fetch function from the asset**

The asset should call `run_brreg_financial_statement_fetches(...)` and return `MaterializeResult(metadata=counts)`. It should no longer call `dlt.run(...)` for financial fetches.

- [x] **Step 3: Remove the dlt financial source/resource**

Remove `norway_brreg_financial_fetches_source()` and `_financial_fetches_resource()` from `resources.py` because production no longer uses dlt for this long crawl. Keep entity dlt source/resource unchanged.

- [x] **Step 4: Update asset graph tests**

Update tests that currently assert `norway_brreg_financial_fetches_source` exists. Replace them with assertions that `norway_brreg_financial_fetches_duckdb` is a regular asset depending on `norway_brreg_entities_duckdb` and has `duckdb` but not `dlt` in its kinds.

### Task 3: Update Integration Tests And Docs

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/docs/README.md`

- [x] **Step 1: Replace dlt financial pipeline helper in tests**

Replace `_run_financial_fetches_dlt_pipeline_for_test(...)` with a direct call to `run_brreg_financial_statement_fetches(...)`.

- [x] **Step 2: Keep normalization tests unchanged in outcome**

Existing normalization tests should still pass: `norway_brreg.financial_fetches` remains the input table for `norway_brreg_financial_statements_duckdb`.

- [x] **Step 3: Update README flow**

Update the Norway BRREG documentation to show:

- entities are loaded by dlt
- financial fetches are a resumable regular Dagster asset
- `norway_brreg.financial_fetches` is a durable checkpoint/audit table
- reruns skip already fetched `org_number` values

### Task 4: Verify And Commit

**Files:**
- No additional source files.

- [x] **Step 1: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_financial_fetches.py tests/test_norway_brreg_assets.py -q
```

- [x] **Step 2: Validate Dagster definitions**

Run:

```bash
uv run dg check defs
```

- [x] **Step 3: Check whitespace**

Run:

```bash
git diff --check
```

- [x] **Step 4: Commit only this change set**

Run:

```bash
git add \
  src/dagster_v3/defs/norway_brreg/financial_fetches.py \
  src/dagster_v3/defs/norway_brreg/assets.py \
  src/dagster_v3/defs/norway_brreg/resources.py \
  src/dagster_v3/defs/norway_brreg/docs/README.md \
  tests/test_norway_brreg_financial_fetches.py \
  tests/test_norway_brreg_assets.py \
  docs/superpowers/plans/2026-06-18-norway-brreg-resumable-financial-fetches.md
git commit -m "fix: make norway financial fetches resumable"
```
