# Norway Missing FX Date Null Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix Norway financial statement normalization so rows without an exchange rate insert into DuckDB without invalid date conversion errors.

**Architecture:** Keep missing FX rows, but represent missing date fields as `None` so DuckDB inserts `NULL` into nullable `date` columns. Add coverage at both row-construction level and DuckDB normalization level.

**Tech Stack:** Python, Dagster asset helpers, DuckDB, pytest.

---

### Task 1: Add Failing Coverage for Missing FX Date Nulls

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Update the row-level expectation**

Change:

```python
assert rows[1]["fx_rate_date"] == ""
```

to:

```python
assert rows[1]["fx_rate_date"] is None
```

- [ ] **Step 2: Add a DuckDB insertion regression test**

Add a fake exchange-rate client that raises for `USN`, then run `normalize_norway_brreg_financial_statements_duckdb` against a test DuckDB containing one `USN` financial record. Assert the resulting `fx_rate_date` is `None`.

- [ ] **Step 3: Run the tests red**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest \
  tests/test_norway_brreg_financial_normalize.py::test_build_financial_statement_rows_keeps_rows_without_fx_rate \
  tests/test_norway_brreg_assets.py::test_financial_normalize_persists_missing_fx_as_null_dates \
  -q
```

Expected: FAIL because `fx_rate_date` is currently an empty string.

### Task 2: Emit `None` for Missing FX Dates

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`

- [ ] **Step 1: Change missing FX date value**

Replace:

```python
"fx_rate_date": "" if fx_rate is None else fx_rate.rate_date,
```

with:

```python
"fx_rate_date": None if fx_rate is None else fx_rate.rate_date,
```

- [ ] **Step 2: Run the red tests green**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest \
  tests/test_norway_brreg_financial_normalize.py::test_build_financial_statement_rows_keeps_rows_without_fx_rate \
  tests/test_norway_brreg_assets.py::test_financial_normalize_persists_missing_fx_as_null_dates \
  -q
```

Expected: PASS.

### Task 3: Verify and Commit

**Files:**
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py`
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_normalize.py tests/test_norway_brreg_assets.py -q
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
rm -rf corpscout/dagster_v3/storage
git add docs/superpowers/plans/2026-06-18-fix-norway-missing-fx-date-null.md \
  corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py \
  corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py \
  corpscout/dagster_v3/tests/test_norway_brreg_assets.py
git commit -m "fix: store missing norway fx dates as null"
```

Expected: Commit succeeds on `main`.

## Self-Review

Spec coverage: the plan addresses the exact DuckDB conversion error and protects both row construction and DuckDB insertion behavior.

Placeholder scan: no placeholders remain.

Type consistency: missing date values use Python `None`, which maps to SQL `NULL` for nullable DuckDB date columns.
