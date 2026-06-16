# YTJ dlt DuckDB Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Finland YTJ normalized parquet multi-asset with one dlt-backed DuckDB asset loaded directly from the PRH `/all_companies` REST payload.

**Architecture:** Keep the raw JSON snapshot asset for inspection and idempotent API download. Add `finland_ytj_all_companies_duckdb`, which reads the base JSON and runs a dlt pipeline into a local DuckDB database. Rewire XBRL eligibility to depend on that DuckDB asset and query active companies with websites from the loaded dlt table.

**Tech Stack:** Dagster assets, dlt DuckDB destination, DuckDB SQL, pytest, dg CLI.

---

### Task 1: Replace YTJ Normalized Parquet with dlt DuckDB

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py`
- Modify: `src/dagster_v3/defs/finland_ytj/resources.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py`
- Modify: `tests/test_finland_ytj_assets.py`
- Modify: `tests/test_finland_xbrl_assets.py`
- Modify: `README.md`

- [x] **Step 1: Write failing tests**

Update tests so they expect `finland_ytj_all_companies_duckdb`, no normalized parquet assets, and XBRL eligibility depending on the DuckDB asset.

- [x] **Step 2: Run focused tests**

Run:

```bash
uv run pytest tests/test_finland_ytj_assets.py tests/test_finland_xbrl_assets.py::test_xbrl_asset_graph_models_eligible_companies_downstream_of_ytj_duckdb -v
```

Expected: tests fail because the dlt DuckDB asset and graph dependency do not exist yet.

- [x] **Step 3: Add dlt dependency**

Add `dlt[duckdb]` to project dependencies.

- [x] **Step 4: Implement dlt DuckDB asset**

Create `finland_ytj_all_companies_duckdb`, load `base/base.json`, flatten the minimal top-level fields needed for XBRL eligibility, and run dlt with DuckDB destination and replace disposition.

- [x] **Step 5: Rewire XBRL eligibility**

Remove parquet dependencies from `finland_xbrl_eligible_companies`; depend on `finland_ytj_all_companies_duckdb` and query the local DuckDB table.

- [x] **Step 6: Remove normalized parquet asset wiring**

Remove the normalized parquet multi-asset from active Dagster definitions and update README text.

- [x] **Step 7: Verify**

Run:

```bash
uv run pytest -v
uv run dg check defs
```

Expected: tests pass and Dagster definitions load.
