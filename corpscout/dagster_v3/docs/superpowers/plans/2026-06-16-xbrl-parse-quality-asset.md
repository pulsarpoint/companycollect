# XBRL Parse Quality Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `fi_prh_xbrl_parse_quality` Dagster asset that summarizes whether the parsed XBRL layer is trustworthy before financial metric mapping.

**Architecture:** Add a one-row Parquet table under `parsed/fi_prh_xbrl_parse_quality.parquet`. The asset reads the XML document catalog, statement document table, and raw facts table from S3, computes count and quality metrics with Polars, writes the summary Parquet table, and exposes key counts as Dagster metadata.

**Tech Stack:** Dagster `@asset`, `MaterializeResult`, Polars Parquet, pytest, `dg check defs`.

---

### Task 1: Add Parse Quality Table Contract

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/tables.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py`
- Modify: `tests/test_finland_xbrl_assets.py`
- Modify: `tests/test_finland_xbrl_parsed_assets.py`
- Modify: `README.md`

- [x] **Step 1: Write failing tests**

Add a pure-function test that builds tiny XML, statement, and fact DataFrames and expects one parse-quality row. Add a graph test that `fi_prh_xbrl_parse_quality` depends on `fi_prh_xbrl_xml_documents`, `fi_prh_xbrl_statement_documents`, and `fi_prh_xbrl_facts_raw`.

- [x] **Step 2: Run focused tests**

Run: `uv run pytest tests/test_finland_xbrl_assets.py::test_xbrl_asset_graph_models_parse_quality_downstream_of_parsed_tables tests/test_finland_xbrl_parsed_assets.py::test_parse_quality_summary_reports_counts_and_common_concepts -v`

Expected: tests fail because the table and functions do not exist yet.

- [x] **Step 3: Implement table contract and summary function**

Add `PARSE_QUALITY_TABLE = "fi_prh_xbrl_parse_quality"` and columns for counts, top namespaces/concepts, warning counts, duplicate statement keys, missing IDs/dates, facts-per-statement statistics, and generation timestamp.

- [x] **Step 4: Add Dagster asset**

Add `finland_xbrl_parse_quality` as a regular asset that reads the upstream Parquet tables, writes `parsed/fi_prh_xbrl_parse_quality.parquet`, and returns metadata.

- [x] **Step 5: Update docs**

Extend the README graph and add the materialization command for the quality asset.

- [x] **Step 6: Verify**

Run:

```bash
uv run pytest -v
uv run dg check defs
uv run dg launch --assets fi_prh_xbrl_parse_quality
```

Expected: tests pass, definitions load, and the quality asset materializes locally.
