# XBRL XML Documents Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Model `fi_prh_xbrl_xml_documents` as an explicit Dagster asset instead of only a side-effect Parquet file.

**Architecture:** Keep the raw XBRL downloader as a normal asset so existing raw-only materialization remains simple. Add a lightweight `fi_prh_xbrl_xml_documents` asset that validates/counts the Parquet catalog written by the downloader, and make parsed statement/fact assets depend on that catalog asset.

**Tech Stack:** Dagster `@asset`, `AssetSpec`, `MaterializeResult`, Polars Parquet, pytest, `dg check defs`.

---

### Task 1: Add Explicit XML Document Catalog Asset

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py`
- Modify: `tests/test_finland_xbrl_assets.py`
- Modify: `README.md`

- [x] **Step 1: Write failing tests**

Add tests that assert `finland_xbrl_raw_xml_documents` and `fi_prh_xbrl_xml_documents` are both executable Dagster assets, and that parsed XBRL assets depend on `fi_prh_xbrl_xml_documents`.

- [x] **Step 2: Run focused tests**

Run: `uv run pytest tests/test_finland_xbrl_assets.py -v`

Expected: tests fail because the XML catalog is not yet modeled as an asset.

- [x] **Step 3: Add catalog asset**

Keep `finland_xbrl_raw_xml_documents` as `@dg.asset`. Add `@dg.asset(name="fi_prh_xbrl_xml_documents", deps=[finland_xbrl_raw_xml_documents])` that reads `raw/fi_prh_xbrl_xml_documents.parquet` and returns metadata with the object key and row count.

- [x] **Step 4: Update parsed asset dependencies**

Change parsed statement/fact `AssetSpec` dependencies from `finland_xbrl_raw_xml_documents` to `fi_prh_xbrl_xml_documents`.

- [x] **Step 5: Update docs**

Document the graph as:

```text
finland_xbrl_raw_xml_documents
  -> fi_prh_xbrl_xml_documents
  -> fi_prh_xbrl_statement_documents
  -> fi_prh_xbrl_facts_raw
```

- [x] **Step 6: Verify**

Run:

```bash
uv run pytest -v
uv run dg check defs
```

Expected: all tests pass and Dagster definitions load.
