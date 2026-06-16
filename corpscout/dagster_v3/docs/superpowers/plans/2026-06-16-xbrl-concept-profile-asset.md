# XBRL Concept Profile Asset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `fi_prh_xbrl_concept_profile`, a downstream Dagster asset that profiles raw XBRL fact concepts before financial metric mapping.

**Architecture:** Read `parsed/fi_prh_xbrl_facts_raw.parquet`, group by XBRL concept, and write `parsed/fi_prh_xbrl_concept_profile.parquet`. The asset stays descriptive: counts, value-kind usage, current/comparative usage, entity/date coverage, and example values.

**Tech Stack:** Dagster `@asset`, Polars Parquet, pytest, `dg check defs`.

---

### Task 1: Add Concept Profile Asset

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/tables.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets.py`
- Modify: `tests/test_finland_xbrl_assets.py`
- Modify: `tests/test_finland_xbrl_parsed_assets.py`
- Modify: `README.md`

- [x] **Step 1: Write failing tests**

Add a graph test proving `fi_prh_xbrl_concept_profile` depends on `fi_prh_xbrl_facts_raw`. Add a pure-function test for concept grouping, counts, current/comparative counts, and example values.

- [x] **Step 2: Run focused tests**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_xbrl_asset_graph_models_concept_profile_downstream_of_facts tests/test_finland_xbrl_parsed_assets.py::test_concept_profile_groups_facts_by_concept -v
```

Expected: tests fail because the table constant and builder function do not exist.

- [x] **Step 3: Implement table contract and profile builder**

Add `CONCEPT_PROFILE_TABLE = "fi_prh_xbrl_concept_profile"` and a `build_concept_profile_rows` helper.

- [x] **Step 4: Add Dagster asset**

Add `finland_xbrl_concept_profile`, depending on `fi_prh_xbrl_facts_raw`, that reads facts, writes the concept profile table, and returns metadata.

- [x] **Step 5: Update docs**

Add the concept profile asset to the README graph and document the launch command.

- [x] **Step 6: Verify**

Run:

```bash
uv run pytest -v
uv run dg check defs
uv run dg launch --assets fi_prh_xbrl_concept_profile
```

Expected: tests pass, definitions load, and the concept profile materializes locally.
