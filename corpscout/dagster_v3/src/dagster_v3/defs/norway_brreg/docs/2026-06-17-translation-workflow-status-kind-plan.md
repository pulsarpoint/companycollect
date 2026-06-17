# Norway Brreg Translation Workflow Status Kind Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `norway_brreg_translation_workflow_status` expose Temporal as an asset kind in Dagster UI.

**Architecture:** `@observable_source_asset` and `SourceAsset` do not accept a real `kinds` parameter in this Dagster version. Regular Dagster asset kinds are stored as tags named `dagster/kind/<kind>`, so add `dagster/kind/temporal` directly to the source asset tags.

**Tech Stack:** Dagster source asset tags, pytest, dg definitions validation.

---

## Tasks

### Task 1: Add Temporal Kind Tag

- [ ] In `src/dagster_v3/defs/norway_brreg/assets.py`, add:

```python
"dagster/kind/temporal": "",
```

to `norway_brreg_translation_workflow_status` tags.

### Task 2: Verify Kind Metadata

- [ ] In `tests/test_norway_brreg_assets.py`, assert:

```python
assert workflow_status_node.tags["dagster/kind/temporal"] == ""
assert "temporal" in workflow_status_node.to_asset_spec().kinds
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

## Self-Review

- The source asset remains observable.
- Temporal appears as a Dagster kind through Dagster's own kind tag convention.
- Existing `temporal=true` filtering tag remains.
