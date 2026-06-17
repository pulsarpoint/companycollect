# Norway Brreg Translation Workflow Status Tags Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `norway_brreg_translation_workflow_status` show a direct Temporal tag in Dagster UI.

**Architecture:** Keep the observable source asset model. Add a direct `temporal=true` asset tag alongside the existing `system=temporal` and `source_slug` tags because this Dagster version does not expose `kinds` on observable source assets.

**Tech Stack:** Dagster observable source asset tags, pytest, dg definitions validation.

---

## Tasks

### Task 1: Add Temporal Tag

- [ ] In `src/dagster_v3/defs/norway_brreg/assets.py`, update the observable source asset:

```python
tags={
    "system": "temporal",
    "temporal": "true",
    "source_slug": NORWAY_BRREG_TRANSLATION_SOURCE_SLUG,
}
```

### Task 2: Add Regression Assertion

- [ ] In `tests/test_norway_brreg_assets.py`, assert the source asset has `temporal=true`.

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

- The status asset remains an observable source asset.
- The UI can show/filter a direct `temporal` tag.
- No runtime behavior changes.
