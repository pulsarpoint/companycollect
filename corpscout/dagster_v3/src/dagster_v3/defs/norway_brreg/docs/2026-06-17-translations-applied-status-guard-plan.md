# Norway BRREG Translations Applied Status Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent manual `norway_brreg_translations_applied` materializations from applying queue results before the Temporal translation workflow is completed.

**Architecture:** Keep `norway_brreg_translations_applied` as a regular Dagster asset. Add a workflow-status guard before queue processing. Move the Temporal workflow status lookup into `assets.py` because both the asset and `sensors.py` need it; keep the sensor definition and sensor decision helper in `sensors.py`.

**Tech Stack:** Dagster assets, Dagster sensor helper, Temporal Python client, pytest.

---

### Task 1: Guard Translation Application On Workflow Status

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/sensors.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Write failing tests**

Add tests that monkeypatch `describe_norway_brreg_translation_workflow`:

```python
def test_norway_translations_applied_skips_when_workflow_is_running(monkeypatch):
    monkeypatch.setattr(brreg_assets, "describe_norway_brreg_translation_workflow", lambda: {... "workflow_status": "RUNNING"})
    monkeypatch.setattr(brreg_assets, "apply_norway_brreg_translation_queue_results", fail_if_called)
    result = brreg_assets.norway_brreg_translations_applied(dg.build_asset_context())
    assert result.metadata["applied"].value is False
    assert result.metadata["workflow_status"].value == "RUNNING"
```

Also add a test that the asset applies queue results when status is `COMPLETED`.

- [ ] **Step 2: Verify failing tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_translations_applied_skips_when_workflow_is_running tests/test_norway_brreg_assets.py::test_norway_translations_applied_applies_when_workflow_is_completed -q
```

Expected: FAIL because the asset currently applies queue results unconditionally.

- [ ] **Step 3: Move shared workflow status lookup**

Move `describe_norway_brreg_translation_workflow`, `_describe_norway_brreg_translation_workflow`, and `_workflow_status_name` from `sensors.py` to `assets.py`.

Keep `sensors.py` importing `describe_norway_brreg_translation_workflow` from `assets.py`.

- [ ] **Step 4: Add the asset guard**

At the start of `norway_brreg_translations_applied`, call `describe_norway_brreg_translation_workflow()`.

If status is not `COMPLETED`, return `MaterializeResult` metadata:

```python
{
    "applied": False,
    "workflow_id": workflow["workflow_id"],
    "workflow_run_id": workflow["workflow_run_id"],
    "workflow_status": workflow["workflow_status"],
}
```

Do not call `apply_norway_brreg_translation_queue_results`.

If the workflow lookup fails, return metadata with `workflow_status="unavailable"` and do not apply queue results.

If status is `COMPLETED`, apply queue results and include `applied=True` plus workflow metadata.

- [ ] **Step 5: Verify**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
uv run dg check defs
uv run pytest -q
```

Expected: all pass.
