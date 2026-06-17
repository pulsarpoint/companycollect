# Norway Brreg Translation Workflow UI State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show Temporal translation workflow state in the Dagster asset graph without adding a fake manually materializable completion asset.

**Architecture:** Add one observable source asset named `norway_brreg_translation_workflow_status` to represent Temporal workflow state. Keep the existing completion sensor as the polling and trigger boundary; each tick emits an `AssetObservation` for that source asset and triggers `norway_brreg_translations_applied` only when Temporal reports `COMPLETED`. Make `norway_brreg_translations_applied` depend on both the queue and the observable status asset so the UI shows the external wait point.

**Tech Stack:** Dagster assets, Dagster observable source assets, Dagster sensors, Temporal client, pytest.

---

## File Structure

- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Add `NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY`.
  - Add `norway_brreg_translation_workflow_status` as an observable source asset.
  - Add a direct metadata helper for workflow status observations.
  - Add the status asset as a dependency of `norway_brreg_translations_applied`.
- Modify `src/dagster_v3/defs/norway_brreg/sensors.py`
  - Emit `AssetObservation` on every sensor tick when Temporal status can be read.
  - Emit an unavailable observation when Temporal cannot be reached.
  - Keep the existing completion trigger/cursor behavior.
- Modify `src/dagster_v3/defs/norway_brreg/definitions.py`
  - Register the observable source asset in the definitions asset list.
- Modify `tests/test_norway_brreg_assets.py`
  - Update graph registration expectations.
  - Add sensor tests for RUNNING, missing, and COMPLETED observations.
  - Add a direct observable source asset smoke test.

## Tasks

### Task 1: Add the Observable Status Asset

- [ ] Add a constant in `assets.py`:

```python
NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY = dg.AssetKey(
    "norway_brreg_translation_workflow_status"
)
```

- [ ] Add a direct metadata builder in `assets.py`:

```python
def norway_brreg_translation_workflow_status_metadata(
    workflow: dict[str, str] | None = None,
    *,
    error: str = "",
) -> dict[str, Any]:
    if workflow is None:
        return {
            "workflow_id": NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
            "workflow_status": "unavailable",
            "workflow_available": False,
            "workflow_error": error,
        }
    return {
        "workflow_id": workflow["workflow_id"],
        "workflow_run_id": workflow["workflow_run_id"],
        "workflow_status": workflow["workflow_status"],
        "workflow_available": True,
        "workflow_complete": workflow["workflow_status"] == "COMPLETED",
    }
```

- [ ] Add the observable source asset in `assets.py`:

```python
@dg.observable_source_asset(
    key=NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
    group_name=GROUP_NAME,
    description="Observed Temporal status for the serialized Norway Brreg translation workflow.",
    tags={"system": "temporal", "source_slug": NORWAY_BRREG_TRANSLATION_SOURCE_SLUG},
)
def norway_brreg_translation_workflow_status() -> dg.ObserveResult:
    try:
        workflow = describe_norway_brreg_translation_workflow()
    except Exception as exc:
        return dg.ObserveResult(
            metadata=norway_brreg_translation_workflow_status_metadata(error=str(exc))
        )
    return dg.ObserveResult(
        metadata=norway_brreg_translation_workflow_status_metadata(workflow)
    )
```

- [ ] Update `norway_brreg_translations_applied` dependencies:

```python
deps=[
    dg.AssetKey("norway_brreg_translation_queue"),
    NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
]
```

### Task 2: Emit Status Observations From the Sensor

- [ ] Import the status asset key and metadata helper into `sensors.py`.

- [ ] In the exception branch, return a `SensorResult` with an `AssetObservation`:

```python
metadata = norway_brreg_translation_workflow_status_metadata(error=str(exc))
return dg.SensorResult(
    skip_reason=f"Norway Brreg translation workflow status not available: {exc}",
    cursor=context.cursor,
    asset_events=[
        dg.AssetObservation(
            asset_key=NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
            metadata=metadata,
        )
    ],
)
```

- [ ] After a successful Temporal status read, build the observation once:

```python
status_observation = dg.AssetObservation(
    asset_key=NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
    metadata=norway_brreg_translation_workflow_status_metadata(workflow),
)
```

- [ ] Include `asset_events=[status_observation]` in the RUNNING, duplicate COMPLETED, and first COMPLETED `SensorResult` paths.

### Task 3: Register the Observable Source Asset

- [ ] Import `norway_brreg_translation_workflow_status` in `definitions.py`.

- [ ] Add it to the `assets=[...]` list so Dagster shows it in the graph:

```python
assets=[
    norway_brreg_entities_duckdb_asset,
    norway_brreg_financial_fetches_duckdb_asset,
    norway_brreg_financial_statements_duckdb_asset,
    norway_brreg_translation_queue,
    norway_brreg_translation_workflow_status,
    norway_brreg_translations_applied,
    norway_brreg_clickhouse_tables,
]
```

### Task 4: Update Tests

- [ ] Update `test_norway_entity_asset_is_registered`:

```python
assert "norway_brreg_translation_workflow_status" in asset_names
assert {key.path[-1] for key in applied_node.parent_keys} == {
    "norway_brreg_translation_queue",
    "norway_brreg_translation_workflow_status",
}
```

- [ ] In `test_norway_translation_completion_sensor_skips_running_workflow`, assert one observation:

```python
assert len(result.asset_events) == 1
assert result.asset_events[0].asset_key == brreg_assets.NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY
assert result.asset_events[0].metadata["workflow_status"].value == "RUNNING"
```

- [ ] In `test_norway_translation_completion_sensor_skips_missing_workflow`, assert unavailable metadata:

```python
assert len(result.asset_events) == 1
assert result.asset_events[0].metadata["workflow_status"].value == "unavailable"
assert result.asset_events[0].metadata["workflow_available"].value is False
```

- [ ] In `test_norway_translation_completion_sensor_launches_apply_once`, assert the first and duplicate completed paths both emit status observations:

```python
assert len(result.asset_events) == 1
assert result.asset_events[0].metadata["workflow_status"].value == "COMPLETED"
assert len(duplicate.asset_events) == 1
assert duplicate.asset_events[0].metadata["workflow_status"].value == "COMPLETED"
```

- [ ] Add a direct observable source asset test:

```python
def test_norway_translation_workflow_status_observe_result(monkeypatch) -> None:
    monkeypatch.setattr(
        brreg_assets,
        "describe_norway_brreg_translation_workflow",
        lambda: {
            "workflow_id": "translation-norway-brreg",
            "workflow_run_id": "temporal-run-123",
            "workflow_status": "RUNNING",
        },
    )

    result = brreg_assets.norway_brreg_translation_workflow_status()

    assert result.metadata["workflow_status"] == "RUNNING"
    assert result.metadata["workflow_complete"] is False
```

### Task 5: Verify

- [ ] Run focused tests:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
```

- [ ] Run project definition validation:

```bash
uv run dg check defs
```

- [ ] Run full tests if the focused tests or definition validation reveal import interactions:

```bash
uv run pytest -q
```

## Self-Review

- The plan does not add a fake materializable `workflow_completed` asset.
- The sensor remains the only active trigger for `norway_brreg_translations_applied`.
- The UI gets a visible external-state asset and repeated status observations.
- Runtime behavior stays guarded in `norway_brreg_translations_applied`.
- No new config wrapper or pass-through abstraction is introduced.
