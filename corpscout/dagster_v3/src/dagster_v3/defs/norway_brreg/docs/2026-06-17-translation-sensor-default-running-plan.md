# Norway BRREG Translation Sensor Default Running Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Norway BRREG translation completion sensor load as running by default on fresh Dagster instances.

**Architecture:** Keep the current Temporal polling sensor and completion job. Add `default_status=dg.DefaultSensorStatus.RUNNING` to the sensor decorator, and pin that behavior with a focused test.

**Tech Stack:** Dagster sensors, pytest.

---

### Task 1: Default Translation Completion Sensor To Running

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/sensors.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Write the failing test**

Add this assertion to `test_norway_brreg_translation_sensor_is_defined_in_sensors_module`:

```python
assert brreg_sensors.norway_brreg_translation_completion_sensor.default_status == (
    dg.DefaultSensorStatus.RUNNING
)
```

- [ ] **Step 2: Verify it fails**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_brreg_translation_sensor_is_defined_in_sensors_module -q
```

Expected: FAIL because the sensor default is currently `STOPPED`.

- [ ] **Step 3: Implement**

Update the sensor decorator:

```python
@dg.sensor(
    job=norway_brreg_translation_completion_job,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
```

- [ ] **Step 4: Verify**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
uv run dg check defs
uv run pytest -q
```

Expected: all pass.

**Operational note:** Dagster stores sensor enabled/disabled state in the instance database. This change affects new/fresh sensor state. If this sensor was already loaded as stopped, the UI may still require manually toggling it on or resetting the sensor state.
