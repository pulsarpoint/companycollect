# Norway BRREG Entity Progress Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit progress logs while downloading and streaming the BRREG entity bulk source.

**Architecture:** Keep logging inside `resources.py`, where entity downloading and streaming happen. Add default progress interval constants and optional log callables for focused tests while defaulting production runs to the module logger.

**Tech Stack:** Python logging, dlt resource/source functions, pytest.

---

### Task 1: Add Entity Download And Stream Progress Logs

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/resources.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Write the failing test**

Add tests that:

- stream 2001 fake entity records through `iter_brreg_entity_rows(..., log=capture_log)` and assert progress messages are emitted at 1000 and 2000 rows.
- download a fake response through `_download_bytes(..., log=capture_log, progress_every_bytes=...)` and assert progress messages are emitted as byte thresholds are crossed.

- [ ] **Step 2: Verify the test fails**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_iter_brreg_entity_rows_logs_every_1000_rows -q
```

Expected: FAIL because `iter_brreg_entity_rows` and `_download_bytes` do not accept progress logging parameters.

- [ ] **Step 3: Implement logging**

In `resources.py`, add:

```python
ENTITY_PROGRESS_LOG_EVERY_ROWS = 1000
DOWNLOAD_PROGRESS_LOG_EVERY_BYTES = 100 * 1024 * 1024
LOGGER = logging.getLogger(__name__)
```

Add `log`, `progress_every_rows`, and `download_progress_every_bytes` parameters to `norway_brreg_entities_source`, `_entities_resource`, and `iter_brreg_entity_rows`.

Use `stream=True` in `_download_bytes` and iterate response chunks when available. Emit:

```python
progress_log(
    "Downloaded Norway Brreg entity archive: downloaded_mb=%.1f",
    downloaded_bytes / 1024 / 1024,
)
```

In the entity loop, emit:

```python
if progress_every_rows > 0 and line_number % progress_every_rows == 0:
    progress_log("Processed Norway Brreg entity rows: rows=%s", line_number)
```

- [ ] **Step 4: Verify**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
uv run pytest -q
```

Expected: all pass.
