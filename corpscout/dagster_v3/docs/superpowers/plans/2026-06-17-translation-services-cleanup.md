# Translation Services Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clean up translation workflow configuration so Dagster owns operator defaults, Temporal runtime inputs are explicit/default-free, failed-batch handling does not stop when `max_batch_failures=0`, and simple queue-claim logic is readable without needless helper indirection.

**Architecture:** Use a Norway-specific Dagster config class on the `norway_brreg_translation_queue` asset for operator-tunable values. Build `TranslationQueueWorkflowInput` explicitly at the asset boundary and pass it to a direct `start_translation_workflow(workflow_id=..., params=...)` function. Keep queue claiming direct: select pending rows first, then retryable rows only when no pending rows remain. Temporal activity execution policy values are also runtime inputs so Dagster operators can tune timeouts and retry attempts without code edits.

**Tech Stack:** Python 3.14, Dagster, Temporal Python SDK, DuckDB, pytest.

---

## File Structure

- Modify `src/dagster_v3/temporal/translations/queue.py`
  - Remove default values from `TranslationQueueWorkflowInput`.
  - Remove default values from `ProcessTranslationBatchInput`.
  - Change workflow failed-batch stopping rule so `max_batch_failures=0` disables the limit.
  - Use explicit workflow input fields for initialize timeout, batch timeout buffer, summarize timeout, and activity retry attempts.
- Modify `src/dagster_v3/defs/translations/assets.py`
  - Remove `TranslationTemporalStartConfig`.
  - Change `start_translation_workflow` to accept explicit `workflow_id`, `params`, and optional `temporal_address`.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Add `NorwayBrregTranslationConfig`.
  - Remove `norway_brreg_translation_start_config()`.
  - Build `TranslationQueueWorkflowInput` directly in `norway_brreg_translation_queue`.
  - Expose Temporal activity execution policy values on `NorwayBrregTranslationConfig`.
- Modify `src/dagster_v3/translations/queue.py`
  - Inline queue claim SQL instead of `_claimable_rows(...)`.
  - Preserve pending-before-retryable behavior.
- Modify `src/dagster_v3/translations/queue_smoke.py`
  - Match workflow failed-batch stopping rule.
- Modify tests:
  - `tests/test_translation_temporal_queue.py`
  - `tests/test_translation_dagster_assets.py`
  - `tests/test_translation_temporal_queue_cli.py`
  - `tests/test_translation_queue.py`
  - `tests/test_translation_queue_smoke.py`
  - `tests/test_norway_brreg_assets.py`

---

### Task 1: Make Temporal Runtime Inputs Explicit

**Files:**
- Modify: `src/dagster_v3/temporal/translations/queue.py`
- Test: `tests/test_translation_temporal_queue.py`
- Test: `tests/test_translation_temporal_queue_cli.py`
- Test: `tests/test_translation_dagster_assets.py`

- [ ] **Step 1: Write failing tests for no workflow input defaults**

In `tests/test_translation_temporal_queue.py`, replace `test_translation_queue_workflow_input_defaults_are_serialized` with:

```python
def test_translation_queue_workflow_input_requires_explicit_runtime_values() -> None:
    field_defaults = {
        field.name: field.default
        for field in dataclasses.fields(TranslationQueueWorkflowInput)
    }

    assert set(field_defaults) == {
        "duckdb_path",
        "item_count",
        "batch_size",
        "timeout_seconds",
        "max_batch_failures",
        "worker_id",
        "max_tokens",
        "extra_body_json",
    }
    assert all(default is dataclasses.MISSING for default in field_defaults.values())

    with pytest.raises(TypeError):
        TranslationQueueWorkflowInput(
            duckdb_path="/tmp/translations.duckdb",
            item_count=2000,
        )

    params = TranslationQueueWorkflowInput(
        duckdb_path="/tmp/translations.duckdb",
        item_count=2000,
        batch_size=50,
        timeout_seconds=120,
        max_batch_failures=0,
        worker_id="translation-temporal-worker",
        max_tokens=4096,
        extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}',
    )

    assert asdict(params) == {
        "duckdb_path": "/tmp/translations.duckdb",
        "item_count": 2000,
        "batch_size": 50,
        "timeout_seconds": 120,
        "max_batch_failures": 0,
        "worker_id": "translation-temporal-worker",
        "max_tokens": 4096,
        "extra_body_json": '{"chat_template_kwargs":{"enable_thinking":false}}',
    }
    assert LOCAL_LLM_TRANSLATION_TASK_QUEUE == "translation-local-llm"
```

Add imports at the top of the same file:

```python
import dataclasses
from dataclasses import asdict

import pytest
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py::test_translation_queue_workflow_input_requires_explicit_runtime_values -q
```

Expected: FAIL because `TranslationQueueWorkflowInput` still has defaults.

- [ ] **Step 3: Remove defaults from runtime dataclasses**

Change `TranslationQueueWorkflowInput` in `src/dagster_v3/temporal/translations/queue.py` to:

```python
@dataclass(frozen=True)
class TranslationQueueWorkflowInput:
    duckdb_path: str
    item_count: int
    batch_size: int
    timeout_seconds: int
    max_batch_failures: int
    worker_id: str
    max_tokens: int
    extra_body_json: str
```

Change `ProcessTranslationBatchInput` to:

```python
@dataclass(frozen=True)
class ProcessTranslationBatchInput:
    duckdb_path: str
    batch_size: int
    timeout_seconds: int
    worker_id: str
    max_tokens: int
    extra_body_json: str
```

- [ ] **Step 4: Update explicit test constructors**

Every `TranslationQueueWorkflowInput(...)` in tests must include:

```python
extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}'
```

Every `ProcessTranslationBatchInput(...)` in tests must include:

```python
max_tokens=4096,
extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}',
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py tests/test_translation_temporal_queue_cli.py tests/test_translation_dagster_assets.py -q
```

Expected: PASS.

---

### Task 1A: Move Temporal Activity Execution Policy Into Dagster Config

**Files:**
- Modify: `src/dagster_v3/temporal/translations/queue.py`
- Modify: `src/dagster_v3/temporal/translations/queue_cli.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Test: `tests/test_translation_temporal_queue.py`
- Test: `tests/test_translation_temporal_queue_cli.py`
- Test: `tests/test_norway_brreg_assets.py`
- Test: `tests/test_translation_dagster_assets.py`

- [x] **Step 1: Write failing tests for explicit activity policy inputs**

In `tests/test_translation_temporal_queue.py`, extend `test_translation_queue_workflow_input_requires_explicit_runtime_values` so `field_defaults` includes:

```python
assert set(field_defaults) == {
    "duckdb_path",
    "item_count",
    "batch_size",
    "timeout_seconds",
    "max_batch_failures",
    "worker_id",
    "max_tokens",
    "extra_body_json",
    "initialize_timeout_seconds",
    "batch_timeout_buffer_seconds",
    "summarize_timeout_seconds",
    "activity_maximum_attempts",
}
```

Construct `TranslationQueueWorkflowInput` with:

```python
initialize_timeout_seconds=300,
batch_timeout_buffer_seconds=30,
summarize_timeout_seconds=30,
activity_maximum_attempts=1,
```

and assert `asdict(params)` contains the same four keys and values.

In `tests/test_norway_brreg_assets.py`, extend the Norway config default test with:

```python
assert config.initialize_timeout_seconds == 300
assert config.batch_timeout_buffer_seconds == 30
assert config.summarize_timeout_seconds == 30
assert config.activity_maximum_attempts == 1
```

In `tests/test_translation_temporal_queue_cli.py`, extend the expected parsed `TranslationQueueWorkflowInput` with the same values.

- [x] **Step 2: Run focused tests after implementation**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py::test_translation_queue_workflow_input_requires_explicit_runtime_values tests/test_norway_brreg_assets.py::test_norway_translation_config_exposes_operator_tunable_defaults tests/test_translation_temporal_queue_cli.py -q
```

Expected after implementation: PASS with the workflow input and config exposing those fields.

- [x] **Step 3: Add explicit workflow input fields**

In `src/dagster_v3/temporal/translations/queue.py`, change `TranslationQueueWorkflowInput` to:

```python
@dataclass(frozen=True)
class TranslationQueueWorkflowInput:
    duckdb_path: str
    item_count: int
    batch_size: int
    timeout_seconds: int
    max_batch_failures: int
    worker_id: str
    max_tokens: int
    extra_body_json: str
    initialize_timeout_seconds: int
    batch_timeout_buffer_seconds: int
    summarize_timeout_seconds: int
    activity_maximum_attempts: int
```

Update workflow activity calls to use the fields:

```python
start_to_close_timeout=timedelta(seconds=params.initialize_timeout_seconds),
retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
```

```python
start_to_close_timeout=timedelta(
    seconds=params.timeout_seconds + params.batch_timeout_buffer_seconds
),
retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
```

```python
start_to_close_timeout=timedelta(seconds=params.summarize_timeout_seconds),
retry_policy=RetryPolicy(maximum_attempts=params.activity_maximum_attempts),
```

- [x] **Step 4: Expose the fields at user-facing launch boundaries**

In `src/dagster_v3/defs/norway_brreg/assets.py`, add the fields to `NorwayBrregTranslationConfig`:

```python
initialize_timeout_seconds: int = 300
batch_timeout_buffer_seconds: int = 30
summarize_timeout_seconds: int = 30
activity_maximum_attempts: int = 1
```

Then pass them when constructing `TranslationQueueWorkflowInput`.

In `src/dagster_v3/temporal/translations/queue_cli.py`, add CLI arguments:

```python
parser.add_argument("--initialize-timeout-seconds", default=300, type=int)
parser.add_argument("--batch-timeout-buffer-seconds", default=30, type=int)
parser.add_argument("--summarize-timeout-seconds", default=30, type=int)
parser.add_argument("--activity-maximum-attempts", default=1, type=int)
```

Then pass them into `TranslationQueueWorkflowInput`.

- [x] **Step 5: Update remaining explicit constructors**

Every test or helper that constructs `TranslationQueueWorkflowInput` must pass:

```python
initialize_timeout_seconds=300,
batch_timeout_buffer_seconds=30,
summarize_timeout_seconds=30,
activity_maximum_attempts=1,
```

Do not add defaults to `TranslationQueueWorkflowInput`.

- [x] **Step 6: Run focused validation**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py tests/test_translation_temporal_queue_cli.py tests/test_translation_dagster_assets.py tests/test_norway_brreg_assets.py -q
uv run dg check defs
```

Expected: PASS.

---

### Task 2: Make `max_batch_failures=0` Mean Unlimited

**Files:**
- Modify: `src/dagster_v3/temporal/translations/queue.py`
- Modify: `src/dagster_v3/translations/queue_smoke.py`
- Test: `tests/test_translation_queue_smoke.py`

- [ ] **Step 1: Write failing smoke-loop test**

Add this test to `tests/test_translation_queue_smoke.py`:

```python
def test_run_translation_queue_smoke_zero_max_batch_failures_continues_after_failure(
    tmp_path,
) -> None:
    provider = _FailOnceProvider()

    metrics = run_translation_queue_smoke(
        duckdb_path=tmp_path / "queue.duckdb",
        item_count=60,
        batch_size=50,
        timeout_seconds=30,
        provider=provider,
        worker_id="worker-a",
        max_batch_failures=0,
    )

    assert metrics.total_items == 60
    assert metrics.completed_items == 60
    assert metrics.failed_batches == 1
    assert metrics.provider_failure_count == 1
    assert metrics.successful_batches == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_translation_queue_smoke.py::test_run_translation_queue_smoke_zero_max_batch_failures_continues_after_failure -q
```

Expected: FAIL because `max_batch_failures=0` currently stops after the first failed batch.

- [ ] **Step 3: Change stopping conditions**

In `src/dagster_v3/temporal/translations/queue.py`, change:

```python
if provider_failure_count > params.max_batch_failures:
    break
```

to:

```python
if params.max_batch_failures > 0 and provider_failure_count > params.max_batch_failures:
    break
```

In `src/dagster_v3/translations/queue_smoke.py`, change:

```python
if provider_failure_count > max_batch_failures:
    break
```

to:

```python
if max_batch_failures > 0 and provider_failure_count > max_batch_failures:
    break
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_translation_queue_smoke.py tests/test_translation_temporal_queue.py -q
```

Expected: PASS.

---

### Task 3: Replace Config-Returning Helper With Dagster Asset Config

**Files:**
- Modify: `src/dagster_v3/defs/translations/assets.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Test: `tests/test_translation_dagster_assets.py`
- Test: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Write tests for direct Dagster config model**

In `tests/test_norway_brreg_assets.py`, replace `test_norway_translation_temporal_configs_use_country_queue_defaults` with:

```python
def test_norway_translation_config_exposes_operator_tunable_defaults() -> None:
    config = brreg_assets.NorwayBrregTranslationConfig()

    assert config.batch_size == 50
    assert config.timeout_seconds == 120
    assert config.max_batch_failures == 0
    assert config.worker_id == "translation-temporal-worker"
    assert config.max_tokens == 4096
    assert config.extra_body_json == '{"chat_template_kwargs":{"enable_thinking":false}}'
    assert config.temporal_address == ""
    assert "norway_brreg_translation_start_config" not in brreg_assets.__dict__
```

In `tests/test_translation_dagster_assets.py`, update `test_start_translation_workflow_uses_generic_temporal_queue` to construct params directly:

```python
params = TranslationQueueWorkflowInput(
    duckdb_path="/tmp/translations.duckdb",
    item_count=2000,
    batch_size=50,
    timeout_seconds=120,
    max_batch_failures=0,
    worker_id="translation-temporal-worker",
    max_tokens=4096,
    extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}',
)

result = start_translation_workflow(
    workflow_id="translation-smoke-source-fixed",
    params=params,
    client=client,
)
```

The expected params assertion should be:

```python
assert client.started[0]["params"] == params
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_translation_config_exposes_operator_tunable_defaults tests/test_translation_dagster_assets.py::test_start_translation_workflow_uses_generic_temporal_queue -q
```

Expected: FAIL because `NorwayBrregTranslationConfig` does not exist and `start_translation_workflow` still expects `TranslationTemporalStartConfig`.

- [ ] **Step 3: Change generic starter API**

In `src/dagster_v3/defs/translations/assets.py`, remove `TranslationTemporalStartConfig` and change `start_translation_workflow` to:

```python
def start_translation_workflow(
    *,
    workflow_id: str,
    params: TranslationQueueWorkflowInput,
    temporal_address: str = "",
    client: TemporalClient | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        _start_translation_workflow(
            workflow_id=workflow_id,
            params=params,
            temporal_address=temporal_address,
            client=client,
        )
    )
```

Change `_start_translation_workflow` to:

```python
async def _start_translation_workflow(
    *,
    workflow_id: str,
    params: TranslationQueueWorkflowInput,
    temporal_address: str,
    client: TemporalClient | None = None,
) -> dict[str, Any]:
    active_client = client or await Client.connect(_temporal_address(temporal_address))
    handle = await active_client.start_workflow(
        TranslationQueueWorkflow.run,
        params,
        id=workflow_id,
        task_queue=LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return {
        "workflow_id": handle.id,
        "run_id": handle.result_run_id,
        "task_queue": LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        "input": asdict(params),
    }
```

- [ ] **Step 4: Add Norway Dagster config and remove helper**

In `src/dagster_v3/defs/norway_brreg/assets.py`, add:

```python
class NorwayBrregTranslationConfig(dg.Config):
    batch_size: int = 50
    timeout_seconds: int = 120
    max_batch_failures: int = 0
    worker_id: str = "translation-temporal-worker"
    max_tokens: int = 4096
    extra_body_json: str = '{"chat_template_kwargs":{"enable_thinking":false}}'
    temporal_address: str = ""
```

Change `norway_brreg_translation_queue` signature to:

```python
def norway_brreg_translation_queue(
    context: AssetExecutionContext,
    config: NorwayBrregTranslationConfig,
) -> dg.MaterializeResult:
```

Inside that asset, replace:

```python
workflow = start_translation_workflow(norway_brreg_translation_start_config())
```

with:

```python
workflow = start_translation_workflow(
    workflow_id=NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
    params=TranslationQueueWorkflowInput(
        duckdb_path=str(NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH),
        item_count=0,
        batch_size=config.batch_size,
        timeout_seconds=config.timeout_seconds,
        max_batch_failures=config.max_batch_failures,
        worker_id=config.worker_id,
        max_tokens=config.max_tokens,
        extra_body_json=config.extra_body_json,
    ),
    temporal_address=config.temporal_address,
)
```

Delete `norway_brreg_translation_start_config()`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_translation_config_exposes_operator_tunable_defaults tests/test_translation_dagster_assets.py::test_start_translation_workflow_uses_generic_temporal_queue -q
```

Expected: PASS.

---

### Task 4: Inline Queue Claim SQL

**Files:**
- Modify: `src/dagster_v3/translations/queue.py`
- Test: `tests/test_translation_queue.py`

- [ ] **Step 1: Keep existing behavior test**

Use the existing test:

```python
def test_queue_claims_pending_items_before_retryable_failures(tmp_path) -> None:
    ...
```

It must assert that failed retryable rows are not claimed before remaining pending rows.

- [ ] **Step 2: Remove `_claimable_rows` helper**

In `TranslationQueue.claim_batch`, replace helper calls with direct SQL:

```python
rows = conn.execute(
    """
    select item_id, source_field, source_text, target_language, attempt_count
    from translation_items
    where status = ?
    order by source_table, source_pk, source_field, item_id
    limit ?
    """,
    [QUEUE_STATUS_PENDING, limit],
).fetchall()
if not rows:
    rows = conn.execute(
        """
        select item_id, source_field, source_text, target_language, attempt_count
        from translation_items
        where status = ?
        order by source_table, source_pk, source_field, item_id
        limit ?
        """,
        [QUEUE_STATUS_FAILED_RETRYABLE, limit],
    ).fetchall()
```

Delete the `_claimable_rows` method.

- [ ] **Step 3: Run focused queue tests**

Run:

```bash
uv run pytest tests/test_translation_queue.py -q
```

Expected: PASS.

---

### Task 5: Full Verification

**Files:**
- Check all modified files.

- [ ] **Step 1: Run Dagster definition validation**

Run:

```bash
uv run dg check defs
```

Expected:

```text
All component YAML validated successfully.
All definitions loaded successfully.
```

- [ ] **Step 2: Run full test suite**

Run:

```bash
uv run pytest -q
```

Expected all tests pass. The exact count may differ from the current `172 passed, 1 skipped` if tests are added or removed.

- [ ] **Step 3: Confirm no runtime defaults remain**

Run:

```bash
uv run python - <<'PY'
import dataclasses
from dagster_v3.temporal.translations.queue import (
    ProcessTranslationBatchInput,
    TranslationQueueWorkflowInput,
)

for cls in (TranslationQueueWorkflowInput, ProcessTranslationBatchInput):
    print(cls.__name__)
    for field in dataclasses.fields(cls):
        print(field.name, field.default is dataclasses.MISSING)
PY
```

Expected every printed field line ends with `True`.

---

## Self-Review

**Spec coverage:** This plan covers the user’s explicit requirements: keep Dagster config for values that may be set through Dagster, remove defaults from `TranslationQueueWorkflowInput`, make `max_batch_failures=0` mean no failure-limit check, remove `norway_brreg_translation_start_config()`, and simplify the claim-batch helper indirection.

**Placeholder scan:** No TODO/TBD placeholders remain. Every changed code shape includes exact snippets and commands.

**Type consistency:** `TranslationQueueWorkflowInput` is consistently used as an explicit runtime payload, while `NorwayBrregTranslationConfig` is consistently used as the Dagster operator config.
