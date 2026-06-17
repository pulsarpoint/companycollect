# Translation Workflow Input Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Dagster config the place where translation workflow defaults live, make `TranslationQueueWorkflowInput` require every runtime field explicitly, and make `max_batch_failures=0` mean “do not stop because of failed batches.”

**Architecture:** Keep `TranslationTemporalStartConfig` as the Dagster-facing configuration object with defaults suitable for UI/config-driven launches. Keep `TranslationQueueWorkflowInput` as the Temporal payload, but remove all default values so every workflow start serializes an explicit runtime contract. Keep retry/failure-limit behavior local to the workflow loop and smoke loop with the same simple rule: only enforce a max failure count when `max_batch_failures > 0`.

**Tech Stack:** Python 3.14, Dagster, Temporal Python SDK, DuckDB, pytest.

---

## File Structure

- Modify `src/dagster_v3/temporal/translations/queue.py`
  - Owns Temporal workflow input/output dataclasses, activities, and workflow loop.
  - Remove defaults from `TranslationQueueWorkflowInput`.
  - Change failure-limit condition so `0` means unlimited.
- Modify `src/dagster_v3/translations/queue_smoke.py`
  - Owns local queue smoke loop used to validate long-running translation queue behavior without Temporal.
  - Match workflow failure-limit semantics.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Owns Norway-specific Dagster config for the BRREG translation workflow start.
  - Stop overriding `max_batch_failures` to `1000`; use Dagster config default `0`.
- Modify `tests/test_translation_temporal_queue.py`
  - Pin that `TranslationQueueWorkflowInput` has no defaults.
  - Add a Temporal workflow test proving `max_batch_failures=0` continues after a failed batch.
- Modify `tests/test_translation_queue_smoke.py`
  - Pin smoke-loop semantics for `max_batch_failures=0`.
- Modify `tests/test_norway_brreg_assets.py`
  - Pin Norway config returns `max_batch_failures == 0`.
- Modify `tests/test_translation_dagster_assets.py`
  - Ensure tests construct `TranslationQueueWorkflowInput` with all required fields explicitly.
- Modify `tests/test_translation_temporal_queue_cli.py`
  - Ensure CLI parsing still builds a fully explicit `TranslationQueueWorkflowInput`.

---

### Task 1: Require Explicit Temporal Workflow Input Fields

**Files:**
- Modify: `src/dagster_v3/temporal/translations/queue.py:17-26`
- Test: `tests/test_translation_temporal_queue.py:1-65`

- [ ] **Step 1: Write the failing test**

Replace `test_translation_queue_workflow_input_defaults_are_serialized` in `tests/test_translation_temporal_queue.py` with this test:

```python
from dataclasses import fields

import pytest


def test_translation_queue_workflow_input_requires_explicit_runtime_values() -> None:
    field_defaults = {
        field.name: field.default
        for field in fields(TranslationQueueWorkflowInput)
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

Also add the missing import at the top of the file:

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

Expected: FAIL because fields such as `batch_size`, `timeout_seconds`, and `max_batch_failures` still have defaults on `TranslationQueueWorkflowInput`.

- [ ] **Step 3: Remove defaults from `TranslationQueueWorkflowInput`**

Change `src/dagster_v3/temporal/translations/queue.py` from:

```python
@dataclass(frozen=True)
class TranslationQueueWorkflowInput:
    duckdb_path: str
    item_count: int
    batch_size: int = 50
    timeout_seconds: int = 120
    max_batch_failures: int = 0
    worker_id: str = "translation-temporal-worker"
    max_tokens: int = 2048
    extra_body_json: str = '{"chat_template_kwargs":{"enable_thinking":false}}'
```

to:

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

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py::test_translation_queue_workflow_input_requires_explicit_runtime_values -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/temporal/translations/queue.py tests/test_translation_temporal_queue.py
git commit -m "refactor: require explicit translation workflow input"
```

---

### Task 2: Make `max_batch_failures=0` Mean Unlimited in the Temporal Workflow

**Files:**
- Modify: `src/dagster_v3/temporal/translations/queue.py:170-220`
- Test: `tests/test_translation_temporal_queue.py`

- [ ] **Step 1: Write the failing workflow test**

Add these imports to `tests/test_translation_temporal_queue.py`:

```python
import asyncio
from uuid import uuid4

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
```

Add this test to `tests/test_translation_temporal_queue.py`:

```python
def test_translation_queue_workflow_zero_max_batch_failures_continues_after_failure() -> None:
    async def run_workflow() -> None:
        process_calls = 0

        @activity.defn(name="initialize_translation_queue")
        async def fake_initialize_translation_queue(params: InitializeTranslationQueueInput) -> int:
            assert params.duckdb_path == "/tmp/translations.duckdb"
            assert params.item_count == 0
            return 0

        @activity.defn(name="process_translation_batch")
        async def fake_process_translation_batch(
            params: ProcessTranslationBatchInput,
        ) -> ProcessTranslationBatchResult:
            nonlocal process_calls
            process_calls += 1
            assert params.batch_size == 50
            assert params.timeout_seconds == 120
            assert params.worker_id == "translation-temporal-worker"
            assert params.max_tokens == 4096
            if process_calls == 1:
                return ProcessTranslationBatchResult(
                    status="failed",
                    item_count=50,
                    duration_seconds=1.0,
                    error_category="invalid_json",
                    error_message="invalid json",
                )
            return ProcessTranslationBatchResult(
                status="empty",
                item_count=0,
                duration_seconds=0.0,
            )

        @activity.defn(name="summarize_translation_queue")
        async def fake_summarize_translation_queue(duckdb_path: str) -> dict[str, int]:
            assert duckdb_path == "/tmp/translations.duckdb"
            return {
                "total_items": 50,
                "completed_items": 0,
                "failed_retryable_items": 50,
                "successful_batches": 0,
                "failed_batches": 1,
            }

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="translation-test-queue",
                workflows=[TranslationQueueWorkflow],
                activities=[
                    fake_initialize_translation_queue,
                    fake_process_translation_batch,
                    fake_summarize_translation_queue,
                ],
            ):
                result = await env.client.execute_workflow(
                    TranslationQueueWorkflow.run,
                    TranslationQueueWorkflowInput(
                        duckdb_path="/tmp/translations.duckdb",
                        item_count=0,
                        batch_size=50,
                        timeout_seconds=120,
                        max_batch_failures=0,
                        worker_id="translation-temporal-worker",
                        max_tokens=4096,
                        extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}',
                    ),
                    id=f"translation-test-{uuid4()}",
                    task_queue="translation-test-queue",
                )

        assert process_calls == 2
        assert result.provider_failure_count == 1
        assert result.failed_batches == 1
        assert result.failed_retryable_items == 50

    asyncio.run(run_workflow())
```

Update the import from `dagster_v3.temporal.translations.queue` to include these names:

```python
from dagster_v3.temporal.translations.queue import (
    InitializeTranslationQueueInput,
    LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    ProcessTranslationBatchInput,
    ProcessTranslationBatchResult,
    TranslationQueueWorkflow,
    TranslationQueueWorkflowInput,
    process_translation_batch_once,
)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py::test_translation_queue_workflow_zero_max_batch_failures_continues_after_failure -q
```

Expected: FAIL because the workflow currently exits after the first failed batch when `max_batch_failures=0`, so `process_calls` is `1`.

- [ ] **Step 3: Change workflow failure-limit condition**

Change this block in `src/dagster_v3/temporal/translations/queue.py`:

```python
provider_failure_count += 1
if provider_failure_count > params.max_batch_failures:
    break
```

to:

```python
provider_failure_count += 1
if params.max_batch_failures > 0 and provider_failure_count > params.max_batch_failures:
    break
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py::test_translation_queue_workflow_zero_max_batch_failures_continues_after_failure -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/temporal/translations/queue.py tests/test_translation_temporal_queue.py
git commit -m "fix: treat zero translation batch failures as unlimited"
```

---

### Task 3: Match Failure-Limit Semantics in the Local Smoke Loop

**Files:**
- Modify: `src/dagster_v3/translations/queue_smoke.py:140-160`
- Test: `tests/test_translation_queue_smoke.py`

- [ ] **Step 1: Write the failing smoke-loop test**

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

Expected: FAIL because the smoke loop currently exits after the first failed batch when `max_batch_failures=0`.

- [ ] **Step 3: Change smoke-loop failure-limit condition**

Change this block in `src/dagster_v3/translations/queue_smoke.py`:

```python
if provider_failure_count > max_batch_failures:
    break
```

to:

```python
if max_batch_failures > 0 and provider_failure_count > max_batch_failures:
    break
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_translation_queue_smoke.py::test_run_translation_queue_smoke_zero_max_batch_failures_continues_after_failure -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/translations/queue_smoke.py tests/test_translation_queue_smoke.py
git commit -m "fix: align translation smoke failure limit semantics"
```

---

### Task 4: Restore Norway to the Dagster Config Default

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py:594-602`
- Test: `tests/test_norway_brreg_assets.py:1029-1038`

- [ ] **Step 1: Write the failing test**

In `tests/test_norway_brreg_assets.py`, update `test_norway_translation_temporal_configs_use_country_queue_defaults` so it expects the Dagster config default:

```python
def test_norway_translation_temporal_configs_use_country_queue_defaults() -> None:
    start_config = brreg_assets.norway_brreg_translation_start_config()

    assert start_config.source_slug == "norway-brreg"
    assert start_config.duckdb_path == "data/norway_brreg_translation_queue.duckdb"
    assert start_config.item_count == 0
    assert start_config.batch_size == 50
    assert start_config.max_batch_failures == 0
    assert start_config.workflow_id == "translation-norway-brreg"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_translation_temporal_configs_use_country_queue_defaults -q
```

Expected: FAIL if `norway_brreg_translation_start_config()` still sets `max_batch_failures=1000`.

- [ ] **Step 3: Remove the Norway override**

Change `src/dagster_v3/defs/norway_brreg/assets.py` from:

```python
def norway_brreg_translation_start_config() -> TranslationTemporalStartConfig:
    return TranslationTemporalStartConfig(
        source_slug=NORWAY_BRREG_TRANSLATION_SOURCE_SLUG,
        duckdb_path=str(NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH),
        item_count=0,
        batch_size=50,
        max_batch_failures=1000,
        workflow_id=NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
    )
```

to:

```python
def norway_brreg_translation_start_config() -> TranslationTemporalStartConfig:
    return TranslationTemporalStartConfig(
        source_slug=NORWAY_BRREG_TRANSLATION_SOURCE_SLUG,
        duckdb_path=str(NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH),
        item_count=0,
        batch_size=50,
        workflow_id=NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_translation_temporal_configs_use_country_queue_defaults -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "fix: use unlimited default for norway translation failures"
```

---

### Task 5: Update All Workflow Input Construction Sites

**Files:**
- Modify: `tests/test_translation_dagster_assets.py`
- Modify: `tests/test_translation_temporal_queue_cli.py`
- Check: `src/dagster_v3/defs/translations/assets.py`
- Check: `src/dagster_v3/temporal/translations/queue_cli.py`

- [ ] **Step 1: Write/update tests for explicit workflow input values**

In `tests/test_translation_dagster_assets.py`, keep the expected params explicit:

```python
assert client.started[0]["params"] == TranslationQueueWorkflowInput(
    duckdb_path="/tmp/translations.duckdb",
    item_count=2000,
    batch_size=50,
    timeout_seconds=120,
    max_batch_failures=0,
    worker_id="translation-temporal-worker",
    max_tokens=4096,
    extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}',
)
```

In `tests/test_translation_temporal_queue_cli.py`, keep the expected params explicit:

```python
assert params == TranslationQueueWorkflowInput(
    duckdb_path="/tmp/translations.duckdb",
    item_count=2000,
    batch_size=50,
    timeout_seconds=120,
    max_batch_failures=0,
    worker_id="translation-temporal-worker",
    max_tokens=4096,
    extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}',
)
```

- [ ] **Step 2: Run tests to identify stale constructors**

Run:

```bash
uv run pytest tests/test_translation_dagster_assets.py tests/test_translation_temporal_queue_cli.py -q
```

Expected: FAIL if any constructor omits `extra_body_json` or another now-required field.

- [ ] **Step 3: Make construction sites explicit**

Confirm `src/dagster_v3/defs/translations/assets.py` creates `TranslationQueueWorkflowInput` with every field:

```python
params = TranslationQueueWorkflowInput(
    duckdb_path=config.duckdb_path,
    item_count=config.item_count,
    batch_size=config.batch_size,
    timeout_seconds=config.timeout_seconds,
    max_batch_failures=config.max_batch_failures,
    worker_id=config.worker_id,
    max_tokens=config.max_tokens,
    extra_body_json=config.extra_body_json,
)
```

Confirm `src/dagster_v3/temporal/translations/queue_cli.py` creates `TranslationQueueWorkflowInput` with every field:

```python
return workflow_id, TranslationQueueWorkflowInput(
    duckdb_path=args.duckdb_path,
    item_count=args.item_count,
    batch_size=args.batch_size,
    timeout_seconds=args.timeout_seconds,
    max_batch_failures=args.max_batch_failures,
    worker_id=args.worker_id,
    max_tokens=args.max_tokens,
    extra_body_json=args.extra_body_json,
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_translation_dagster_assets.py tests/test_translation_temporal_queue_cli.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_translation_dagster_assets.py tests/test_translation_temporal_queue_cli.py src/dagster_v3/defs/translations/assets.py src/dagster_v3/temporal/translations/queue_cli.py
git commit -m "test: require explicit translation workflow construction"
```

---

### Task 6: Full Verification

**Files:**
- Check: all modified files

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

Expected:

```text
172 passed, 1 skipped
```

The exact warning count may vary because current Dagster/Pydantic dependencies emit deprecation warnings.

- [ ] **Step 3: Confirm no default values remain on `TranslationQueueWorkflowInput`**

Run:

```bash
uv run python - <<'PY'
import dataclasses
from dagster_v3.temporal.translations.queue import TranslationQueueWorkflowInput

for field in dataclasses.fields(TranslationQueueWorkflowInput):
    print(field.name, field.default is dataclasses.MISSING)
PY
```

Expected:

```text
duckdb_path True
item_count True
batch_size True
timeout_seconds True
max_batch_failures True
worker_id True
max_tokens True
extra_body_json True
```

- [ ] **Step 4: Commit verification-only test updates if needed**

If Task 6 required additional test-only changes, commit them:

```bash
git add tests
git commit -m "test: verify translation workflow input contract"
```

If there are no additional changes, do not create an empty commit.

---

## Self-Review

**Spec coverage:** The plan keeps defaults in the Dagster config object, removes defaults from `TranslationQueueWorkflowInput`, and changes `max_batch_failures=0` to mean no failure-limit check. It also removes the Norway-specific `1000` override that was added only to work around the old semantics.

**Placeholder scan:** The plan contains no TBD/TODO placeholders. Each task includes exact files, exact code snippets, commands, and expected outcomes.

**Type consistency:** `TranslationQueueWorkflowInput` is consistently shown with eight required fields in tests, Dagster starter construction, CLI construction, and workflow execution tests.
