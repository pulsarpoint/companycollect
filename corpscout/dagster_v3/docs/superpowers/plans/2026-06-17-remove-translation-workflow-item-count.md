# Remove Translation Workflow Item Count Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `item_count` and synthetic item generation from the Temporal translation queue workflow so production workflows only process translation items already seeded in DuckDB.

**Architecture:** Keep synthetic translation item generation in `dagster_v3.translations.queue_smoke`, which is explicitly smoke-test tooling. Make `TranslationQueueWorkflowInput` describe only the queue-processing runtime settings. Change the Temporal initialization activity to only create/verify DuckDB queue tables and return the current total queue item count.

**Tech Stack:** Python 3.14, Temporal Python SDK, Dagster, DuckDB, pytest.

---

## File Structure

- Modify `src/dagster_v3/temporal/translations/queue.py`
  - Remove `item_count` from `TranslationQueueWorkflowInput`.
  - Remove `item_count` from `InitializeTranslationQueueInput`.
  - Remove `generate_synthetic_translation_items` from Temporal queue initialization.
  - Make `initialize_translation_queue_once` initialize queue tables and return `queue.summary().total_items`.
- Modify `src/dagster_v3/temporal/translations/queue_cli.py`
  - Remove required `--item-count` argument from the Temporal queue start CLI.
  - Stop passing `item_count` into `TranslationQueueWorkflowInput`.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Stop passing `item_count=0` into `TranslationQueueWorkflowInput`.
- Modify tests:
  - `tests/test_translation_temporal_queue.py`
  - `tests/test_translation_temporal_queue_cli.py`
  - `tests/test_translation_dagster_assets.py`

---

### Task 1: Remove Synthetic Item Count From Temporal Workflow

**Files:**
- Modify: `tests/test_translation_temporal_queue.py`
- Modify: `tests/test_translation_temporal_queue_cli.py`
- Modify: `tests/test_translation_dagster_assets.py`
- Modify: `src/dagster_v3/temporal/translations/queue.py`
- Modify: `src/dagster_v3/temporal/translations/queue_cli.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`

- [x] **Step 1: Write failing tests for the production workflow input contract**

In `tests/test_translation_temporal_queue.py`, update `test_translation_queue_workflow_input_requires_explicit_runtime_values` so the expected field set removes `"item_count"`:

```python
assert set(field_defaults) == {
    "duckdb_path",
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

Update the `TypeError` assertion to prove incomplete runtime settings still fail:

```python
with pytest.raises(TypeError):
    TranslationQueueWorkflowInput(
        duckdb_path="/tmp/translations.duckdb",
    )
```

Construct `TranslationQueueWorkflowInput` without `item_count`, and remove `"item_count": 2000` from the `asdict(params)` assertion.

Add this test to the same file:

```python
def test_initialize_translation_queue_once_only_initializes_tables(tmp_path) -> None:
    duckdb_path = tmp_path / "translations.duckdb"

    count = initialize_translation_queue_once(
        InitializeTranslationQueueInput(duckdb_path=str(duckdb_path))
    )

    assert count == 0
    queue = TranslationQueue(duckdb_path)
    summary = queue.summary()
    assert summary.total_items == 0
```

Import `InitializeTranslationQueueInput` and `initialize_translation_queue_once` from `dagster_v3.temporal.translations.queue`.

- [x] **Step 2: Update caller tests to remove `item_count`**

In `tests/test_translation_temporal_queue_cli.py`, remove the CLI args:

```python
"--item-count",
"2000",
```

and remove `item_count=2000` from the expected `TranslationQueueWorkflowInput`.

In `tests/test_translation_dagster_assets.py`, remove `item_count=2000` from the `TranslationQueueWorkflowInput` constructor.

- [x] **Step 3: Run focused tests to verify they fail**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py::test_translation_queue_workflow_input_requires_explicit_runtime_values tests/test_translation_temporal_queue.py::test_initialize_translation_queue_once_only_initializes_tables tests/test_translation_temporal_queue_cli.py::test_parse_start_args_builds_workflow_input tests/test_translation_dagster_assets.py::test_start_translation_workflow_uses_generic_temporal_queue -q
```

Expected: FAIL because production code still requires and passes `item_count`.

- [x] **Step 4: Remove `item_count` from workflow input and initialization**

In `src/dagster_v3/temporal/translations/queue.py`, change the dataclasses to:

```python
@dataclass(frozen=True)
class TranslationQueueWorkflowInput:
    duckdb_path: str
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


@dataclass(frozen=True)
class InitializeTranslationQueueInput:
    duckdb_path: str
```

Change `initialize_translation_queue_once` to:

```python
def initialize_translation_queue_once(params: InitializeTranslationQueueInput) -> int:
    from dagster_v3.translations.queue import TranslationQueue

    queue = TranslationQueue(params.duckdb_path)
    queue.initialize()
    return queue.summary().total_items
```

Change the workflow initialization activity input to:

```python
InitializeTranslationQueueInput(duckdb_path=params.duckdb_path)
```

- [x] **Step 5: Remove `item_count` from start boundaries**

In `src/dagster_v3/temporal/translations/queue_cli.py`, delete:

```python
parser.add_argument("--item-count", required=True, type=int)
```

and remove:

```python
item_count=args.item_count,
```

from `TranslationQueueWorkflowInput`.

In `src/dagster_v3/defs/norway_brreg/assets.py`, remove:

```python
item_count=0,
```

from `TranslationQueueWorkflowInput`.

- [x] **Step 6: Run focused and full validation**

Run:

```bash
uv run pytest tests/test_translation_temporal_queue.py tests/test_translation_temporal_queue_cli.py tests/test_translation_dagster_assets.py tests/test_norway_brreg_assets.py -q
uv run dg check defs
uv run pytest -q
```

Expected: all commands pass.

---

## Self-Review

**Spec coverage:** This removes `item_count` completely from the production Temporal queue workflow and Norway start path. It keeps smoke queue `item_count` in `translations.queue_smoke`, where it still names real smoke-test behavior.

**Placeholder scan:** No placeholders remain.

**Type consistency:** `TranslationQueueWorkflowInput` and `InitializeTranslationQueueInput` are shown without `item_count` in tests, CLI, Norway asset, and workflow code.
