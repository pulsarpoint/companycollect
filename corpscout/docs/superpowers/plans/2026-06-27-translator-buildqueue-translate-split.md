# Translator BuildQueue / Translate Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the monolithic `TranslateSourceWorkflow` into two independent Temporal workflows (`BuildQueueWorkflow` + `TranslateWorkflow`), reorganize the translator into per-source packages with a shared core, replace Python row-by-row seeding with a bulk Arrow load, and replace fixed activity duration timeouts with heartbeat-based liveness.

**Architecture:** `BuildQueueWorkflow` (per source, under `translator/norway_brreg/`) drives a bulk ClickHouse → DuckDB seed using `clickhouse_connect`'s `query_arrow()` and set-based DuckDB SQL (hashes computed in SQL), then starts `TranslateWorkflow` as a separate top-level workflow using `USE_EXISTING` and completes. `TranslateWorkflow` runs the LLM drain loop (via a long heartbeating activity) then dumps results to `corpscout.text_translations` in batched staging-table inserts. All long activities heartbeat every ≈30 s; `heartbeat_timeout=150 s`; `start_to_close_timeout=24 h` is the backstop.

**Tech Stack:** Python 3.14, temporalio (Python SDK), DuckDB, clickhouse-connect (PyArrow path), Dagster, uv.

## Global Constraints

- Per-source package `translator/norway_brreg/` over a shared core; nothing source-specific in the shared core.
- Two workflows: `BuildQueueWorkflow` (bulk seed) + `TranslateWorkflow` (drain + dump). `BuildQueueWorkflow`'s final step calls `start_workflow(TranslateWorkflow, id="translate-norway_brreg", id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING, task_queue=BUILD_TASK_QUEUE)` then completes. This is a **separate top-level workflow, NOT a child**.
- Two shared task queues + a global LLM gate. `BUILD_TASK_QUEUE = "translation-build"` runs the workflows (BuildQueue + Translate) and the seed / dump / summarize / start-translate-handoff activities at normal concurrency. `LLM_TASK_QUEUE = "translation-llm"` runs **only** `translate_loop_activity`, bounded to `K` concurrent (`max_concurrent_activities=K` on a single shared worker) — a true global cap on how many sources hit the one LLM at once. `K` from env `TRANSLATOR_LLM_CONCURRENCY` (default 2). `worker.py` starts BOTH workers in one process via `asyncio.gather`. `TranslateWorkflow` dispatches `translate_loop_activity` with `task_queue=LLM_TASK_QUEUE` and **no `schedule_to_start_timeout`** (loops wait in queue for a slot); `dump`/`summarize` stay on the default (build) queue. The two constants live in shared `translator/task_queues.py`.
- `.env` loading uses **python-dotenv** (`load_dotenv(path, override=False)` — shell / `docker -e` still win), a direct dependency; the hand-rolled `load_env_file` parser is deleted.
- Seed = `clickhouse-connect` `query_arrow()` of the per-field `LEFT ANTI JOIN` scan → register Arrow table in DuckDB → set-based `INSERT … SELECT` into queue tables with `item_id = sha256(sha256(source_text) || '|en')` computed in DuckDB SQL. NO per-row Python.
- Dump = fully self-contained per source (`norway_brreg/dump.py`), batched staging-table writes to `corpscout.text_translations` keyed `(source_table, source_column, source_text_hash=cityHash64(source_text))`, hashing in ClickHouse SQL. Reuses `translator/flush.py:flush_translations`.
- De-smoke first: the historical `Smoke*` names are smoke-test leftovers on production code. Before any new code, rename `translator/types.py` `SmokeTranslationInput`/`SmokeTranslationResult` → `TranslationInput`/`TranslationResult`, `translator/smoke.py` → `translator/provider.py` (`build_smoke_translation_prompt`→`build_translation_prompt`, `parse_smoke_translation_response`→`parse_translation_response`, keep the class `LocalOpenAICompatibleTranslationProvider`, add `_parse_extra_body`), move error helpers to `translator/errors.py`, and delete the smoke-test files (`provider_smoke.py`, `queue_smoke.py`). No production `Smoke`/`smoke` identifiers may remain anywhere.
- Shared `translator/llm_batch.py` = the common "translate N claimed queue items via the LLM" function (it builds the provider inputs + queue-id mapping itself, superseding the old `queue_smoke._provider_inputs`/`_map_provider_results_to_queue_ids`).
- Liveness = heartbeat only: long activities call `activity.heartbeat()` (~30 s / N items); activity options `heartbeat_timeout=timedelta(seconds=150)`, `start_to_close_timeout=timedelta(hours=24)` backstop, `RetryPolicy(maximum_attempts=3)`. REMOVE `scan_timeout_seconds` / `flush_timeout_seconds` from the workflow input.
- Reuse unchanged: `corpscout.text_translations` + `corpscout.no_companies_translated` view + migrations 000056 / 000059–000070; the LLM provider (`translator/provider.py`, formerly `smoke.py`); the DuckDB queue schema (`translation_items` / `translation_locations` / `translation_results` / `translation_batch_attempts`).
- Cutover: delete `data/translator/norway_brreg.duckdb` (+ `.wal`) so the new seed builds a fresh queue. The Norway Dagster trigger now fires `BuildQueueWorkflow` (id `build-queue-norway_brreg`).
- No `from __future__ import annotations` in modules defining `@dg.asset` / `@dlt_assets` / `@dbt_assets` — translator modules are temporalio so this rule applies only to the Dagster trigger file (`assets.py`).
- Validate with: `uv run dg check defs` + `uv run pytest -q`.
- All commits use `git add <explicit paths>` — never `git add -A`.
- Run from `corpscout/dagster_v3/` with `uv run`.

---

## File Map

| Action | Path |
|--------|------|
| **Modify (Task 1)** | `translator/types.py` — `Smoke*` → `Translation*` renames |
| **Create (Task 1)** | `translator/provider.py` — from `smoke.py` (renamed fns + `_parse_extra_body`) |
| **Create (Task 1)** | `translator/errors.py` — relocated error-categorisation helpers |
| **Modify (Task 1)** | `translator/queue.py`, `translator/activities.py` — de-smoke imports |
| **Delete (Task 1)** | `translator/smoke.py`, `translator/provider_smoke.py`, `translator/queue_smoke.py` |
| **Create** | `translator/llm_batch.py` |
| **Modify** | `translator/clickhouse.py` |
| **Create** | `translator/norway_brreg/__init__.py` |
| **Create** | `translator/norway_brreg/config.py` |
| **Create** | `translator/norway_brreg/seed.py` |
| **Create** | `translator/norway_brreg/dump.py` |
| **Create (Task 7)** | `translator/task_queues.py` — shared `BUILD_TASK_QUEUE` / `LLM_TASK_QUEUE` constants |
| **Create** | `translator/norway_brreg/workflows.py` |
| **Modify (Task 8)** | `pyproject.toml` — add `python-dotenv>=1.0` dependency |
| **Modify** | `translator/worker.py` — two-worker fleet + python-dotenv |
| **Modify (Task 8)** | `translator/import_legacy.py` — switch env load to `load_dotenv` |
| **Modify** | `src/dagster_v3/defs/norway_brreg/assets.py` |
| **Delete (Task 10)** | `translator/workflow.py` |
| **Delete (Task 10)** | `translator/activities.py` |
| **Modify (Task 10)** | `translator/registry.py` → deleted; `import_legacy.py` updated |
| **Modify (Task 1)** | `tests/test_translator_workflow.py`, `tests/test_translator_queue_flush_results.py`, `tests/test_translator_imports.py`, `tests/test_translator_import_legacy.py` — de-smoke names |
| **Create** | `tests/test_translator_llm_batch.py` |
| **Create** | `tests/test_norway_brreg_config.py` |
| **Create** | `tests/test_norway_brreg_seed.py` |
| **Create** | `tests/test_norway_brreg_dump.py` |
| **Create** | `tests/test_norway_brreg_workflows.py` |
| **Replace** | `tests/test_translator_workflow.py` |
| **Modify** | `tests/test_translator_worker.py` |
| **Modify** | `tests/test_translator_trigger.py` |
| **Replace** | `tests/test_translator_registry.py` → `tests/test_norway_brreg_config.py` (Task 4) |
| **Modify** | `tests/test_translator_imports.py` |

---

### Task 1: De-smoke the translator

The `Smoke*` names are historical smoke-test leftovers on production code. This task is a **behaviour-preserving refactor of the existing translator** — it renames the production types/modules, relocates the production helpers into clearly-named modules, and deletes the genuine one-off smoke-test CLIs. It runs **before** any new code so every later task references the clean names. The safety net is the existing test suite (kept green throughout). It runs **before** the dead-code deletion (Task 10).

**Smoke-test CLI decision:** `pyproject.toml [project.scripts]` exposes only `translator-worker` and `translator-import-legacy-queue` — neither `provider_smoke:main` nor `queue_smoke:main`. No test imports `run_translation_provider_smoke` / `run_translation_queue_smoke`. They are unreferenced one-off prototypes → **deleted** (no `provider_check.py` kept).

**Files:**
- Modify: `translator/types.py` — rename the two dataclasses
- Create: `translator/provider.py` — from `translator/smoke.py` (renamed functions + `_parse_extra_body`)
- Create: `translator/errors.py` — relocated error-categorisation helpers
- Modify: `translator/queue.py` — de-smoke type import + annotation
- Modify: `translator/activities.py` — de-smoke imports; inline the two superseded helpers
- Delete: `translator/smoke.py`, `translator/provider_smoke.py`, `translator/queue_smoke.py`
- Modify: `tests/test_translator_workflow.py`, `tests/test_translator_queue_flush_results.py`, `tests/test_translator_imports.py`, `tests/test_translator_import_legacy.py`

**Interfaces:**
- Produces (consumed by Tasks 2–10):
  - `translator.types.TranslationInput(item_id: str, source_text: str)`, `translator.types.TranslationResult(item_id: str, translated_text: str)` — frozen dataclasses (renamed from `Smoke*`)
  - `translator.provider`: `build_translation_prompt(items) -> str`, `parse_translation_response(text, *, expected_item_ids) -> list[TranslationResult]`, `_parse_extra_body(value: str) -> dict | None`, class `LocalOpenAICompatibleTranslationProvider`
  - `translator.errors`: `_categorize_exception(exc) -> str`, `_exception_chain_messages(exc) -> list[str]`, `_exception_chain_class_names(exc) -> str`

- [ ] **Step 1.1: Capture the green baseline**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest -q 2>&1 | tail -5
```
Expected: the suite passes (this is the refactor's safety net). Note the passing count.

- [ ] **Step 1.2: Rename the dataclasses in `translator/types.py`**

Replace the whole body below the `from dataclasses import dataclass` line:

```python
# translator/types.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TranslationInput:
    item_id: str
    source_text: str


@dataclass(frozen=True)
class TranslationResult:
    item_id: str
    translated_text: str
```

- [ ] **Step 1.3: Create `translator/errors.py`**

Move `_categorize_exception`, `_exception_chain_messages`, `_exception_chain_class_names` out of `provider_smoke.py` verbatim:

```python
# translator/errors.py
"""Error categorisation helpers for the translation provider."""
from __future__ import annotations


def _categorize_exception(exc: Exception) -> str:
    message = str(exc).lower()
    chain_messages = _exception_chain_messages(exc)
    combined = " ".join([message, *chain_messages])
    class_names = _exception_chain_class_names(exc)

    if "connectionrefusederror" in class_names or "connection refused" in combined:
        return "connection_refused"
    if "timeout" in combined or "timeout" in class_names:
        return "timeout"
    if "json" in combined or "jsondecodeerror" in class_names:
        return "invalid_json"
    if "missing item_id" in combined:
        return "missing_item_ids"
    if "item_id" in combined or "translation response" in combined:
        return "invalid_response"
    return "provider_error"


def _exception_chain_messages(exc: BaseException) -> list[str]:
    messages: list[str] = []
    current = exc.__cause__ or exc.__context__
    while current is not None:
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    return messages


def _exception_chain_class_names(exc: BaseException) -> str:
    names: list[str] = []
    current: BaseException | None = exc
    while current is not None:
        names.append(type(current).__name__.lower())
        current = current.__cause__ or current.__context__
    return " ".join(names)
```

- [ ] **Step 1.4: Create `translator/provider.py` (from `smoke.py`)**

Rename `build_smoke_translation_prompt`→`build_translation_prompt`, `parse_smoke_translation_response`→`parse_translation_response`, switch to `TranslationInput`/`TranslationResult`, and fold in `_parse_extra_body` (moved from `provider_smoke.py`). The class keeps its name.

```python
# translator/provider.py
"""OpenAI-compatible local translation provider (formerly smoke.py)."""
from __future__ import annotations

import json

from openai import OpenAI

from translator.types import TranslationInput, TranslationResult


DEFAULT_MAX_TOKENS = 2048
DEFAULT_EXTRA_BODY = {"chat_template_kwargs": {"enable_thinking": False}}


def build_translation_prompt(items: list[TranslationInput]) -> str:
    payload = {
        "source_language": "Norwegian",
        "target_language": "English",
        "items": [
            {"item_id": item.item_id, "source_text": item.source_text}
            for item in items
        ],
    }
    return (
        "/no_think\n"
        "Translate each Norwegian company registry text fragment to English. "
        "Preserve legal and business meaning. Do not add explanations. "
        "Do not produce reasoning, chain-of-thought, thinking tags, or Markdown. "
        "Return only valid JSON with shape "
        '{"translations":[{"item_id":"...","translated_text":"..."}]}. '
        "Every item_id in the response must match an input item_id. Input JSON: "
        f"{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    )


def parse_translation_response(
    response_text: str,
    *,
    expected_item_ids: set[str],
) -> list[TranslationResult]:
    payload = json.loads(_strip_json_fence(response_text))
    translations = payload.get("translations")
    if not isinstance(translations, list):
        raise ValueError("translation response must contain translations list")

    results: list[TranslationResult] = []
    seen: set[str] = set()
    for row in translations:
        if not isinstance(row, dict):
            raise ValueError("translation response row must be object")
        item_id = row.get("item_id")
        translated_text = row.get("translated_text")
        if not isinstance(item_id, str) or item_id not in expected_item_ids:
            raise ValueError(f"unexpected item_id: {item_id}")
        if item_id in seen:
            raise ValueError(f"duplicate item_id: {item_id}")
        if not isinstance(translated_text, str) or translated_text.strip() == "":
            raise ValueError(f"empty translated_text for item_id: {item_id}")
        seen.add(item_id)
        results.append(
            TranslationResult(
                item_id=item_id,
                translated_text=translated_text.strip(),
            )
        )
    missing_item_ids = expected_item_ids - seen
    if missing_item_ids:
        raise ValueError(f"missing item_id values: {sorted(missing_item_ids)}")
    return results


def _parse_extra_body(value: str) -> dict[str, object] | None:
    if value.strip() == "":
        return None
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("extra body JSON must be an object")
    return payload


class LocalOpenAICompatibleTranslationProvider:
    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str,
        client: OpenAI | None = None,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        extra_body: dict[str, object] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.max_tokens = max_tokens
        self.extra_body = extra_body if extra_body is not None else DEFAULT_EXTRA_BODY
        self._owns_client = client is None
        if client is None:
            self.client = OpenAI(
                base_url=self.base_url,
                api_key=self.api_key,
                max_retries=0,
            )
        else:
            self.client = client

    def translate(
        self,
        items: list[TranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[TranslationResult]:
        prompt = build_translation_prompt(items)
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=self.max_tokens,
            extra_body=self.extra_body,
            timeout=timeout_seconds,
        )
        content = response.choices[0].message.content
        if content is None:
            raise ValueError("translation response content is empty")
        return parse_translation_response(
            content,
            expected_item_ids={item.item_id for item in items},
        )

    def close(self) -> None:
        if self._owns_client:
            self.client.close()


def _strip_json_fence(response_text: str) -> str:
    stripped = response_text.strip()
    if stripped.startswith("```json") and stripped.endswith("```"):
        return stripped.removeprefix("```json").removesuffix("```").strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        return stripped.removeprefix("```").removesuffix("```").strip()
    return stripped
```

- [ ] **Step 1.5: Update `translator/queue.py`**

Replace the import (line 11) and the `complete_batch` annotation:

```python
# In translator/queue.py — replace:
from translator.types import SmokeTranslationResult
# With:
from translator.types import TranslationResult
```

```python
# In translator/queue.py — replace the complete_batch signature line:
        translations: list[SmokeTranslationResult],
# With:
        translations: list[TranslationResult],
```

- [ ] **Step 1.6: Update `translator/activities.py`**

(a) Replace the top-of-file types import (line 13):

```python
# replace:
from translator.types import SmokeTranslationResult
# with:
from typing import TYPE_CHECKING

from translator.types import TranslationInput, TranslationResult

if TYPE_CHECKING:
    from translator.queue import ClaimedTranslationItem
```

(b) Replace the lazy imports inside `process_translation_batch_once`:

```python
# replace:
    from translator.provider_smoke import _categorize_exception, _parse_extra_body
    from translator.queue import TranslationQueue
    from translator.queue_smoke import (
        _map_provider_results_to_queue_ids,
        _provider_inputs,
    )
    from translator.smoke import LocalOpenAICompatibleTranslationProvider
# with:
    from translator.errors import _categorize_exception
    from translator.provider import (
        LocalOpenAICompatibleTranslationProvider,
        _parse_extra_body,
    )
    from translator.queue import TranslationQueue
```

(c) Add the two superseded helpers (formerly in `queue_smoke.py`) as module-level functions at the end of `translator/activities.py`:

```python
def _provider_inputs(items: list["ClaimedTranslationItem"]) -> list[TranslationInput]:
    return [
        TranslationInput(
            item_id=f"batch-item-{index:03d}",
            source_text=item.source_text,
        )
        for index, item in enumerate(items)
    ]


def _map_provider_results_to_queue_ids(
    translations: list[TranslationResult],
    *,
    queue_id_by_provider_id: dict[str, str],
) -> list[TranslationResult]:
    return [
        TranslationResult(
            item_id=queue_id_by_provider_id[translation.item_id],
            translated_text=translation.translated_text,
        )
        for translation in translations
    ]
```

- [ ] **Step 1.7: De-smoke the four affected test files**

Apply these exact substitutions in `tests/test_translator_workflow.py`, `tests/test_translator_queue_flush_results.py`, `tests/test_translator_imports.py`, and `tests/test_translator_import_legacy.py`:

- `SmokeTranslationResult` → `TranslationResult`
- `SmokeTranslationInput` → `TranslationInput`
- `from translator.smoke import LocalOpenAICompatibleTranslationProvider` → `from translator.provider import LocalOpenAICompatibleTranslationProvider`

Run the substitutions and verify nothing was missed:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
sed -i '' \
  -e 's/SmokeTranslationResult/TranslationResult/g' \
  -e 's/SmokeTranslationInput/TranslationInput/g' \
  -e 's/from translator\.smoke import/from translator.provider import/g' \
  tests/test_translator_workflow.py \
  tests/test_translator_queue_flush_results.py \
  tests/test_translator_imports.py \
  tests/test_translator_import_legacy.py
rg -n "Smoke|translator\.smoke" tests/test_translator_*.py
```
Expected: the `rg` prints nothing (no residual smoke references in tests).

- [ ] **Step 1.8: Delete the smoke files**

```bash
git rm translator/smoke.py translator/provider_smoke.py translator/queue_smoke.py
```

- [ ] **Step 1.9: Run the full suite + dg check — expect GREEN**

```bash
uv run pytest -q 2>&1 | tail -5
uv run dg check defs
echo "=== no production smoke references remain ==="
rg -n "Smoke|from translator\.smoke|translator\.provider_smoke|translator\.queue_smoke" translator/ tests/
```
Expected: suite passes (same count as Step 1.1), `dg check defs` exits 0, and the final `rg` prints nothing.

- [ ] **Step 1.10: Commit**

```bash
git add translator/types.py translator/provider.py translator/errors.py \
        translator/queue.py translator/activities.py \
        tests/test_translator_workflow.py tests/test_translator_queue_flush_results.py \
        tests/test_translator_imports.py tests/test_translator_import_legacy.py
# the smoke files were already staged for deletion by `git rm` in Step 1.8
git commit -m "refactor(translator): de-smoke — rename Translation types, add provider.py + errors.py, drop smoke-test CLIs"
```

---

### Task 2: `translator/llm_batch.py` — shared LLM batch call

**Files:**
- Create: `translator/llm_batch.py`
- Create: `tests/test_translator_llm_batch.py`

**Interfaces:**
- Consumes: `ClaimedTranslationItem` from `translator.queue`; `TranslationInput`, `TranslationResult` from `translator.types`; `LocalOpenAICompatibleTranslationProvider` protocol from `translator.provider` (the `.translate(items, *, timeout_seconds)` method).
- Produces: `translate_batch(items: list[ClaimedTranslationItem], *, provider: Any, timeout: int) -> list[TranslationResult]` — results have `item_id` values matching the **queue** `item_id` (not positional provider ids). Raises on provider error — caller handles fail_batch.

- [ ] **Step 2.1: Write the failing test**

```python
# tests/test_translator_llm_batch.py
from translator.llm_batch import translate_batch
from translator.queue import ClaimedTranslationItem
from translator.types import TranslationInput, TranslationResult


def _claimed(n: int) -> list[ClaimedTranslationItem]:
    return [
        ClaimedTranslationItem(
            item_id=f"queue-id-{i:02d}",
            batch_id="batch-1",
            source_text=f"tekst {i}",
            target_language="en",
            attempt_count=0,
        )
        for i in range(n)
    ]


class _FakeProvider:
    """Echoes source_text uppercased; tracks calls."""

    called_with: list[list[TranslationInput]] = []

    def translate(
        self,
        items: list[TranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[TranslationResult]:
        _FakeProvider.called_with.append(items)
        return [
            TranslationResult(item_id=item.item_id, translated_text=item.source_text.upper())
            for item in items
        ]


def test_translate_batch_maps_results_to_queue_ids():
    _FakeProvider.called_with = []
    items = _claimed(3)
    provider = _FakeProvider()

    results = translate_batch(items, provider=provider, timeout=30)

    # Results must be keyed by QUEUE item_id, not positional provider ids.
    assert len(results) == 3
    result_by_id = {r.item_id: r for r in results}
    for i, item in enumerate(items):
        assert item.item_id in result_by_id
        assert result_by_id[item.item_id].translated_text == f"TEKST {i}"


def test_translate_batch_calls_provider_once():
    _FakeProvider.called_with = []
    items = _claimed(2)
    translate_batch(items, provider=_FakeProvider(), timeout=30)
    assert len(_FakeProvider.called_with) == 1
    # Provider sees positional ids (batch-item-00, batch-item-01), not queue ids.
    sent_ids = {item.item_id for item in _FakeProvider.called_with[0]}
    queue_ids = {item.item_id for item in items}
    assert sent_ids.isdisjoint(queue_ids)  # provider ids differ from queue ids


def test_translate_batch_propagates_provider_error():
    class _ErrorProvider:
        def translate(self, items, *, timeout_seconds):
            raise RuntimeError("LLM unreachable")

    import pytest
    with pytest.raises(RuntimeError, match="LLM unreachable"):
        translate_batch(_claimed(1), provider=_ErrorProvider(), timeout=30)


def test_translate_batch_empty_items_returns_empty():
    results = translate_batch([], provider=_FakeProvider(), timeout=30)
    assert results == []
```

- [ ] **Step 2.2: Run test — expect FAIL**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translator_llm_batch.py -v 2>&1 | head -20
```
Expected: `ModuleNotFoundError: No module named 'translator.llm_batch'`

- [ ] **Step 2.3: Implement `translator/llm_batch.py`**

```python
# translator/llm_batch.py
"""Shared LLM batch translation call used by every source's TranslateWorkflow."""
from __future__ import annotations

from typing import Any

from translator.queue import ClaimedTranslationItem
from translator.types import TranslationInput, TranslationResult


def translate_batch(
    items: list[ClaimedTranslationItem],
    *,
    provider: Any,
    timeout: int,
) -> list[TranslationResult]:
    """Call the LLM provider for a list of claimed queue items.

    Constructs positional provider item_ids (batch-item-NNN) so the provider
    response can be matched back to queue item_ids.  Raises on provider error —
    the caller must call ``queue.fail_batch`` and categorise the exception.

    Args:
        items:    Claimed items from the DuckDB queue.
        provider: Any object with a ``.translate(items, *, timeout_seconds)``
                  method returning ``list[TranslationResult]``.
        timeout:  Per-call timeout in seconds passed to the provider.

    Returns:
        ``TranslationResult`` list with ``item_id`` values matching the
        input ``ClaimedTranslationItem.item_id`` values (queue ids).
    """
    if not items:
        return []

    provider_inputs = [
        TranslationInput(
            item_id=f"batch-item-{index:03d}",
            source_text=item.source_text,
        )
        for index, item in enumerate(items)
    ]
    # Map provider positional ids back to queue ids.
    queue_id_by_provider_id = {
        f"batch-item-{index:03d}": item.item_id
        for index, item in enumerate(items)
    }

    raw_results = provider.translate(provider_inputs, timeout_seconds=timeout)

    return [
        TranslationResult(
            item_id=queue_id_by_provider_id[result.item_id],
            translated_text=result.translated_text,
        )
        for result in raw_results
    ]
```

- [ ] **Step 2.4: Run tests — expect PASS**

```bash
uv run pytest tests/test_translator_llm_batch.py -v
```
Expected: 4 tests pass.

- [ ] **Step 2.5: Commit**

```bash
git add translator/llm_batch.py tests/test_translator_llm_batch.py
git commit -m "feat(translator): add shared llm_batch.translate_batch"
```

---

### Task 3: `translator/clickhouse.py` — add `query_arrow()` helper

**Files:**
- Modify: `translator/clickhouse.py`
- Modify: `tests/test_translator_scan.py` (add 2 tests for `query_arrow` shape)

**Interfaces:**
- Consumes: `clickhouse_connect` client (already has `.query_arrow(sql, parameters=dict)` method returning a `pyarrow.Table`).
- Produces: `query_arrow(client: Any, sql: str, parameters: dict[str, Any] | None = None) -> Any` — returns whatever the client's `query_arrow` returns (a PyArrow Table). Type annotation uses `Any` to avoid a hard pyarrow dep at import time.

- [ ] **Step 3.1: Write the failing tests**

Add to `tests/test_translator_scan.py`:

```python
# Add at the bottom of tests/test_translator_scan.py
from translator.clickhouse import query_arrow


class _FakeArrowClient:
    """Minimal fake that records calls and returns a list as a stand-in for a Table."""

    def __init__(self, rows):
        self._rows = rows
        self.calls: list[dict] = []

    def query_arrow(self, sql, *, parameters=None):
        self.calls.append({"sql": sql, "parameters": parameters})
        return self._rows  # stand-in for pa.Table


def test_query_arrow_delegates_to_client_query_arrow():
    client = _FakeArrowClient(["row1", "row2"])
    result = query_arrow(client, "SELECT 1", {"p": "v"})
    assert result == ["row1", "row2"]
    assert len(client.calls) == 1
    assert client.calls[0]["sql"] == "SELECT 1"
    assert client.calls[0]["parameters"] == {"p": "v"}


def test_query_arrow_passes_empty_dict_when_parameters_is_none():
    client = _FakeArrowClient([])
    query_arrow(client, "SELECT 2")
    assert client.calls[0]["parameters"] == {}
```

- [ ] **Step 3.2: Run tests — expect FAIL**

```bash
uv run pytest tests/test_translator_scan.py::test_query_arrow_delegates_to_client_query_arrow -v
```
Expected: `ImportError: cannot import name 'query_arrow'`

- [ ] **Step 3.3: Add `query_arrow` to `translator/clickhouse.py`**

Open `translator/clickhouse.py` and add after the `scan_untranslated_terms` function:

```python
def query_arrow(client: Any, sql: str, parameters: dict[str, Any] | None = None) -> Any:
    """Execute a ClickHouse query and return the result as a PyArrow Table.

    Wraps ``client.query_arrow()`` (clickhouse-connect) with a default empty
    parameters dict so callers never need to special-case None.
    """
    return client.query_arrow(sql, parameters=parameters or {})
```

Also add `Any` to the imports at the top if not already there:
```python
from typing import Any
```
(`clickhouse.py` already imports `Any` via `from typing import Any`.)

- [ ] **Step 3.4: Run tests — expect PASS**

```bash
uv run pytest tests/test_translator_scan.py -v
```
Expected: all tests (including the 2 new ones) pass.

- [ ] **Step 3.5: Commit**

```bash
git add translator/clickhouse.py tests/test_translator_scan.py
git commit -m "feat(translator/clickhouse): add query_arrow() helper"
```

---

### Task 4: `translator/norway_brreg/` package + `config.py`

**Files:**
- Create: `translator/norway_brreg/__init__.py`
- Create: `translator/norway_brreg/config.py`
- Create: `tests/test_norway_brreg_config.py`
- Modify: `translator/registry.py` — delegate to `norway_brreg/config.py` so `import_legacy.py` keeps working

**Interfaces:**
- Produces:
  - `FieldConfig(original_col: str, static_map: tuple[tuple[str,str],...] | None, static_key_col: str | None)` — same as current `registry.FieldConfig`
  - `SourceConfig(source_slug: str, source_lang: str, ch_table: str, fields: tuple[FieldConfig,...])` — same as current `registry.SourceConfig`
  - `get_config() -> SourceConfig` — returns the Norway Brreg config
  - `translator.registry.get_source_config("norway_brreg")` continues to work (imports from `norway_brreg/config.py`)

- [ ] **Step 4.1: Write the failing tests**

```python
# tests/test_norway_brreg_config.py
import pytest

from translator.norway_brreg.config import FieldConfig, SourceConfig, get_config
from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_get_config_returns_source_config():
    cfg = get_config()
    assert isinstance(cfg, SourceConfig)
    assert cfg.source_slug == "norway_brreg"
    assert cfg.source_lang == "no"
    assert cfg.ch_table == "corpscout.no_companies"


def test_config_has_three_fields_two_dynamic_one_static():
    cfg = get_config()
    assert len(cfg.fields) == 3

    assert cfg.fields[0] == FieldConfig(original_col="articles_purpose_original")
    assert cfg.fields[1] == FieldConfig(original_col="activity_text_original")

    lf = cfg.fields[2]
    assert lf.original_col == "legal_form_description_original"
    assert lf.static_key_col == "legal_form_code"
    assert lf.static_map is not None
    assert lf.static_map_dict() == LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_dynamic_fields_have_no_static_map():
    cfg = get_config()
    for f in cfg.fields[:2]:
        assert f.static_map is None
        assert f.static_key_col is None


def test_static_map_covers_all_40_legal_form_codes():
    cfg = get_config()
    mapping = cfg.fields[2].static_map_dict() or {}
    for code in ("FLI", "ESEK", "UTLA", "BRL", "KBO", "SAM", "ANNA", "KF", "AS", "ENK"):
        assert mapping.get(code), f"{code} must have an English translation"
    assert len(mapping) >= 40


def test_registry_still_resolves_norway_brreg():
    """registry.get_source_config must still work for import_legacy.py compatibility."""
    from translator.registry import get_source_config
    cfg = get_source_config("norway_brreg")
    assert cfg.source_slug == "norway_brreg"


def test_registry_unknown_source_raises_key_error():
    from translator.registry import get_source_config
    with pytest.raises(KeyError):
        get_source_config("atlantis")
```

- [ ] **Step 4.2: Run tests — expect FAIL**

```bash
uv run pytest tests/test_norway_brreg_config.py -v 2>&1 | head -10
```
Expected: `ModuleNotFoundError: No module named 'translator.norway_brreg'`

- [ ] **Step 4.3: Create `translator/norway_brreg/__init__.py`**

```python
# translator/norway_brreg/__init__.py
```
(empty file)

- [ ] **Step 4.4: Create `translator/norway_brreg/config.py`**

```python
# translator/norway_brreg/config.py
"""Per-source translation config for Norway Brreg.

Owns the FieldConfig / SourceConfig dataclasses (shared across the translator
package via imports from this module or from translator.registry which delegates
here).
"""
from __future__ import annotations

from dataclasses import dataclass

from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE


@dataclass(frozen=True)
class FieldConfig:
    """Config for a single translatable field.

    ``static_map`` is a tuple-of-pairs (hashable) so FieldConfig itself is
    hashable.  Convert to dict at use-time via ``static_map_dict()``.
    ``static_key_col`` is the companion CH column whose value is the map key.
    """

    original_col: str
    static_map: tuple[tuple[str, str], ...] | None = None
    static_key_col: str | None = None

    def static_map_dict(self) -> dict[str, str] | None:
        if self.static_map is None:
            return None
        return dict(self.static_map)


@dataclass(frozen=True)
class SourceConfig:
    source_slug: str
    source_lang: str
    ch_table: str
    fields: tuple[FieldConfig, ...]


_NORWAY_BRREG_CONFIG = SourceConfig(
    source_slug="norway_brreg",
    source_lang="no",
    ch_table="corpscout.no_companies",
    fields=(
        FieldConfig(original_col="articles_purpose_original"),
        FieldConfig(original_col="activity_text_original"),
        FieldConfig(
            original_col="legal_form_description_original",
            static_map=tuple(LEGAL_FORM_DESCRIPTION_EN_BY_CODE.items()),
            static_key_col="legal_form_code",
        ),
    ),
)


def get_config() -> SourceConfig:
    """Return the Norway Brreg translation source config."""
    return _NORWAY_BRREG_CONFIG
```

- [ ] **Step 4.5: Update `translator/registry.py` to delegate**

Replace the entire `registry.py` with a thin shim that imports from the per-source package. This preserves `import_legacy.py` and the existing tests that import from `translator.registry`.

```python
# translator/registry.py
"""Backward-compat shim — imports FieldConfig / SourceConfig from per-source packages.

New code should import directly from e.g. ``translator.norway_brreg.config``.
This module exists solely so ``translator.import_legacy`` and legacy tests keep
working without change until they are updated in Task 10.
"""
from __future__ import annotations

from translator.norway_brreg.config import FieldConfig, SourceConfig, get_config

_SOURCES = {
    "norway_brreg": get_config(),
}


def get_source_config(source_slug: str) -> SourceConfig:
    return _SOURCES[source_slug]


__all__ = ["FieldConfig", "SourceConfig", "get_source_config"]
```

- [ ] **Step 4.6: Run tests — expect PASS**

```bash
uv run pytest tests/test_norway_brreg_config.py tests/test_translator_registry.py -v
```
Expected: all pass (both old registry tests and new config tests).

- [ ] **Step 4.7: Commit**

```bash
git add translator/norway_brreg/__init__.py translator/norway_brreg/config.py \
        translator/registry.py tests/test_norway_brreg_config.py
git commit -m "feat(translator): add norway_brreg/config.py, shim registry.py"
```

---

### Task 5: `translator/norway_brreg/seed.py` — Arrow bulk INSERT seed

**Files:**
- Create: `translator/norway_brreg/seed.py`
- Create: `tests/test_norway_brreg_seed.py`

**Interfaces:**
- Consumes:
  - `query_arrow(client, sql, parameters)` from `translator.clickhouse`
  - `build_scan_sql(config, field)` from `translator.clickhouse` (unchanged, same signature)
  - `TranslationQueue(path).initialize()` from `translator.queue`
  - `flush_translations(client, config, rows, *, provider, model, version, run_id)` from `translator.flush`
  - `FlushTranslationRow` from `translator.queue`
  - `FieldConfig`, `SourceConfig` from `translator.norway_brreg.config`
- Produces:
  - `SeedResult(dynamic_enqueued: int, static_flushed: int)` — frozen dataclass
  - `build_queue(config: SourceConfig, ch_client: Any, queue_duckdb_path: str | Path, *, heartbeat_fn: Callable[[], None] | None = None) -> SeedResult`

**Key SQL invariants:**
- `item_id = sha256(sha256(source_text) || '|en')` in DuckDB SQL (matches `TranslationQueueItem.item_id` from Python).
- `source_text_hash = sha256(source_text)` in DuckDB SQL.
- Location hash: `sha256(concat_ws('|', 'clickhouse', ch_table, '', field_col, sha256(source_text), 'en'))`.
- Arrow table registered as `_scan_result` in DuckDB, then unregistered after INSERT.
- Static fields resolved via `static_map_dict().get(static_key, "")`, written via `flush_translations(..., provider="static", model="static")`.

- [ ] **Step 5.1: Write the failing tests**

```python
# tests/test_norway_brreg_seed.py
"""Unit tests for translator/norway_brreg/seed.py.

Uses a real temp DuckDB queue and a fake ClickHouse client that returns a small
PyArrow table.  Does NOT connect to a real ClickHouse instance.
"""
import pyarrow as pa
import pytest

from translator.norway_brreg.config import get_config
from translator.norway_brreg.seed import SeedResult, build_queue
from translator.queue import TranslationQueue


# ---------------------------------------------------------------------------
# Fake ClickHouse client
# ---------------------------------------------------------------------------


class _FakeCHClient:
    """Returns pre-canned Arrow tables per (sql, parameters) call; records calls."""

    def __init__(self, arrow_per_column: dict[str, pa.Table]):
        """arrow_per_column: maps original_col name → Arrow table to return."""
        self._data = arrow_per_column
        self.calls: list[dict] = []
        self.flush_calls: list[dict] = []

    def query_arrow(self, sql: str, *, parameters: dict | None = None) -> pa.Table:
        col = (parameters or {}).get("column", "")
        self.calls.append({"sql": sql, "column": col, "parameters": parameters})
        return self._data.get(col, pa.table({"source_text": pa.array([], type=pa.string())}))

    # flush_translations uses client.command() and client.insert()
    def command(self, sql, parameters=None):
        self.flush_calls.append({"type": "command", "sql": sql})

    def insert(self, table, data, column_names=None):
        self.flush_calls.append({"type": "insert", "table": table, "data": data})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dynamic_arrow(texts: list[str]) -> pa.Table:
    return pa.table({"source_text": pa.array(texts, type=pa.string())})


def _static_arrow(texts: list[str], keys: list[str]) -> pa.Table:
    return pa.table({
        "source_text": pa.array(texts, type=pa.string()),
        "static_key": pa.array(keys, type=pa.string()),
    })


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_queue_inserts_dynamic_items_into_duckdb(tmp_path):
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["Holding", "Bygg"]),
        "activity_text_original": _dynamic_arrow(["Energi"]),
        "legal_form_description_original": _static_arrow(["Aksjeselskap"], ["AS"]),
    })

    result = build_queue(config, ch, str(tmp_path / "q.duckdb"))

    assert isinstance(result, SeedResult)
    assert result.dynamic_enqueued == 3  # 2 + 1 dynamic terms
    # static term flushed directly to CH (not in DuckDB queue)
    assert result.static_flushed == 1

    # DuckDB queue must contain exactly the 3 dynamic items.
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    summary = q.summary()
    assert summary.total_items == 3
    assert summary.pending_items == 3


def test_build_queue_item_ids_match_python_sha256(tmp_path):
    """Item IDs inserted by SQL must match the Python hash in TranslationQueueItem."""
    from translator.queue import TranslationQueueItem

    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["Holdingselskap"]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow([], []),
    })
    build_queue(config, ch, str(tmp_path / "q.duckdb"))

    # Compute expected item_id the same way Python does.
    expected = TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.no_companies",
        source_pk="",
        source_field="articles_purpose_original",
        source_text="Holdingselskap",
        target_language="en",
    ).item_id

    import duckdb
    with duckdb.connect(str(tmp_path / "q.duckdb")) as conn:
        row = conn.execute("SELECT item_id FROM translation_items LIMIT 1").fetchone()
    assert row is not None
    assert row[0] == expected


def test_build_queue_idempotent_on_second_call(tmp_path):
    """Re-seeding must not duplicate items (ON CONFLICT DO NOTHING)."""
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["Holding"]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow([], []),
    })
    build_queue(config, ch, str(tmp_path / "q.duckdb"))
    result2 = build_queue(config, ch, str(tmp_path / "q.duckdb"))

    # Second call enqueues 0 new items (idempotent).
    assert result2.dynamic_enqueued == 0
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    assert q.summary().total_items == 1


def test_build_queue_static_unknown_code_not_flushed(tmp_path):
    """Unknown static-map codes must produce no flush rows."""
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow([]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow(["Ukjent form"], ["UNKNOWN_CODE"]),
    })
    result = build_queue(config, ch, str(tmp_path / "q.duckdb"))
    assert result.static_flushed == 0
    # No INSERT command fired for unknown code.
    assert not any("INSERT INTO corpscout.text_translations" in c.get("sql", "") for c in ch.flush_calls)


def test_build_queue_calls_heartbeat(tmp_path):
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["A"]),
        "activity_text_original": _dynamic_arrow(["B"]),
        "legal_form_description_original": _static_arrow(["Aksjeselskap"], ["AS"]),
    })
    heartbeat_calls = []
    build_queue(config, ch, str(tmp_path / "q.duckdb"), heartbeat_fn=heartbeat_calls.append)
    # At least one heartbeat per field (3 fields).
    assert len(heartbeat_calls) >= 3


def test_build_queue_empty_source_returns_zero_counts(tmp_path):
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow([]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow([], []),
    })
    result = build_queue(config, ch, str(tmp_path / "q.duckdb"))
    assert result.dynamic_enqueued == 0
    assert result.static_flushed == 0
```

- [ ] **Step 5.2: Run tests — expect FAIL**

```bash
uv run pytest tests/test_norway_brreg_seed.py -v 2>&1 | head -10
```
Expected: `ModuleNotFoundError: No module named 'translator.norway_brreg.seed'`

- [ ] **Step 5.3: Implement `translator/norway_brreg/seed.py`**

```python
# translator/norway_brreg/seed.py
"""Bulk seed: ClickHouse → DuckDB translation queue via Arrow."""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from translator.clickhouse import build_scan_sql, query_arrow
from translator.flush import FlushTranslationRow, flush_translations
from translator.norway_brreg.config import SourceConfig
from translator.queue import TranslationQueue


@dataclass(frozen=True)
class SeedResult:
    dynamic_enqueued: int
    static_flushed: int


def build_queue(
    config: SourceConfig,
    ch_client: Any,
    queue_duckdb_path: str | Path,
    *,
    heartbeat_fn: Callable[[], None] | None = None,
) -> SeedResult:
    """Seed the DuckDB translation queue for one source.

    For each dynamic field: ``query_arrow`` (LEFT ANTI JOIN) → register in
    DuckDB → bulk INSERT into queue tables, hashes computed in DuckDB SQL,
    ON CONFLICT DO NOTHING (idempotent).

    For each static field: resolve via static-map dict → flush directly to
    ``corpscout.text_translations`` (provider='static').

    Calls ``heartbeat_fn`` after every field for activity liveness.
    """
    queue_path = Path(queue_duckdb_path)
    TranslationQueue(queue_path).initialize()

    dynamic_enqueued = 0
    static_flushed = 0
    version = int(time.time())

    with duckdb.connect(str(queue_path)) as conn:
        for field in config.fields:
            sql = build_scan_sql(config, field)
            params = {"table": config.ch_table, "column": field.original_col}
            arrow_table = query_arrow(ch_client, sql, params)
            field_col = field.original_col
            ch_table = config.ch_table

            if field.static_map is None:
                # Dynamic field → bulk insert into DuckDB queue.
                conn.register("_scan_result", arrow_table)
                try:
                    pre_count = int(
                        conn.execute("SELECT count(*) FROM translation_items").fetchone()[0]
                    )
                    conn.execute(f"""
                        INSERT INTO translation_items (
                            item_id, source_text, source_text_hash,
                            target_language, status, attempt_count,
                            created_at, updated_at
                        )
                        SELECT
                            sha256(sha256(source_text) || '|en'),
                            source_text,
                            sha256(source_text),
                            'en',
                            'pending',
                            0,
                            current_timestamp,
                            current_timestamp
                        FROM _scan_result
                        ON CONFLICT (item_id) DO NOTHING
                    """)
                    conn.execute(f"""
                        INSERT INTO translation_locations (
                            location_id, item_id, source_duckdb_path,
                            source_table, source_pk, source_field,
                            created_at, updated_at
                        )
                        SELECT
                            sha256(concat_ws('|', 'clickhouse', '{ch_table}', '',
                                '{field_col}', sha256(source_text), 'en')),
                            sha256(sha256(source_text) || '|en'),
                            'clickhouse', '{ch_table}', '', '{field_col}',
                            current_timestamp, current_timestamp
                        FROM _scan_result
                        ON CONFLICT (location_id) DO NOTHING
                    """)
                    post_count = int(
                        conn.execute("SELECT count(*) FROM translation_items").fetchone()[0]
                    )
                    dynamic_enqueued += post_count - pre_count
                finally:
                    try:
                        conn.unregister("_scan_result")
                    except Exception:
                        pass
            else:
                # Static field → resolve dict, write directly to CH.
                mapping = field.static_map_dict() or {}
                if hasattr(arrow_table, "to_pydict"):
                    col_data = arrow_table.to_pydict()
                    texts = col_data.get("source_text", [])
                    keys = col_data.get("static_key", [""] * len(texts))
                else:
                    texts, keys = [], []

                static_rows: list[FlushTranslationRow] = []
                for source_text, static_key in zip(texts, keys):
                    translation = mapping.get(static_key or "", "")
                    if translation:
                        static_rows.append(
                            FlushTranslationRow(
                                source_column=field_col,
                                source_text=source_text,
                                translated_text=translation,
                            )
                        )
                if static_rows:
                    static_flushed += flush_translations(
                        ch_client,
                        config,
                        static_rows,
                        provider="static",
                        model="static",
                        version=version,
                        run_id="seed-static",
                    )

            if heartbeat_fn is not None:
                heartbeat_fn(f"seeded field={field_col}")

    return SeedResult(dynamic_enqueued=dynamic_enqueued, static_flushed=static_flushed)
```

- [ ] **Step 5.4: Run tests — expect PASS**

```bash
uv run pytest tests/test_norway_brreg_seed.py -v
```
Expected: all 6 tests pass.

- [ ] **Step 5.5: Commit**

```bash
git add translator/norway_brreg/seed.py tests/test_norway_brreg_seed.py
git commit -m "feat(translator/norway_brreg): add seed.py — Arrow bulk INSERT queue seed"
```

---

### Task 6: `translator/norway_brreg/dump.py` — queue → ClickHouse

**Files:**
- Create: `translator/norway_brreg/dump.py`
- Create: `tests/test_norway_brreg_dump.py`

**Interfaces:**
- Consumes:
  - `TranslationQueue(path).completed_results_for_flush()` → `list[FlushTranslationRow]`
  - `flush_translations(client, config, rows, *, provider, model, version, run_id)` from `translator.flush`
  - `SourceConfig` from `translator.norway_brreg.config`
- Produces: `dump_to_clickhouse(queue_duckdb_path: str | Path, ch_client: Any, config: SourceConfig, *, provider: str, model: str, batch_size: int = 50_000, heartbeat_fn: Callable[[], None] | None = None) -> int` — total rows written.

- [ ] **Step 6.1: Write the failing tests**

```python
# tests/test_norway_brreg_dump.py
"""Unit tests for translator/norway_brreg/dump.py."""
from __future__ import annotations

from translator.norway_brreg.config import get_config
from translator.norway_brreg.dump import dump_to_clickhouse
from translator.queue import TranslationQueue, TranslationQueueItem
from translator.types import TranslationResult


# ---------------------------------------------------------------------------
# Fake ClickHouse client (same shape as in test_translator_flush.py)
# ---------------------------------------------------------------------------


class _FakeCHClient:
    def __init__(self):
        self.commands: list[str] = []
        self.inserts: list[tuple] = []

    def command(self, sql, parameters=None):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, tuple(column_names or ())))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _enqueue_and_complete(tmp_path, texts: list[str]) -> None:
    """Seed the queue and mark all items completed."""
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    items = [
        TranslationQueueItem(
            source_duckdb_path="clickhouse",
            source_table="corpscout.no_companies",
            source_pk="",
            source_field="activity_text_original",
            source_text=text,
            target_language="en",
        )
        for text in texts
    ]
    q.enqueue_items(items)
    claimed = q.claim_batch(limit=len(items), worker_id="test")
    q.complete_batch(
        claimed,
        [TranslationResult(item_id=c.item_id, translated_text=c.source_text.upper()) for c in claimed],
        provider="fake",
        model="fake-model",
        duration_seconds=0.1,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_dump_writes_completed_rows_to_clickhouse(tmp_path):
    _enqueue_and_complete(tmp_path, ["Holdingselskap", "Bygg"])
    ch = _FakeCHClient()
    config = get_config()

    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="test-model",
    )

    assert written == 2
    # Must have created a staging table, inserted rows, and fired the INSERT … SELECT.
    assert any("CREATE TABLE" in c and "ENGINE = Memory" in c for c in ch.commands)
    assert any("INSERT INTO corpscout.text_translations" in c for c in ch.commands)
    assert any("DROP TABLE" in c for c in ch.commands)
    # Data rows were inserted into the staging table.
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0][1]) == 2


def test_dump_empty_queue_is_noop(tmp_path):
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    ch = _FakeCHClient()
    config = get_config()

    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="m",
    )

    assert written == 0
    assert ch.commands == []


def test_dump_batches_large_result_sets(tmp_path):
    texts = [f"tekst {i}" for i in range(150)]
    _enqueue_and_complete(tmp_path, texts)
    ch = _FakeCHClient()
    config = get_config()

    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="m",
        batch_size=100,
    )

    assert written == 150
    # Two batches → two inserts into two different staging tables.
    assert len(ch.inserts) == 2
    assert len(ch.inserts[0][1]) == 100
    assert len(ch.inserts[1][1]) == 50


def test_dump_calls_heartbeat_once_per_batch(tmp_path):
    texts = [f"tekst {i}" for i in range(200)]
    _enqueue_and_complete(tmp_path, texts)
    ch = _FakeCHClient()
    config = get_config()
    heartbeats: list = []

    dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="m",
        batch_size=100,
        heartbeat_fn=heartbeats.append,
    )

    assert len(heartbeats) == 2  # 200 rows / 100 per batch = 2 batches


def test_dump_skips_empty_translations(tmp_path):
    """flush_translations already drops empty translated_text; dump honours that."""
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    item = TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.no_companies",
        source_pk="",
        source_field="activity_text_original",
        source_text="Tomtekst",
        target_language="en",
    )
    q.enqueue_items([item])
    claimed = q.claim_batch(limit=1, worker_id="t")
    # Translate to empty string — flush_translations will skip it.
    q.complete_batch(
        claimed,
        [TranslationResult(item_id=claimed[0].item_id, translated_text="")],
        provider="fake",
        model="fake",
        duration_seconds=0.0,
    )

    ch = _FakeCHClient()
    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, get_config(),
        provider="local-llm", model="m",
    )
    # Empty translation → 0 written (flush_translations skips it).
    assert written == 0
```

- [ ] **Step 6.2: Run tests — expect FAIL**

```bash
uv run pytest tests/test_norway_brreg_dump.py -v 2>&1 | head -10
```
Expected: `ModuleNotFoundError: No module named 'translator.norway_brreg.dump'`

- [ ] **Step 6.3: Implement `translator/norway_brreg/dump.py`**

```python
# translator/norway_brreg/dump.py
"""Queue → ClickHouse dump for Norway Brreg translations.

Reads completed results from the DuckDB queue and writes them to
``corpscout.text_translations`` in batched staging-table inserts (reusing
``translator.flush.flush_translations``).  Self-contained — no shared dump core.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from translator.flush import flush_translations
from translator.norway_brreg.config import SourceConfig
from translator.queue import TranslationQueue


def dump_to_clickhouse(
    queue_duckdb_path: str | Path,
    ch_client: Any,
    config: SourceConfig,
    *,
    provider: str,
    model: str,
    batch_size: int = 50_000,
    heartbeat_fn: Callable[[], None] | None = None,
) -> int:
    """Write completed queue results to ``corpscout.text_translations``.

    Reads all completed items from the DuckDB queue, chunks them into batches
    of ``batch_size``, and writes each chunk via a ClickHouse staging table
    (``flush_translations`` pattern: CREATE Memory table → INSERT → INSERT INTO
    text_translations → DROP).  Calls ``heartbeat_fn`` after each batch.

    Returns the total number of rows written (empty translations are skipped).
    """
    rows = TranslationQueue(queue_duckdb_path).completed_results_for_flush()
    if not rows:
        return 0

    version = int(time.time())
    written = 0

    for batch_idx, start in enumerate(range(0, len(rows), batch_size)):
        chunk = rows[start : start + batch_size]
        written += flush_translations(
            ch_client,
            config,
            chunk,
            provider=provider,
            model=model,
            version=version,
            run_id=f"dump-batch-{batch_idx}",
        )
        if heartbeat_fn is not None:
            heartbeat_fn(f"dumped batch {batch_idx}, total_written={written}")

    return written
```

- [ ] **Step 6.4: Run tests — expect PASS**

```bash
uv run pytest tests/test_norway_brreg_dump.py -v
```
Expected: all 5 tests pass.

- [ ] **Step 6.5: Commit**

```bash
git add translator/norway_brreg/dump.py tests/test_norway_brreg_dump.py
git commit -m "feat(translator/norway_brreg): add dump.py — batched queue→ClickHouse dump"
```

---

### Task 7: `translator/norway_brreg/workflows.py` — BuildQueueWorkflow + TranslateWorkflow

**Files:**
- Create: `translator/norway_brreg/workflows.py`
- Create: `tests/test_norway_brreg_workflows.py`

**Interfaces:**
- Consumes: `SeedResult`, `build_queue` (Task 5); `dump_to_clickhouse` (Task 6); `translate_batch` (Task 2); `TranslationQueue`, `ClaimedTranslationItem` (queue.py); `clickhouse_client_from_env` (clickhouse.py); `_categorize_exception` (`translator.errors`); `_parse_extra_body`, `LocalOpenAICompatibleTranslationProvider` (`translator.provider`).
- Also creates the shared `translator/task_queues.py` with `BUILD_TASK_QUEUE = "translation-build"` and `LLM_TASK_QUEUE = "translation-llm"` (imported by `workflows.py`, `worker.py`, `assets.py`).
- Produces (used by Tasks 8 and 9):
  - `BUILD_TASK_QUEUE`, `LLM_TASK_QUEUE` (in `translator/task_queues.py`); re-imported into `workflows.py`
  - `BuildQueueWorkflowInput(source_slug, queue_duckdb_path, translate_workflow_id, translate_task_queue, batch_size, max_tokens, extra_body_json, max_batch_failures)` — frozen dataclass
  - `BuildQueueWorkflowOutput(dynamic_enqueued, static_flushed)` — frozen dataclass
  - `TranslateWorkflowInput(source_slug, queue_duckdb_path, batch_size, max_tokens, extra_body_json, max_batch_failures)` — frozen dataclass
  - `TranslateWorkflowOutput(completed_items, failed_retryable_items, flushed_rows, successful_batches, failed_batches)` — frozen dataclass
  - `BuildQueueWorkflow` — `@workflow.defn` class
  - `TranslateWorkflow` — `@workflow.defn` class
  - Activities (for worker registration): `build_queue_activity`, `start_translate_workflow_activity`, `translate_loop_activity`, `dump_activity`, `summarize_queue_activity`

**Activity options (applied to every long activity):**
```python
HEARTBEAT_TIMEOUT = timedelta(seconds=150)
START_TO_CLOSE_TIMEOUT = timedelta(hours=24)
SHORT_TIMEOUT = timedelta(seconds=60)   # for the handoff activity
RETRY_POLICY = RetryPolicy(maximum_attempts=3)
```

- [ ] **Step 7.1: Write the failing tests**

```python
# tests/test_norway_brreg_workflows.py
"""Temporal WorkflowEnvironment tests for BuildQueueWorkflow + TranslateWorkflow."""
from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from translator.norway_brreg.seed import SeedResult
from translator.norway_brreg import workflows as wf
from translator.queue import TranslationQueue, TranslationQueueItem
from translator.types import TranslationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_queue(tmp_path, texts: list[str]) -> str:
    """Pre-seed a DuckDB queue with pending items and return path as str."""
    path = tmp_path / "q.duckdb"
    q = TranslationQueue(path)
    q.initialize()
    items = [
        TranslationQueueItem(
            source_duckdb_path="clickhouse",
            source_table="corpscout.no_companies",
            source_pk="",
            source_field="activity_text_original",
            source_text=text,
            target_language="en",
        )
        for text in texts
    ]
    q.enqueue_items(items)
    return str(path)


# ---------------------------------------------------------------------------
# BuildQueueWorkflow tests
# ---------------------------------------------------------------------------


def test_build_queue_workflow_seeds_then_starts_translate(tmp_path, monkeypatch):
    """BuildQueueWorkflow must call seed, then start TranslateWorkflow (USE_EXISTING)."""
    seed_calls = []
    start_calls = []

    def fake_build_queue_once(params: wf.BuildQueueActivityInput) -> SeedResult:
        seed_calls.append(params)
        return SeedResult(dynamic_enqueued=7, static_flushed=1)

    monkeypatch.setattr(wf, "build_queue_once", fake_build_queue_once)

    # Register a fake handoff activity (same name as the real one) so the
    # workflow's start_translate_workflow_activity call resolves to this stub
    # instead of connecting to a real Temporal server.
    @activity.defn(name="start_translate_workflow_activity")
    async def _fake_start(params: wf.StartTranslateWorkflowInput) -> str:
        start_calls.append(params)
        return "fake-translate-run-id"

    params = wf.BuildQueueWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path=str(tmp_path / "q.duckdb"),
        translate_workflow_id="translate-norway_brreg",
        translate_task_queue="translation-build",
        batch_size=50,
        max_tokens=8192,
        extra_body_json="{}",
        max_batch_failures=5,
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-bq",
                workflows=[wf.BuildQueueWorkflow],
                activities=[wf.build_queue_activity, _fake_start],
            ):
                return await env.client.execute_workflow(
                    wf.BuildQueueWorkflow.run,
                    params,
                    id=f"test-{uuid.uuid4()}",
                    task_queue="test-bq",
                )

    result = asyncio.run(_run())

    assert result.dynamic_enqueued == 7
    assert result.static_flushed == 1
    assert len(seed_calls) == 1
    assert len(start_calls) == 1
    # Handoff must target the correct workflow id.
    assert start_calls[0].workflow_id == "translate-norway_brreg"
    assert start_calls[0].source_slug == "norway_brreg"


# ---------------------------------------------------------------------------
# TranslateWorkflow tests
# ---------------------------------------------------------------------------


def test_translate_workflow_drains_queue_and_dumps(tmp_path, monkeypatch):
    """TranslateWorkflow must drain queue items then dump to ClickHouse."""
    queue_path = _seed_queue(tmp_path, ["Holdingselskap", "Bygg"])
    dump_calls = []

    def fake_translate_loop_once(params: wf.TranslateLoopActivityInput) -> wf.TranslateLoopResult:
        # Directly drain the real DuckDB queue using the fake provider.
        from translator.queue import TranslationQueue as TQ
        q = TQ(params.queue_duckdb_path)
        q.initialize()
        total_completed = 0
        while True:
            claimed = q.claim_batch(limit=params.batch_size, worker_id="test")
            if not claimed:
                break
            q.complete_batch(
                claimed,
                [TranslationResult(item_id=c.item_id, translated_text=c.source_text.upper()) for c in claimed],
                provider="fake",
                model="fake",
                duration_seconds=0.0,
            )
            total_completed += len(claimed)
        return wf.TranslateLoopResult(
            completed_items=total_completed, failed_batches=0, successful_batches=1
        )

    monkeypatch.setattr(wf, "translate_loop_once", fake_translate_loop_once)

    def fake_dump_once(params: wf.DumpActivityInput) -> int:
        dump_calls.append(params)
        return 2

    monkeypatch.setattr(wf, "dump_once", fake_dump_once)

    translate_params = wf.TranslateWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path=queue_path,
        batch_size=10,
        max_tokens=64,
        extra_body_json="{}",
        max_batch_failures=5,
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            # Build worker (workflow + dump + summarize on the workflow's own queue)
            # AND a separate LLM-queue worker for translate_loop_activity — this
            # proves the loop is routed to LLM_TASK_QUEUE (only that worker can
            # run it; if routing were wrong the workflow would never complete).
            async with Worker(
                env.client,
                task_queue="test-tr",
                workflows=[wf.TranslateWorkflow],
                activities=[wf.dump_activity, wf.summarize_queue_activity],
            ), Worker(
                env.client,
                task_queue=wf.LLM_TASK_QUEUE,
                activities=[wf.translate_loop_activity],
            ):
                return await env.client.execute_workflow(
                    wf.TranslateWorkflow.run,
                    translate_params,
                    id=f"test-{uuid.uuid4()}",
                    task_queue="test-tr",
                )

    result = asyncio.run(_run())

    assert result.completed_items == 2
    assert result.flushed_rows == 2
    assert len(dump_calls) == 1
    assert dump_calls[0].queue_duckdb_path == queue_path


def test_translate_workflow_tolerates_max_batch_failures(tmp_path, monkeypatch):
    """TranslateWorkflow must stop after max_batch_failures and still dump."""
    queue_path = _seed_queue(tmp_path, ["A", "B"])
    dump_calls = []

    def always_fail_loop(params: wf.TranslateLoopActivityInput) -> wf.TranslateLoopResult:
        return wf.TranslateLoopResult(completed_items=0, failed_batches=6, successful_batches=0)

    monkeypatch.setattr(wf, "translate_loop_once", always_fail_loop)

    def fake_dump_once(params: wf.DumpActivityInput) -> int:
        dump_calls.append(params)
        return 0

    monkeypatch.setattr(wf, "dump_once", fake_dump_once)

    translate_params = wf.TranslateWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path=queue_path,
        batch_size=10,
        max_tokens=64,
        extra_body_json="{}",
        max_batch_failures=5,
    )

    async def _run():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-tr2",
                workflows=[wf.TranslateWorkflow],
                activities=[wf.dump_activity, wf.summarize_queue_activity],
            ), Worker(
                env.client,
                task_queue=wf.LLM_TASK_QUEUE,
                activities=[wf.translate_loop_activity],
            ):
                return await env.client.execute_workflow(
                    wf.TranslateWorkflow.run,
                    translate_params,
                    id=f"test-{uuid.uuid4()}",
                    task_queue="test-tr2",
                )

    result = asyncio.run(_run())
    # dump still called even on failure.
    assert len(dump_calls) == 1
    assert result.failed_batches == 6
```

- [ ] **Step 7.2: Run tests — expect FAIL**

```bash
uv run pytest tests/test_norway_brreg_workflows.py -v 2>&1 | head -10
```
Expected: `ModuleNotFoundError: No module named 'translator.norway_brreg.workflows'`

- [ ] **Step 7.3: Implement `translator/task_queues.py` + `translator/norway_brreg/workflows.py`**

First the shared task-queue constants:

```python
# translator/task_queues.py
"""Shared Temporal task-queue names for the translator worker fleet.

- BUILD_TASK_QUEUE runs the workflows (BuildQueue + Translate) and the
  seed / dump / summarize / start-translate-handoff activities (normal concurrency).
- LLM_TASK_QUEUE runs ONLY translate_loop_activity, bounded to K concurrent on a
  single shared worker — the global LLM gate.
"""
from __future__ import annotations

BUILD_TASK_QUEUE = "translation-build"
LLM_TASK_QUEUE = "translation-llm"
```

Then the workflows:

```python
# translator/norway_brreg/workflows.py
"""BuildQueueWorkflow + TranslateWorkflow for Norway Brreg translations.

BuildQueueWorkflow:
  1. build_queue_activity  — Arrow seed → DuckDB queue (heartbeating)
  2. start_translate_workflow_activity — fires TranslateWorkflow (USE_EXISTING),
     then BuildQueueWorkflow COMPLETES.

TranslateWorkflow:
  1. translate_loop_activity — long heartbeating LLM drain loop
  2. dump_activity           — queue → corpscout.text_translations (batched, heartbeating)
  3. summarize_queue_activity — read final queue summary
"""
import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy

from translator.task_queues import LLM_TASK_QUEUE

logger = logging.getLogger("translator.norway_brreg.workflows")

HEARTBEAT_TIMEOUT = timedelta(seconds=150)
START_TO_CLOSE_TIMEOUT = timedelta(hours=24)
SHORT_TIMEOUT = timedelta(seconds=60)
RETRY_POLICY = RetryPolicy(maximum_attempts=3)

# ---------------------------------------------------------------------------
# Input / Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BuildQueueActivityInput:
    source_slug: str
    queue_duckdb_path: str


@dataclass(frozen=True)
class StartTranslateWorkflowInput:
    workflow_id: str
    task_queue: str
    source_slug: str
    queue_duckdb_path: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class TranslateLoopActivityInput:
    queue_duckdb_path: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class TranslateLoopResult:
    completed_items: int
    failed_batches: int
    successful_batches: int


@dataclass(frozen=True)
class DumpActivityInput:
    source_slug: str
    queue_duckdb_path: str


@dataclass(frozen=True)
class BuildQueueWorkflowInput:
    source_slug: str
    queue_duckdb_path: str
    translate_workflow_id: str
    translate_task_queue: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class BuildQueueWorkflowOutput:
    dynamic_enqueued: int
    static_flushed: int


@dataclass(frozen=True)
class TranslateWorkflowInput:
    source_slug: str
    queue_duckdb_path: str
    batch_size: int
    max_tokens: int
    extra_body_json: str
    max_batch_failures: int


@dataclass(frozen=True)
class TranslateWorkflowOutput:
    completed_items: int
    failed_retryable_items: int
    flushed_rows: int
    successful_batches: int
    failed_batches: int


# ---------------------------------------------------------------------------
# Activity sync implementations (_once functions for monkeypatching in tests)
# ---------------------------------------------------------------------------


def build_queue_once(params: BuildQueueActivityInput):
    from translator.clickhouse import clickhouse_client_from_env
    from translator.norway_brreg.config import get_config
    from translator.norway_brreg.seed import build_queue

    config = get_config()
    ch_client = clickhouse_client_from_env()
    try:
        return build_queue(
            config,
            ch_client,
            params.queue_duckdb_path,
            heartbeat_fn=activity.heartbeat,
        )
    finally:
        close = getattr(ch_client, "close", None)
        if callable(close):
            close()


def translate_loop_once(params: TranslateLoopActivityInput):
    from translator.errors import _categorize_exception
    from translator.llm_batch import translate_batch
    from translator.provider import (
        LocalOpenAICompatibleTranslationProvider,
        _parse_extra_body,
    )
    from translator.queue import TranslationQueue

    queue = TranslationQueue(params.queue_duckdb_path)
    queue.initialize()

    model = os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"]
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url=os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"],
        model=model,
        api_key=os.getenv("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
        max_tokens=params.max_tokens,
        extra_body=_parse_extra_body(params.extra_body_json),
    )
    success_count = 0
    failure_count = 0
    completed_items = 0
    last_heartbeat_at = time.time()

    try:
        while True:
            now = time.time()
            if now - last_heartbeat_at >= 30:
                activity.heartbeat(completed_items)
                last_heartbeat_at = now

            claimed = queue.claim_batch(limit=params.batch_size, worker_id="translate-worker")
            if not claimed:
                break

            started_at = time.perf_counter()
            try:
                results = translate_batch(claimed, provider=provider, timeout=120)
                duration = time.perf_counter() - started_at
                queue.complete_batch(
                    claimed,
                    results,
                    provider=type(provider).__name__,
                    model=model,
                    duration_seconds=duration,
                )
                success_count += 1
                completed_items += len(claimed)
                activity.heartbeat(completed_items)
                last_heartbeat_at = time.time()
            except Exception as exc:
                duration = time.perf_counter() - started_at
                error_category = _categorize_exception(exc)
                queue.fail_batch(
                    claimed,
                    error_category=error_category,
                    error_message=str(exc),
                    duration_seconds=duration,
                )
                failure_count += 1
                logger.warning(
                    "translate_loop: batch failed (%s): %s", error_category, exc
                )
                if params.max_batch_failures > 0 and failure_count > params.max_batch_failures:
                    logger.warning(
                        "translate_loop: exceeded max_batch_failures=%d, stopping",
                        params.max_batch_failures,
                    )
                    break
    finally:
        provider.close()

    return TranslateLoopResult(
        completed_items=completed_items,
        failed_batches=failure_count,
        successful_batches=success_count,
    )


def dump_once(params: DumpActivityInput):
    from translator.clickhouse import clickhouse_client_from_env
    from translator.norway_brreg.config import get_config
    from translator.norway_brreg.dump import dump_to_clickhouse

    # Resolve the model from env inside the activity (env access is not allowed
    # in the workflow sandbox); dumped rows are labelled provider='local-llm'.
    model = os.environ.get("TRANSLATION_PROVIDER_LOCAL_MODEL", "local-llm")
    config = get_config()
    ch_client = clickhouse_client_from_env()
    try:
        return dump_to_clickhouse(
            params.queue_duckdb_path,
            ch_client,
            config,
            provider="local-llm",
            model=model,
            heartbeat_fn=activity.heartbeat,
        )
    finally:
        close = getattr(ch_client, "close", None)
        if callable(close):
            close()


def summarize_queue_once(queue_duckdb_path: str) -> dict:
    from translator.queue import TranslationQueue

    s = TranslationQueue(queue_duckdb_path).summary()
    return {
        "total_items": s.total_items,
        "completed_items": s.completed_items,
        "failed_retryable_items": s.failed_retryable_items,
        "pending_items": s.pending_items,
    }


# ---------------------------------------------------------------------------
# Activity definitions
# ---------------------------------------------------------------------------


@activity.defn
async def build_queue_activity(params: BuildQueueActivityInput):
    return await asyncio.to_thread(build_queue_once, params)


@activity.defn
async def start_translate_workflow_activity(params: StartTranslateWorkflowInput) -> str:
    address = os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    client = await Client.connect(address)
    handle = await client.start_workflow(
        TranslateWorkflow.run,
        TranslateWorkflowInput(
            source_slug=params.source_slug,
            queue_duckdb_path=params.queue_duckdb_path,
            batch_size=params.batch_size,
            max_tokens=params.max_tokens,
            extra_body_json=params.extra_body_json,
            max_batch_failures=params.max_batch_failures,
        ),
        id=params.workflow_id,
        task_queue=params.task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return handle.id


@activity.defn
async def translate_loop_activity(params: TranslateLoopActivityInput) -> TranslateLoopResult:
    return await asyncio.to_thread(translate_loop_once, params)


@activity.defn
async def dump_activity(params: DumpActivityInput) -> int:
    return await asyncio.to_thread(dump_once, params)


@activity.defn
async def summarize_queue_activity(queue_duckdb_path: str) -> dict:
    return await asyncio.to_thread(summarize_queue_once, queue_duckdb_path)


# ---------------------------------------------------------------------------
# Workflow definitions
# ---------------------------------------------------------------------------


@workflow.defn
class BuildQueueWorkflow:
    """Bulk-seed the DuckDB queue from ClickHouse, then hand off to TranslateWorkflow."""

    @workflow.run
    async def run(self, params: BuildQueueWorkflowInput) -> BuildQueueWorkflowOutput:
        from translator.norway_brreg.seed import SeedResult

        seed_result: SeedResult = await workflow.execute_activity(
            build_queue_activity,
            BuildQueueActivityInput(
                source_slug=params.source_slug,
                queue_duckdb_path=params.queue_duckdb_path,
            ),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            start_to_close_timeout=START_TO_CLOSE_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        await workflow.execute_activity(
            start_translate_workflow_activity,
            StartTranslateWorkflowInput(
                workflow_id=params.translate_workflow_id,
                task_queue=params.translate_task_queue,
                source_slug=params.source_slug,
                queue_duckdb_path=params.queue_duckdb_path,
                batch_size=params.batch_size,
                max_tokens=params.max_tokens,
                extra_body_json=params.extra_body_json,
                max_batch_failures=params.max_batch_failures,
            ),
            start_to_close_timeout=SHORT_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        return BuildQueueWorkflowOutput(
            dynamic_enqueued=seed_result.dynamic_enqueued,
            static_flushed=seed_result.static_flushed,
        )


@workflow.defn
class TranslateWorkflow:
    """Drain the DuckDB queue via LLM, then dump to corpscout.text_translations."""

    @workflow.run
    async def run(self, params: TranslateWorkflowInput) -> TranslateWorkflowOutput:
        # Route the LLM drain loop to the gated LLM queue. NO schedule_to_start_timeout:
        # the loop waits in the queue until one of the K global slots frees up.
        loop_result: TranslateLoopResult = await workflow.execute_activity(
            translate_loop_activity,
            TranslateLoopActivityInput(
                queue_duckdb_path=params.queue_duckdb_path,
                batch_size=params.batch_size,
                max_tokens=params.max_tokens,
                extra_body_json=params.extra_body_json,
                max_batch_failures=params.max_batch_failures,
            ),
            task_queue=LLM_TASK_QUEUE,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            start_to_close_timeout=START_TO_CLOSE_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        # dump + summarize stay on the workflow's own (build) task queue.
        flushed: int = await workflow.execute_activity(
            dump_activity,
            DumpActivityInput(
                source_slug=params.source_slug,
                queue_duckdb_path=params.queue_duckdb_path,
            ),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            start_to_close_timeout=START_TO_CLOSE_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )

        summary: dict = await workflow.execute_activity(
            summarize_queue_activity,
            params.queue_duckdb_path,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RETRY_POLICY,
        )

        return TranslateWorkflowOutput(
            completed_items=summary["completed_items"],
            failed_retryable_items=summary["failed_retryable_items"],
            flushed_rows=flushed,
            successful_batches=loop_result.successful_batches,
            failed_batches=loop_result.failed_batches,
        )
```

> **Sandbox safety:** Neither `BuildQueueWorkflow.run` nor `TranslateWorkflow.run` reads `os.environ` or does any I/O in the workflow body — all env access happens inside activities (which run outside the deterministic sandbox). This keeps the workflows replay-deterministic.

- [ ] **Step 7.4: Run tests — expect PASS**

```bash
uv run pytest tests/test_norway_brreg_workflows.py -v
```
Expected: all 3 tests pass.

- [ ] **Step 7.5: Commit**

```bash
git add translator/task_queues.py translator/norway_brreg/workflows.py tests/test_norway_brreg_workflows.py
git commit -m "feat(translator/norway_brreg): add BuildQueueWorkflow + TranslateWorkflow + shared task_queues"
```

---

### Task 8: `translator/worker.py` — two-worker fleet + python-dotenv

**Files:**
- Modify: `pyproject.toml` — add `python-dotenv>=1.0`
- Modify: `translator/worker.py` — two-worker fleet (build + LLM gate); python-dotenv
- Modify: `translator/import_legacy.py` — switch env loading to `load_dotenv`
- Modify: `tests/test_translator_worker.py`

**Interfaces:**
- Consumes: `BUILD_TASK_QUEUE`, `LLM_TASK_QUEUE` from `translator.task_queues`; all activities + workflows from `translator.norway_brreg.workflows`.
- Produces:
  - `build_build_worker(client) -> Worker` — `task_queue=BUILD_TASK_QUEUE`, `workflows=[BuildQueueWorkflow, TranslateWorkflow]`, `activities=[build_queue_activity, start_translate_workflow_activity, dump_activity, summarize_queue_activity]`
  - `build_llm_worker(client, *, max_concurrent: int) -> Worker` — `task_queue=LLM_TASK_QUEUE`, `activities=[translate_loop_activity]`, `max_concurrent_activities=max_concurrent`
  - `llm_concurrency() -> int` — reads `TRANSLATOR_LLM_CONCURRENCY` (default 2)
  - `run_worker(...)` runs both via `asyncio.gather`

- [ ] **Step 8.1: Add `python-dotenv` to `pyproject.toml`**

Add the dependency to the `[project] dependencies` array (keep it alphabetical-ish, after `polars`):

```toml
    "polars[rtcompat]>=1.41.2",
    "pyarrow>=24.0.0",
    "python-dotenv>=1.0",
    "requests>=2.34.2",
```

Then sync the env:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv sync
```

- [ ] **Step 8.2: Update `tests/test_translator_worker.py`**

Replace the entire file (drops the two `load_env_file` unit tests; adds a `load_dotenv` override-False test and the two-worker assertions):

```python
# tests/test_translator_worker.py
import os

from dotenv import load_dotenv

from translator import worker as w
from translator.task_queues import BUILD_TASK_QUEUE, LLM_TASK_QUEUE
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    TranslateWorkflow,
)


class _FakeWorker:
    """Captures Worker(...) kwargs; one instance recorded per construction."""

    instances: list[dict] = []

    def __init__(self, client, *, task_queue, activities, workflows=None, max_concurrent_activities=None):
        _FakeWorker.instances.append({
            "task_queue": task_queue,
            "workflows": list(workflows or []),
            "activities": list(activities),
            "max_concurrent_activities": max_concurrent_activities,
        })


def _activity_names(activities) -> set[str]:
    return {getattr(a, "__name__", getattr(a, "name", "")) for a in activities}


def test_build_build_worker_registers_workflows_on_build_queue(monkeypatch):
    _FakeWorker.instances = []
    monkeypatch.setattr(w, "Worker", _FakeWorker)
    w.build_build_worker(object())

    rec = _FakeWorker.instances[0]
    assert rec["task_queue"] == BUILD_TASK_QUEUE
    assert BuildQueueWorkflow in rec["workflows"]
    assert TranslateWorkflow in rec["workflows"]
    names = _activity_names(rec["activities"])
    assert {"build_queue_activity", "start_translate_workflow_activity",
            "dump_activity", "summarize_queue_activity"} <= names
    # translate_loop runs ONLY on the LLM worker, never the build worker.
    assert "translate_loop_activity" not in names


def test_build_llm_worker_gates_translate_loop_with_k(monkeypatch):
    _FakeWorker.instances = []
    monkeypatch.setattr(w, "Worker", _FakeWorker)
    w.build_llm_worker(object(), max_concurrent=3)

    rec = _FakeWorker.instances[0]
    assert rec["task_queue"] == LLM_TASK_QUEUE
    assert rec["max_concurrent_activities"] == 3
    names = _activity_names(rec["activities"])
    assert names == {"translate_loop_activity"}
    # No workflows on the LLM worker.
    assert rec["workflows"] == []


def test_llm_concurrency_defaults_to_2_and_reads_env(monkeypatch):
    monkeypatch.delenv("TRANSLATOR_LLM_CONCURRENCY", raising=False)
    assert w.llm_concurrency() == 2
    monkeypatch.setenv("TRANSLATOR_LLM_CONCURRENCY", "5")
    assert w.llm_concurrency() == 5


def test_load_dotenv_does_not_override_existing(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("FOO=fromfile\nALREADY=fromfile\n", encoding="utf-8")
    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.setenv("ALREADY", "fromenv")

    load_dotenv(env, override=False)

    assert os.environ["FOO"] == "fromfile"     # newly set from file
    assert os.environ["ALREADY"] == "fromenv"  # real env wins; not overridden
```

- [ ] **Step 8.3: Run tests — expect FAIL**

```bash
uv run pytest tests/test_translator_worker.py -v 2>&1 | head -20
```
Expected: `AttributeError` / `ImportError` — `w.build_build_worker` / `w.llm_concurrency` / `translator.task_queues` do not exist yet.

- [ ] **Step 8.4: Rewrite `translator/worker.py`**

```python
# translator/worker.py
"""Translator Temporal worker fleet.

Starts TWO workers in one process:
  - a BUILD worker (BUILD_TASK_QUEUE): all source workflows + the
    seed / dump / summarize / start-translate handoff activities, normal concurrency;
  - an LLM worker (LLM_TASK_QUEUE): ONLY translate_loop_activity, bounded to K
    concurrent — the single global LLM gate shared by every source.

.env loading uses python-dotenv (load_dotenv(..., override=False) — shell / docker -e win).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.worker import Worker

from translator.task_queues import BUILD_TASK_QUEUE, LLM_TASK_QUEUE
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    TranslateWorkflow,
    build_queue_activity,
    dump_activity,
    start_translate_workflow_activity,
    summarize_queue_activity,
    translate_loop_activity,
)

logger = logging.getLogger("translator.worker")

DEFAULT_LLM_CONCURRENCY = 2


def llm_concurrency() -> int:
    """Global LLM gate size K, from TRANSLATOR_LLM_CONCURRENCY (default 2)."""
    return int(os.environ.get("TRANSLATOR_LLM_CONCURRENCY", str(DEFAULT_LLM_CONCURRENCY)))


def build_build_worker(client: object) -> Worker:
    """Worker for the workflows + build/dump/summarize/handoff activities."""
    return Worker(
        client,
        task_queue=BUILD_TASK_QUEUE,
        workflows=[BuildQueueWorkflow, TranslateWorkflow],
        activities=[
            build_queue_activity,
            start_translate_workflow_activity,
            dump_activity,
            summarize_queue_activity,
        ],
    )


def build_llm_worker(client: object, *, max_concurrent: int) -> Worker:
    """Worker that runs ONLY translate_loop_activity, capped at max_concurrent (global gate)."""
    return Worker(
        client,
        task_queue=LLM_TASK_QUEUE,
        activities=[translate_loop_activity],
        max_concurrent_activities=max_concurrent,
    )


async def run_worker(temporal_address: str | None = None) -> None:
    address = temporal_address or os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    client = await Client.connect(address)
    k = llm_concurrency()
    logger.info(
        "connected to Temporal at %s | build queue %r, llm queue %r (K=%d) (Ctrl-C to stop)",
        address,
        BUILD_TASK_QUEUE,
        LLM_TASK_QUEUE,
        k,
    )
    build_worker = build_build_worker(client)
    llm_worker = build_llm_worker(client, max_concurrent=k)
    await asyncio.gather(build_worker.run(), llm_worker.run())


def worker_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="translator-worker",
        description="Run the standalone translator Temporal worker fleet.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to a .env file to load before starting (default: .env).",
    )
    parser.add_argument(
        "--temporal-address",
        default=None,
        help="Temporal frontend address (overrides TEMPORAL_ADDRESS).",
    )
    args = parser.parse_args(argv)
    load_dotenv(args.env_file, override=False)
    logging.basicConfig(
        level=os.environ.get("TRANSLATOR_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    logger.info("starting translator worker fleet")
    try:
        asyncio.run(run_worker(temporal_address=args.temporal_address))
    except KeyboardInterrupt:
        return 130
    return 0
```

- [ ] **Step 8.5: Switch `translator/import_legacy.py` env loading to `load_dotenv`**

`import_legacy.py` imported the now-deleted `load_env_file` from `worker.py`. Replace the import and call:

```python
# In translator/import_legacy.py — replace:
from translator.worker import load_env_file
# With:
from dotenv import load_dotenv
```

```python
# In translator/import_legacy.py — replace:
    load_env_file(Path(args.env_file))
# With:
    load_dotenv(args.env_file, override=False)
```

If `Path` is now otherwise unused in `import_legacy.py`, leave its import (the `--duckdb` handling still uses `Path`).

- [ ] **Step 8.6: Run tests — expect PASS**

```bash
uv run pytest tests/test_translator_worker.py tests/test_translator_import_legacy.py -v
```
Expected: all pass (4 worker tests + the legacy-import tests).

- [ ] **Step 8.7: Commit**

```bash
git add pyproject.toml translator/worker.py \
        translator/import_legacy.py tests/test_translator_worker.py
git commit -m "feat(translator): two-queue worker fleet (build + K-gated LLM) on python-dotenv"
```

---

### Task 9: Dagster trigger — fire `BuildQueueWorkflow`

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_translator_trigger.py`

**Interfaces:**
- Consumes: `BuildQueueWorkflow`, `BuildQueueWorkflowInput` from `translator.norway_brreg.workflows`; `BUILD_TASK_QUEUE` from `translator.task_queues`.
- Produces:
  - `NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID = "build-queue-norway_brreg"` (module-level constant)
  - `build_norway_brreg_build_queue_input() -> BuildQueueWorkflowInput`
  - `norway_brreg_translation_trigger` asset fires `BuildQueueWorkflow` (fire-and-forget, same as before but for the new workflow)

**Constraint:** No `from __future__ import annotations` in `assets.py` (Dagster context-type rule).

- [ ] **Step 9.1: Update `tests/test_translator_trigger.py`**

```python
# tests/test_translator_trigger.py
from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
    build_norway_brreg_build_queue_input,
)
from translator.task_queues import BUILD_TASK_QUEUE


def test_build_queue_workflow_id_is_stable():
    assert NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID == "build-queue-norway_brreg"


def test_build_queue_input_targets_norway_brreg():
    params = build_norway_brreg_build_queue_input()
    assert params.source_slug == "norway_brreg"
    assert params.queue_duckdb_path == "data/translator/norway_brreg.duckdb"
    assert params.translate_workflow_id == "translate-norway_brreg"
    # The build-queue trigger and the Translate handoff both run on the build queue.
    assert params.translate_task_queue == BUILD_TASK_QUEUE
    assert BUILD_TASK_QUEUE == "translation-build"
    assert params.batch_size == 50
    assert params.max_tokens == 8192
    assert params.max_batch_failures == 20


def test_build_queue_input_has_no_scan_or_flush_timeout():
    """Confirm the removed fields are absent from the new input dataclass."""
    params = build_norway_brreg_build_queue_input()
    assert not hasattr(params, "scan_timeout_seconds")
    assert not hasattr(params, "flush_timeout_seconds")
    assert not hasattr(params, "source_slug") or params.source_slug == "norway_brreg"
```

- [ ] **Step 9.2: Run tests — expect FAIL**

```bash
uv run pytest tests/test_translator_trigger.py -v 2>&1 | head -10
```
Expected: `ImportError: cannot import name 'NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID'`

- [ ] **Step 9.3: Update `src/dagster_v3/defs/norway_brreg/assets.py`**

Find and replace the translator-related imports and functions. The lines to change are near the bottom of the file (lines 37–38 imports and lines 196–262 — the `NORWAY_BRREG_TRANSLATE_WORKFLOW_ID`, `build_norway_brreg_translate_input`, `_start_norway_brreg_translation`, and `norway_brreg_translation_trigger` definitions).

Replace these lines:

```python
from translator.activities import LOCAL_LLM_TRANSLATION_TASK_QUEUE
from translator.workflow import TranslateSourceWorkflow, TranslateSourceWorkflowInput
```

With:

```python
from translator.task_queues import BUILD_TASK_QUEUE
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    BuildQueueWorkflowInput,
)
```

Replace the `NORWAY_BRREG_TRANSLATE_WORKFLOW_ID` block and the three related functions/constant with:

```python
NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID = "build-queue-norway_brreg"


def build_norway_brreg_build_queue_input() -> BuildQueueWorkflowInput:
    return BuildQueueWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path="data/translator/norway_brreg.duckdb",
        translate_workflow_id="translate-norway_brreg",
        translate_task_queue=BUILD_TASK_QUEUE,
        batch_size=50,
        max_tokens=8192,
        extra_body_json='{"chat_template_kwargs": {"enable_thinking": false}}',
        max_batch_failures=20,
    )


async def _start_norway_brreg_build_queue(temporal_address: str) -> str:
    client = await Client.connect(temporal_address)
    handle = await client.start_workflow(
        BuildQueueWorkflow.run,
        build_norway_brreg_build_queue_input(),
        id=NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
        task_queue=BUILD_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return handle.result_run_id


@dg.asset(
    deps=[dg.AssetKey("norway_resolved_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "temporal"},
    description=(
        "Fire-and-forget: start (or reuse) the BuildQueueWorkflow Temporal workflow "
        "after no_companies lands in ClickHouse. BuildQueueWorkflow seeds the DuckDB "
        "queue then starts TranslateWorkflow autonomously. Does not wait for completion."
    ),
)
def norway_brreg_translation_trigger(context: AssetExecutionContext) -> dg.MaterializeResult:
    import asyncio as _asyncio
    import os

    address = os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    run_id = _asyncio.run(_start_norway_brreg_build_queue(address))
    context.log.info(
        "Started Norway Brreg BuildQueueWorkflow: workflow_id=%s run_id=%s",
        NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
        run_id,
    )
    return dg.MaterializeResult(
        metadata={
            "workflow_id": NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
            "workflow_run_id": run_id,
            "task_queue": BUILD_TASK_QUEUE,
        }
    )
```

Also remove the now-unused `NORWAY_BRREG_TRANSLATE_WORKFLOW_ID` constant and `build_norway_brreg_translate_input` function.

- [ ] **Step 9.4: Run tests — expect PASS**

```bash
uv run pytest tests/test_translator_trigger.py -v
uv run dg check defs
```
Expected: 3 tests pass; `dg check defs` exits 0.

- [ ] **Step 9.5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_translator_trigger.py
git commit -m "feat(dagster/norway_brreg): trigger BuildQueueWorkflow, remove scan/flush timeouts"
```

---

### Task 10: Remove dead code + final suite

**Files:**
- Delete: `translator/workflow.py`
- Delete: `translator/activities.py`
- Delete: `translator/registry.py` (replaced by shim in Task 4, now fully superseded)
- Modify: `translator/import_legacy.py` — update import from `norway_brreg/config.py`
- Replace: `tests/test_translator_workflow.py` — replace with new workflow tests (old ones test deleted code)
- Modify: `tests/test_translator_registry.py` — remove (covered by `test_norway_brreg_config.py`); or keep as a redirect
- Modify: `tests/test_translator_imports.py` — update import assertions
- Validate: `uv run dg check defs` + `uv run pytest -q` fully green

**Before deleting files, verify no living code imports from the modules being deleted:**

- [ ] **Step 10.1: Audit remaining imports**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
rg "from translator\.workflow import|from translator\.activities import|from translator\.registry import" \
   --include="*.py" -l
```

Expected: only `translator/import_legacy.py` imports from `translator.registry` (and possibly `tests/test_translator_workflow.py`). If any other files still import from these modules, update them before deleting.

- [ ] **Step 10.2: Update `translator/import_legacy.py`**

Replace the registry import and the `get_source_config` call:

```python
# In translator/import_legacy.py, replace:
from translator.registry import get_source_config

# With:
from translator.norway_brreg.config import get_config as _get_norway_config

def _get_source_config_by_slug(slug: str):
    if slug == "norway_brreg":
        return _get_norway_config()
    raise KeyError(f"Unknown source slug: {slug!r}")
```

And replace `config = get_source_config(args.source)` with:
```python
    try:
        config = _get_source_config_by_slug(args.source)
    except KeyError:
        print(f"ERROR: Unknown source slug: {args.source!r}")
        return 1
```

Remove the now-redundant `except KeyError` block that was after the old call.

- [ ] **Step 10.3: Delete the dead modules**

```bash
git rm translator/workflow.py translator/activities.py translator/registry.py
```

- [ ] **Step 10.4: Replace `tests/test_translator_workflow.py`**

The old file tests `TranslateSourceWorkflow` (deleted). Replace it with a thin import-sanity test confirming the new workflows can be imported:

```python
# tests/test_translator_workflow.py
"""Import-sanity tests confirming the new per-source workflow modules are importable and well-formed."""
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    BuildQueueWorkflowInput,
    BuildQueueWorkflowOutput,
    TranslateWorkflow,
    TranslateWorkflowInput,
    TranslateWorkflowOutput,
)


def test_build_queue_workflow_defn_importable():
    assert BuildQueueWorkflow is not None


def test_translate_workflow_defn_importable():
    assert TranslateWorkflow is not None


def test_build_queue_workflow_input_has_no_scan_or_flush_timeout():
    """scan_timeout_seconds / flush_timeout_seconds must not exist on the new input."""
    fields = {f.name for f in BuildQueueWorkflowInput.__dataclass_fields__.values()}
    assert "scan_timeout_seconds" not in fields
    assert "flush_timeout_seconds" not in fields


def test_translate_workflow_input_has_no_scan_or_flush_timeout():
    fields = {f.name for f in TranslateWorkflowInput.__dataclass_fields__.values()}
    assert "scan_timeout_seconds" not in fields
    assert "flush_timeout_seconds" not in fields
```

- [ ] **Step 10.5: Update `tests/test_translator_registry.py`**

The file tests `translator.registry.get_source_config`. Since `registry.py` is deleted, redirect tests to `norway_brreg/config.py`:

```python
# tests/test_translator_registry.py
"""Backward-compat: registry tests now verify norway_brreg/config.py (registry.py deleted)."""
import pytest

from translator.norway_brreg.config import get_config


def test_norway_brreg_config_has_three_fields_two_dynamic_one_static():
    config = get_config()
    assert config.source_lang == "no"
    assert config.ch_table == "corpscout.no_companies"
    assert len(config.fields) == 3

    for f in config.fields[:2]:
        assert f.static_map is None
        assert f.static_key_col is None

    lf = config.fields[2]
    assert lf.original_col == "legal_form_description_original"
    assert lf.static_key_col == "legal_form_code"
    assert lf.static_map is not None


def test_unknown_source_raises_key_error():
    from translator.import_legacy import _get_source_config_by_slug
    with pytest.raises(KeyError):
        _get_source_config_by_slug("atlantis")
```

- [ ] **Step 10.6: Update `tests/test_translator_imports.py`**

Replace the imports sanity test to reflect the new module layout:

```python
# tests/test_translator_imports.py
def test_translator_core_is_self_contained_and_importable():
    from translator.llm_batch import translate_batch
    from translator.norway_brreg.config import get_config
    from translator.norway_brreg.dump import dump_to_clickhouse
    from translator.norway_brreg.seed import SeedResult, build_queue
    from translator.norway_brreg.workflows import (
        BuildQueueWorkflow,
        BuildQueueWorkflowInput,
        TranslateWorkflow,
        TranslateWorkflowInput,
    )
    from translator.task_queues import BUILD_TASK_QUEUE, LLM_TASK_QUEUE
    from translator.queue import TranslationQueue, TranslationQueueItem
    from translator.provider import LocalOpenAICompatibleTranslationProvider
    from translator.types import TranslationInput, TranslationResult

    assert BUILD_TASK_QUEUE == "translation-build"
    assert LLM_TASK_QUEUE == "translation-llm"
    assert callable(translate_batch)
    assert callable(build_queue)
    assert callable(dump_to_clickhouse)
    assert BuildQueueWorkflow and TranslateWorkflow
    assert TranslationQueue and TranslationQueueItem
    assert LocalOpenAICompatibleTranslationProvider
    assert TranslationInput and TranslationResult
    assert get_config is not None


def test_translator_modules_do_not_reference_deleted_modules():
    import pathlib

    pkg = pathlib.Path(__file__).resolve().parents[1] / "translator"
    # workflow/activities/registry deleted here in Task 10; smoke/provider_smoke/
    # queue_smoke deleted in Task 1.
    deleted = {
        "workflow.py", "activities.py", "registry.py",
        "smoke.py", "provider_smoke.py", "queue_smoke.py",
    }
    for py in pkg.glob("*.py"):
        assert py.name not in deleted, f"{py.name} was supposed to be deleted"

    for py in pkg.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert "from translator.workflow import" not in text, f"{py} still imports translator.workflow"
        assert "from translator.activities import" not in text, f"{py} still imports translator.activities"
        assert "from translator.smoke import" not in text, f"{py} still imports translator.smoke"
        assert "Smoke" not in text, f"{py} still references a Smoke* identifier"


def test_no_production_smoke_identifiers_remain():
    """No production module may carry the historical Smoke*/smoke names."""
    import pathlib

    pkg = pathlib.Path(__file__).resolve().parents[1] / "translator"
    for py in pkg.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert "SmokeTranslation" not in text, f"{py} still references SmokeTranslation*"
        assert "smoke" not in text.lower(), f"{py} still references 'smoke'"
```

- [ ] **Step 10.7: Run the full test suite**

```bash
uv run pytest -q 2>&1 | tail -20
```
Expected: all tests pass, 0 failures.

- [ ] **Step 10.8: Validate Dagster definitions**

```bash
uv run dg check defs
```
Expected: exits 0 with no errors.

- [ ] **Step 10.9: Commit**

```bash
git add translator/import_legacy.py \
        tests/test_translator_workflow.py \
        tests/test_translator_registry.py \
        tests/test_translator_imports.py
# the dead modules were already staged for deletion by `git rm` in Step 10.3
git commit -m "refactor(translator): remove TranslateSourceWorkflow, activities, registry; finalize per-source package"
```

---

## Self-Review

### 1. Spec Coverage

| Spec requirement | Task |
|-----------------|------|
| De-smoke: no production `Smoke`/`smoke` identifiers | Task 1: `Translation*` types, `provider.py`, `errors.py`; smoke files deleted |
| python-dotenv replaces hand-rolled `load_env_file` (`override=False`) | Task 8: `pyproject.toml` dep; `worker.py` + `import_legacy.py` use `load_dotenv` |
| Two task queues + global LLM gate (`K` concurrent) | Task 7 (`task_queues.py`, route `translate_loop` to `LLM_TASK_QUEUE`, no `schedule_to_start`); Task 8 (two-worker fleet, `max_concurrent_activities=K`, `K` from `TRANSLATOR_LLM_CONCURRENCY`) |
| One shared worker fleet (no process per source) | Task 8: `run_worker` starts both workers via `asyncio.gather` |
| Per-source packages, nothing source-specific in shared core | Tasks 4–7: all Norway-specific code in `norway_brreg/`; shared core unchanged |
| Two workflows: BuildQueueWorkflow + TranslateWorkflow | Task 7 |
| BuildQueue's final step starts TranslateWorkflow (USE_EXISTING) then completes | Task 7: `start_translate_workflow_activity` |
| Seed = query_arrow + set-based INSERT, sha256 in DuckDB SQL | Tasks 3, 5 |
| NO per-row Python in seed | Task 5: Arrow registered → single INSERT…SELECT |
| Static fields → text_translations directly (provider='static') | Task 5: static branch in `build_queue` |
| Shared `llm_batch.py` | Task 2 |
| Dump = self-contained per source, batched staging inserts | Task 6 |
| heartbeat_timeout=150s, start_to_close_timeout=24h, max_attempts=3 | Task 7: HEARTBEAT_TIMEOUT, START_TO_CLOSE_TIMEOUT, RETRY_POLICY constants |
| Remove scan_timeout_seconds / flush_timeout_seconds | Tasks 7, 9, 10: not present in new input dataclasses |
| Dagster trigger fires BuildQueueWorkflow (id build-queue-norway_brreg) | Task 9 |
| Delete data/translator/norway_brreg.duckdb on cutover | MANUAL operator step (outside code) — noted in spec, not in code |
| uv run dg check defs + pytest green | Tasks 1, 9, 10 |
| Reuse queue schema (translation_items/locations/results/batch_attempts) | Task 5: same table names, same schema |
| Reuse text_translations + no_companies_translated | Tasks 5, 6: flush_translations writes to same tables |

**Gap: cutover instruction (delete the DuckDB file)** is an operator step, not encoded in code. No code change needed, but the plan should remind the executor: before the first production run of `BuildQueueWorkflow`, delete `data/translator/norway_brreg.duckdb` and `data/translator/norway_brreg.duckdb.wal`.

### 2. Placeholder Scan

- No "TBD", "TODO", or "fill in" language found.
- Every step with code changes shows the complete code.
- The `_once` function pattern is repeated in full in Task 7 (not referred to as "similar to Task N").
- The `dynamic_enqueued` counting note in Task 5 explains the design choice explicitly.
- No production `Smoke`/`smoke` identifiers remain (verified by grep); only the rename-explaining prose mentions them.

### 3. Type Consistency

- `TranslationInput` / `TranslationResult` defined in `translator/types.py` (Task 1), used by `provider.py`, `llm_batch.py`, and every test. ✓
- `_categorize_exception` in `translator/errors.py` and `_parse_extra_body` in `translator/provider.py` (Task 1), imported by `activities.py` and Task 7's `translate_loop_once`. ✓
- `BUILD_TASK_QUEUE` / `LLM_TASK_QUEUE` defined in `translator/task_queues.py` (Task 7); imported by `workflows.py` (uses `LLM_TASK_QUEUE` for the loop dispatch), `worker.py` (both queues, Task 8), `assets.py` (`BUILD_TASK_QUEUE`, Task 9), and the worker/trigger tests. The single `translation-local-llm` queue is fully removed. ✓
- `llm_concurrency()` / `build_build_worker` / `build_llm_worker(max_concurrent=K)` defined in `worker.py` (Task 8); `run_worker` runs both via `asyncio.gather`; `K` from `TRANSLATOR_LLM_CONCURRENCY` (default 2). ✓
- `load_dotenv(path, override=False)` replaces `load_env_file` in `worker.py` + `import_legacy.py` (Task 8); `python-dotenv>=1.0` added to `pyproject.toml`. ✓
- `SeedResult` defined in `translator/norway_brreg/seed.py` (Task 5), consumed in `workflows.py` (Task 7, `build_queue_activity` → `build_queue_once` → `build_queue` returns `SeedResult`). ✓
- `BuildQueueActivityInput` defined in `workflows.py` (Task 7), `build_queue_once(params: BuildQueueActivityInput)` uses it. ✓
- `TranslateLoopActivityInput` / `TranslateLoopResult` defined and used in Task 7. ✓
- `DumpActivityInput` defined and used in Task 7; `dump_once` consumes it. ✓
- `FlushTranslationRow` from `translator.queue` (existing, unchanged) — used in both seed.py and dump.py via `flush_translations`. ✓
- `translate_batch(items: list[ClaimedTranslationItem], *, provider, timeout)` — Task 2 signature; Task 7 `translate_loop_once` calls `translate_batch(claimed, provider=provider, timeout=120)`. ✓
- `query_arrow(client, sql, parameters)` — Task 3 signature; Task 5 `build_queue` calls `query_arrow(ch_client, sql, params)`. ✓
- `build_norway_brreg_build_queue_input() -> BuildQueueWorkflowInput` — Task 9; test imports same name. ✓
- `NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID` — defined in `assets.py` Task 9, imported in `test_translator_trigger.py` Task 9. ✓
- Old constants (`NORWAY_BRREG_TRANSLATE_WORKFLOW_ID`, `build_norway_brreg_translate_input`) removed in Task 9 and confirmed absent in Task 10's import audit. ✓
