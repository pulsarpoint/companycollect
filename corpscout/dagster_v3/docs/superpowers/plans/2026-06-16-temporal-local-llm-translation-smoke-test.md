# Temporal Local LLM Translation Smoke Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the smallest isolated proof that a Temporal workflow can send 50 text fragments to the real local OpenAI-compatible LLM endpoint and receive parseable JSON translations.

**Architecture:** This chunk does not touch Dagster assets, DuckDB queues, BRREG tables, or ClickHouse. It creates a plain translation client, a Temporal workflow/activity pair, a local worker entrypoint, and an opt-in integration test that runs against the real Temporal server at `companycollect:8089` and the local LLM endpoint configured through environment variables.

**Tech Stack:** Python, Temporal Python SDK, requests, pytest, OpenAI-compatible chat completions API.

---

## Scope

This plan is intentionally narrow. It proves only this loop:

```text
pytest integration test
  -> start Temporal worker in-process
  -> start Temporal workflow on companycollect:8089
  -> workflow executes one activity
  -> activity sends 50 entries to local OpenAI-compatible LLM
  -> activity parses strict JSON
  -> workflow returns 50 translations
```

This plan does not create:

- shared translation DuckDB tables
- BRREG translation queue assets
- Dagster assets
- ClickHouse export changes
- multi-day production workflow loop

Those belong in later smaller plans after this smoke test works.

## Environment

Create `companycollect/corpscout/dagster_v3/.env` locally with:

```bash
TEMPORAL_ADDRESS=companycollect:8089
TRANSLATION_PROVIDER=local_openai_compatible
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888
TRANSLATION_PROVIDER_LOCAL_MODEL=qwen3:6b
TRANSLATION_PROVIDER_LOCAL_API_KEY=
```

The implementation also works if these values are exported in the shell. The `.env` file should not be committed unless this project already commits non-secret local defaults.

## File Structure

- Modify `companycollect/corpscout/dagster_v3/pyproject.toml` and `uv.lock` to add `temporalio`.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/translations/__init__.py` to export translation smoke-test primitives.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/translations/smoke.py` for request/response dataclasses, prompt creation, response parsing, and local OpenAI-compatible provider.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/__init__.py`.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/translations/__init__.py`.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/translations/smoke.py` for the one-activity smoke workflow.
- Create `companycollect/corpscout/dagster_v3/tests/test_translation_temporal_smoke.py` for pure unit tests and one opt-in real integration test.
- Create `companycollect/corpscout/dagster_v3/.env.example` with non-secret defaults for the smoke test.

---

### Task 1: Add Temporal Dependency And Env Example

**Files:**
- Modify: `companycollect/corpscout/dagster_v3/pyproject.toml`
- Modify: `companycollect/corpscout/dagster_v3/uv.lock`
- Create: `companycollect/corpscout/dagster_v3/.env.example`

- [ ] **Step 1: Add Temporal SDK**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv add temporalio
```

Expected: `pyproject.toml` includes `temporalio` and `uv.lock` changes.

- [ ] **Step 2: Create `.env.example`**

Create `companycollect/corpscout/dagster_v3/.env.example`:

```bash
TEMPORAL_ADDRESS=companycollect:8089
TRANSLATION_PROVIDER=local_openai_compatible
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888
TRANSLATION_PROVIDER_LOCAL_MODEL=qwen3:6b
TRANSLATION_PROVIDER_LOCAL_API_KEY=
```

- [ ] **Step 3: Verify Temporal imports**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run python - <<'PY'
from temporalio.client import Client
from temporalio.worker import Worker
print(Client.__name__, Worker.__name__)
PY
```

Expected output:

```text
Client Worker
```

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
git add pyproject.toml uv.lock .env.example
git commit -m "chore: add temporal smoke test dependency"
```

---

### Task 2: Local OpenAI-Compatible Translation Client

**Files:**
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/translations/__init__.py`
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/translations/smoke.py`
- Create: `companycollect/corpscout/dagster_v3/tests/test_translation_temporal_smoke.py`

- [ ] **Step 1: Write failing unit tests**

Create `companycollect/corpscout/dagster_v3/tests/test_translation_temporal_smoke.py`:

```python
import json

from dagster_v3.translations.smoke import (
    SmokeTranslationInput,
    SmokeTranslationResult,
    build_smoke_translation_prompt,
    parse_smoke_translation_response,
)


def test_build_smoke_translation_prompt_contains_50_ids() -> None:
    items = [
        SmokeTranslationInput(item_id=f"item-{index:02d}", source_text=f"Tekst {index}")
        for index in range(50)
    ]

    prompt = build_smoke_translation_prompt(items)

    assert "Return only valid JSON" in prompt
    assert '"item_id":"item-00"' in prompt
    assert '"item_id":"item-49"' in prompt
    assert '"translated_text"' in prompt


def test_parse_smoke_translation_response_returns_results_by_id() -> None:
    payload = {
        "translations": [
            {"item_id": "item-00", "translated_text": "Text 0"},
            {"item_id": "item-01", "translated_text": "Text 1"},
        ]
    }

    results = parse_smoke_translation_response(
        json.dumps(payload),
        expected_item_ids={"item-00", "item-01"},
    )

    assert results == [
        SmokeTranslationResult(item_id="item-00", translated_text="Text 0"),
        SmokeTranslationResult(item_id="item-01", translated_text="Text 1"),
    ]
```

- [ ] **Step 2: Run unit tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.translations'`.

- [ ] **Step 3: Create translation smoke client**

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/translations/__init__.py`:

```python
"""Translation helpers and providers."""
```

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/translations/smoke.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Protocol

import requests


@dataclass(frozen=True)
class SmokeTranslationInput:
    item_id: str
    source_text: str


@dataclass(frozen=True)
class SmokeTranslationResult:
    item_id: str
    translated_text: str


class HttpSession(Protocol):
    def post(self, url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: int) -> Any: ...


def build_smoke_translation_prompt(items: list[SmokeTranslationInput]) -> str:
    payload = {
        "source_language": "Norwegian",
        "target_language": "English",
        "items": [
            {"item_id": item.item_id, "source_text": item.source_text}
            for item in items
        ],
    }
    return (
        "Translate each Norwegian company registry text fragment to English. "
        "Preserve legal and business meaning. Do not add explanations. "
        "Return only valid JSON with shape "
        '{"translations":[{"item_id":"...","translated_text":"..."}]}. '
        "Every item_id in the response must match an input item_id. Input JSON: "
        f"{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    )


def parse_smoke_translation_response(
    response_text: str,
    *,
    expected_item_ids: set[str],
) -> list[SmokeTranslationResult]:
    payload = json.loads(_strip_json_fence(response_text))
    translations = payload.get("translations")
    if not isinstance(translations, list):
        raise ValueError("translation response must contain translations list")

    results: list[SmokeTranslationResult] = []
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
            SmokeTranslationResult(
                item_id=item_id,
                translated_text=translated_text.strip(),
            )
        )
    return results


class LocalOpenAICompatibleTranslationProvider:
    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str = "",
        session: HttpSession | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.session = session or requests.Session()

    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        prompt = build_smoke_translation_prompt(items)
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        response = self.session.post(
            f"{self.base_url}/v1/chat/completions",
            json={
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "stream": False,
            },
            headers=headers,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return parse_smoke_translation_response(
            content,
            expected_item_ids={item.item_id for item in items},
        )


def _strip_json_fence(response_text: str) -> str:
    stripped = response_text.strip()
    if stripped.startswith("```json") and stripped.endswith("```"):
        return stripped.removeprefix("```json").removesuffix("```").strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        return stripped.removeprefix("```").removesuffix("```").strip()
    return stripped
```

- [ ] **Step 4: Run unit tests to verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py -q
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
git add src/dagster_v3/translations tests/test_translation_temporal_smoke.py
git commit -m "feat: add local llm translation smoke client"
```

---

### Task 3: Isolated Temporal Smoke Workflow

**Files:**
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/__init__.py`
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/translations/__init__.py`
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/translations/smoke.py`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_translation_temporal_smoke.py`

- [ ] **Step 1: Add failing Temporal workflow unit test**

Append this to `companycollect/corpscout/dagster_v3/tests/test_translation_temporal_smoke.py`:

```python


def test_smoke_items_returns_exactly_50_inputs() -> None:
    from dagster_v3.temporal.translations.smoke import build_smoke_items

    items = build_smoke_items()

    assert len(items) == 50
    assert items[0].item_id == "brreg-smoke-00"
    assert items[-1].item_id == "brreg-smoke-49"
    assert all(item.source_text.strip() for item in items)
```

- [ ] **Step 2: Run workflow unit test to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py::test_smoke_items_returns_exactly_50_inputs -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.temporal'`.

- [ ] **Step 3: Create Temporal smoke workflow implementation**

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/__init__.py`:

```python
"""Temporal workflows for dagster_v3."""
```

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/translations/__init__.py`:

```python
SMOKE_TRANSLATION_TASK_QUEUE = "dagster-v3-translation-smoke"
```

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/temporal/translations/smoke.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import os

from temporalio import activity, workflow

from dagster_v3.temporal.translations import SMOKE_TRANSLATION_TASK_QUEUE
from dagster_v3.translations.smoke import (
    LocalOpenAICompatibleTranslationProvider,
    SmokeTranslationInput,
    SmokeTranslationResult,
)


@dataclass(frozen=True)
class SmokeWorkflowInput:
    timeout_seconds: int = 600


@dataclass(frozen=True)
class SmokeWorkflowOutput:
    translated_count: int
    translations: list[SmokeTranslationResult]


def build_smoke_items() -> list[SmokeTranslationInput]:
    samples = [
        "Allmennaksjeselskap",
        "Utvinning av raolje",
        "Utvinning av naturgass",
        "Produksjon av raffinerte petroleumsprodukter",
        "Selv, eller gjennom andre selskaper aa utvikle energi",
    ]
    return [
        SmokeTranslationInput(
            item_id=f"brreg-smoke-{index:02d}",
            source_text=samples[index % len(samples)],
        )
        for index in range(50)
    ]


@activity.defn
def translate_smoke_batch(params: SmokeWorkflowInput) -> list[SmokeTranslationResult]:
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url=os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"],
        model=os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"],
        api_key=os.getenv("TRANSLATION_PROVIDER_LOCAL_API_KEY", ""),
    )
    return provider.translate(
        build_smoke_items(),
        timeout_seconds=params.timeout_seconds,
    )


@workflow.defn
class SmokeTranslationWorkflow:
    @workflow.run
    async def run(self, params: SmokeWorkflowInput) -> SmokeWorkflowOutput:
        translations = await workflow.execute_activity(
            translate_smoke_batch,
            params,
            start_to_close_timeout=timedelta(seconds=params.timeout_seconds),
        )
        return SmokeWorkflowOutput(
            translated_count=len(translations),
            translations=translations,
        )
```

- [ ] **Step 4: Run workflow unit test to verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py::test_smoke_items_returns_exactly_50_inputs -q
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
git add src/dagster_v3/temporal tests/test_translation_temporal_smoke.py
git commit -m "feat: add temporal translation smoke workflow"
```

---

### Task 4: Real Temporal And Real Local LLM Integration Test

**Files:**
- Modify: `companycollect/corpscout/dagster_v3/tests/test_translation_temporal_smoke.py`

- [ ] **Step 1: Add opt-in integration test**

Append this to `companycollect/corpscout/dagster_v3/tests/test_translation_temporal_smoke.py`:

```python

import asyncio
import os
from uuid import uuid4

import pytest
from temporalio.client import Client
from temporalio.worker import Worker


def _load_dotenv_if_present() -> None:
    env_path = os.path.join(os.getcwd(), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as env_file:
        for line in env_file:
            stripped = line.strip()
            if stripped == "" or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


@pytest.mark.integration
def test_real_temporal_workflow_translates_50_items_with_local_llm() -> None:
    _load_dotenv_if_present()
    required = [
        "TEMPORAL_ADDRESS",
        "TRANSLATION_PROVIDER_LOCAL_BASE_URL",
        "TRANSLATION_PROVIDER_LOCAL_MODEL",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        pytest.skip(f"missing integration environment variables: {', '.join(missing)}")

    from dagster_v3.temporal.translations import SMOKE_TRANSLATION_TASK_QUEUE
    from dagster_v3.temporal.translations.smoke import (
        SmokeTranslationWorkflow,
        SmokeWorkflowInput,
        translate_smoke_batch,
    )

    async def run_smoke() -> None:
        client = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
        async with Worker(
            client,
            task_queue=SMOKE_TRANSLATION_TASK_QUEUE,
            workflows=[SmokeTranslationWorkflow],
            activities=[translate_smoke_batch],
        ):
            handle = await client.start_workflow(
                SmokeTranslationWorkflow.run,
                SmokeWorkflowInput(timeout_seconds=900),
                id=f"translation-smoke-{uuid4()}",
                task_queue=SMOKE_TRANSLATION_TASK_QUEUE,
            )
            result = await handle.result()

        assert result.translated_count == 50
        assert len(result.translations) == 50
        assert {item.item_id for item in result.translations} == {
            f"brreg-smoke-{index:02d}" for index in range(50)
        }
        assert all(item.translated_text.strip() for item in result.translations)

    asyncio.run(run_smoke())
```

- [ ] **Step 2: Run non-integration tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py -q -m "not integration"
```

Expected: unit tests pass and the real Temporal/LLM test is not executed.

- [ ] **Step 3: Create local `.env` for real smoke test**

Create or update `companycollect/corpscout/dagster_v3/.env`:

```bash
TEMPORAL_ADDRESS=companycollect:8089
TRANSLATION_PROVIDER=local_openai_compatible
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888
TRANSLATION_PROVIDER_LOCAL_MODEL=qwen3:6b
TRANSLATION_PROVIDER_LOCAL_API_KEY=
```

- [ ] **Step 4: Run real Temporal + local LLM integration test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py::test_real_temporal_workflow_translates_50_items_with_local_llm -q -s
```

Expected: the test starts an in-process Temporal worker, starts one workflow on `companycollect:8089`, calls `http://100.77.62.33:8888/v1/chat/completions` with model `qwen3:6b`, and passes with 50 translated results.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
git add tests/test_translation_temporal_smoke.py
git commit -m "test: add real temporal local llm smoke test"
```

---

### Task 5: Verification

**Files:**
- No new files.

- [ ] **Step 1: Run focused smoke tests except integration**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py -q -m "not integration"
```

Expected: all non-integration smoke tests pass.

- [ ] **Step 2: Run real integration test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_translation_temporal_smoke.py::test_real_temporal_workflow_translates_50_items_with_local_llm -q -s
```

Expected: 1 passed, with 50 returned translations.

- [ ] **Step 3: Run package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest -q -m "not integration"
```

Expected: all non-integration tests pass.

- [ ] **Step 4: Run Dagster definition check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected:

```text
All component YAML validated successfully.
All definitions loaded successfully.
```

- [ ] **Step 5: Run whitespace check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
git diff --check
```

Expected: no output and exit code 0.

---

## Self-Review

**Spec coverage:** This plan covers only the requested first chunk: real Temporal server, real local OpenAI-compatible LLM endpoint, `.env` configuration, one workflow, one activity, 50 entries sent in one request, strict JSON response parsing, and an integration test that verifies the loop returns 50 translations.

**Placeholder scan:** No placeholder implementation instructions remain. Every file creation/modification step includes exact code or exact commands.

**Type consistency:** `SmokeTranslationInput`, `SmokeTranslationResult`, `SmokeWorkflowInput`, `SmokeWorkflowOutput`, `SmokeTranslationWorkflow`, `translate_smoke_batch`, and `SMOKE_TRANSLATION_TASK_QUEUE` are consistently named across tests and implementation steps.
