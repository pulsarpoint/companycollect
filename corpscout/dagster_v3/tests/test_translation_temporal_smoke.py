import json
import asyncio
import os
from uuid import uuid4

import pytest
from temporalio.client import Client
from temporalio.worker import Worker

from translations.smoke import (
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


def test_smoke_items_returns_exactly_50_inputs() -> None:
    from temporal.translations.smoke import build_smoke_items

    items = build_smoke_items()

    assert len(items) == 50
    assert items[0].item_id == "brreg-smoke-00"
    assert items[-1].item_id == "brreg-smoke-49"
    assert all(item.source_text.strip() for item in items)


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
    if os.getenv("RUN_TRANSLATION_INTEGRATION_TESTS") != "1":
        pytest.skip("set RUN_TRANSLATION_INTEGRATION_TESTS=1 to run real Temporal/LLM smoke test")

    required = [
        "TEMPORAL_ADDRESS",
        "TRANSLATION_PROVIDER_LOCAL_BASE_URL",
        "TRANSLATION_PROVIDER_LOCAL_MODEL",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        pytest.skip(f"missing integration environment variables: {', '.join(missing)}")

    from temporal.translations import SMOKE_TRANSLATION_TASK_QUEUE
    from temporal.translations.smoke import (
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
