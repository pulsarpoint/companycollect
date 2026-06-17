from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
import os

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from translations.types import SmokeTranslationInput, SmokeTranslationResult


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
async def translate_smoke_batch(params: SmokeWorkflowInput) -> list[SmokeTranslationResult]:
    from translations.smoke import LocalOpenAICompatibleTranslationProvider

    items = build_smoke_items()
    activity.logger.info(
        "starting smoke translation batch",
        extra={"item_count": len(items), "timeout_seconds": params.timeout_seconds},
    )
    provider = LocalOpenAICompatibleTranslationProvider(
        base_url=os.environ["TRANSLATION_PROVIDER_LOCAL_BASE_URL"],
        model=os.environ["TRANSLATION_PROVIDER_LOCAL_MODEL"],
        api_key=os.getenv("TRANSLATION_PROVIDER_LOCAL_API_KEY", "not-needed"),
    )
    translations = await asyncio.to_thread(
        provider.translate,
        items,
        timeout_seconds=params.timeout_seconds,
    )
    activity.logger.info(
        "finished smoke translation batch",
        extra={"item_count": len(items), "translated_count": len(translations)},
    )
    return translations


@workflow.defn
class SmokeTranslationWorkflow:
    @workflow.run
    async def run(self, params: SmokeWorkflowInput) -> SmokeWorkflowOutput:
        translations = await workflow.execute_activity(
            translate_smoke_batch,
            params,
            start_to_close_timeout=timedelta(seconds=params.timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return SmokeWorkflowOutput(
            translated_count=len(translations),
            translations=translations,
        )
