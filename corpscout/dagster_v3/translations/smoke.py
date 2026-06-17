from __future__ import annotations

import json

from openai import OpenAI

from translations.types import SmokeTranslationInput, SmokeTranslationResult


DEFAULT_MAX_TOKENS = 2048
DEFAULT_EXTRA_BODY = {"chat_template_kwargs": {"enable_thinking": False}}


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
        "/no_think\n"
        "Translate each Norwegian company registry text fragment to English. "
        "Preserve legal and business meaning. Do not add explanations. "
        "Do not produce reasoning, chain-of-thought, thinking tags, or Markdown. "
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
    missing_item_ids = expected_item_ids - seen
    if missing_item_ids:
        raise ValueError(f"missing item_id values: {sorted(missing_item_ids)}")
    return results


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
        self.client = client or OpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
            max_retries=0,
        )

    def translate(
        self,
        items: list[SmokeTranslationInput],
        *,
        timeout_seconds: int,
    ) -> list[SmokeTranslationResult]:
        prompt = build_smoke_translation_prompt(items)
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
