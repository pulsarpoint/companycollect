"""Source-grounded relationship answers shared by report and website adapters."""

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import uuid4

from openai import OpenAI, OpenAIError
from pydantic import BaseModel, ConfigDict, Field

from dagster_v3.defs.se_company.info import LlmProfileConfig


class EvidenceQuote(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    evidence_id: str = Field(min_length=1)
    quote: str = Field(min_length=3, max_length=4_000)


class RelationshipStatement(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    related_entity_name: str = Field(max_length=500)
    description: str = Field(min_length=1, max_length=4_000)
    relationship_supported: bool
    evidence: list[EvidenceQuote] = Field(min_length=1, max_length=10)


class RelationshipAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    statements: list[RelationshipStatement] = Field(min_length=1, max_length=20)


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def model_settings(profile: LlmProfileConfig) -> str:
    # Concurrency does not change an answer. Every semantic call setting does.
    return json_text({"provider": profile.provider, "model": profile.model,
                      "base_url": profile.base_url.rstrip("/"), "temperature": profile.temperature,
                      "max_tokens": profile.max_tokens, "response_format": "json_object",
                      "thinking": "enabled" if profile.provider == "deepseek" else "default"})


def parse_relationship_answer(content: str, payload: Mapping[str, Any]) -> RelationshipAnswer:
    text = content.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*\n(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    answer = RelationshipAnswer.model_validate_json(fenced[1] if fenced else text)
    source_text = {}
    for item in payload["evidence"]:
        context = item["source_context"]
        source_text[item["id"]] = [" ".join(str(part).split()) for part in (
            context["text"], context.get("local_text", ""),
            *context.get("headings", []), *context.get("table_headers", []),
            *context.get("attributes", []),
        )]
    for statement in answer.statements:
        cited_text = []
        for evidence in statement.evidence:
            if evidence.evidence_id not in source_text:
                raise ValueError("The response cites an unknown evidence ID")
            fragments = source_text[evidence.evidence_id]
            if not any(" ".join(evidence.quote.split()) in fragment for fragment in fragments):
                raise ValueError("The response quotes text absent from its cited evidence")
            cited_text.extend(fragments)
        if statement.related_entity_name and not any(
            statement.related_entity_name.casefold() in fragment.casefold() for fragment in cited_text
        ):
            raise ValueError("The related entity name is absent from its cited evidence")
    return answer


def analyze_context(
    payload: Mapping[str, Any], *, system_prompt: str, profile: LlmProfileConfig,
    client: OpenAI, run_id: str, max_input_chars: int,
) -> dict[str, Any]:
    input_json = json_text(payload)
    settings = model_settings(profile)
    result = dict(
        attempt_id=str(uuid4()),
        input_hash=digest(json_text([input_json, system_prompt, profile.prompt_version, settings])),
        analysis_version=profile.prompt_version, prompt_hash=digest(system_prompt),
        model_config_json=settings, input_json=input_json, raw_response="", statements_json="[]",
        status="needs_context", error_message="", model_provider=profile.provider,
        model_name=profile.model, prompt_tokens=0, completion_tokens=0,
        source_run_id=run_id, analyzed_at=datetime.now(UTC),
    )
    if not payload["evidence"]:
        result["error_message"] = "No usable saved source context is available"
        return result
    if len(input_json) > max_input_chars:
        result.update(status="context_limit", error_message="Input exceeds max_input_chars; source evidence was retained")
        return result
    try:
        response = client.chat.completions.create(
            model=profile.model, temperature=profile.temperature, max_tokens=profile.max_tokens,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": input_json}],
            **({"extra_body": {"thinking": {"type": "enabled"}}} if profile.provider == "deepseek" else {}),
        )
    except OpenAIError as exc:
        result.update(status="http_error", error_message=str(exc)[:4_000])
        return result
    if response.usage is not None:
        result.update(prompt_tokens=response.usage.prompt_tokens or 0,
                      completion_tokens=response.usage.completion_tokens or 0)
    result["raw_response"] = (response.choices[0].message.content or "") if response.choices else ""
    if response.choices and response.choices[0].finish_reason == "length":
        result.update(status="invalid_response", error_message="Model reached max_tokens; increase profile.max_tokens and retry",
                      analyzed_at=datetime.now(UTC))
        return result
    try:
        answer = parse_relationship_answer(result["raw_response"], payload)
        result.update(status="success", statements_json=json_text(answer.model_dump()["statements"]))
    except ValueError as exc:
        result.update(status="invalid_response", error_message=str(exc)[:4_000])
    result["analyzed_at"] = datetime.now(UTC)
    return result
