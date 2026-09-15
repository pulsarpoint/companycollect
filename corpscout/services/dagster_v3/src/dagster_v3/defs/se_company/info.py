"""LLM helpers shared by the basic-info LLM extractor (basic_info/llm.py).

This module used to be the Swedish company-information publisher; slice 4 of the basic-info
design (2026-09-08) retired that publisher, its jobs, its schedule and its field-value
sensor together with the se_company_info* tables. What stays is what the extractor imports:
the model profile config, the client factory keyed by provider, the description-answer
parser, the ordered concurrent map, and the names of the observation cache
(se_company_info_enrichment_observation) the extractor reads and writes through
common.py's observation helpers.
"""

import os
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

import dagster as dg
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field, ValidationError

DESCRIPTION_PROMPT_VERSION = "se-company-info-description-v3"
# The observation cache: one row per (company, input_hash) model answer, kept across the
# publisher's retirement because every row is a paid call the extractor can reuse.
SE_COMPANY_INFO_OBSERVATION = "se_company_info_enrichment_observation"
OBSERVATION_FLUSH_ROWS = 200
OBSERVATION_COLUMNS = ("suggestion_id", "company_id", "input_hash", "suggestion", "raw_response",
                       "model_provider", "model_name", "prompt_version", "prompt_tokens", "completion_tokens",
                       "source_run_id", "created_at")


class LlmProfileConfig(dg.Config):
    """The model this run may call, as run config -- never read from host env.

    The host contributes exactly one thing: the API key, looked up by provider name
    (``DEEPSEEK_API_KEY`` for provider ``deepseek``). Everything that decides what the
    call costs and what it says travels in the run config, so a run's own record shows
    which model wrote its descriptions and a caller can switch models without a
    deployment. ``prompt_version`` is part of the observation cache key, so changing it
    invalidates every stored suggestion rather than silently reusing answers to a
    different prompt.
    """

    provider: str = Field(default="deepseek", min_length=1, max_length=64)
    model: str = Field(default="deepseek-v4-flash", min_length=1, max_length=200)
    base_url: str = Field(default="https://api.deepseek.com", min_length=1, max_length=2_048)
    temperature: float = Field(default=0, ge=0, le=2)
    # deepseek-v4-flash is a reasoning model: reasoning_content counts against
    # max_tokens, and the answer carries two summaries.
    max_tokens: int = Field(default=6_000, ge=256, le=32_000)
    prompt_version: str = Field(default=DESCRIPTION_PROMPT_VERSION, min_length=1, max_length=120)
    # Model calls issued at once. 1 is the sequential path; the cap is deliberately low --
    # these are paid calls against one vendor account.
    concurrency: int = Field(default=1, ge=1, le=8)


# Today's production model, pinned here rather than left to the field defaults so that
# changing a default can never silently change what an automated run calls.
DEFAULT_LLM_PROFILE: dict[str, Any] = {
    "provider": "deepseek",
    "model": "deepseek-v4-flash",
    "base_url": "https://api.deepseek.com",
    "temperature": 0,
    "max_tokens": 6_000,
    "prompt_version": DESCRIPTION_PROMPT_VERSION,
    "concurrency": 1,
}


def llm_api_key_variable(provider: str) -> str:
    """The host environment variable holding this provider's key."""
    return f"{provider.upper()}_API_KEY"


def build_llm_client(
    profile: LlmProfileConfig, *, timeout_seconds: int, api_key_environment_variable: str = ""
) -> OpenAI:
    """The OpenAI-compatible client for ``profile``, or a clear failure.

    Called before any page is touched, so a run configured for a provider whose key this
    host does not carry fails without having written a row or spent a call.
    """
    variable = api_key_environment_variable or llm_api_key_variable(profile.provider)
    api_key = os.getenv(variable, "").strip()
    if not api_key:
        raise ValueError(
            f"No API key for LLM provider {profile.provider!r}: set {variable} on the "
            "Dagster host, or run with resolve_multi_source_with_llm: false")
    return OpenAI(base_url=profile.base_url.rstrip("/"), api_key=api_key,
                  timeout=float(timeout_seconds), max_retries=2)


class DescriptionSuggestion(BaseModel):
    """One model answer: the same company description in both published languages.

    Both are required. A reply carrying only the English half would publish a company
    whose Swedish column silently falls back to another source's text. ``language``
    describes ``description`` (always "en" in practice); ``description_sv`` is Swedish
    by definition, so it carries no language of its own.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    description: str = Field(min_length=1, max_length=2000)
    description_sv: str = Field(min_length=1, max_length=2000)
    language: str = Field(min_length=2, max_length=2, pattern="^[a-z]{2}$")
    rationale: str = Field(default="", max_length=2000)


def parse_description_suggestion(content: str | None) -> DescriptionSuggestion:
    if content is None:
        raise ValueError("Description request returned no content")
    start, end = content.find("{"), content.rfind("}")
    if start < 0 or end < start:
        raise ValueError(f"Description request did not return a JSON object: {content[:160]!r}")
    try:
        return DescriptionSuggestion.model_validate_json(content[start : end + 1])
    except ValidationError as exc:
        raise ValueError(f"Description response failed validation: {exc}") from exc


T = TypeVar("T")
R = TypeVar("R")


def map_ordered(call: Callable[[T], R], items: Sequence[T], *, concurrency: int) -> Iterator[R]:
    """``call`` over every item, at most ``concurrency`` at a time, results in item order.

    concurrency=1 runs inline with no thread pool at all, so the default path is
    literally the sequential one -- and the caller's own ordering (observation rows,
    the flush every OBSERVATION_FLUSH_ROWS, the published row order) holds unchanged at
    every concurrency, because results are consumed in item order however they finish.
    """
    if concurrency <= 1 or len(items) <= 1:
        for item in items:
            yield call(item)
        return
    with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="se_company_info_llm") as pool:
        yield from pool.map(call, items)
