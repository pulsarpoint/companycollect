"""The LLM helpers se_company/info.py keeps for the basic-info LLM extractor (slice 4 of
the basic-info design retired the publisher around them)."""

import threading

import pytest

from dagster_v3.defs.se_company.info import (
    DESCRIPTION_PROMPT_VERSION,
    DescriptionSuggestion,
    LlmProfileConfig,
    build_llm_client,
    llm_api_key_variable,
    map_ordered,
    parse_description_suggestion,
)


def _profile(model: str, **overrides) -> LlmProfileConfig:
    return LlmProfileConfig(model=model, **overrides)


def test_parse_description_suggestion_validates_shape() -> None:
    suggestion = parse_description_suggestion(
        '{"description": "Alpha AB is a Swedish fintech company offering IT consulting.",'
        ' "description_sv": "Alpha AB aer ett svenskt fintechbolag.",'
        ' "language": "en", "rationale": "both"}'
    )
    assert isinstance(suggestion, DescriptionSuggestion) and suggestion.language == "en"
    assert suggestion.description_sv == "Alpha AB aer ett svenskt fintechbolag."
    for bad in (
        '{"description": "", "description_sv": "sv", "language": "en", "rationale": ""}',
        '{"description": "ok", "description_sv": "", "language": "en", "rationale": ""}',
        # Both languages are required: a reply with only the English half would publish a
        # company whose Swedish column silently reverts to another source's text.
        '{"description": "ok", "language": "en", "rationale": ""}',
        '{"description": "ok", "description_sv": "sv", "language": "EN"}',  # two letters, not a code
        '{"description": "ok", "description_sv": "sv", "language": "en", "extra": 1}',  # extra="forbid"
        "no json here",
        None,
    ):
        with pytest.raises(ValueError):
            parse_description_suggestion(bad)
    # v3: the prompt asks for both languages, so every v2 suggestion answers a request
    # this pipeline no longer makes (input_hash covers the prompt version).
    assert DESCRIPTION_PROMPT_VERSION == "se-company-info-description-v3"


def test_the_api_key_is_read_from_the_host_by_provider_name(monkeypatch) -> None:
    """The one thing the run config never carries. A provider whose key this host does
    not have fails with the variable's name, before any write and before any call."""
    assert llm_api_key_variable("deepseek") == "DEEPSEEK_API_KEY"
    monkeypatch.setenv("DEEPSEEK_API_KEY", "   ")
    with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
        build_llm_client(_profile("deepseek-v4-flash"), timeout_seconds=10)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    built = build_llm_client(_profile("deepseek-v4-flash", base_url="https://api.deepseek.com/"),
                             timeout_seconds=30)
    assert str(built.base_url).rstrip("/") == "https://api.deepseek.com"
    monkeypatch.delenv("OTHER_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OTHER_API_KEY"):
        build_llm_client(_profile("m", provider="other"), timeout_seconds=10)


def test_map_ordered_keeps_item_order_at_any_concurrency() -> None:
    """Results are consumed in item order however they finish; concurrency 1 runs inline."""
    barrier = threading.Barrier(2, timeout=10)

    def call(item: int) -> int:
        barrier.wait()  # both calls must be in flight together, or this deadlocks
        return item * 10

    assert list(map_ordered(call, [1, 2], concurrency=2)) == [10, 20]
    assert list(map_ordered(lambda item: item + 1, [3, 4, 5], concurrency=1)) == [4, 5, 6]
    assert list(map_ordered(lambda item: item, [], concurrency=4)) == []
