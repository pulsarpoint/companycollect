"""The LLM handoff contains encrypted credentials without changing content identity."""

import json
from urllib.parse import urlsplit

import pytest
from pydantic import ValidationError

from dagster_v3.defs.website_crawl.results import (
    CrawlResultsConfig,
    effective_payload,
)

SETTINGS = {
    "challenge_agent_model": "deepseek-flash",
    "challenge_agent_max_runs": 3,
    "api": "deepseek",
    "model": "deepseek-flash",
    "max_pages": 1,
    "max_model_calls": 20,
    "page_selection": "basic_info",
}
LLM = {
    "provider": "deepseek",
    "base_url": "https://api.deepseek.com/v1",
    "model": "deepseek-flash",
    "api_key_encrypted": "v1.AAECAwQFBgcICQoL." + "A" * 23,
}
ROTATED_LLM = {**LLM, "api_key_encrypted": "v1.DAECAwQFBgcICQoL." + "B" * 23}
ROW = {
    "domain": "example.com",
    "website_url": "https://example.com/",
    "revision": 1,
    "preset_version": 1,
    "proxy_route": "direct",
    "config_json": "{}",
    "save_artifacts": True,
    "headless": True,
    "page_mode": "auto",
    "pages": [],
    "instructions": "",
}


def test_encrypted_profile_reaches_crawler_without_changing_work_key_on_rotation():
    payload, key = effective_payload(
        ROW, "site_info", "batch", CrawlResultsConfig(**SETTINGS, llm=LLM)
    )
    rotated, rotated_key = effective_payload(
        ROW, "site_info", "batch", CrawlResultsConfig(**SETTINGS, llm=ROTATED_LLM)
    )
    assert payload["llm"] == LLM
    assert rotated["llm"] == ROTATED_LLM
    assert payload["request_id"] == rotated["request_id"]
    assert key == rotated_key


@pytest.mark.parametrize(
    "change",
    [
        {"provider": "openrouter"},
        {"base_url": "https://other-model-host.example/v1"},
        {"model": "new-model"},
    ],
)
def test_public_llm_profile_changes_work_key(change):
    _, original = effective_payload(
        ROW, "site_info", "batch", CrawlResultsConfig(**SETTINGS, llm=LLM)
    )
    llm = {**LLM, **change}
    config = {
        **SETTINGS,
        "model": llm["model"],
        "api": "deepseek"
        if llm["provider"] == "deepseek"
        or urlsplit(llm["base_url"]).hostname == "api.deepseek.com"
        else "openrouter",
        "llm": llm,
    }
    _, changed = effective_payload(
        ROW, "site_info", "batch", CrawlResultsConfig(**config)
    )
    assert changed != original


@pytest.mark.parametrize("field,value", [("model", "different"), ("api", "openrouter")])
def test_conflicting_model_or_api_is_rejected(field, value):
    with pytest.raises(ValidationError, match="must match"):
        CrawlResultsConfig(**{**SETTINGS, field: value, "llm": LLM})


@pytest.mark.parametrize(
    "override",
    [
        {"api_key": "plaintext-test-key"},
        {"provider": {"apiKey": "plaintext-test-key"}},
        {"headers": {"Authorization": "plaintext-test-key"}},
        {"fallbacks": [{"OPENROUTER_API_KEY": "plaintext-test-key"}]},
        {"llm": LLM},
    ],
)
def test_plaintext_credentials_are_refused_in_execution_and_saved_overrides(override):
    with pytest.raises(ValidationError, match="cannot contain credentials") as caught:
        CrawlResultsConfig(**SETTINGS, crawler_config=override)
    assert "plaintext-test-key" not in str(caught.value)
    with pytest.raises(ValueError, match="cannot contain credentials"):
        effective_payload(
            {**ROW, "config_json": json.dumps(override)},
            "site_info",
            "batch",
            CrawlResultsConfig(**SETTINGS),
        )


@pytest.mark.parametrize(
    "change",
    [
        {"api_key": "plaintext-test-key"},
        {"api_key_encrypted": "plaintext-test-key"},
        {"base_url": "https://user:plaintext-test-key@example.com/v1"},
        {"base_url": "https://example.com/v1?api_key=plaintext-test-key"},
        {"base_url": "file:///tmp/model"},
        {"base_url": "https://example.com:bad-port/v1"},
        {"base_url": "https://example.com/v 1"},
        {"base_url": "\nhttps://example.com/v1"},
        {"model": " deepseek-flash"},
        {"provider": "deepseek\n"},
    ],
)
def test_envelope_rejects_plaintext_and_credential_urls(change):
    with pytest.raises(ValidationError) as caught:
        CrawlResultsConfig(**SETTINGS, llm={**LLM, **change})
    assert "plaintext-test-key" not in str(caught.value)


def test_legacy_config_still_has_no_llm_payload():
    payload, _ = effective_payload(
        ROW, "site_info", "batch", CrawlResultsConfig(**SETTINGS)
    )
    assert "llm" not in payload


def test_deepseek_endpoint_uses_deepseek_api_with_a_custom_provider_label():
    config = CrawlResultsConfig(
        **SETTINGS, llm={**LLM, "provider": "Our DeepSeek profile"}
    )
    payload, _ = effective_payload(ROW, "site_info", "batch", config)
    assert payload["api"] == "deepseek"
