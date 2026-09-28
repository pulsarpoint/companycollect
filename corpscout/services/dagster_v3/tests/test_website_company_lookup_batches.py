import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from corpscout_identity.urls import website_reference

from dagster_v3.defs.website_crawl.matching_batches import process_matching_batch
from dagster_v3.defs.website_crawl.queue_execution import run_crawl_window
from dagster_v3.defs.website_crawl.results import CrawlResultsConfig, effective_payload
from tests.test_website_crawl_llm import LLM, ROW, SETTINGS


def test_failed_admission_keeps_original_error_when_cleanup_cannot_find_batch():
    context = SimpleNamespace(log=Mock())
    http = Mock()
    http.request.side_effect = [SimpleNamespace(status_code=500), SimpleNamespace(status_code=404)]
    item = {"request_json": json.dumps({"request_id": "request", "url": "https://example.se"}),
            "request_id": "request", "crawl_type": "site_info", "input_revision": 1,
            "work_key": "key", "run_id": "execution"}
    with pytest.raises(RuntimeError, match="POST with HTTP 500"):
        process_matching_batch(context, Mock(), http, "http://crawler", [item])
    context.log.warning.assert_called_once()


def test_saved_request_matching_flags_change_work_identity_and_can_be_overridden():
    config = CrawlResultsConfig(**SETTINGS, llm=LLM)
    _, plain_key = effective_payload(ROW, "site_info", "batch", config)
    row = dict(
        ROW,
        match_company=True,
        company_country="SE",
        skip_company_matching_if_mapped=True,
    )
    payload, matching_key = effective_payload(row, "site_info", "batch", config)
    assert payload["crawl"] is False
    assert payload["company_lookup"] == {"country": "SE", "skip_if_mapped": True}
    assert matching_key != plain_key
    _, always_key = effective_payload(
        dict(row, skip_company_matching_if_mapped=False), "site_info", "batch", config
    )
    assert always_key != matching_key
    plain, key = effective_payload(
        row, "site_info", "batch", config.model_copy(update={"match_company": False})
    )
    assert "company_lookup" not in plain and key == plain_key
    full = config.model_copy(
        update={"max_pages": 20, "page_selection": "saved", "match_company": True}
    )
    assert effective_payload(ROW, "full", "batch", full)[0]["crawl"] == "full"


def test_existing_queue_waits_for_200_published_and_confirmed_before_next_batch():
    events, sizes = [], {}
    items = [
        {
            "crawl_type": "site_info",
            "domain": f"example-{i:04d}.se",
            "website_id": website_reference(f"https://example-{i:04d}.se/"),
            "request_id": f"crawl-{i}",
            "input_revision": 1,
            "work_key": "a" * 64,
            "run_id": "execution",
            "request_json": json.dumps(
                {
                    "request_id": f"crawl-{i}",
                    "url": f"https://example-{i:04d}.se/",
                    "crawl": False,
                    "company_lookup": {"country": "SE"},
                }
            ),
        }
        for i in range(205)
    ]

    class Client:
        def execute(self, sql, params, **kwargs):
            if kwargs.get("with_column_types"):
                events.append("diagnostics")
                return [], []
            events.append("confirmed")
            return [(len(params["ids"]),)]

    class Http:
        def request(self, method, url, **kwargs):
            if method == "POST":
                body = kwargs["json"]
                size = len(body["entries"])
                sizes[body["batch_id"]] = size
                events.append(("submitted", size))
                state = {
                    "state": "running", "processed": 0, "total": size, "matched": 0, "failed": 0
                }
            else:
                events.append("published")
                state = {"state": "published", "processed": sizes[url.rsplit("/", 1)[1]]}
            return SimpleNamespace(status_code=200, json=lambda: state)

    context = SimpleNamespace(log=Mock(), log_event=Mock())
    config = CrawlResultsConfig(**SETTINGS, llm=LLM)
    with (
        patch("dagster_v3.defs.website_crawl.matching_batches.sleep"),
        patch(
            "dagster_v3.defs.website_crawl.queue_execution.remaining_crawl_entries",
            side_effect=[items, []],
        ),
        patch(
            "dagster_v3.defs.website_crawl.queue_execution.dispatchable_entries",
            side_effect=lambda client, rows, **_: rows,
        ),
    ):
        count = run_crawl_window(
            context,
            Client(),
            Http(),
            "http://crawler",
            {"task_id": "task"},
            "site_info",
            config,
        )
    assert count == 205
    assert events == [
        ("submitted", 200),
        "published",
        "confirmed",
        "confirmed",
        "diagnostics",
        ("submitted", 5),
        "published",
        "confirmed",
        "confirmed",
        "diagnostics",
    ]
