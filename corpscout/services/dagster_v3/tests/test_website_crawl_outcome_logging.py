import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import dagster as dg
import pytest

from dagster_v3.defs.website_crawl.matching_batches import process_matching_batch
from dagster_v3.defs.website_crawl.outcome_logging import log_crawl_outcomes


def outcome(**changes):
    return (
        dict(
            domain="example.se",
            request_id="crawl-test",
            attempt=1,
            state="failed",
            crawl_status="failed",
            successful=False,
            error="initial_page_unavailable",
            pages="[]",
            site_info=None,
        )
        | changes
    )


def test_navigation_errors_are_visible_and_link_to_exact_attempt():
    context = SimpleNamespace(log=Mock())
    failures = log_crawl_outcomes(
        context,
        [
            outcome(
                pages=json.dumps(
                    [
                        {
                            "source_url": "https://example.se/",
                            "errors": [],
                            "navigation_attempts": [{"error": "ERR_NAME_NOT_RESOLVED"}],
                        }
                    ]
                )
            )
        ],
        "site_info",
    )
    assert "ERR_NAME_NOT_RESOLVED" in failures[0]["reason"]
    assert "initial_page_unavailable" in failures[0]["reason"]
    assert "debug_request=crawl-test&debug_attempt=1" in failures[0]["backoffice_trace"]
    context.log.warning.assert_called_once()


def test_uncertain_classification_is_reported_but_not_found_is_not_a_failure():
    context = SimpleNamespace(log=Mock())
    record = outcome(
        crawl_status="needs_review",
        error="site_eligibility_uncertain",
        site_info=json.dumps(
            {"site_description": "The site's purpose could not be determined."}
        ),
        matching_status="not_found",
        matching_reasons=["No legal entity on the site"],
    )
    failures = log_crawl_outcomes(context, [record], "site_info")
    assert len(failures) == 1
    assert "purpose could not be determined" in failures[0]["reason"]
    assert (
        log_crawl_outcomes(context, [record | {"successful": True}], "site_info") == []
    )


def test_company_matching_failure_keeps_specific_browser_error():
    context = SimpleNamespace(log=Mock())
    record = outcome(
        successful=True,
        matching_status="failed",
        matching_stop_reason="initial_page_unavailable",
        matching_reasons=["Assigned browser is unavailable; retry in the same session"],
    )
    [failure] = log_crawl_outcomes(context, [record], "full")
    assert failure["stage"] == "company_matching"
    assert "Assigned browser is unavailable" in failure["reason"]


def test_diagnostics_are_bounded_and_do_not_log_credentials_or_url_secrets():
    context = SimpleNamespace(log=Mock())
    [failure] = log_crawl_outcomes(
        context,
        [
            outcome(
                error=(
                    "Bad https://user:private-password@api.example/path?token=query-secret "
                    'Bearer bearer-secret api_key=labelled-secret sk-provider-secret {"api_key": "quoted-secret"}\n'
                    + "x"
                    * 3000
                )
            )
        ],
        "site_info",
    )
    for secret in (
        "private-password",
        "query-secret",
        "bearer-secret",
        "labelled-secret",
        "sk-provider-secret",
        "quoted-secret",
    ):
        assert secret not in failure["reason"]
    assert len(failure["reason"]) < 2050
    assert "\n" not in failure["reason"]


def test_invalid_legacy_diagnostics_do_not_block_persisted_results():
    [failure] = log_crawl_outcomes(
        SimpleNamespace(log=Mock()), [outcome(pages="broken")], "site_info"
    )
    assert "initial_page_unavailable" in failure["reason"]
    assert "invalid JSON" in failure["reason"]


@pytest.mark.parametrize("published_count", [0, 1])
def test_batch_logs_only_verified_results_and_attaches_failure_table(published_count):
    record = outcome(
        matching_status="failed",
        matching_stop_reason="initial_page_unavailable",
        matching_reasons=["Assigned browser is unavailable"],
    )
    client = Mock()
    client.execute.side_effect = [
        [(published_count,)],
        [(published_count,)],
        ([tuple(record.values())], [(key, "String") for key in record]),
    ]
    http = Mock()
    http.request.return_value = SimpleNamespace(
        status_code=200, json=lambda: {"state": "published", "processed": 1}
    )
    context = SimpleNamespace(log=Mock(), log_event=Mock())
    items = [
        dict(
            crawl_type="site_info",
            input_revision=1,
            work_key="work",
            run_id="run",
            request_id="crawl-test",
            request_json=json.dumps({"url": "https://example.se/"}),
        )
    ]
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep"):
        if published_count == 0:
            with pytest.raises(dg.Failure, match="Published batch contains 0/1"):
                process_matching_batch(context, client, http, "http://crawler", items)
            context.log.warning.assert_not_called()
            context.log_event.assert_not_called()
            return
        assert (
            process_matching_batch(context, client, http, "http://crawler", items) == 1
        )
    assert context.log.warning.call_count == 2
    observations = [
        call.args[0]
        for call in context.log_event.call_args_list
        if isinstance(call.args[0], dg.AssetObservation)
    ]
    assert observations[0].metadata["failed_attempts"].value == 1
    assert len(observations[0].metadata["failure_details"].records) == 2
    assert (
        "Assigned browser is unavailable"
        in observations[0].metadata["failure_details"].records[1].data["reason"]
    )
