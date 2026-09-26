from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest

from dagster_v3.defs.company_domains import captcha_stats as stats
from dagster_v3.defs.common.llm_control import control_transaction
from tests.test_processing_store import processing_postgres_url, store


def test_older_success_preserves_usage_without_inventing_confirmation_time():
    result = stats.request_stats({
        "status": "success", "route": "crawl_proxy1", "challenge_runs": [
            {"model": "model", "startedAt": "2026-09-26T10:00:00+00:00", "state": "interrupted",
             "usage": {"prompt_tokens": 90, "completion_tokens": 12}},
            {"model": "model", "usage": {"prompt_tokens": 10, "completion_tokens": 2}},
        ],
    })
    assert result["presented"] is True
    assert result["cleared"] is True
    assert result["confirmed_at"] is None
    assert result["detection_source"] == "agent_start"
    assert (result["prompt_tokens"], result["completion_tokens"]) == (100, 14)
    assert result["models"] == ["model"]
    assert result["proxy_route"] == "crawl_proxy1"


def test_model_claim_is_not_confirmation_and_usage_can_be_unknown():
    result = stats.request_stats({"status": "blocked", "error_stage": "captcha",
                                  "challenge_runs": [{"state": "appears_clear"}]})
    assert result["presented"] is True
    assert result["cleared"] is False
    assert result["confirmed_at"] is None
    assert result["prompt_tokens"] is None


def test_disabled_agent_still_records_challenge_and_no_challenge_is_explicit():
    blocked = stats.request_stats({"status": "blocked", "error_stage": "captcha", "challenge_runs": []})
    assert blocked["presented"] is True
    clear = stats.request_stats({"status": "success", "challenge_runs": []})
    assert clear["presented"] is False
    assert clear["prompt_tokens"] == 0
    assert stats.request_stats({"status": "interrupted"})["presented"] is None


def test_live_operation_records_detection_usage_and_verified_time():
    result = stats.request_stats({"status": "running", "operation": {
        "route": "direct", "captcha": {"presented": True, "detected_at": "2026-09-26T10:00:00+00:00",
                                          "confirmed_at": "2026-09-26T10:00:10+00:00"},
        "challenge_runs": [{"usage": {"prompt_tokens": 123, "completion_tokens": 4}}],
    }})
    assert result["confirmed_at"] == "2026-09-26T10:00:10+00:00"
    assert result["detection_source"] == "page"
    assert result["prompt_tokens"] == 123
    assert result["proxy_route"] == "direct"
    assert stats.request_stats({"route": "http://user:secret@proxy"})["proxy_route"] is None


@pytest.fixture
def database(store, monkeypatch):
    _, dsn = store
    directory = Path(__file__).parents[3] / "database/migrations"
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        for name in ("000127_llm_lifecycle", "000130_brave_request_captcha_stats"):
            cursor.execute((directory / f"{name}.up.sql").read_text())
    monkeypatch.setenv("LLM_CONTROL_PG_URL", dsn)
    monkeypatch.setenv("BROWSER_API_URL", "http://browser.test")
    monkeypatch.setenv("BROWSER_API_TOKEN", "private-test-key")


def test_collector_is_idempotent_logs_detection_and_completion_and_never_changes_task_state(database, monkeypatch):
    result_id, owner_id, run_id = [str(uuid4()) for _ in range(3)]
    external_id = f"dagster-{result_id}-abcdef1234"
    with control_transaction() as cursor:
        cursor.execute("INSERT INTO processing.run_requests(request_id,dagster_run_id,job_name) VALUES (%s,%s,'test')", (owner_id, run_id))
        cursor.execute("INSERT INTO processing.llm_external_requests(service,external_request_id,request_id) VALUES ('brave',%s,%s)", (external_id, owner_id))
    logs, events = [], []
    context = SimpleNamespace(log=SimpleNamespace(info=logs.append), instance=SimpleNamespace(
        get_run_by_id=lambda identifier: SimpleNamespace(run_id=identifier),
        report_engine_event=lambda message, **kwargs: events.append((message, kwargs)),
    ))
    payload = {"request_id": external_id, "status": "running", "operation": {"stage": "captcha", "agent_runs": 0}}
    monkeypatch.setattr(stats.requests, "get", lambda *args, **kwargs: SimpleNamespace(status_code=200, json=lambda: payload, raise_for_status=lambda: None))
    assert stats.collect_request_stats(context) == 1
    assert stats.collect_request_stats(context) == 1
    assert len(events) == 1
    payload = {"request_id": external_id, "status": "running", "operation": {
        "captcha": {"presented": True, "confirmed_at": "2026-09-26T10:00:05+00:00"}}}
    stats.collect_request_stats(context)
    payload["operation"]["captcha"]["confirmed_at"] = None
    stats.collect_request_stats(context)
    with control_transaction() as cursor:
        cursor.execute("SELECT captcha_stats FROM processing.llm_external_requests")
        assert cursor.fetchone()["captcha_stats"]["confirmed_at"] is None
    payload = {"request_id": external_id, "status": "success", "route": "crawl_proxy2",
               "captcha": {"presented": True, "confirmed_at": "2026-09-26T10:00:10+00:00"},
               "challenge_runs": [{"usage": {"prompt_tokens": 150, "completion_tokens": 10}}]}
    assert stats.collect_request_stats(context) == 1
    assert stats.collect_request_stats(context) == 0
    assert len(logs) == len(events) == 3
    assert "private-test-key" not in str(events)
    with control_transaction() as cursor:
        cursor.execute("SELECT * FROM processing.llm_external_requests")
        row = cursor.fetchone()
        assert str(row["brave_result_id"]) == result_id
        assert row["captcha_stats"]["prompt_tokens"] == 150
        assert row["captcha_stats_complete"] is True
        assert row["state"] == "submitted"
    assert events[-1][1]["dagster_run"].run_id == run_id
