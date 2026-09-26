"""Exercise Brave concurrency and recovery across a real local HTTP boundary."""

import json
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from uuid import uuid4

import dagster as dg
import pytest

from dagster_v3.defs.company_domains import browser as brave
from dagster_v3.defs.company_domains import assets
from dagster_v3.defs.company_domains.assets import (
    BraveSearchConfig,
    company_brave_search_results,
)

SERVICE = {"api_url": "http://127.0.0.1:1", "api_token": "fixture-secret"}
LLM = {
    "provider": "openrouter",
    "base_url": "https://openrouter.ai/api/v1",
    "model": "selected-model",
    "api_key_encrypted": "v1." + "a" * 16 + "." + "b" * 32,
}


class BraveAPIFixture:
    def __init__(self, slots=1):
        self.lock = threading.Lock()
        self.started = threading.Barrier(4 * slots)
        self.release_slow = threading.Event()
        self.refilled = threading.Event()
        self.active = Counter()
        self.peak = Counter()
        self.total_peak = 0
        self.queries = []
        self.payloads = []
        self.failing_company = ""
        self.responder = None
        self.capacity_failures = 0
        self.lost_response = False
        self.results = {}
        self.verifications = []
        self.verification_result = {"ok": True}
        self.cancelled = set()
        self.sessions = {}
        self.closed_sessions = set()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def reply(self, status, data):
                body = json.dumps(data).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                identifier = self.path.rsplit("/", 1)[-1]
                result = (fixture.sessions if "/browser/sessions/" in self.path else fixture.results).get(identifier)
                self.reply(200 if result else 404, result or {})

            def do_DELETE(self):
                identifier = self.path.rsplit("/", 1)[-1]
                session = fixture.sessions[identifier]
                if self.headers.get("X-Browser-Execution-Id") != session["executionId"]:
                    self.reply(409, {})
                    return
                fixture.closed_sessions.add(identifier)
                self.reply(200, {})

            def do_POST(self):
                if self.headers.get("Authorization") != "Bearer fixture-secret":
                    self.reply(401, {})
                    return
                payload = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                if self.path == "/v1/brave/llm/verify":
                    fixture.verifications.append(payload)
                    self.reply(200, fixture.verification_result)
                    return
                if self.path.endswith("/cancel"):
                    identifier = self.path.split("/")[-2]
                    fixture.cancelled.add(identifier)
                    fixture.release_slow.set()
                    self.reply(200, {"status": "error", "error_type": "Cancelled"})
                    return
                if fixture.capacity_failures:
                    fixture.capacity_failures -= 1
                    self.reply(503, {})
                    return
                route, query = payload["route"], payload["query"]
                with fixture.lock:
                    fixture.payloads.append(payload)
                    if payload.get("session_id"):
                        identifier = payload["session_id"]
                        fixture.sessions[identifier] = {
                            "requestId": f"brave-{identifier}",
                            "executionId": f"execution-{identifier}",
                        }
                    fixture.active[route] += 1
                    fixture.peak[route] = max(
                        fixture.peak[route], fixture.active[route]
                    )
                    fixture.total_peak = max(
                        fixture.total_peak, sum(fixture.active.values())
                    )
                    fixture.queries.append(query)
                    first_wave = len(fixture.queries) <= fixture.started.parties
                try:
                    if fixture.responder is None:
                        if first_wave:
                            fixture.started.wait(5)
                        else:
                            fixture.refilled.set()
                        if route == "direct":
                            assert fixture.release_slow.wait(5)
                    result = {
                        "request_id": payload["request_id"],
                        "query": query,
                        "route": route,
                        "source_url": "https://search.brave.com/ask",
                        "fetched_at": datetime.now(UTC).isoformat(),
                        "status": "success",
                        "answer": f"Answer for {query}\nhttps://company.test/",
                        "error_type": "",
                        "error_stage": "",
                        "elapsed_ms": 10,
                        "challenge_runs": [],
                    }
                    if fixture.failing_company and query.endswith(
                        fixture.failing_company
                    ):
                        result.update(
                            status="error",
                            answer="",
                            error_type="RuntimeError",
                            error_stage="answer_generation",
                        )
                    if fixture.responder:
                        result.update(fixture.responder(payload))
                    if payload["request_id"] in fixture.cancelled:
                        result.update(status="error", answer="", error_type="Cancelled")
                    fixture.results[payload["request_id"]] = result
                    if fixture.lost_response:
                        self.close_connection = True
                    else:
                        self.reply(200, result)
                finally:
                    with fixture.lock:
                        fixture.active[route] -= 1

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.config = dict(
            SERVICE, api_url=f"http://127.0.0.1:{self.server.server_port}"
        )

    def close(self):
        self.release_slow.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


@pytest.fixture
def brave_api(request):
    def create(slots=1):
        fixture = BraveAPIFixture(slots)
        request.addfinalizer(fixture.close)
        return fixture

    return create


def companies(count):
    for i in range(count):
        yield brave.CompanySearchInput(
            str(i), f"Company {i} AB", f"Find Company {i} AB", str(i), 60_000
        )


@pytest.mark.parametrize("slots", [1, 2])
def test_fast_routes_refill_while_a_slow_route_is_still_busy(brave_api, slots):
    fixture = brave_api(slots)
    resource = brave.BraveBrowserResource(**fixture.config)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            lambda: list(
                resource.iter_answers(
                    companies(24), requests_per_route=slots, on_result=lambda _: None
                )
            )
        )
        try:
            assert fixture.refilled.wait(5)
        finally:
            fixture.release_slow.set()
        results = future.result(timeout=10)
    assert len(results) == 24
    assert fixture.total_peak == 4 * slots
    assert dict(fixture.peak) == {route: slots for route in brave.ROUTES}
    assert all(
        r.status == "success" and r.answer.startswith(f"Answer for {r.query}")
        for r in results
    )
    assert all("secret" not in repr(r) for r in results)
    # Every sequential route worker retains its identity, without sharing profiles
    # with concurrent workers or another proxy route.
    session_routes = {}
    for payload in fixture.payloads:
        identifier = payload["session_id"]
        assert len(identifier) == 32
        assert (
            session_routes.setdefault(identifier, payload["route"]) == payload["route"]
        )
    assert Counter(session_routes.values()) == {route: slots for route in brave.ROUTES}
    assert len(fixture.payloads) > len(session_routes)
    assert fixture.closed_sessions == set(session_routes)


def test_browser_cleanup_does_not_release_another_owner(brave_api):
    fixture = brave_api()
    identifier = uuid4().hex
    fixture.sessions[identifier] = {"requestId": "another-owner", "executionId": "foreign"}
    brave.BraveBrowserResource(**fixture.config).close_session(identifier)
    assert fixture.closed_sessions == set()


def test_one_company_failure_does_not_stop_remaining_queue(brave_api):
    fixture = brave_api()
    fixture.release_slow.set()
    fixture.failing_company = "Company 0 AB"
    results = list(
        brave.BraveBrowserResource(**fixture.config).iter_answers(
            companies(12), requests_per_route=1, on_result=lambda _: None
        )
    )
    assert Counter(r.status for r in results) == {"success": 11, "error": 1}
    assert next(r for r in results if r.status == "error").company.company_id == "0"


def test_closing_results_stops_lazy_input(brave_api):
    fixture = brave_api()
    fixture.release_slow.set()
    claimed = []

    def inputs():
        for company in companies(100_000):
            claimed.append(company)
            yield company

    with closing(
        brave.BraveBrowserResource(**fixture.config).iter_answers(
            inputs(), requests_per_route=1, on_result=lambda _: None
        )
    ) as results:
        next(results)
    assert len(claimed) < 25
    assert not any(fixture.active.values())


def test_empty_queue_opens_no_http_session(monkeypatch):
    def unexpected(**kwargs):
        pytest.fail("empty queue must not open HTTP sessions")

    monkeypatch.setattr(brave, "Session", unexpected)
    assert (
        list(
            brave.BraveBrowserResource(**SERVICE).iter_answers(
                iter(()), requests_per_route=1, on_result=lambda _: None
            )
        )
        == []
    )


def test_failed_http_setup_is_sanitized_and_does_not_hang(monkeypatch):
    def fail(**kwargs):
        raise RuntimeError("http://user:secret@proxy.test")

    monkeypatch.setattr(brave, "Session", fail)
    with pytest.raises(RuntimeError) as caught:
        list(
            brave.BraveBrowserResource(**SERVICE).iter_answers(
                companies(1), requests_per_route=1, on_result=lambda _: None
            )
        )
    assert "secret" not in str(caught.value)


def test_rendered_query_is_saved_before_refill(brave_api):
    fixture = brave_api()
    fixture.release_slow.set()
    saved = set()

    def inputs():
        for i in range(8):
            if i >= 4:
                assert saved
            yield brave.CompanySearchInput(
                str(i), f"Company {i}", f"Who owns example{i}.se?", str(i), 60_000
            )

    results = list(
        brave.BraveBrowserResource(**fixture.config).iter_answers(
            inputs(),
            requests_per_route=1,
            on_result=lambda r: saved.add(r.company.request_id),
        )
    )
    assert len(saved) == len(results) == 8
    assert {r.query for r in results} == {f"Who owns example{i}.se?" for i in range(8)}


def test_capacity_retry_and_lost_response_do_not_repeat_query(brave_api):
    fixture = brave_api()
    fixture.capacity_failures = 1
    fixture.lost_response = True
    fixture.responder = lambda _: {}
    results = list(
        brave.BraveBrowserResource(**fixture.config).iter_answers(
            companies(1), requests_per_route=1, on_result=lambda _: None
        )
    )
    assert results[0].status == "success"
    assert len(fixture.queries) == 1


def test_agent_options_and_failure_details_cross_http_boundary(brave_api):
    fixture = brave_api()
    fixture.responder = lambda _: {
        "status": "blocked",
        "error_type": "AgentBudgetExhausted",
        "error_stage": "captcha",
        "challenge_runs": [{"state": "blocked"}],
    }
    resource = brave.BraveBrowserResource(
        **fixture.config,
        challenge_agent_max_runs=6,
        challenge_agent_model="z-ai/glm-5.3-flash",
        max_requests_per_browser=20,
    )
    [result] = list(
        resource.iter_answers(
            companies(1), requests_per_route=1, on_result=lambda _: None
        )
    )
    assert result.error_type == "AgentBudgetExhausted"
    assert result.error_stage == "captcha"
    assert result.answer == ""
    assert result.challenge_runs == [{"state": "blocked"}]
    assert fixture.payloads[0]["challenge_agent_max_runs"] == 6
    assert fixture.payloads[0]["challenge_agent_model"] == "z-ai/glm-5.3-flash"
    assert fixture.payloads[0]["max_requests_per_browser"] == 20


def test_asset_serializes_runs_and_config_bounds_route_concurrency():
    assert company_brave_search_results.group_names_by_key == {
        dg.AssetKey("company_brave_search_results"): "brave_domain_search",
    }
    assert company_brave_search_results.op.pool == "company_domains_brave"
    assert BraveSearchConfig().requests_per_route == 1
    assert BraveSearchConfig().input_relation is None
    assert BraveSearchConfig().input_batch_size == 500
    for invalid in [0, -1]:
        with pytest.raises(ValueError):
            BraveSearchConfig(requests_per_route=invalid)
        with pytest.raises(ValueError):
            BraveSearchConfig(input_batch_size=invalid)


def test_timeout_configuration_rejects_unbounded_or_reversed_limits():
    assert BraveSearchConfig().answer_timeout_seconds == 60
    assert BraveSearchConfig().force is False
    assert BraveSearchConfig().rescan_old is False
    for config in (
        {"answer_timeout_seconds": 0},
        {"answer_timeout_seconds": 601},
        {"execution_id": "not-a-uuid"},
    ):
        with pytest.raises(ValueError):
            BraveSearchConfig(**config)


def test_selected_profile_is_verified_before_input_and_transported_unchanged(brave_api):
    fixture = brave_api()
    fixture.responder = lambda _: {}
    claimed = []

    def inputs():
        assert fixture.verifications == [{"llm": LLM}]
        claimed.append(True)
        yield from companies(1)

    resource = brave.BraveBrowserResource(
        **fixture.config, challenge_agent_model="deepseek-flash"
    )
    results = list(resource.iter_answers(
        inputs(), requests_per_route=1, on_result=lambda _: None, llm=LLM
    ))
    assert len(results) == 1 and claimed
    assert fixture.payloads[0]["llm"] == LLM
    assert "challenge_agent_model" not in fixture.payloads[0]
    company = next(companies(1))
    assert fixture.payloads[0]["request_id"] == brave.browser_request_id(company, LLM)
    rotated = dict(LLM, api_key_encrypted="v1." + "c" * 16 + "." + "d" * 32)
    assert brave.browser_request_id(company, rotated) == brave.browser_request_id(company, LLM)
    assert brave.browser_request_id(company, dict(LLM, model="other")) != brave.browser_request_id(company, LLM)


@pytest.mark.parametrize("response", [{"ok": False, "error": "Model has no endpoints"}, {}, []])
def test_failed_verification_does_not_consume_or_submit_inputs(brave_api, response):
    fixture = brave_api()
    fixture.verification_result = response

    def inputs():
        pytest.fail("failed preflight must not consume queued companies")
        yield

    with pytest.raises(ValueError, match="No Brave searches|no Brave searches"):
        list(brave.BraveBrowserResource(**fixture.config).iter_answers(
            inputs(), requests_per_route=1, on_result=lambda _: None, llm=LLM
        ))
    assert fixture.queries == []


def test_interrupted_consumer_cancels_only_active_requests_without_failed_outcomes(brave_api):
    fixture = brave_api()
    saved = []
    with closing(brave.BraveBrowserResource(**fixture.config).iter_answers(
        companies(4), requests_per_route=1, on_result=saved.append
    )) as results:
        next(results)
    assert fixture.cancelled
    assert fixture.cancelled.issubset({payload["request_id"] for payload in fixture.payloads})
    assert all(result.status == "success" for result in saved)
    assert not any(fixture.active.values())


def test_resume_retries_only_cancelled_request_under_deterministic_suffix(brave_api):
    fixture = brave_api()
    fixture.responder = lambda _: {}
    fixture.results["dagster-0"] = {
        "request_id": "dagster-0", "query": "Find Company 0 AB", "route": "direct",
        "status": "error", "error_type": "Cancelled",
    }
    [result] = list(brave.BraveBrowserResource(**fixture.config).iter_answers(
        companies(1), requests_per_route=1, on_result=lambda _: None
    ))
    assert result.status == "success"
    assert len(fixture.payloads) == 1
    assert fixture.payloads[0]["request_id"] == "dagster-0-retry-1"
    assert fixture.results["dagster-0"]["error_type"] == "Cancelled"


def resume_context(execution, supplied):
    original_id = execution["execution_id"]
    resumed_id = str(uuid4())
    original = SimpleNamespace(tags={assets.EXECUTION_TAG: json.dumps(execution)})
    resumed = SimpleNamespace(tags={"processing/task_id": execution["task_id"]})
    runs = {original_id: original, resumed_id: resumed}
    instance = SimpleNamespace(
        get_run_by_id=lambda run_id: runs[run_id],
        add_run_tags=lambda run_id, tags: runs[run_id].tags.update(tags),
    )
    context = SimpleNamespace(
        run=SimpleNamespace(
            run_id=resumed_id, root_run_id=None,
            run_config={"ops": {"company_brave_search_results": {"config": supplied}}},
        ),
        instance=instance,
    )
    return context, runs


def test_legacy_execution_adopts_profile_once_and_resume_keeps_frozen_ciphertext():
    execution = {"execution_id": str(uuid4()), "task_id": str(uuid4()), "query_type": "official_website"}
    context, runs = resume_context(execution, {"llm": LLM})
    result = assets.prepare_execution(
        context, BraveSearchConfig(execution_id=execution["execution_id"], llm=LLM), None, None
    )
    assert result["llm"] == LLM
    assert result["execution_id"] == execution["execution_id"]
    assert json.loads(runs[execution["execution_id"]].tags[assets.EXECUTION_TAG])["llm"] == LLM

    rotated = dict(LLM, api_key_encrypted="v1." + "c" * 16 + "." + "d" * 32)
    context, runs = resume_context(result, {"llm": rotated})
    resumed = assets.prepare_execution(
        context, BraveSearchConfig(execution_id=execution["execution_id"], llm=rotated), None, None
    )
    assert resumed["llm"] == LLM
    assert json.loads(runs[context.run.run_id].tags[assets.EXECUTION_TAG])["llm"] == LLM

    context, _ = resume_context(result, {"llm": dict(LLM, model="changed")})
    with pytest.raises(ValueError, match="selected LLM"):
        assets.prepare_execution(
            context, BraveSearchConfig(execution_id=execution["execution_id"], llm=dict(LLM, model="changed")), None, None
        )


def test_brave_rejects_plaintext_credentials_in_selected_profile_and_config():
    with pytest.raises(ValueError):
        BraveSearchConfig(llm=dict(LLM, api_key="never-persist"))
    with pytest.raises(ValueError):
        BraveSearchConfig(api_key="never-persist")
    with pytest.raises(ValueError):
        BraveSearchConfig(llm=dict(LLM, base_url="https://key:secret@example.test/v1"))


def test_resume_recovers_completed_browser_response_with_new_worker_session(brave_api):
    fixture = brave_api()
    fixture.responder = lambda _: {}
    resource = brave.BraveBrowserResource(**fixture.config)
    first = list(resource.iter_answers(
        companies(1), requests_per_route=1, on_result=lambda _: None
    ))
    resumed = list(resource.iter_answers(
        companies(1), requests_per_route=1, on_result=lambda _: None
    ))
    assert len(fixture.payloads) == 1
    assert resumed == first
