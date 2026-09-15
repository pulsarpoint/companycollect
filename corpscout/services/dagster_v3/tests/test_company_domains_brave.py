"""Brave's worker pool must refill idle routes and preserve per-company answers."""

import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_v3.defs.company_domains import browser as brave
from dagster_v3.defs.company_domains.assets import (
    BraveSearchConfig,
    RESULT_COLUMNS,
    company_brave_search_results,
    iter_pending_companies,
    persist_result,
)

PROXIES = {
    f"crawl_proxy{i}": f"http://user:secret@proxy{i}.test:8080" for i in range(1, 4)
}


class BrowserFixture:
    """A browser boundary with deterministic slow and fast routes."""

    def __init__(self, slots: int = 1):
        self.lock = threading.Lock()
        self.started = threading.Barrier(4 * slots)
        self.release_slow = threading.Event()
        self.refilled = threading.Event()
        self.active = Counter()
        self.peak = Counter()
        self.total_peak = 0
        self.queries = []
        self.closed = []
        self.pages_closed = 0
        self.launches = []
        self.failing_company = ""

    def launch(self, *, headless, proxy):
        route = proxy or "direct"
        with self.lock:
            self.launches.append(route)
        fixture = self

        class Page:
            url = "https://search.brave.com/search?q=fixture"

            def set_default_timeout(self, value):
                pass

            def add_init_script(self, script):
                pass

            def goto(self, url, **kwargs):
                pass

            def get_by_test_id(self, name):
                return self

            def get_by_role(self, role, **kwargs):
                return self

            def fill(self, query):
                self.query = query

            def press(self, key):
                with fixture.lock:
                    fixture.active[route] += 1
                    fixture.peak[route] = max(
                        fixture.peak[route], fixture.active[route]
                    )
                    fixture.total_peak = max(
                        fixture.total_peak, sum(fixture.active.values())
                    )
                    fixture.queries.append(self.query)
                    first_wave = len(fixture.queries) <= fixture.started.parties
                if first_wave:
                    fixture.started.wait(timeout=5)
                else:
                    fixture.refilled.set()
                if route == "direct":
                    assert fixture.release_slow.wait(5), "slow route was never released"

            def wait_for(self, **kwargs):
                if fixture.failing_company and self.query.endswith(
                    fixture.failing_company
                ):
                    raise RuntimeError(
                        "browser error containing http://user:secret@proxy1.test"
                    )

            def click(self):
                pass

            def wait_for_function(self, *args, **kwargs):
                pass

            def evaluate(self, script):
                return f"Answer for {self.query}\nhttps://company.test/"

            def close(self):
                with fixture.lock:
                    fixture.active[route] -= 1
                    fixture.pages_closed += 1

        class Context:
            def new_page(self):
                return Page()

            def close(self):
                pass

        class Browser:
            def new_context(self, **kwargs):
                return Context()

            def close(self):
                with fixture.lock:
                    fixture.closed.append(route)

        return Browser()


@pytest.mark.parametrize("slots", [1, 2])
def test_fast_routes_refill_while_a_slow_route_is_still_busy(monkeypatch, slots):
    fixture = BrowserFixture(slots)
    monkeypatch.setattr(brave, "launch", fixture.launch)
    resource = brave.BraveBrowserResource(**PROXIES)
    companies = [brave.CompanySearchInput(str(i), f"Company {i} AB") for i in range(24)]
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            lambda: list(
                resource.iter_answers(iter(companies), requests_per_route=slots)
            )
        )
        try:
            assert fixture.refilled.wait(5), (
                "pool waited for the slow route before refilling"
            )
        finally:
            fixture.release_slow.set()
        results = future.result(timeout=10)
    assert len(results) == 24
    assert {r.company.company_id for r in results} == {str(i) for i in range(24)}
    assert fixture.total_peak == 4 * slots
    assert dict(fixture.peak) == {
        route: slots for route in ["direct", *PROXIES.values()]
    }
    assert Counter(fixture.closed) == Counter(fixture.launches)
    assert fixture.pages_closed == 24
    for result in results:
        assert result.status == "success"
        assert (
            result.query
            == f"Can you give me more information about Sweden company {result.company.company_name}"
        )
        assert result.answer.startswith(f"Answer for {result.query}")
        assert "secret" not in repr(result)


def test_one_company_failure_does_not_stop_the_remaining_queue(monkeypatch):
    fixture = BrowserFixture()
    fixture.failing_company = "Company 0 AB"
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    results = list(
        brave.BraveBrowserResource(**PROXIES).iter_answers(
            (brave.CompanySearchInput(str(i), f"Company {i} AB") for i in range(12)),
            requests_per_route=1,
        )
    )
    assert Counter(r.status for r in results) == {"success": 11, "error": 1}
    [failed] = [r for r in results if r.status == "error"]
    assert failed.company.company_id == "0"
    assert failed.answer == ""
    assert "secret" not in repr(failed)
    assert fixture.pages_closed == 12


def test_closing_results_stops_lazy_input_and_closes_all_browsers(monkeypatch):
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    claimed = []

    def companies():
        for i in range(100_000):
            claimed.append(i)
            yield brave.CompanySearchInput(str(i), f"Company {i} AB")

    with closing(
        brave.BraveBrowserResource(**PROXIES).iter_answers(
            companies(), requests_per_route=1
        )
    ) as results:
        next(results)
    assert len(claimed) < 25
    assert Counter(fixture.closed) == Counter(fixture.launches)


def test_empty_queue_starts_no_browser(monkeypatch):
    def unexpected_launch(**kwargs):
        pytest.fail("empty input must not launch a browser")

    monkeypatch.setattr(brave, "launch", unexpected_launch)
    assert (
        list(
            brave.BraveBrowserResource(**PROXIES).iter_answers(
                iter(()), requests_per_route=1
            )
        )
        == []
    )


def test_browser_start_failure_is_sanitized_and_does_not_hang(monkeypatch):
    def fail(**kwargs):
        raise RuntimeError("http://user:secret@proxy.test")

    monkeypatch.setattr(brave, "launch", fail)
    with pytest.raises(RuntimeError, match="failed") as caught:
        list(
            brave.BraveBrowserResource(**PROXIES).iter_answers(
                iter([brave.CompanySearchInput("1", "Example AB")]),
                requests_per_route=1,
            )
        )
    assert "secret" not in str(caught.value)


def test_results_are_indexed_only_after_the_complete_answer_is_saved():
    objects = {}
    rows = []
    result = brave.BraveSearchResult(
        company=brave.CompanySearchInput("5560004615", "Skanska AB"),
        query="Can you give me more information about Sweden company Skanska AB",
        route="crawl_proxy2",
        source_url="https://search.brave.com/search?q=Skanska",
        fetched_at=datetime(2026, 9, 15, tzinfo=UTC),
        status="success",
        answer="Skanska AB\nhttps://skanska.com/\nÅrsrapport",
    )

    def insert(sql, values):
        record = dict(zip(RESULT_COLUMNS, values[0], strict=True))
        assert objects[record["answer_object_key"]].decode() == result.answer
        rows.append(record)

    store = SimpleNamespace(
        bucket="test-answers", write_bytes=lambda key, body: objects.update({key: body})
    )
    persist_result(SimpleNamespace(execute=insert), store, result, run_id="run-1")
    assert len(rows) == 1
    assert rows[0]["company_id"] == "5560004615"
    assert rows[0]["source_run_id"] == "run-1"
    assert rows[0]["proxy_name"] == "crawl_proxy2"
    assert rows[0]["answer_bytes"] == len(result.answer.encode())

    def fail_write(key, body):
        raise OSError("storage unavailable")

    store.write_bytes = fail_write
    with pytest.raises(OSError):
        persist_result(SimpleNamespace(execute=insert), store, result, run_id="run-2")
    assert len(rows) == 1


def test_asset_serializes_runs_and_config_bounds_route_concurrency():
    assert company_brave_search_results.group_names_by_key == {
        dg.AssetKey("company_brave_search_results"): "company_domains",
    }
    assert company_brave_search_results.op.pool == "company_domains_brave"
    assert BraveSearchConfig().requests_per_route == 1
    assert BraveSearchConfig().max_companies is None
    for invalid in [0, -1]:
        with pytest.raises(ValueError):
            BraveSearchConfig(requests_per_route=invalid)
        with pytest.raises(ValueError):
            BraveSearchConfig(max_companies=invalid)


def test_proxy_routes_must_be_present_and_distinct():
    with pytest.raises(ValueError):
        brave.BraveBrowserResource(**{**PROXIES, "crawl_proxy2": " "})
    with pytest.raises(ValueError):
        brave.BraveBrowserResource(
            **{**PROXIES, "crawl_proxy2": PROXIES["crawl_proxy1"]}
        )


def test_lazy_selection_pages_and_honors_the_run_limit():
    pages = []
    closed = []

    class Reader:
        @contextmanager
        def get_connection(self):
            try:
                yield self
            finally:
                closed.append(True)

        def execute(self, sql, params):
            pages.append(dict(params))
            after = int(params["after_company_id"] or "0")
            return [
                (str(i), f"Company {i} AB")
                for i in range(after + 1, after + 1 + params["page_size"])
            ]

    with closing(
        iter_pending_companies(
            Reader(),
            BraveSearchConfig(max_companies=5, page_size=3),
            started_at=datetime(2026, 9, 15, tzinfo=UTC),
        )
    ) as pending:
        assert pages == []
        assert next(pending).company_id == "1"
        assert len(pages) == 1
        assert [company.company_id for company in pending] == ["2", "3", "4", "5"]
    assert [page["page_size"] for page in pages] == [3, 2]
    assert [page["after_company_id"] for page in pages] == ["", "3"]
    assert closed == [True]


def test_failed_search_records_an_attempt_without_a_success_object():
    saved = []

    def unexpected_write(key, body):
        pytest.fail("failed searches must not write successful answer objects")

    result = brave.BraveSearchResult(
        company=brave.CompanySearchInput("123", "Example AB"),
        query="test query",
        route="direct",
        source_url=brave.BRAVE_ORIGIN,
        fetched_at=datetime.now(UTC),
        status="error",
        error_type="TimeoutError",
    )
    persist_result(
        SimpleNamespace(execute=lambda sql, rows: saved.extend(rows)),
        SimpleNamespace(bucket="test", write_bytes=unexpected_write),
        result,
        run_id="failure-run",
    )
    record = dict(zip(RESULT_COLUMNS, saved[0], strict=True))
    assert record["status"] == "error"
    assert record["error_type"] == "TimeoutError"
    assert (
        record["answer_bucket"]
        == record["answer_object_key"]
        == record["proxy_name"]
        == ""
    )
    assert record["connection_mode"] == "direct"
    assert record["answer_bytes"] == 0
