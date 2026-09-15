"""Brave's worker pool must refill idle routes and preserve per-company answers."""

import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from urllib.parse import parse_qs, urlsplit

import dagster as dg
import pytest

from dagster_v3.defs.company_domains import browser as brave
from dagster_v3.defs.company_domains.assets import (
    BraveSearchConfig,
    company_brave_search_results,
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
        self.fail_close = False

    def launch(self, *, headless, proxy):
        route = proxy or "direct"
        with self.lock:
            self.launches.append(route)
        fixture = self

        class Page:
            def set_default_timeout(self, value):
                pass

            def add_init_script(self, script):
                pass

            def goto(self, url, **kwargs):
                self.url = url
                self.query = parse_qs(urlsplit(url).query)["q"][0]
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

            def get_by_role(self, role, **kwargs):
                return self

            def filter(self, **kwargs):
                return self

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
                if fixture.fail_close:
                    raise RuntimeError("page close failed")

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
    companies = [
        brave.CompanySearchInput(
            str(i), f"Company {i} AB", f"Find Company {i} AB", str(i)
        )
        for i in range(24)
    ]
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            lambda: list(
                resource.iter_answers(
                    iter(companies),
                    requests_per_route=slots,
                    on_result=lambda result: None,
                )
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
        assert result.query == f"Find {result.company.company_name}"
        assert result.answer.startswith(f"Answer for {result.query}")
        assert "secret" not in repr(result)


def test_one_company_failure_does_not_stop_the_remaining_queue(monkeypatch):
    fixture = BrowserFixture()
    fixture.failing_company = "Company 0 AB"
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    results = list(
        brave.BraveBrowserResource(**PROXIES).iter_answers(
            (
                brave.CompanySearchInput(
                    str(i), f"Company {i} AB", f"Find Company {i} AB", str(i)
                )
                for i in range(12)
            ),
            requests_per_route=1,
            on_result=lambda result: None,
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
            yield brave.CompanySearchInput(
                str(i), f"Company {i} AB", f"Find Company {i} AB", str(i)
            )

    with closing(
        brave.BraveBrowserResource(**PROXIES).iter_answers(
            companies(), requests_per_route=1, on_result=lambda result: None
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
                iter(()), requests_per_route=1, on_result=lambda result: None
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
                iter(
                    [
                        brave.CompanySearchInput(
                            "1", "Example AB", "Find Example AB", "1"
                        )
                    ]
                ),
                requests_per_route=1,
                on_result=lambda result: None,
            )
        )
    assert "secret" not in str(caught.value)


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


def test_rendered_query_is_used_and_result_is_saved_before_refill(monkeypatch):
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    saved = set()
    lock = threading.Lock()

    def companies():
        for i in range(8):
            if i >= 4:
                with lock:
                    assert saved, (
                        "a route refilled before saving any completed response"
                    )
            yield brave.CompanySearchInput(
                str(i), f"Company {i}", f"Who owns example{i}.se?", str(i)
            )

    def save(result):
        with lock:
            saved.add(result.company.request_id)

    results = list(
        brave.BraveBrowserResource(**PROXIES).iter_answers(
            companies(), requests_per_route=1, on_result=save
        )
    )
    assert len(saved) == len(results) == 8
    assert {r.query for r in results} == {f"Who owns example{i}.se?" for i in range(8)}


def test_page_cleanup_failure_keeps_the_already_copied_response(monkeypatch):
    fixture = BrowserFixture()
    fixture.release_slow.set()
    fixture.fail_close = True
    monkeypatch.setattr(brave, "launch", fixture.launch)
    saved = []
    companies = (
        brave.CompanySearchInput(str(i), f"Company {i}", f"Find {i}", str(i))
        for i in range(4)
    )
    with pytest.raises(RuntimeError):
        list(
            brave.BraveBrowserResource(**PROXIES).iter_answers(
                companies, requests_per_route=1, on_result=saved.append
            )
        )
    assert len(saved) == 4
    assert all(result.answer.startswith("Answer for Find") for result in saved)
