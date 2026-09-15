"""Brave search → More → Copy, using Ratsit's direct/proxy browser topology."""

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from queue import Full, Queue
from threading import Event, Lock
from typing import Literal, Self

import dagster as dg
from cloakbrowser import launch
from playwright.sync_api import Page
from pydantic import Field, model_validator

BRAVE_ORIGIN = "https://search.brave.com"
ROUTES = ("direct", "crawl_proxy1", "crawl_proxy2", "crawl_proxy3")

# Capture what Brave's Copy button writes without using the OS-wide clipboard.
# Multiple browsers (even separate contexts) can otherwise read each other's answer.
COPY_CAPTURE_SCRIPT = """(() => {
    window.__companyBraveCopiedText = null;
    Object.defineProperty(navigator.clipboard, 'writeText', {
        configurable: true,
        value: async (text) => { window.__companyBraveCopiedText = String(text); }
    });
})();"""


@dataclass(frozen=True)
class CompanySearchInput:
    company_id: str
    company_name: str
    query: str
    request_id: str

    def __post_init__(self) -> None:
        if not self.company_id.strip() or not self.company_name.strip():
            raise ValueError("Brave requires a company ID and a nonempty company name")


@dataclass(frozen=True)
class BraveSearchResult:
    company: CompanySearchInput
    query: str
    route: str
    source_url: str
    fetched_at: datetime
    status: Literal["success", "error"]
    answer: str = ""
    error_type: str = ""


def copy_brave_answer(page: Page, query: str, *, timeout_ms: int) -> str:
    """The searcher/brave.py interaction, with a fresh page's private Copy capture."""
    page.set_default_timeout(timeout_ms)
    page.add_init_script(COPY_CAPTURE_SCRIPT)
    page.goto(BRAVE_ORIGIN, wait_until="domcontentloaded")
    searchbox = page.get_by_test_id("searchbox")
    searchbox.fill(query)
    searchbox.press("Enter")
    more = page.get_by_role("button", name="More", exact=True)
    more.wait_for(state="visible")
    more.click()
    page.get_by_role("button", name="Copy", exact=True).click()
    page.wait_for_function(
        "() => typeof window.__companyBraveCopiedText === 'string' "
        "&& window.__companyBraveCopiedText.trim().length > 0"
    )
    answer = page.evaluate("() => window.__companyBraveCopiedText")
    if not isinstance(answer, str) or not answer.strip():
        raise ValueError("Brave Copy returned no answer")
    return answer


class BraveBrowserResource(dg.ConfigurableResource):
    """Persistent, thread-confined browsers drawing from one lazy company iterator."""

    crawl_proxy1: str
    crawl_proxy2: str
    crawl_proxy3: str
    page_timeout_ms: int = Field(default=60_000, gt=0)

    @model_validator(mode="after")
    def validate_routes(self) -> Self:
        proxies = [
            self.crawl_proxy1.strip(),
            self.crawl_proxy2.strip(),
            self.crawl_proxy3.strip(),
        ]
        if any(not proxy for proxy in proxies) or len(set(proxies)) != 3:
            raise ValueError(
                "all three Brave crawl proxies must be configured and distinct"
            )
        return self

    def iter_answers(
        self,
        companies: Iterator[CompanySearchInput],
        *,
        requests_per_route: int,
        on_result: Callable[[BraveSearchResult], None],
    ) -> Iterator[BraveSearchResult]:
        """Refill a route immediately when it finishes, without fixed batch barriers.

        Only one thread advances the input at a time. Browser/Playwright objects never
        cross threads. The bounded result queue also bounds unpersisted answers when
        storage slows down. Closing this generator stops further company claims.
        """
        if requests_per_route < 1:
            raise ValueError("requests_per_route must be positive")
        worker_count = len(ROUTES) * requests_per_route
        events: Queue[BraveSearchResult | BaseException | None] = Queue(
            maxsize=worker_count
        )
        stopped = Event()
        input_lock = Lock()
        proxies = (None, self.crawl_proxy1, self.crawl_proxy2, self.crawl_proxy3)

        def next_company() -> CompanySearchInput | None:
            with input_lock:
                return None if stopped.is_set() else next(companies, None)

        def send(event: BraveSearchResult | BaseException | None) -> None:
            while not stopped.is_set():
                try:
                    events.put(event, timeout=0.1)
                    return
                except Full:
                    continue

        def worker(route: str, proxy: str | None) -> None:
            browser = None
            try:
                company = next_company()
                if company is None:
                    return
                try:
                    browser = launch(headless=True, proxy=proxy)
                    browser_context = browser.new_context(locale="en-US")
                except Exception:
                    raise RuntimeError(
                        f"Brave browser route {route} failed to start"
                    ) from None
                try:
                    while company is not None and not stopped.is_set():
                        page = None
                        query = company.query
                        try:
                            page = browser_context.new_page()
                            answer = copy_brave_answer(
                                page, query, timeout_ms=self.page_timeout_ms
                            )
                            result = BraveSearchResult(
                                company,
                                query,
                                route,
                                page.url,
                                datetime.now(UTC),
                                "success",
                                answer,
                            )
                        except Exception as error:
                            # Playwright error messages can contain credential-bearing
                            # proxy URLs. Keep the error category, never the raw message.
                            result = BraveSearchResult(
                                company,
                                query,
                                route,
                                BRAVE_ORIGIN,
                                datetime.now(UTC),
                                "error",
                                error_type=type(error).__name__,
                            )
                        try:
                            # Preserve the answer even when browser cleanup fails.
                            on_result(result)
                        finally:
                            if page is not None:
                                page.close()
                        send(result)
                        company = next_company()
                finally:
                    browser_context.close()
            except BaseException as error:
                # Do not strand the consumer if a worker or the input iterator fails.
                # Sanitize browser lifecycle errors, which can expose proxy credentials.
                send(
                    RuntimeError(f"Brave route {route} failed ({type(error).__name__})")
                )
            finally:
                try:
                    if browser is not None:
                        browser.close()
                except Exception:
                    send(RuntimeError(f"Brave route {route} failed to close"))
                finally:
                    send(None)

        with ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="brave_browser"
        ) as executor:
            futures = [
                executor.submit(worker, route, proxy)
                for _ in range(requests_per_route)
                for route, proxy in zip(ROUTES, proxies, strict=True)
            ]
            finished = 0
            try:
                while finished < worker_count:
                    event = events.get()
                    if event is None:
                        finished += 1
                    elif isinstance(event, BaseException):
                        raise event
                    else:
                        yield event
                for future in futures:
                    future.result()
            finally:
                stopped.set()
