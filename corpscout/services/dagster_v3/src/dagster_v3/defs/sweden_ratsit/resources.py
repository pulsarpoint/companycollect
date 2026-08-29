import hashlib
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from queue import Queue
from threading import Event
from typing import Any, Literal, Self
from urllib.parse import parse_qsl, urlsplit

import dagster as dg
from cloakbrowser import launch
from pydantic import model_validator

from dagster_v3.defs.sweden_ratsit.parser import (
    PAGE_CONTENT_SELECTOR,
    JsonObject,
    parse_company_page,
    validate_ratsit_url,
)

RATSIT_BASE_URL = "https://www.ratsit.se"
RATSIT_HARD_MAX_COMPANIES = 50_000
RATSIT_MIN_REQUEST_INTERVAL_SECONDS = 2.0
RATSIT_DEFAULT_PAGE_TIMEOUT_MS = 60_000
RATSIT_DEFAULT_HTTP_429_MAX_ATTEMPTS = 4
RATSIT_DEFAULT_HTTP_429_RETRY_BACKOFF_SECONDS = 30.0
RATSIT_DEFAULT_HTTP_429_SLOWDOWN_MULTIPLIER = 1.5
type RatsitBrowserWorkerName = Literal[
    "direct",
    "crawl_proxy1",
    "crawl_proxy2",
    "crawl_proxy3",
]
type RatsitConnectionMode = Literal["direct", "proxy"]
type RatsitProxyName = Literal["", "crawl_proxy1", "crawl_proxy2", "crawl_proxy3"]

RATSIT_BROWSER_WORKER_NAMES: tuple[RatsitBrowserWorkerName, ...] = (
    "direct",
    "crawl_proxy1",
    "crawl_proxy2",
    "crawl_proxy3",
)
RATSIT_BROWSER_WORKER_COUNT = len(RATSIT_BROWSER_WORKER_NAMES)

type RatsitFailureType = Literal["navigation", "http", "parse"]
type RatsitNotFoundReason = Literal["http_not_found", "ratsit_missing"]


@dataclass(frozen=True)
class RatsitCompanyReport:
    company_id: str
    connection_mode: RatsitConnectionMode
    proxy_name: RatsitProxyName
    requested_url: str
    source_url: str
    fetched_at: datetime
    html_sha256: str
    report: JsonObject
    http_status: int = 200
    attempt_count: int = 1


@dataclass(frozen=True)
class RatsitCompanyFailure:
    company_id: str
    connection_mode: RatsitConnectionMode
    proxy_name: RatsitProxyName
    requested_url: str
    source_url: str
    fetched_at: datetime
    error_type: RatsitFailureType
    message: str
    http_status: int | None
    html_sha256: str | None
    diagnostic_html: bytes | None
    attempt_count: int = 1


@dataclass(frozen=True)
class RatsitCompanyNotFound:
    company_id: str
    connection_mode: RatsitConnectionMode
    proxy_name: RatsitProxyName
    requested_url: str
    source_url: str
    fetched_at: datetime
    reason: RatsitNotFoundReason
    message: str
    http_status: int
    html_sha256: str | None
    diagnostic_html: bytes | None
    attempt_count: int = 1


type RatsitCompanyResult = (
    RatsitCompanyReport | RatsitCompanyFailure | RatsitCompanyNotFound
)


@dataclass(frozen=True)
class _RatsitQueuedCompany:
    company_id: str
    worker_index: int
    attempt_count: int


class SwedenRatsitBrowserResource(dg.ConfigurableResource):
    """Four concurrent CloakBrowser workers for one Ratsit company partition."""

    base_url: str = RATSIT_BASE_URL
    headless: bool = True
    max_companies: int = RATSIT_HARD_MAX_COMPANIES
    request_interval_seconds: float = RATSIT_MIN_REQUEST_INTERVAL_SECONDS
    page_timeout_ms: int = RATSIT_DEFAULT_PAGE_TIMEOUT_MS
    http_429_max_attempts: int = RATSIT_DEFAULT_HTTP_429_MAX_ATTEMPTS
    http_429_retry_backoff_seconds: float = (
        RATSIT_DEFAULT_HTTP_429_RETRY_BACKOFF_SECONDS
    )
    http_429_slowdown_multiplier: float = RATSIT_DEFAULT_HTTP_429_SLOWDOWN_MULTIPLIER
    crawl_proxy1: str
    crawl_proxy2: str
    crawl_proxy3: str

    @model_validator(mode="after")
    def validate_configuration(self) -> Self:
        validate_ratsit_url(f"{self.base_url.rstrip('/')}/0000000000")
        if self.headless is not True:
            raise ValueError("the Ratsit browser must run headless")
        if not 1 <= self.max_companies <= RATSIT_HARD_MAX_COMPANIES:
            raise ValueError(
                f"max_companies must be between 1 and {RATSIT_HARD_MAX_COMPANIES}"
            )
        if self.request_interval_seconds < RATSIT_MIN_REQUEST_INTERVAL_SECONDS:
            raise ValueError(
                "request_interval_seconds must be at least "
                f"{RATSIT_MIN_REQUEST_INTERVAL_SECONDS:g}"
            )
        if self.page_timeout_ms <= 0:
            raise ValueError("page_timeout_ms must be positive")
        if not 1 <= self.http_429_max_attempts <= RATSIT_BROWSER_WORKER_COUNT:
            raise ValueError(
                "http_429_max_attempts must be between 1 and "
                f"{RATSIT_BROWSER_WORKER_COUNT}"
            )
        if self.http_429_retry_backoff_seconds <= 0:
            raise ValueError("http_429_retry_backoff_seconds must be positive")
        if self.http_429_slowdown_multiplier <= 1:
            raise ValueError("http_429_slowdown_multiplier must be greater than 1")
        proxies = (self.crawl_proxy1, self.crawl_proxy2, self.crawl_proxy3)
        if any(proxy.strip() == "" for proxy in proxies):
            raise ValueError("all three Ratsit crawl proxies must be configured")
        if len(set(proxies)) != len(proxies):
            raise ValueError("Ratsit crawl proxies must be distinct")
        return self

    def iter_company_reports(
        self,
        company_ids: Sequence[str],
        *,
        launcher: Callable[..., Any] = launch,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> Iterator[RatsitCompanyResult]:
        selected_company_ids = _validate_company_ids(
            company_ids,
            max_companies=self.max_companies,
        )
        if not selected_company_ids:
            return

        proxy_urls = (None, self.crawl_proxy1, self.crawl_proxy2, self.crawl_proxy3)
        queued_companies = tuple(
            _RatsitQueuedCompany(
                company_id=company_id,
                worker_index=company_index % RATSIT_BROWSER_WORKER_COUNT,
                attempt_count=1,
            )
            for company_index, company_id in enumerate(selected_company_ids)
        )
        resolved_company_ids: list[str] = []

        while queued_companies:
            attempt_counts = {company.attempt_count for company in queued_companies}
            if len(attempt_counts) != 1:
                raise RuntimeError("Ratsit retry queue contains mixed attempt counts")
            attempt_count = attempt_counts.pop()
            if attempt_count > 1:
                retry_backoff_seconds = self.http_429_retry_backoff_seconds * (
                    self.http_429_slowdown_multiplier ** (attempt_count - 2)
                )
                sleep(retry_backoff_seconds)

            worker_company_ids = tuple(
                tuple(
                    queued.company_id
                    for queued in queued_companies
                    if queued.worker_index == worker_index
                )
                for worker_index in range(RATSIT_BROWSER_WORKER_COUNT)
            )
            active_workers = tuple(
                (worker_name, proxy_url, assigned_company_ids)
                for worker_name, proxy_url, assigned_company_ids in zip(
                    RATSIT_BROWSER_WORKER_NAMES,
                    proxy_urls,
                    worker_company_ids,
                    strict=True,
                )
                if assigned_company_ids
            )
            queued_by_company_id = {
                company.company_id: company for company in queued_companies
            }
            retry_company_ids: set[str] = set()
            wave_result_company_ids: set[str] = set()
            request_interval_seconds = self.request_interval_seconds * (
                self.http_429_slowdown_multiplier ** (attempt_count - 1)
            )

            for result in self._iter_browser_wave(
                active_workers,
                request_interval_seconds=request_interval_seconds,
                launcher=launcher,
                monotonic=monotonic,
                sleep=sleep,
                now=now,
            ):
                if result.company_id in wave_result_company_ids:
                    raise RuntimeError(
                        "Ratsit browser workers returned a company more than once "
                        "in one attempt"
                    )
                wave_result_company_ids.add(result.company_id)
                result = replace(result, attempt_count=attempt_count)
                if (
                    isinstance(result, RatsitCompanyFailure)
                    and result.http_status == 429
                    and attempt_count < self.http_429_max_attempts
                ):
                    retry_company_ids.add(result.company_id)
                    continue

                resolved_company_ids.append(result.company_id)
                yield result

            if wave_result_company_ids != set(queued_by_company_id):
                missing_company_ids = sorted(
                    set(queued_by_company_id) - wave_result_company_ids
                )
                raise RuntimeError(
                    "Ratsit browser workers did not return every queued company; "
                    f"missing {', '.join(missing_company_ids[:5])}"
                )

            queued_companies = tuple(
                _RatsitQueuedCompany(
                    company_id=queued.company_id,
                    worker_index=(
                        (queued.worker_index + 1) % RATSIT_BROWSER_WORKER_COUNT
                    ),
                    attempt_count=queued.attempt_count + 1,
                )
                for queued in queued_companies
                if queued.company_id in retry_company_ids
            )

        if len(resolved_company_ids) != len(selected_company_ids) or set(
            resolved_company_ids
        ) != set(selected_company_ids):
            raise RuntimeError(
                "Ratsit browser workers did not return each selected company exactly once"
            )

    def _iter_browser_wave(
        self,
        active_workers: tuple[
            tuple[RatsitBrowserWorkerName, str | None, tuple[str, ...]], ...
        ],
        *,
        request_interval_seconds: float,
        launcher: Callable[..., Any],
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
        now: Callable[[], datetime],
    ) -> Iterator[RatsitCompanyResult]:

        events: Queue[RatsitCompanyResult | Exception | None] = Queue()
        stop_requested = Event()

        def fetch_worker(
            worker: tuple[RatsitBrowserWorkerName, str | None, tuple[str, ...]],
        ) -> None:
            worker_name, proxy_url, worker_company_ids = worker
            try:
                for result in self._iter_browser_reports(
                    worker_company_ids,
                    worker_name=worker_name,
                    proxy_url=proxy_url,
                    request_interval_seconds=request_interval_seconds,
                    launcher=launcher,
                    monotonic=monotonic,
                    sleep=sleep,
                    now=now,
                ):
                    if stop_requested.is_set():
                        break
                    events.put(result)
            except Exception as error:
                stop_requested.set()
                events.put(error)
            finally:
                events.put(None)

        with ThreadPoolExecutor(
            max_workers=len(active_workers),
            thread_name_prefix="ratsit_browser",
        ) as executor:
            futures = tuple(
                executor.submit(fetch_worker, worker) for worker in active_workers
            )
            completed_workers = 0
            worker_error: Exception | None = None
            try:
                while completed_workers < len(active_workers):
                    event = events.get()
                    if event is None:
                        completed_workers += 1
                        continue
                    if isinstance(event, Exception):
                        worker_error = worker_error or event
                        continue
                    if worker_error is None:
                        yield event
                for future in futures:
                    future.result()
                if worker_error is not None:
                    raise worker_error
            finally:
                stop_requested.set()

    def _iter_browser_reports(
        self,
        company_ids: tuple[str, ...],
        *,
        worker_name: RatsitBrowserWorkerName,
        proxy_url: str | None,
        request_interval_seconds: float,
        launcher: Callable[..., Any],
        monotonic: Callable[[], float],
        sleep: Callable[[float], None],
        now: Callable[[], datetime],
    ) -> Iterator[RatsitCompanyResult]:
        try:
            browser = launcher(headless=self.headless, proxy=proxy_url)
        except Exception:
            raise RuntimeError(
                f"Ratsit browser worker {worker_name} failed to start; verify the "
                "CloakBrowser binary, proxy, and runtime dependencies"
            ) from None

        page: Any | None = None
        try:
            page = browser.new_page()
            previous_request_started_at: float | None = None
            connection_mode: RatsitConnectionMode = (
                "direct" if worker_name == "direct" else "proxy"
            )
            proxy_name: RatsitProxyName = "" if worker_name == "direct" else worker_name
            for company_id in company_ids:
                previous_request_started_at = _wait_for_request_slot(
                    previous_request_started_at,
                    request_interval_seconds=request_interval_seconds,
                    monotonic=monotonic,
                    sleep=sleep,
                )
                requested_url = ratsit_company_url(self.base_url, company_id)
                yield self._fetch_company(
                    page,
                    company_id=company_id,
                    connection_mode=connection_mode,
                    proxy_name=proxy_name,
                    requested_url=requested_url,
                    now=now,
                )
        finally:
            try:
                if page is not None:
                    page.close()
            finally:
                browser.close()

    def _fetch_company(
        self,
        page: Any,
        *,
        company_id: str,
        connection_mode: RatsitConnectionMode,
        proxy_name: RatsitProxyName,
        requested_url: str,
        now: Callable[[], datetime],
    ) -> RatsitCompanyResult:
        try:
            response = page.goto(
                requested_url,
                wait_until="domcontentloaded",
                timeout=self.page_timeout_ms,
            )
        except Exception:
            return RatsitCompanyFailure(
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=requested_url,
                fetched_at=_aware_utc_now(now),
                error_type="navigation",
                message="Ratsit navigation failed",
                http_status=None,
                html_sha256=None,
                diagnostic_html=None,
            )

        source_url = _source_url(page, requested_url)
        if response is None:
            return RatsitCompanyFailure(
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                error_type="navigation",
                message="Ratsit navigation returned no HTTP response",
                http_status=None,
                html_sha256=None,
                diagnostic_html=None,
            )

        status = int(response.status)
        if status == 404:
            return RatsitCompanyNotFound(
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                reason="http_not_found",
                message="Ratsit returned HTTP status 404",
                http_status=status,
                html_sha256=None,
                diagnostic_html=None,
            )
        if not 200 <= status < 400:
            return RatsitCompanyFailure(
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                error_type="http",
                message=f"Ratsit returned HTTP status {status}",
                http_status=status,
                html_sha256=None,
                diagnostic_html=None,
            )

        if _is_missing_company_redirect(source_url):
            diagnostic_html = _rendered_html_diagnostic(page)
            return RatsitCompanyNotFound(
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                reason="ratsit_missing",
                message="Ratsit redirected to /foretag?saknas",
                http_status=status,
                html_sha256=(
                    hashlib.sha256(diagnostic_html).hexdigest()
                    if diagnostic_html is not None
                    else None
                ),
                diagnostic_html=diagnostic_html,
            )

        try:
            page.locator(PAGE_CONTENT_SELECTOR).first.wait_for(
                state="visible",
                timeout=self.page_timeout_ms,
            )
        except Exception:
            return _diagnostic_failure(
                page,
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                status=status,
                message=(
                    f"Ratsit content selector was not visible: {PAGE_CONTENT_SELECTOR}"
                ),
            )

        try:
            page_html = page.content()
        except Exception:
            return RatsitCompanyFailure(
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                error_type="navigation",
                message="Ratsit rendered HTML could not be read",
                http_status=status,
                html_sha256=None,
                diagnostic_html=None,
            )

        html_bytes = page_html.encode("utf-8")
        html_sha256 = hashlib.sha256(html_bytes).hexdigest()
        try:
            report = parse_company_page(page_html, source_url=source_url)
            validate_ratsit_company_report(
                report,
                expected_company_id=company_id,
            )
        except Exception as error:
            return RatsitCompanyFailure(
                company_id=company_id,
                connection_mode=connection_mode,
                proxy_name=proxy_name,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                error_type="parse",
                message=f"Ratsit page parsing failed: {error}",
                http_status=status,
                html_sha256=html_sha256,
                diagnostic_html=html_bytes,
            )

        return RatsitCompanyReport(
            company_id=company_id,
            connection_mode=connection_mode,
            proxy_name=proxy_name,
            requested_url=requested_url,
            source_url=source_url,
            fetched_at=_aware_utc_now(now),
            html_sha256=html_sha256,
            report=report,
            http_status=status,
        )


def ratsit_company_url(base_url: str, company_id: str) -> str:
    _validate_company_id(company_id)
    return validate_ratsit_url(f"{base_url.rstrip('/')}/{company_id[-10:]}")


def ratsit_round_robin_assignments(
    company_ids: Sequence[str],
) -> tuple[tuple[RatsitBrowserWorkerName, tuple[str, ...]], ...]:
    selected_company_ids = tuple(company_ids)
    return tuple(
        (worker_name, selected_company_ids[worker_index::RATSIT_BROWSER_WORKER_COUNT])
        for worker_index, worker_name in enumerate(RATSIT_BROWSER_WORKER_NAMES)
    )


def _validate_company_ids(
    company_ids: Sequence[str],
    *,
    max_companies: int,
) -> tuple[str, ...]:
    selected_company_ids = tuple(company_ids)
    if len(selected_company_ids) > max_companies:
        raise ValueError(f"Ratsit accepts at most {max_companies} companies")
    if len(set(selected_company_ids)) != len(selected_company_ids):
        raise ValueError("Ratsit company IDs must be unique")
    for company_id in selected_company_ids:
        _validate_company_id(company_id)
    return selected_company_ids


def _validate_company_id(company_id: str) -> None:
    if re.fullmatch(r"(?:[0-9]{10}|[0-9]{12})", company_id) is None:
        raise ValueError("Ratsit company ID must contain ten or twelve digits")


def _wait_for_request_slot(
    previous_request_started_at: float | None,
    *,
    request_interval_seconds: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> float:
    current_time = monotonic()
    if previous_request_started_at is not None:
        remaining_seconds = (
            previous_request_started_at + request_interval_seconds - current_time
        )
        if remaining_seconds > 0:
            sleep(remaining_seconds)
            current_time = monotonic()
    return current_time


def _source_url(page: Any, requested_url: str) -> str:
    page_url = getattr(page, "url", requested_url)
    if not isinstance(page_url, str):
        return requested_url
    try:
        return validate_ratsit_url(page_url)
    except ValueError:
        return requested_url


def _is_missing_company_redirect(url: str) -> bool:
    parsed_url = urlsplit(url)
    if parsed_url.hostname not in {"ratsit.se", "www.ratsit.se"}:
        return False
    if parsed_url.path.rstrip("/") != "/foretag":
        return False
    return any(
        name == "saknas"
        for name, _value in parse_qsl(parsed_url.query, keep_blank_values=True)
    )


def _diagnostic_failure(
    page: Any,
    *,
    company_id: str,
    connection_mode: RatsitConnectionMode,
    proxy_name: RatsitProxyName,
    requested_url: str,
    source_url: str,
    fetched_at: datetime,
    status: int,
    message: str,
) -> RatsitCompanyFailure:
    html_bytes = _rendered_html_diagnostic(page)
    return RatsitCompanyFailure(
        company_id=company_id,
        connection_mode=connection_mode,
        proxy_name=proxy_name,
        requested_url=requested_url,
        source_url=source_url,
        fetched_at=fetched_at,
        error_type="parse",
        message=message,
        http_status=status,
        html_sha256=(
            hashlib.sha256(html_bytes).hexdigest() if html_bytes is not None else None
        ),
        diagnostic_html=html_bytes,
    )


def _rendered_html_diagnostic(page: Any) -> bytes | None:
    try:
        return page.content().encode("utf-8")
    except Exception:
        return None


def validate_ratsit_company_report(
    report: Mapping[str, object],
    *,
    expected_company_id: str,
) -> None:
    company = report.get("company")
    if not isinstance(company, Mapping):
        raise ValueError("Ratsit company page did not contain company data")
    company_name = company.get("name")
    if not isinstance(company_name, str) or company_name.strip() == "":
        raise ValueError("Ratsit company page did not contain a company name")
    organization_number = company.get("organization_number")
    if not isinstance(organization_number, str):
        raise ValueError("Ratsit company page did not contain an organization number")
    normalized_number = re.sub(r"\D", "", organization_number)
    if normalized_number != expected_company_id[-10:]:
        raise ValueError(
            "Ratsit organization number did not match the requested company"
        )


def _aware_utc_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    if value.utcoffset() is None:
        raise ValueError("Ratsit fetched_at timestamp must include a timezone")
    return value.astimezone(UTC)
