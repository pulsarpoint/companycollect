import hashlib
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Self

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
RATSIT_HARD_MAX_COMPANIES = 20
RATSIT_MIN_REQUEST_INTERVAL_SECONDS = 2.0
RATSIT_DEFAULT_PAGE_TIMEOUT_MS = 60_000

type RatsitFailureType = Literal["navigation", "http", "parse"]


@dataclass(frozen=True)
class RatsitCompanyReport:
    company_id: str
    requested_url: str
    source_url: str
    fetched_at: datetime
    html_sha256: str
    report: JsonObject
    http_status: int = 200


@dataclass(frozen=True)
class RatsitCompanyFailure:
    company_id: str
    requested_url: str
    source_url: str
    fetched_at: datetime
    error_type: RatsitFailureType
    message: str
    http_status: int | None
    html_sha256: str | None
    diagnostic_html: bytes | None


class SwedenRatsitBrowserResource(dg.ConfigurableResource):
    """One deliberately constrained CloakBrowser for a fixed Ratsit scan."""

    base_url: str = RATSIT_BASE_URL
    headless: bool = True
    max_companies: int = RATSIT_HARD_MAX_COMPANIES
    request_interval_seconds: float = RATSIT_MIN_REQUEST_INTERVAL_SECONDS
    page_timeout_ms: int = RATSIT_DEFAULT_PAGE_TIMEOUT_MS

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
        return self

    def iter_company_reports(
        self,
        company_ids: Sequence[str],
        *,
        launcher: Callable[..., Any] = launch,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> Iterator[RatsitCompanyReport | RatsitCompanyFailure]:
        selected_company_ids = _validate_company_ids(
            company_ids,
            max_companies=self.max_companies,
        )
        if not selected_company_ids:
            return

        try:
            browser = launcher(headless=self.headless)
        except Exception:
            raise RuntimeError(
                "Ratsit browser failed to start; verify the CloakBrowser "
                "binary and runtime dependencies"
            ) from None

        page: Any | None = None
        try:
            page = browser.new_page()
            previous_request_started_at: float | None = None
            for company_id in selected_company_ids:
                previous_request_started_at = _wait_for_request_slot(
                    previous_request_started_at,
                    request_interval_seconds=self.request_interval_seconds,
                    monotonic=monotonic,
                    sleep=sleep,
                )
                requested_url = ratsit_company_url(self.base_url, company_id)
                yield self._fetch_company(
                    page,
                    company_id=company_id,
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
        requested_url: str,
        now: Callable[[], datetime],
    ) -> RatsitCompanyReport | RatsitCompanyFailure:
        try:
            response = page.goto(
                requested_url,
                wait_until="domcontentloaded",
                timeout=self.page_timeout_ms,
            )
        except Exception:
            return RatsitCompanyFailure(
                company_id=company_id,
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
        if not 200 <= status < 400:
            return RatsitCompanyFailure(
                company_id=company_id,
                requested_url=requested_url,
                source_url=source_url,
                fetched_at=_aware_utc_now(now),
                error_type="http",
                message=f"Ratsit returned HTTP status {status}",
                http_status=status,
                html_sha256=None,
                diagnostic_html=None,
            )

        try:
            page.locator(PAGE_CONTENT_SELECTOR).first.wait_for(
                state="visible",
                timeout=self.page_timeout_ms,
            )
        except Exception:
            return _parse_failure(
                page,
                company_id=company_id,
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
            _validate_company_report(report, expected_company_id=company_id)
        except Exception as error:
            return RatsitCompanyFailure(
                company_id=company_id,
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
            requested_url=requested_url,
            source_url=source_url,
            fetched_at=_aware_utc_now(now),
            html_sha256=html_sha256,
            report=report,
            http_status=status,
        )


def ratsit_company_url(base_url: str, company_id: str) -> str:
    _validate_company_id(company_id)
    return validate_ratsit_url(f"{base_url.rstrip('/')}/{company_id}")


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
    if re.fullmatch(r"[0-9]{10}", company_id) is None:
        raise ValueError("Ratsit company ID must contain exactly ten digits")


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


def _parse_failure(
    page: Any,
    *,
    company_id: str,
    requested_url: str,
    source_url: str,
    fetched_at: datetime,
    status: int,
    message: str,
) -> RatsitCompanyFailure:
    try:
        html_bytes = page.content().encode("utf-8")
    except Exception:
        html_bytes = None
    return RatsitCompanyFailure(
        company_id=company_id,
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


def _validate_company_report(
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
    if normalized_number != expected_company_id:
        raise ValueError(
            "Ratsit organization number did not match the requested company"
        )


def _aware_utc_now(now: Callable[[], datetime]) -> datetime:
    value = now()
    if value.utcoffset() is None:
        raise ValueError("Ratsit fetched_at timestamp must include a timezone")
    return value.astimezone(UTC)
