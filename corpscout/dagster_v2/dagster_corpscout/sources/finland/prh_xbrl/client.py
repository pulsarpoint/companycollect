"""HTTP client for the PRH open data XBRL API v3. No Dagster imports."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import requests
from urllib.parse import urlencode

MAX_PAGES = 10_000


@dataclass(frozen=True)
class DiscoveredStatement:
    business_id: str
    financial_date: str
    registration_date: str | None = None


@dataclass(frozen=True)
class DiscoveryPage:
    total_results: int
    statements: list[DiscoveredStatement]


class PRHXBRLClient:
    def __init__(
        self,
        *,
        base_url: str,
        user_agent: str,
        timeout_seconds: int = 120,
        min_request_interval_seconds: float = 1.0,
        max_attempts: int = 8,
        backoff_base_seconds: float = 5.0,
        backoff_max_seconds: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.min_request_interval_seconds = min_request_interval_seconds
        self.max_attempts = max_attempts
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self._last_request_at: float | None = None
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent

    def _throttle(self) -> None:
        if self._last_request_at is not None:
            wait = self.min_request_interval_seconds - (time.monotonic() - self._last_request_at)
            if wait > 0:
                time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _get(self, path: str, params: dict) -> requests.Response:
        """GET with pacing and bounded backoff. PRH rate-limits (429) without a
        Retry-After header, so exponential backoff is the only safe strategy."""
        response: requests.Response | None = None
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            response = self.session.get(
                f"{self.base_url}{path}", params=params, timeout=self.timeout_seconds
            )
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < self.max_attempts:
                    delay = min(
                        self.backoff_base_seconds * 2 ** (attempt - 1),
                        self.backoff_max_seconds,
                    )
                    retry_after = response.headers.get("Retry-After", "")
                    if retry_after.isdigit():
                        delay = max(delay, float(retry_after))
                    time.sleep(delay)
                    continue
            response.raise_for_status()
            return response
        response.raise_for_status()
        return response

    def list_registration_window(
        self, *, registered_date_start: str, registered_date_end: str, page: int = 1
    ) -> DiscoveryPage:
        response = self._get(
            "/all_financial_statements",
            {
                "registeredDateStart": registered_date_start,
                "registeredDateEnd": registered_date_end,
                "page": page,
            },
        )
        return _discovery_page(response.json())

    def iter_registration_window(
        self, *, registered_date_start: str, registered_date_end: str
    ) -> Iterator[DiscoveredStatement]:
        yield from _paginate(
            lambda page: self.list_registration_window(
                registered_date_start=registered_date_start,
                registered_date_end=registered_date_end,
                page=page,
            )
        )

    def list_company_financials(self, business_id: str, *, page: int = 1) -> DiscoveryPage:
        response = self._get(
            "/financials",
            {"businessId": business_id, "page": page},
        )
        return _discovery_page(response.json())

    def iter_company_financials(self, business_id: str) -> Iterator[DiscoveredStatement]:
        yield from _paginate(lambda page: self.list_company_financials(business_id, page=page))

    def statement_xml_url(self, business_id: str, financial_date: str) -> str:
        """The canonical download URL for one statement, without making a request.
        Used when an already-downloaded object is reused and the listing still
        needs source_url — and by download_statement_xml so both paths agree."""
        query = urlencode({"businessId": business_id, "financialDate": financial_date})
        return f"{self.base_url}/financial?{query}"

    def download_statement_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        response = self._get(
            "/financial",
            {"businessId": business_id, "financialDate": financial_date},
        )
        return response.content, self.statement_xml_url(business_id, financial_date)

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "PRHXBRLClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()


def _paginate(fetch_page: Callable[[int], DiscoveryPage]) -> Iterator[DiscoveredStatement]:
    seen = 0
    for page_number in range(1, MAX_PAGES + 1):
        page = fetch_page(page_number)
        if not page.statements:
            return
        yield from page.statements
        seen += len(page.statements)
        if page.total_results and seen >= page.total_results:
            return
    raise RuntimeError(f"pagination exceeded {MAX_PAGES} pages; aborting runaway loop")


def _discovery_page(payload: dict) -> DiscoveryPage:
    return DiscoveryPage(
        total_results=int(payload.get("totalResults") or 0),
        statements=[
            DiscoveredStatement(
                business_id=str(item.get("businessId") or "").strip(),
                financial_date=str(item.get("financialDate") or "").strip(),
                registration_date=(
                    str(item["registrationDate"]).strip() if item.get("registrationDate") else None
                ),
            )
            for item in payload.get("financials", [])
        ],
    )
