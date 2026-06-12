"""HTTP client for the PRH open data XBRL API v3. No Dagster imports."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass

import requests

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
    def __init__(self, *, base_url: str, user_agent: str, timeout_seconds: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers["User-Agent"] = user_agent

    def list_registration_window(
        self, *, registered_date_start: str, registered_date_end: str, page: int = 1
    ) -> DiscoveryPage:
        response = self.session.get(
            f"{self.base_url}/all_financial_statements",
            params={
                "registeredDateStart": registered_date_start,
                "registeredDateEnd": registered_date_end,
                "page": page,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
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
        response = self.session.get(
            f"{self.base_url}/financials",
            params={"businessId": business_id, "page": page},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return _discovery_page(response.json())

    def iter_company_financials(self, business_id: str) -> Iterator[DiscoveredStatement]:
        yield from _paginate(lambda page: self.list_company_financials(business_id, page=page))

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "PRHXBRLClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def download_financial_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        response = self.session.get(
            f"{self.base_url}/financial",
            params={"businessId": business_id, "financialDate": financial_date},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.content, response.url


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
