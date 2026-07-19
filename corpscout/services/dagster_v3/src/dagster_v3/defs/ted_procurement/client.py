"""TED v3 search API client (keyless, country-agnostic)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any, Protocol

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.ted_procurement import tables

DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
PAGE_SLEEP_SECONDS = 0.5
# ted.europa.eu rate-limits the XML endpoint sporadically (429s observed at
# ~13/578 fetches even throttled) — retry with Retry-After support.
XML_FETCH_MAX_ATTEMPTS = 6
XML_FETCH_BASE_SLEEP_SECONDS = 2.0
XML_THROTTLE_SECONDS = 0.2


class HttpSession(Protocol):
    def post(self, url: str, *, json: Any, timeout: int) -> Any: ...

    def get(self, url: str, *, timeout: int) -> Any: ...


def default_session(user_agent: str = DEFAULT_USER_AGENT) -> Any:
    session = dlt_requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def build_monthly_query(
    *, place_codes: tuple[str, ...], date_start: str, date_end: str
) -> str:
    """Expert-syntax query for one [date_start, date_end) publication window.

    Dates are yyyymmdd. Notice types are the award-carrying eForms set.
    """
    places = ", ".join(place_codes)
    types = ", ".join(tables.NOTICE_TYPES)
    return (
        f"place-of-performance IN ({places}) AND notice-type IN ({types}) "
        f"AND publication-date>={date_start} AND publication-date<{date_end}"
    )


def iter_search_notices(
    *,
    query: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    page_sleep_seconds: float = PAGE_SLEEP_SECONDS,
) -> Iterator[dict[str, Any]]:
    """Yield every listing row for the query, paging at the API maximum."""
    client = session or default_session()
    page = 1
    while True:
        response = client.post(
            tables.SEARCH_API_URL,
            json={
                "query": query,
                "fields": list(tables.LISTING_FIELDS),
                "limit": tables.SEARCH_PAGE_LIMIT,
                "page": page,
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        notices = payload.get("notices", [])
        yield from notices
        if len(notices) < tables.SEARCH_PAGE_LIMIT:
            return
        page += 1
        time.sleep(page_sleep_seconds)


def fetch_notice_xml(
    *,
    publication_number: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_attempts: int = XML_FETCH_MAX_ATTEMPTS,
    base_sleep_seconds: float = XML_FETCH_BASE_SLEEP_SECONDS,
) -> bytes:
    client = session or default_session()
    url = tables.NOTICE_XML_URL_TEMPLATE.format(publication_number=publication_number)
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(url, timeout=timeout_seconds)
        except requests.exceptions.HTTPError as exc:
            # The dlt session raises 429s itself after its internal retries.
            resp = exc.response
            if (
                resp is not None
                and resp.status_code == 429
                and attempt < max_attempts
            ):
                retry_after = float(resp.headers.get("Retry-After") or 0)
                time.sleep(max(retry_after, base_sleep_seconds * attempt))
                continue
            raise
        if response.status_code == 429 and attempt < max_attempts:
            retry_after = float(response.headers.get("Retry-After") or 0)
            time.sleep(max(retry_after, base_sleep_seconds * attempt))
            continue
        response.raise_for_status()
        return response.content
    raise AssertionError("unreachable")  # loop always returns or raises
