"""Reading Doffin's public API.

Three measured behaviours shape this module (2026-07-27, see the design doc):

* **Unrecognised query parameters are ignored silently.** Three guessed date
  parameters each returned HTTP 200 and the unfiltered 157,050. A filter that
  does not work looks exactly like one that does, so the parameter names come
  from the published API definition and every partition asserts that its slice
  actually narrowed. Invalid enum *values* do fail loudly (404) -- it is only
  names that vanish.
* **A search returns at most 1,000 results**, whatever the filter.
  ``numHitsAccessible`` reads ``min(numHitsTotal, 1000)``, so a partition that
  exceeds it is silently truncated unless checked.
* **429 arrives with no Retry-After and no rate-limit headers**, and clears
  within seconds. Blind backoff is the only possible handling, which is what
  ``dlt.sources.helpers.requests`` gives.
"""

from __future__ import annotations

import calendar
import os
from collections.abc import Callable, Iterator
from datetime import date
from typing import Any, Protocol

from dagster_v3.defs.norway_doffin import tables


class _Response(Protocol):
    status_code: int
    text: str
    content: bytes

    def json(self) -> Any: ...


class _Session(Protocol):
    def get(
        self, url: str, params: dict[str, Any], headers: dict[str, str], timeout: int
    ) -> _Response: ...


def api_key() -> str:
    key = os.getenv(tables.API_KEY_ENV, "").strip()
    if not key:
        raise RuntimeError(
            f"{tables.API_KEY_ENV} is not set. Doffin's public API requires an "
            f"Azure APIM subscription key; register at "
            f"https://dof-notices-prod-api.developer.azure-api.net/signup"
        )
    return key


def month_bounds(partition_key: str) -> tuple[str, str]:
    """``issueDateFrom``/``issueDateTo`` for a ``YYYY-MM-DD`` monthly partition.

    Both bounds are inclusive and the format is ``yyyy-mm-dd``, per the API
    definition. These are the ONLY date parameters that exist -- there is no
    publication-date filter -- which is why partitions key on issue date.
    """
    start = date.fromisoformat(partition_key).replace(day=1)
    last_day = calendar.monthrange(start.year, start.month)[1]
    return start.isoformat(), start.replace(day=last_day).isoformat()


def search_page(
    session: _Session,
    *,
    issue_date_from: str,
    issue_date_to: str,
    page: int,
    hits_per_page: int = tables.MAX_HITS_PER_PAGE,
    notice_type: str = tables.AWARD_NOTICE_TYPE,
    timeout: int = 60,
) -> dict[str, Any]:
    """One page of award notices for a date slice."""
    if hits_per_page > tables.MAX_HITS_PER_PAGE:
        raise ValueError(
            f"numHitsPerPage={hits_per_page} exceeds the API maximum of "
            f"{tables.MAX_HITS_PER_PAGE}"
        )
    response = session.get(
        f"{tables.API_BASE_URL}{tables.SEARCH_PATH}",
        params={
            "type": notice_type,
            "numHitsPerPage": hits_per_page,
            "page": page,
            # Read from the API definition, never guessed -- a misspelling here
            # returns the whole register looking like a filtered slice.
            "issueDateFrom": issue_date_from,
            "issueDateTo": issue_date_to,
        },
        headers={tables.API_KEY_HEADER: api_key()},
        timeout=timeout,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Doffin search {issue_date_from}..{issue_date_to} page {page}: "
            f"HTTP {response.status_code} {response.text[:200]}"
        )
    return response.json()


def assert_slice_is_filtered_and_complete(
    payload: dict[str, Any],
    *,
    issue_date_from: str,
    issue_date_to: str,
    unfiltered_total: int | None = None,
) -> int:
    """Two failure modes that both look like success. Returns numHitsTotal.

    A date filter that was ignored returns the whole register, and a slice
    beyond the 1,000-result ceiling returns a truncated page with no marker.
    Neither raises on its own, and a backfill built on either produces a table
    that looks populated and is wrong -- the first by loading the same 31,097
    notices into every partition, the second by dropping the tail of a busy
    month.
    """
    total = int(payload.get("numHitsTotal") or 0)
    accessible = int(payload.get("numHitsAccessible") or 0)

    if unfiltered_total is not None and total >= unfiltered_total:
        raise RuntimeError(
            f"Doffin returned {total} notices for {issue_date_from}..{issue_date_to}, "
            f"which is the unfiltered total ({unfiltered_total}). The date filter "
            f"was ignored -- check the parameter names against the API definition "
            f"rather than trusting the HTTP 200."
        )
    if total > accessible:
        raise RuntimeError(
            f"Doffin has {total} notices for {issue_date_from}..{issue_date_to} but "
            f"only {accessible} are reachable; the slice exceeds the "
            f"{tables.RESULT_CEILING}-result ceiling and would load truncated. "
            f"Narrow the partition below a month."
        )
    return total


def iter_search_hits(
    session: _Session,
    *,
    issue_date_from: str,
    issue_date_to: str,
    hits_per_page: int = tables.MAX_HITS_PER_PAGE,
    on_page: Callable[[int, int, int], None] | None = None,
) -> Iterator[dict[str, Any]]:
    """Every award notice in a date slice, page by page."""
    page = 1
    seen = 0
    total: int | None = None
    while True:
        payload = search_page(
            session,
            issue_date_from=issue_date_from,
            issue_date_to=issue_date_to,
            page=page,
            hits_per_page=hits_per_page,
        )
        if total is None:
            total = assert_slice_is_filtered_and_complete(
                payload,
                issue_date_from=issue_date_from,
                issue_date_to=issue_date_to,
            )
        hits = payload.get("hits") or []
        if not hits:
            return
        seen += len(hits)
        if on_page is not None:
            on_page(page, total, seen)
        yield from hits
        if seen >= total:
            return
        page += 1


def fetch_notice_xml(
    session: _Session, *, doffin_id: str, timeout: int = 60
) -> bytes:
    """The full eForms UBL notice. This is where the realized money lives."""
    response = session.get(
        f"{tables.API_BASE_URL}"
        f"{tables.DOWNLOAD_PATH_TEMPLATE.format(doffin_id=doffin_id)}",
        params={},
        headers={tables.API_KEY_HEADER: api_key()},
        timeout=timeout,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Doffin download {doffin_id}: HTTP {response.status_code} "
            f"{response.text[:200]}"
        )
    return response.content
