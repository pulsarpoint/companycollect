"""Paging PNCP's consultation API.

PNCP publishes no bulk file, so every contract arrives through a paginated,
rate-limited endpoint. Two measured facts shape this module (2026-07-26):

* ``tamanhoPagina`` maxes out at 500 — 1000 is rejected outright.
* 429 arrives with **no** ``Retry-After`` and no rate-limit headers at all, so a
  client cannot be told when to retry and must back off blindly. That is what
  ``dlt.sources.helpers.requests`` already does, which is why the guidelines
  mandate it over plain ``requests``.

Each page is written to its own file before the next is fetched, so a partition
that dies on page 300 resumes at 300 rather than re-fetching 299 pages it
already paid for. At ~340 pages a month against a limit that refuses roughly
fifteen rapid requests, re-fetching is not a minor waste.
"""

from __future__ import annotations

import calendar
import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from dagster_v3.defs.brazil_pncp import tables

# The API says so explicitly rather than returning an empty page, which gives an
# unambiguous stop condition instead of one inferred from emptiness.
_PAGE_PAST_END_MARKER = "inexistente"


class _Response(Protocol):
    status_code: int
    text: str

    def json(self) -> Any: ...


class _Session(Protocol):
    def get(self, url: str, params: dict[str, Any], timeout: int) -> _Response: ...


def month_bounds(partition_key: str) -> tuple[str, str]:
    """The API's ``YYYYMMDD`` bounds for a ``YYYY-MM-DD`` monthly partition.

    Both bounds are inclusive — verified arithmetically, not assumed: two single
    days returned 5,936 and 8,888 records and the range covering them returned
    14,824, exactly their sum. So a month is its first day to its last, with no
    adjustment, and consecutive partitions neither drop nor double-count a day.
    """
    start = date.fromisoformat(partition_key).replace(day=1)
    last_day = calendar.monthrange(start.year, start.month)[1]
    return start.strftime("%Y%m%d"), start.replace(day=last_day).strftime("%Y%m%d")


def fetch_page(
    session: _Session,
    *,
    path: str,
    start: str,
    end: str,
    page: int,
    page_size: int = tables.MAX_PAGE_SIZE,
    timeout: int = 90,
) -> dict[str, Any] | None:
    """One page, or None when the page is past the end of the result set.

    Anything else that fails raises: a 400 the API did not explain, or a 429 the
    session gave up retrying, are both real failures and must not be mistaken
    for "no more data" — that would silently truncate a partition.
    """
    if page_size > tables.MAX_PAGE_SIZE:
        raise ValueError(
            f"tamanhoPagina={page_size} exceeds the API maximum of "
            f"{tables.MAX_PAGE_SIZE}; larger values are rejected outright"
        )
    response = session.get(
        f"{tables.API_BASE_URL}{path}",
        params={
            "dataInicial": start,
            "dataFinal": end,
            "pagina": page,
            "tamanhoPagina": page_size,
        },
        timeout=timeout,
    )
    if response.status_code == 400 and _PAGE_PAST_END_MARKER in response.text:
        return None
    if response.status_code != 200:
        raise RuntimeError(
            f"PNCP {path} page {page} for {start}..{end}: "
            f"HTTP {response.status_code} {response.text[:200]}"
        )
    return response.json()


def iter_pages(
    session: _Session,
    *,
    path: str,
    start: str,
    end: str,
    page_size: int = tables.MAX_PAGE_SIZE,
    resume_from: int = 1,
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    """Yield ``(page_number, records)`` from resume_from to the last page.

    ``totalPaginas`` from the first page bounds the walk, and the explicit
    past-the-end 400 backstops it. Both are used rather than either alone: the
    count could drift while a long month is being read, and trusting only the
    marker would mean one wasted request per partition.
    """
    page = resume_from
    total_pages: int | None = None
    while total_pages is None or page <= total_pages:
        payload = fetch_page(
            session, path=path, start=start, end=end, page=page, page_size=page_size
        )
        if payload is None:
            return
        if total_pages is None:
            total_pages = int(payload.get("totalPaginas") or 0)
        records = payload.get("data") or []
        if not records:
            return
        yield page, records
        page += 1


def download_partition(
    session: _Session,
    *,
    destination: Path,
    path: str,
    start: str,
    end: str,
    page_size: int = tables.MAX_PAGE_SIZE,
) -> dict[str, int]:
    """Write each page to its own JSONL file under destination.

    One file per page is what makes a resume cheap and obviously correct: a page
    already on disk is skipped, with no progress marker to keep in step with the
    files themselves. Written to a temporary name and renamed, so a partial file
    from a killed run is never mistaken for a complete page.
    """
    destination.mkdir(parents=True, exist_ok=True)
    existing = sorted(destination.glob("page-*.jsonl"))
    resume_from = len(existing) + 1

    records = sum(1 for f in existing for _ in f.open(encoding="utf-8"))
    pages = len(existing)

    for page, page_records in iter_pages(
        session,
        path=path,
        start=start,
        end=end,
        page_size=page_size,
        resume_from=resume_from,
    ):
        target = destination / f"page-{page:05d}.jsonl"
        partial = target.with_suffix(".jsonl.partial")
        with partial.open("w", encoding="utf-8") as handle:
            for record in page_records:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        partial.rename(target)
        pages += 1
        records += len(page_records)

    return {"pages": pages, "records": records, "resumed_from_page": resume_from}
