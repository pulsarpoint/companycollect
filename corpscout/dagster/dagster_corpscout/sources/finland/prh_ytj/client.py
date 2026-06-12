"""HTTP client for the PRH Open Data YTJ API v3."""

import json
import time
from collections.abc import Callable, Iterator
from urllib.parse import urlsplit, urlunsplit

import requests

from dagster_corpscout.lib.streaming import StreamStats
from dagster_corpscout.sources.finland.prh_ytj import spec

_TIMEOUT_SECONDS = 300
_RETRY_DELAYS_SECONDS = [1, 2, 4, 8]
_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def _get_with_transport_retries(
    session: requests.Session,
    url: str,
    **kwargs,
) -> requests.Response:
    attempts = len(_RETRY_DELAYS_SECONDS) + 1
    for attempt in range(attempts):
        try:
            return session.get(url, **kwargs)
        except _RETRYABLE_EXCEPTIONS:
            if attempt == attempts - 1:
                raise
            time.sleep(_RETRY_DELAYS_SECONDS[attempt])
    raise RuntimeError("unreachable retry state")


def iter_companies(
    base_url: str,
    page_size: int = spec.PAGE_SIZE,
    progress_logger: Callable[[int, int, int | None], None] | None = None,
    progress_interval_pages: int = 100,
) -> Iterator[dict]:
    session = requests.Session()
    total_results: int | None = None
    records = 0
    page_number = 1

    while True:
        response = _get_with_transport_retries(
            session,
            base_url,
            params={"page": page_number},
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("totalResults") is not None:
            total_results = int(payload["totalResults"])

        companies = payload.get("companies") or []
        for company in companies:
            records += 1
            yield company

        is_done = (
            (total_results is not None and records >= total_results)
            or not companies
            or (total_results is None and len(companies) < page_size)
        )
        if progress_logger and (
            page_number == 1
            or page_number % progress_interval_pages == 0
            or is_done
        ):
            progress_logger(page_number, records, total_results)

        if total_results is not None and records >= total_results:
            return
        if not companies:
            return
        if total_results is None and len(companies) < page_size:
            return

        page_number += 1


def ndjson_chunks(companies: Iterator[dict], stats: StreamStats) -> Iterator[bytes]:
    for company in companies:
        stats.records += 1
        line = json.dumps(company, separators=(",", ":"), ensure_ascii=False)
        yield line.encode("utf-8") + b"\n"


def fetch_code_list(base_url: str, code: str, lang: str) -> bytes:
    session = requests.Session()
    parts = urlsplit(base_url)
    url = urlunsplit((parts.scheme, parts.netloc, spec.DESCRIPTION_PATH, "", ""))
    response = _get_with_transport_retries(
        session,
        url,
        params={"code": code, "lang": lang},
        timeout=_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.content
