"""HTTP download and URL discovery for the Finland Verohallinto tax data source."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.finland_verotax import tables

DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DOWNLOAD_MAX_ATTEMPTS = 4
DOWNLOAD_RETRY_BASE_SECONDS = 5

LOGGER = logging.getLogger(__name__)

# CSV links on the vero.fi open-data page. The corporate income tax files contain
# "yhteis" (yhteisö/yhteisojen) + the stem "tuloverotu" (covers both nominative
# "tuloverotus" and genitive "tuloverotuksen" filename variants); the amendments
# file additionally contains "muutos" and is excluded. The tax year is the last
# 20xx group in the name.
_CSV_HREF_PATTERN = re.compile(r'href="(/contentassets/[^"]+\.csv)"', re.IGNORECASE)
_YEAR_PATTERN = re.compile(r"(20\d{2})")


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def resolve_year_sources(
    *,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    log: Callable[..., None] | None = None,
) -> dict[int, str]:
    """Discover the year -> CSV URL map from the vero.fi open-data page.

    Filenames rotate per year with no stable pattern (three naming schemes across
    2020-2024), so the page is the authoritative index. Falls back to the static
    map for any expected year the scrape does not surface, and logs years found
    on the page that are missing from EXPECTED_YEARS so the tuple can be extended.
    """
    progress_log = log or LOGGER.info
    discovered: dict[int, str] = {}
    try:
        html = _fetch_index_html(
            session=session, timeout_seconds=timeout_seconds, user_agent=user_agent
        )
        discovered = parse_year_sources(html)
    except _DOWNLOAD_RETRYABLE_ERRORS as exc:
        progress_log(
            "Finland verotax index page fetch failed; using fallback URL map: %s", exc
        )

    sources: dict[int, str] = {}
    for year in tables.EXPECTED_YEARS:
        url = discovered.get(year) or tables.FALLBACK_YEAR_SOURCES.get(year)
        if url is None:
            raise ValueError(
                f"No download URL for Finland verotax year {year}: not on the "
                "vero.fi open-data page and no fallback configured"
            )
        sources[year] = url

    new_years = sorted(set(discovered) - set(tables.EXPECTED_YEARS))
    if new_years:
        progress_log(
            "Finland verotax page lists tax years not in EXPECTED_YEARS "
            "(extend tables.EXPECTED_YEARS to ingest them): %s",
            new_years,
        )
    return sources


def parse_year_sources(html: str) -> dict[int, str]:
    """Extract year -> absolute CSV URL for the corporate income tax files."""
    sources: dict[int, str] = {}
    for href in _CSV_HREF_PATTERN.findall(html):
        name = href.rsplit("/", 1)[-1].lower()
        if "yhteis" not in name or "tuloverotu" not in name or "muutos" in name:
            continue
        years = _YEAR_PATTERN.findall(name)
        if not years:
            continue
        sources[int(years[-1])] = f"{tables.VERO_BASE_URL}{href}"
    return sources


def download_to_path(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    session: HttpSession | None = None,
    log: Callable[..., None] | None = None,
    max_attempts: int = DOWNLOAD_MAX_ATTEMPTS,
    retry_base_seconds: float = DOWNLOAD_RETRY_BASE_SECONDS,
) -> None:
    """Stream a URL to a file, retrying transient mid-stream failures.

    Each attempt re-truncates the destination so a broken download never leaves a
    partial file behind; a short read vs Content-Length is retried too. The dlt
    requests client already retries connection errors and 429/5xx per request —
    this loop covers mid-stream drops, which request-level retry does not.
    """
    progress_log = log or LOGGER.info
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            _stream_download(
                url=url,
                dest=dest,
                timeout_seconds=timeout_seconds,
                user_agent=user_agent,
                session=session,
            )
            return
        except _DOWNLOAD_RETRYABLE_ERRORS as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            wait_seconds = retry_base_seconds * attempt
            progress_log(
                "Finland verotax download failed (attempt %s/%s), retrying in %ss: "
                "url=%s error=%s",
                attempt,
                max_attempts,
                wait_seconds,
                url,
                exc,
            )
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


def _fetch_index_html(
    *,
    session: HttpSession | None,
    timeout_seconds: int,
    user_agent: str,
) -> str:
    client = session or _default_session(user_agent)
    response = client.get(tables.SOURCE_PAGE_URL, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


def _stream_download(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
) -> None:
    client = session or _default_session(user_agent)
    response = client.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    expected = response.headers.get("Content-Length")
    written = 0
    with dest.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if chunk:
                handle.write(chunk)
                written += len(chunk)
    if expected is not None and written < int(expected):
        raise requests.exceptions.ChunkedEncodingError(
            f"Short read downloading {url}: got {written} of {expected} bytes"
        )


def _default_session(user_agent: str) -> Any:
    session = dlt_requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session
