"""Resilient HTTP download for France BCE/INPI financial ratios."""

import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import requests
from dlt.sources.helpers import requests as dlt_requests

DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_USER_AGENT = "corpscout-dagster-v3/0.1"
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DOWNLOAD_MAX_ATTEMPTS = 4
DOWNLOAD_RETRY_BASE_SECONDS = 5

LOGGER = logging.getLogger(__name__)


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


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
    """Stream a Parquet snapshot to disk with whole-download retries."""
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
                "France financial download failed (attempt %s/%s), retrying in "
                "%ss: url=%s error=%s",
                attempt,
                max_attempts,
                wait_seconds,
                url,
                exc,
            )
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


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
