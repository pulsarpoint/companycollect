"""filings.xbrl.org API client: paginated filing index + OIM JSON download.

JSON:API traversal is the whole trick: entities arrive once per page in
`included` (deduplicated by the API, not by us), and each filing references
its entity via `relationships.entity.data.id`. We build a page-local
`{entity_id: attributes}` map and join by that id — never by array position.
"""

import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dlt.sources.helpers import requests as dlt_requests

ESEF_API_BASE = "https://filings.xbrl.org"
ESEF_INDEX_URL = f"{ESEF_API_BASE}/api/filings"
PAGE_SIZE = 200

DEFAULT_TIMEOUT_SECONDS = 120
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DOWNLOAD_MAX_ATTEMPTS = 4
DOWNLOAD_RETRY_BASE_SECONDS = 5.0

# Transient network failures a streamed download must survive (the remote
# drops the stream mid-transfer -> ChunkedEncodingError / IncompleteRead).
# Request-level retry (dlt_requests.Client) does not cover these: they surface
# while iterating the response body, after the retried .get() call returned.
_DOWNLOAD_RETRYABLE_ERRORS = (
    dlt_requests.ChunkedEncodingError,
    dlt_requests.ConnectionError,
    dlt_requests.Timeout,
)


@dataclass(frozen=True)
class EsefFilingRecord:
    lei: str
    entity_name: str
    fxo_id: str
    country: str
    period_end: str | None
    date_added: str | None
    processed_at: str | None
    json_url: str | None  # absolute URL or None
    package_url: str | None
    report_url: str | None
    viewer_url: str | None
    package_sha256: str | None
    error_count: int
    warning_count: int
    inconsistency_count: int


def _absolute(url: str | None) -> str | None:
    if url is None or url == "":
        return None
    return url if url.startswith("http") else f"{ESEF_API_BASE}{url}"


class EsefFilingsClient:
    def __init__(self, session: Any | None = None) -> None:
        self._session = session or dlt_requests.Client(
            request_timeout=120, request_max_attempts=5
        ).session

    def iter_filings(self) -> Iterator[EsefFilingRecord]:
        page = 1
        while True:
            resp = self._session.get(
                ESEF_INDEX_URL,
                params={
                    "page[size]": PAGE_SIZE,
                    "page[number]": page,
                    "include": "entity",
                },
            )
            resp.raise_for_status()
            payload = resp.json()
            entities = {
                inc["id"]: inc["attributes"]
                for inc in payload.get("included", [])
                if inc.get("type") == "entity"
            }
            data = payload.get("data", [])
            if not data:
                return
            for item in data:
                attrs = item["attributes"]
                rel = item.get("relationships", {}).get("entity", {}).get("data")
                ent = entities.get(rel["id"], {}) if rel else {}
                yield EsefFilingRecord(
                    lei=str(ent.get("identifier", "") or ""),
                    entity_name=str(ent.get("name", "") or ""),
                    fxo_id=str(attrs.get("fxo_id", "") or ""),
                    country=str(attrs.get("country", "") or ""),
                    period_end=attrs.get("period_end"),
                    date_added=attrs.get("date_added"),
                    processed_at=attrs.get("processed"),
                    json_url=_absolute(attrs.get("json_url")),
                    package_url=_absolute(attrs.get("package_url")),
                    report_url=_absolute(attrs.get("report_url")),
                    viewer_url=_absolute(attrs.get("viewer_url")),
                    package_sha256=attrs.get("sha256"),
                    error_count=int(attrs.get("error_count") or 0),
                    warning_count=int(attrs.get("warning_count") or 0),
                    inconsistency_count=int(attrs.get("inconsistency_count") or 0),
                )
            page += 1

    def download_json_facts(
        self,
        json_url: str,
        target: Path,
        *,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = DOWNLOAD_MAX_ATTEMPTS,
        retry_base_seconds: float = DOWNLOAD_RETRY_BASE_SECONDS,
    ) -> None:
        """Stream json_url to target, retrying the whole download on transient
        failures.

        Mirrors latvia_ur/resources.py:_download_to_path — each attempt
        truncates target before writing (so a broken download never leaves a
        partial file behind), and a short read vs the server's
        Content-Length is treated as a retryable failure.
        """
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                self._stream_download(
                    json_url, target, timeout_seconds=timeout_seconds
                )
                return
            except _DOWNLOAD_RETRYABLE_ERRORS as exc:
                last_error = exc
                if attempt >= max_attempts:
                    break
                time.sleep(retry_base_seconds * attempt)
        assert last_error is not None
        raise last_error

    def _stream_download(
        self, url: str, target: Path, *, timeout_seconds: int
    ) -> None:
        response = self._session.get(url, timeout=timeout_seconds, stream=True)
        response.raise_for_status()
        written = 0
        with target.open("wb") as out:
            iter_content = getattr(response, "iter_content", None)
            if callable(iter_content):
                for chunk in iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    if chunk:
                        out.write(chunk)
                        written += len(chunk)
            else:
                body = response.content
                out.write(body)
                written = len(body)
        headers = getattr(response, "headers", None)
        expected = headers.get("Content-Length") if hasattr(headers, "get") else None
        if expected is not None and str(expected).isdigit() and written < int(expected):
            raise dlt_requests.ChunkedEncodingError(
                f"incomplete download: {written}/{expected} bytes from {url}"
            )
