from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from xml.etree import ElementTree

import dagster as dg
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

SWEDEN_FINANCIAL_RAW_BUCKET = "source-sweden-financial"
DEFAULT_ARCHIVE_BASE_URL = (
    "https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler"
)
DEFAULT_LISTING_PREFIX = "arsredovisningar/"
DEFAULT_LISTING_DELIMITER = "\\"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 1_800
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-sweden-financial/0.1"

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


@dataclass(frozen=True)
class SwedenFinancialArchive:
    upstream_key: str
    source_last_modified: str
    etag: str
    source_size_bytes: int

    @property
    def year(self) -> str:
        parts = self.upstream_key.split("/")
        if len(parts) >= 3 and parts[1].isdigit():
            return parts[1]
        return "unknown"

    @property
    def archive_name(self) -> str:
        return Path(self.upstream_key).name


@dataclass(frozen=True)
class SwedenFinancialStoredArchive:
    archive: SwedenFinancialArchive
    source_url: str
    s3_key: str
    downloaded: bool
    stored_size_bytes: int | None
    sha256: str | None
    content_type: str


def archive_object_key(*, upstream_key: str, source_last_modified: str) -> str:
    archive_name = Path(upstream_key).name
    year = _year_from_upstream_key(upstream_key)
    return (
        "sweden_financial/raw_archives/"
        f"year={year}/"
        f"archive={archive_name}/"
        f"source_last_modified={_object_key_timestamp(source_last_modified)}/"
        "archive.zip"
    )


class SwedenFinancialReportsResource(dg.ConfigurableResource):
    """Downloads Sweden annual-report batch ZIP archives into object storage."""

    archive_base_url: str = DEFAULT_ARCHIVE_BASE_URL
    listing_prefix: str = DEFAULT_LISTING_PREFIX
    listing_delimiter: str = DEFAULT_LISTING_DELIMITER
    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def download_raw_archives(
        self,
        *,
        object_store: ObjectStoreResource,
        year: str | None = None,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> dg.MaterializeResult:
        http_session = session or self._session()
        object_store.ensure_bucket(SWEDEN_FINANCIAL_RAW_BUCKET)
        archives = list(self.iter_archives(session=http_session, year=year))

        stored_archives = [
            self._download_or_reuse_archive(
                archive=archive,
                object_store=object_store,
                session=http_session,
                log_info=log_info,
            )
            for archive in archives
        ]
        downloaded_count = sum(1 for archive in stored_archives if archive.downloaded)
        reused_count = len(stored_archives) - downloaded_count
        downloaded_size_bytes = sum(
            archive.stored_size_bytes or 0
            for archive in stored_archives
            if archive.downloaded
        )
        if log_info is not None:
            log_info(
                "Sweden financial raw archive download complete: bucket=%s archives=%s "
                "downloaded=%s reused=%s bytes=%s",
                SWEDEN_FINANCIAL_RAW_BUCKET,
                len(stored_archives),
                downloaded_count,
                reused_count,
                downloaded_size_bytes,
            )
        return dg.MaterializeResult(
            metadata={
                "s3_bucket": SWEDEN_FINANCIAL_RAW_BUCKET,
                "archive_count": len(stored_archives),
                "downloaded_archive_count": downloaded_count,
                "reused_archive_count": reused_count,
                "downloaded_size_bytes": downloaded_size_bytes,
                "sample_s3_keys": [archive.s3_key for archive in stored_archives[:10]],
                **({"partition_year": year} if year is not None else {}),
            }
        )

    def iter_archives(
        self,
        *,
        session: Any,
        year: str | None = None,
    ) -> Iterator[SwedenFinancialArchive]:
        marker = self._listing_marker_for_year(year)
        while True:
            url = self.listing_url(marker=marker)
            response = session.get(url, timeout=self.request_timeout_seconds, stream=False)
            response.raise_for_status()
            archives, marker = _parse_listing_xml(response.text)
            if year is None:
                yield from archives
            else:
                yield from (
                    archive for archive in archives if archive.year == year
                )
                if _listing_has_passed_year(archives, year):
                    break
            if marker is None or marker == "":
                break

    def listing_url(
        self,
        *,
        marker: str | None = None,
        year: str | None = None,
    ) -> str:
        query = {
            "prefix": self.listing_prefix,
            "delimiter": self.listing_delimiter,
        }
        if marker is not None:
            query["marker"] = marker
        return f"{self.archive_base_url}?{urlencode(query)}"

    def _listing_marker_for_year(self, year: str | None) -> str | None:
        if year is None:
            return None
        return f"{self.listing_prefix.rstrip('/')}/{year}/"

    def archive_url(self, upstream_key: str) -> str:
        return f"{self.archive_base_url}/{quote(upstream_key, safe='/')}"

    def _download_or_reuse_archive(
        self,
        *,
        archive: SwedenFinancialArchive,
        object_store: ObjectStoreResource,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> SwedenFinancialStoredArchive:
        s3_key = archive_object_key(
            upstream_key=archive.upstream_key,
            source_last_modified=archive.source_last_modified,
        )
        source_url = self.archive_url(archive.upstream_key)
        if object_store.exists(s3_key, bucket=SWEDEN_FINANCIAL_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Sweden financial archive: bucket=%s key=%s",
                    SWEDEN_FINANCIAL_RAW_BUCKET,
                    s3_key,
                )
            return SwedenFinancialStoredArchive(
                archive=archive,
                source_url=source_url,
                s3_key=s3_key,
                downloaded=False,
                stored_size_bytes=archive.source_size_bytes,
                sha256=None,
                content_type="",
            )

        if log_info is not None:
            log_info(
                "Downloading Sweden financial archive: url=%s bucket=%s key=%s",
                source_url,
                SWEDEN_FINANCIAL_RAW_BUCKET,
                s3_key,
            )
        with tempfile.TemporaryDirectory(prefix="sweden_financial_") as tmpdir:
            target_path = Path(tmpdir) / archive.archive_name
            size_bytes, digest, content_type = self._download_to_path(
                url=source_url,
                target_path=target_path,
                session=session,
                log_info=log_info,
            )
            object_store.upload_file(
                s3_key,
                target_path,
                bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
            )
        return SwedenFinancialStoredArchive(
            archive=archive,
            source_url=source_url,
            s3_key=s3_key,
            downloaded=True,
            stored_size_bytes=size_bytes,
            sha256=digest,
            content_type=content_type,
        )

    def _download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> tuple[int, str, str]:
        last_error: Exception | None = None
        for attempt in range(1, self.download_max_attempts + 1):
            try:
                return self._stream_download_to_path(
                    url=url,
                    target_path=target_path,
                    session=session,
                )
            except _DOWNLOAD_RETRYABLE_ERRORS as exc:
                last_error = exc
                target_path.unlink(missing_ok=True)
                if attempt >= self.download_max_attempts:
                    break
                wait_seconds = self.download_retry_base_seconds * attempt
                if log_info is not None:
                    log_info(
                        "Sweden financial archive download failed; retrying: "
                        "attempt=%s/%s wait_seconds=%s url=%s error=%s",
                        attempt,
                        self.download_max_attempts,
                        wait_seconds,
                        url,
                        exc,
                    )
                time.sleep(wait_seconds)
        assert last_error is not None
        raise last_error

    def _stream_download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
    ) -> tuple[int, str, str]:
        response = session.get(url, timeout=self.request_timeout_seconds, stream=True)
        response.raise_for_status()

        digest = sha256()
        size_bytes = 0
        with target_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                digest.update(chunk)
                size_bytes += len(chunk)
                handle.write(chunk)

        expected = response.headers.get("Content-Length")
        if expected is not None and expected.isdigit() and size_bytes < int(expected):
            raise requests.exceptions.ChunkedEncodingError(
                f"incomplete download: {size_bytes}/{expected} bytes from {url}"
            )

        return size_bytes, digest.hexdigest(), response.headers.get("Content-Type", "")

    def _session(self) -> Any:
        session = dlt_requests.Session(timeout=self.request_timeout_seconds)
        session.headers.update({"User-Agent": self.user_agent})
        return session


def _parse_listing_xml(body: str) -> tuple[list[SwedenFinancialArchive], str | None]:
    root = ElementTree.fromstring(body)
    archives: list[SwedenFinancialArchive] = []
    for contents in _children(root, "Contents"):
        upstream_key = _child_text(contents, "Key")
        if not upstream_key.endswith(".zip"):
            continue
        archives.append(
            SwedenFinancialArchive(
                upstream_key=upstream_key,
                source_last_modified=_child_text(contents, "LastModified"),
                etag=_child_text(contents, "ETag").strip('"'),
                source_size_bytes=_int_text(_child_text(contents, "Size")),
            )
        )
    next_marker = _child_text(root, "NextMarker") or None
    return archives, next_marker


def _listing_has_passed_year(
    archives: list[SwedenFinancialArchive],
    target_year: str,
) -> bool:
    for archive in archives:
        if archive.year.isdigit() and archive.year > target_year:
            return True
    return False


def _children(element: ElementTree.Element, local_name: str) -> list[ElementTree.Element]:
    return [child for child in element if _local_name(child.tag) == local_name]


def _child_text(element: ElementTree.Element, local_name: str) -> str:
    for child in element:
        if _local_name(child.tag) == local_name:
            return child.text or ""
    return ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", maxsplit=1)[-1]


def _int_text(value: str) -> int:
    value = value.strip()
    if value == "":
        return 0
    return int(value)


def _year_from_upstream_key(upstream_key: str) -> str:
    parts = upstream_key.split("/")
    if len(parts) >= 3 and parts[1].isdigit():
        return parts[1]
    return "unknown"


def _object_key_timestamp(source_last_modified: str) -> str:
    return source_last_modified.replace(":", "-")
