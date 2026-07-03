import json
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

SWEDEN_COMPANY_RAW_BUCKET = "source-sweden-company"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 1_800
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-sweden-company/0.1"

SCB_BULK_URL = "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip"
BOLAGSVERKET_BULK_URL = (
    "https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip"
)

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


@dataclass(frozen=True)
class SwedenCompanySourceFile:
    source_slug: str
    source_name: str
    url: str


@dataclass(frozen=True)
class SwedenCompanyDownloadedFile:
    source_slug: str
    source_name: str
    source_url: str
    source_last_modified: str
    s3_key: str
    downloaded: bool
    size_bytes: int | None
    sha256: str | None
    content_type: str
    last_modified: str


@dataclass(frozen=True)
class SourceFileHttpMetadata:
    source_last_modified: str
    content_length: int | None
    content_type: str
    last_modified: str


def raw_file_object_key(*, source_slug: str, source_last_modified: str) -> str:
    return (
        "sweden_company/raw/"
        f"source_last_modified={source_last_modified}/"
        f"source={source_slug}/"
        "source.zip"
    )


def manifest_object_key(*, retrieved_date: str, run_id: str) -> str:
    return (
        f"sweden_company/raw/retrieved_date={retrieved_date}/"
        f"run_id={run_id}/manifest.json"
    )


def latest_manifest_object_key(*, retrieved_date: str) -> str:
    return f"sweden_company/raw/retrieved_date={retrieved_date}/manifest.json"


def manifest_for_run(object_store: ObjectStoreResource, run_id: str) -> dict[str, Any]:
    manifest_keys = [
        key
        for key in object_store.list_keys(
            "sweden_company/raw/",
            bucket=SWEDEN_COMPANY_RAW_BUCKET,
        )
        if key.endswith("/manifest.json")
    ]
    if not manifest_keys:
        raise ValueError("No Sweden company raw manifest found in object storage")

    exact_run_keys = [
        key for key in manifest_keys if key.endswith(f"/run_id={run_id}/manifest.json")
    ]
    if exact_run_keys:
        manifests = [
            json.loads(object_store.read_bytes(key, bucket=SWEDEN_COMPANY_RAW_BUCKET))
            for key in exact_run_keys
        ]
        return max(manifests, key=_manifest_retrieved_at)

    manifests = [
        json.loads(object_store.read_bytes(key, bucket=SWEDEN_COMPANY_RAW_BUCKET))
        for key in manifest_keys
    ]
    run_manifests = [
        manifest for manifest in manifests if str(manifest.get("run_id")) == run_id
    ]
    return max(run_manifests or manifests, key=_manifest_retrieved_at)


def _manifest_retrieved_at(manifest: dict[str, Any]) -> str:
    return str(manifest["retrieved_at"])


def build_manifest(
    *,
    run_id: str,
    retrieved_at: datetime,
    files: list[SwedenCompanyDownloadedFile],
) -> dict[str, Any]:
    return {
        "source": "sweden_company",
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "retrieved_date": retrieved_at.date().isoformat(),
        "bucket": SWEDEN_COMPANY_RAW_BUCKET,
        "files": [
            {
                "source_slug": file.source_slug,
                "source_name": file.source_name,
                "source_url": file.source_url,
                "source_last_modified": file.source_last_modified,
                "s3_key": file.s3_key,
                "downloaded": file.downloaded,
                "size_bytes": file.size_bytes,
                "sha256": file.sha256,
                "content_type": file.content_type,
                "last_modified": file.last_modified,
            }
            for file in files
        ],
    }


class SwedenCompanyBulkResource(dg.ConfigurableResource):
    """Downloads Sweden company bulk ZIP snapshots into object storage."""

    scb_bulk_url: str = SCB_BULK_URL
    bolagsverket_bulk_url: str = BOLAGSVERKET_BULK_URL
    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def download_snapshot(
        self,
        *,
        object_store: ObjectStoreResource,
        run_id: str,
        retrieved_at: datetime,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> dg.MaterializeResult:
        retrieved_date = retrieved_at.date().isoformat()
        http_session = session or self._session()
        object_store.ensure_bucket(SWEDEN_COMPANY_RAW_BUCKET)

        files = [
            self._download_or_reuse_file(
                object_store=object_store,
                source_file=source_file,
                retrieved_date=retrieved_date,
                session=http_session,
                log_info=log_info,
            )
            for source_file in self._source_files()
        ]
        manifest_key = manifest_object_key(retrieved_date=retrieved_date, run_id=run_id)
        manifest_body = json.dumps(
            build_manifest(run_id=run_id, retrieved_at=retrieved_at, files=files),
            sort_keys=True,
        )
        object_store.write_json(
            manifest_key,
            manifest_body,
            bucket=SWEDEN_COMPANY_RAW_BUCKET,
        )
        object_store.write_json(
            latest_manifest_object_key(retrieved_date=retrieved_date),
            manifest_body,
            bucket=SWEDEN_COMPANY_RAW_BUCKET,
        )

        downloaded_file_count = sum(1 for file in files if file.downloaded)
        reused_file_count = len(files) - downloaded_file_count
        total_size_bytes = sum(file.size_bytes or 0 for file in files if file.downloaded)
        if log_info is not None:
            log_info(
                "Sweden company raw snapshot complete: bucket=%s manifest_key=%s "
                "downloaded=%s reused=%s bytes=%s",
                SWEDEN_COMPANY_RAW_BUCKET,
                manifest_key,
                downloaded_file_count,
                reused_file_count,
                total_size_bytes,
            )
        return dg.MaterializeResult(
            metadata={
                "s3_bucket": SWEDEN_COMPANY_RAW_BUCKET,
                "manifest_key": manifest_key,
                "retrieved_date": retrieved_date,
                "source_file_count": len(files),
                "downloaded_file_count": downloaded_file_count,
                "reused_file_count": reused_file_count,
                "total_size_bytes": total_size_bytes,
                "s3_keys": [file.s3_key for file in files],
            }
        )

    def _source_files(self) -> tuple[SwedenCompanySourceFile, ...]:
        return (
            SwedenCompanySourceFile(
                source_slug="scb_bulkfil",
                source_name="SCB/FDB company bulk file",
                url=self.scb_bulk_url,
            ),
            SwedenCompanySourceFile(
                source_slug="bolagsverket_bulkfil",
                source_name="Bolagsverket legal-register bulk file",
                url=self.bolagsverket_bulk_url,
            ),
        )

    def _download_or_reuse_file(
        self,
        *,
        object_store: ObjectStoreResource,
        source_file: SwedenCompanySourceFile,
        retrieved_date: str,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> SwedenCompanyDownloadedFile:
        http_metadata = self._http_metadata(
            url=source_file.url,
            retrieved_date=retrieved_date,
            session=session,
        )
        s3_key = raw_file_object_key(
            source_slug=source_file.source_slug,
            source_last_modified=http_metadata.source_last_modified,
        )
        if object_store.exists(s3_key, bucket=SWEDEN_COMPANY_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Sweden company raw ZIP: bucket=%s key=%s",
                    SWEDEN_COMPANY_RAW_BUCKET,
                    s3_key,
                )
            return SwedenCompanyDownloadedFile(
                source_slug=source_file.source_slug,
                source_name=source_file.source_name,
                source_url=source_file.url,
                source_last_modified=http_metadata.source_last_modified,
                s3_key=s3_key,
                downloaded=False,
                size_bytes=http_metadata.content_length,
                sha256=None,
                content_type=http_metadata.content_type,
                last_modified=http_metadata.last_modified,
            )

        if log_info is not None:
            log_info(
                "Downloading Sweden company raw ZIP: source=%s url=%s bucket=%s key=%s",
                source_file.source_slug,
                source_file.url,
                SWEDEN_COMPANY_RAW_BUCKET,
                s3_key,
            )
        with tempfile.TemporaryDirectory(prefix="sweden_company_") as tmpdir:
            temp_path = Path(tmpdir) / f"{source_file.source_slug}.zip"
            size_bytes, digest, content_type, last_modified = self._download_to_path(
                url=source_file.url,
                target_path=temp_path,
                session=session,
                log_info=log_info,
            )
            object_store.upload_file(
                s3_key,
                temp_path,
                bucket=SWEDEN_COMPANY_RAW_BUCKET,
            )
        return SwedenCompanyDownloadedFile(
            source_slug=source_file.source_slug,
            source_name=source_file.source_name,
            source_url=source_file.url,
            source_last_modified=http_metadata.source_last_modified,
            s3_key=s3_key,
            downloaded=True,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=content_type,
            last_modified=last_modified,
        )

    def _http_metadata(
        self,
        *,
        url: str,
        retrieved_date: str,
        session: Any,
    ) -> SourceFileHttpMetadata:
        response = session.head(url, timeout=self.request_timeout_seconds)
        response.raise_for_status()
        last_modified = response.headers.get("Last-Modified", "")
        return SourceFileHttpMetadata(
            source_last_modified=_source_last_modified_key(
                last_modified=last_modified,
                fallback=retrieved_date,
            ),
            content_length=_content_length(response.headers.get("Content-Length")),
            content_type=response.headers.get("Content-Type", ""),
            last_modified=last_modified,
        )

    def _download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> tuple[int, str, str, str]:
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
                        "Sweden company ZIP download failed; retrying: attempt=%s/%s "
                        "wait_seconds=%s url=%s error=%s",
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
    ) -> tuple[int, str, str, str]:
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

        return (
            size_bytes,
            digest.hexdigest(),
            response.headers.get("Content-Type", ""),
            response.headers.get("Last-Modified", ""),
        )

    def _session(self) -> Any:
        session = dlt_requests.Session(timeout=self.request_timeout_seconds)
        session.headers.update({"User-Agent": self.user_agent})
        return session


def _source_last_modified_key(*, last_modified: str, fallback: str) -> str:
    if last_modified == "":
        return fallback
    try:
        parsed = parsedate_to_datetime(last_modified)
    except (TypeError, ValueError, IndexError, AttributeError):
        return fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    timestamp = parsed.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return timestamp


def _content_length(value: str | None) -> int | None:
    if value is None or not value.isdigit():
        return None
    return int(value)
