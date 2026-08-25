import json
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import ijson
import requests
from dlt.sources.helpers import requests as dlt_requests
from ijson.common import JSONError

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.serbia_apr_companies import tables

DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_DOWNLOAD_ATTEMPTS = 3
DEFAULT_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
MINIMUM_SNAPSHOT_BYTES = 10_000_000
MINIMUM_RECORD_COUNT = 100_000
USER_AGENT = "Corpscout/1.0 (Serbia APR companies open-data ingestion)"


@dataclass(frozen=True)
class AprCompaniesSnapshot:
    object_key: str
    manifest_key: str
    snapshot_date: str
    sha256: str
    size_bytes: int
    record_count: int
    source_run_id: str
    retrieved_at: str
    content_type: str
    downloaded: bool


def apr_companies_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
        request_backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
    )
    client.session.headers.update(
        {
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        }
    )
    return client.session


def sync_apr_companies_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
    minimum_size_bytes: int = MINIMUM_SNAPSHOT_BYTES,
    minimum_record_count: int = MINIMUM_RECORD_COUNT,
    download_attempts: int = DEFAULT_DOWNLOAD_ATTEMPTS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    retry_base_seconds: float = DEFAULT_RETRY_BASE_SECONDS,
    log_info: Callable[..., object] | None = None,
) -> AprCompaniesSnapshot:
    """Download, validate, content-address, and manifest an APR snapshot."""
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or apr_companies_http_session()
    try:
        with tempfile.TemporaryDirectory(prefix="serbia_apr_companies_") as temp_dir:
            target_path = Path(temp_dir) / "companies.json"
            metadata = _download_snapshot(
                target_path=target_path,
                session=http_session,
                minimum_size_bytes=minimum_size_bytes,
                minimum_record_count=minimum_record_count,
                download_attempts=download_attempts,
                timeout_seconds=timeout_seconds,
                retry_base_seconds=retry_base_seconds,
                log_info=log_info,
            )
            object_key = raw_snapshot_object_key(
                snapshot_date=str(metadata["snapshot_date"]),
                payload_sha256=str(metadata["sha256"]),
            )
            downloaded = not object_store.exists(
                object_key,
                bucket=tables.S3_BUCKET,
            )
            if downloaded:
                object_store.upload_file(
                    object_key,
                    target_path,
                    bucket=tables.S3_BUCKET,
                )
    finally:
        if owns_session:
            http_session.close()

    retrieved_at_utc = retrieved_at.astimezone(UTC)
    manifest_key = snapshot_manifest_key(
        retrieved_at=retrieved_at_utc,
        run_id=run_id,
    )
    manifest = {
        "bucket": tables.S3_BUCKET,
        "content_type": metadata["content_type"],
        "downloaded": downloaded,
        "object_key": object_key,
        "record_count": metadata["record_count"],
        "retrieved_at": retrieved_at_utc.isoformat(),
        "sha256": metadata["sha256"],
        "size_bytes": metadata["size_bytes"],
        "snapshot_date": metadata["snapshot_date"],
        "source_license": tables.SOURCE_LICENSE,
        "source_run_id": run_id,
        "source_slug": tables.SOURCE_SLUG,
        "source_url": tables.SOURCE_URL,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    if log_info is not None:
        log_info(
            "Serbia APR companies snapshot stored: bucket=%s key=%s "
            "snapshot_date=%s records=%s bytes=%s downloaded=%s",
            tables.S3_BUCKET,
            object_key,
            metadata["snapshot_date"],
            metadata["record_count"],
            metadata["size_bytes"],
            downloaded,
        )
    return AprCompaniesSnapshot(
        object_key=object_key,
        manifest_key=manifest_key,
        snapshot_date=str(metadata["snapshot_date"]),
        sha256=str(metadata["sha256"]),
        size_bytes=int(metadata["size_bytes"]),
        record_count=int(metadata["record_count"]),
        source_run_id=run_id,
        retrieved_at=retrieved_at_utc.isoformat(),
        content_type=str(metadata["content_type"]),
        downloaded=downloaded,
    )


def raw_snapshot_object_key(*, snapshot_date: str, payload_sha256: str) -> str:
    return (
        f"{tables.S3_RAW_PREFIX}/snapshot_date={snapshot_date}/"
        f"sha256={payload_sha256}/companies.json"
    )


def snapshot_manifest_key(*, retrieved_at: datetime, run_id: str) -> str:
    retrieved_timestamp = retrieved_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    return (
        f"{tables.S3_MANIFEST_PREFIX}/retrieved_at={retrieved_timestamp}/"
        f"run_id={run_id}.json"
    )


def _download_snapshot(
    *,
    target_path: Path,
    session: requests.Session,
    minimum_size_bytes: int,
    minimum_record_count: int,
    download_attempts: int,
    timeout_seconds: int,
    retry_base_seconds: float,
    log_info: Callable[..., object] | None,
) -> dict[str, str | int]:
    last_error: Exception | None = None
    for attempt in range(1, download_attempts + 1):
        try:
            return _stream_and_validate_snapshot(
                target_path=target_path,
                session=session,
                minimum_size_bytes=minimum_size_bytes,
                minimum_record_count=minimum_record_count,
                timeout_seconds=timeout_seconds,
            )
        except (requests.RequestException, JSONError, ValueError) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt >= download_attempts:
                break
            wait_seconds = retry_base_seconds * attempt
            if log_info is not None:
                log_info(
                    "Serbia APR companies download failed; retrying: "
                    "attempt=%s/%s wait_seconds=%s error=%s",
                    attempt,
                    download_attempts,
                    wait_seconds,
                    exc,
                )
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


def _stream_and_validate_snapshot(
    *,
    target_path: Path,
    session: requests.Session,
    minimum_size_bytes: int,
    minimum_record_count: int,
    timeout_seconds: int,
) -> dict[str, str | int]:
    response = session.get(tables.SOURCE_URL, timeout=timeout_seconds, stream=True)
    response.raise_for_status()

    digest = sha256()
    size_bytes = 0
    with target_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            handle.write(chunk)
            digest.update(chunk)
            size_bytes += len(chunk)

    expected_length = response.headers.get("Content-Length")
    if (
        expected_length is not None
        and expected_length.isdigit()
        and size_bytes != int(expected_length)
    ):
        raise requests.exceptions.ChunkedEncodingError(
            f"incomplete APR companies download: {size_bytes}/{expected_length} bytes"
        )
    if size_bytes < minimum_size_bytes:
        raise ValueError(
            f"APR companies snapshot is materially truncated: {size_bytes} bytes, "
            f"expected at least {minimum_size_bytes}"
        )

    snapshot_date, record_count = _inspect_snapshot(target_path)
    if record_count < minimum_record_count:
        raise ValueError(
            f"APR companies snapshot has too few company records: {record_count}, "
            f"expected at least {minimum_record_count}"
        )
    return {
        "content_type": response.headers.get("Content-Type", ""),
        "record_count": record_count,
        "sha256": digest.hexdigest(),
        "size_bytes": size_bytes,
        "snapshot_date": snapshot_date,
    }


def _inspect_snapshot(path: Path) -> tuple[str, int]:
    snapshot_date = ""
    record_count = 0
    saw_company_map = False
    with path.open("rb") as handle:
        for prefix, event, value in ijson.parse(handle):
            if prefix == "DatumPreseka" and event == "string":
                snapshot_date = str(value)
            elif prefix == "Podaci" and event == "start_map":
                saw_company_map = True
            elif prefix == "Podaci" and event == "map_key":
                record_count += 1

    if snapshot_date == "":
        raise ValueError("APR companies snapshot is missing DatumPreseka")
    try:
        date.fromisoformat(snapshot_date)
    except ValueError as exc:
        raise ValueError(
            f"APR companies DatumPreseka is not an ISO date: {snapshot_date}"
        ) from exc
    if not saw_company_map:
        raise ValueError("APR companies snapshot is missing the Podaci object")
    return snapshot_date, record_count
