import json
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_uhm_procurement import tables

DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_DOWNLOAD_ATTEMPTS = 3
DEFAULT_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
MINIMUM_SNAPSHOT_BYTES = 50_000_000
MINIMUM_DATA_ROWS = 50_000
USER_AGENT = "Corpscout/1.0 (Sweden procurement open-data ingestion)"


@dataclass(frozen=True)
class UhmSnapshot:
    object_key: str
    manifest_key: str
    sha256: str
    size_bytes: int
    source_run_id: str
    retrieved_at: str
    last_modified: str
    etag: str
    content_type: str
    downloaded: bool


def uhm_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
        request_backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
    )
    client.session.headers.update(
        {"Accept-Encoding": "identity", "User-Agent": USER_AGENT}
    )
    return client.session


def sync_uhm_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
    minimum_size_bytes: int = MINIMUM_SNAPSHOT_BYTES,
    minimum_data_rows: int = MINIMUM_DATA_ROWS,
    download_attempts: int = DEFAULT_DOWNLOAD_ATTEMPTS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> UhmSnapshot:
    """Download, validate, content-address, and manifest the complete UHM CSV."""
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or uhm_http_session()
    try:
        with tempfile.TemporaryDirectory(prefix="sweden_uhm_procurement_") as temp_dir:
            target_path = Path(temp_dir) / "awards.csv"
            metadata = _download_snapshot(
                target_path=target_path,
                session=http_session,
                minimum_size_bytes=minimum_size_bytes,
                minimum_data_rows=minimum_data_rows,
                download_attempts=download_attempts,
                timeout_seconds=timeout_seconds,
            )
            object_key = (
                f"{tables.S3_RAW_PREFIX}/retrieved_date={retrieved_at.date().isoformat()}/"
                f"sha256={metadata['sha256']}/awards.csv"
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

    retrieved_timestamp = retrieved_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    manifest_key = (
        f"{tables.S3_MANIFEST_PREFIX}/retrieved_at={retrieved_timestamp}/"
        f"run_id={run_id}.json"
    )
    manifest = {
        "source_slug": tables.SOURCE_SLUG,
        "source_run_id": run_id,
        "source_url": tables.SOURCE_URL,
        "catalog_url": tables.SOURCE_CATALOG_URL,
        "object_key": object_key,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "sha256": metadata["sha256"],
        "size_bytes": metadata["size_bytes"],
        "content_type": metadata["content_type"],
        "last_modified": metadata["last_modified"],
        "etag": metadata["etag"],
        "downloaded": downloaded,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    return UhmSnapshot(
        object_key=object_key,
        manifest_key=manifest_key,
        sha256=str(metadata["sha256"]),
        size_bytes=int(metadata["size_bytes"]),
        source_run_id=run_id,
        retrieved_at=str(manifest["retrieved_at"]),
        last_modified=str(metadata["last_modified"]),
        etag=str(metadata["etag"]),
        content_type=str(metadata["content_type"]),
        downloaded=downloaded,
    )


def latest_snapshot_manifest(object_store: ObjectStoreResource) -> dict[str, Any]:
    keys = sorted(
        key
        for key in object_store.list_keys(
            tables.S3_MANIFEST_PREFIX,
            bucket=tables.S3_BUCKET,
        )
        if key.endswith(".json")
    )
    if not keys:
        raise ValueError(
            f"No UHM manifests under s3://{tables.S3_BUCKET}/"
            f"{tables.S3_MANIFEST_PREFIX}; materialize the raw snapshot first"
        )
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        for key in keys
    ]
    return max(manifests, key=lambda manifest: str(manifest["retrieved_at"]))


def _download_snapshot(
    *,
    target_path: Path,
    session: requests.Session,
    minimum_size_bytes: int,
    minimum_data_rows: int,
    download_attempts: int,
    timeout_seconds: int,
) -> dict[str, str | int]:
    last_error: Exception | None = None
    for attempt in range(1, download_attempts + 1):
        try:
            return _stream_snapshot(
                target_path=target_path,
                session=session,
                minimum_size_bytes=minimum_size_bytes,
                minimum_data_rows=minimum_data_rows,
                timeout_seconds=timeout_seconds,
            )
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt < download_attempts:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise last_error


def _stream_snapshot(
    *,
    target_path: Path,
    session: requests.Session,
    minimum_size_bytes: int,
    minimum_data_rows: int,
    timeout_seconds: int,
) -> dict[str, str | int]:
    response = session.get(tables.SOURCE_URL, timeout=timeout_seconds, stream=True)
    response.raise_for_status()

    digest = sha256()
    size_bytes = 0
    newline_count = 0
    with target_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            handle.write(chunk)
            digest.update(chunk)
            size_bytes += len(chunk)
            newline_count += chunk.count(b"\n")

    expected_length = response.headers.get("Content-Length")
    if (
        expected_length is not None
        and expected_length.isdigit()
        and size_bytes != int(expected_length)
    ):
        raise requests.exceptions.ChunkedEncodingError(
            f"incomplete UHM download: {size_bytes}/{expected_length} bytes"
        )
    if size_bytes < minimum_size_bytes:
        raise ValueError(
            f"UHM snapshot is materially truncated: {size_bytes} bytes, "
            f"expected at least {minimum_size_bytes}"
        )
    if newline_count - 1 < minimum_data_rows:
        raise ValueError(
            f"UHM snapshot has too few data rows: {max(newline_count - 1, 0)}, "
            f"expected at least {minimum_data_rows}"
        )
    return {
        "sha256": digest.hexdigest(),
        "size_bytes": size_bytes,
        "content_type": response.headers.get("Content-Type", ""),
        "last_modified": response.headers.get("Last-Modified", ""),
        "etag": response.headers.get("ETag", ""),
    }
