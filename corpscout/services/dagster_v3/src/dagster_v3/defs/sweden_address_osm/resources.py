from __future__ import annotations

import hashlib
import json
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from boto3.s3.transfer import TransferConfig
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_address_osm import tables

DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_DOWNLOAD_ATTEMPTS = 4
DEFAULT_UPLOAD_ATTEMPTS = 4
DEFAULT_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
UPLOAD_PART_BYTES = 64 * 1024 * 1024
USER_AGENT = "Corpscout/1.0 (Sweden OpenStreetMap address ingestion)"

SNAPSHOT_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=UPLOAD_PART_BYTES,
    multipart_chunksize=UPLOAD_PART_BYTES,
    max_concurrency=1,
    use_threads=False,
)


@dataclass(frozen=True)
class OsmSnapshot:
    object_key: str
    manifest_key: str
    source_md5: str
    sha256: str
    size_bytes: int
    source_run_id: str
    retrieved_at: str
    resolved_url: str
    last_modified: str
    etag: str
    content_type: str
    downloaded: bool


def osm_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
        request_backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
    )
    client.session.headers.update(
        {"Accept-Encoding": "identity", "User-Agent": USER_AGENT}
    )
    return client.session


def sync_osm_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
    minimum_size_bytes: int = tables.MINIMUM_SNAPSHOT_BYTES,
    download_attempts: int = DEFAULT_DOWNLOAD_ATTEMPTS,
    upload_attempts: int = DEFAULT_UPLOAD_ATTEMPTS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> OsmSnapshot:
    """Archive the current Sweden PBF once per Geofabrik content checksum."""
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or osm_http_session()
    try:
        source_md5 = fetch_source_md5(http_session, timeout_seconds=timeout_seconds)
        object_key = (
            f"{tables.S3_RAW_PREFIX}/md5={source_md5}/sweden-latest.osm.pbf"
        )
        previous_manifest = _manifest_for_object_key(object_store, object_key)
        downloaded = previous_manifest is None or not object_store.exists(
            object_key,
            bucket=tables.S3_BUCKET,
        )
        if downloaded:
            with tempfile.TemporaryDirectory(prefix="sweden_address_osm_") as temp_dir:
                target_path = Path(temp_dir) / "sweden-latest.osm.pbf"
                metadata = _download_snapshot(
                    target_path=target_path,
                    session=http_session,
                    expected_md5=source_md5,
                    minimum_size_bytes=minimum_size_bytes,
                    download_attempts=download_attempts,
                    timeout_seconds=timeout_seconds,
                )
                _upload_snapshot(
                    object_store=object_store,
                    object_key=object_key,
                    source_path=target_path,
                    upload_attempts=upload_attempts,
                )
        else:
            metadata = {
                key: previous_manifest[key]
                for key in (
                    "sha256",
                    "size_bytes",
                    "resolved_url",
                    "last_modified",
                    "etag",
                    "content_type",
                )
            }
    finally:
        if owns_session:
            http_session.close()

    retrieved_timestamp = retrieved_at.astimezone(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S.%fZ"
    )
    manifest_key = (
        f"{tables.S3_MANIFEST_PREFIX}/retrieved_at={retrieved_timestamp}/"
        f"run_id={run_id}.json"
    )
    manifest = {
        "source_slug": tables.SOURCE_SLUG,
        "country_code": tables.COUNTRY_CODE,
        "source_run_id": run_id,
        "source_url": tables.SOURCE_URL,
        "source_md5_url": tables.SOURCE_MD5_URL,
        "source_catalog_url": tables.SOURCE_CATALOG_URL,
        "source_license_url": tables.SOURCE_LICENSE_URL,
        "resolved_url": metadata["resolved_url"],
        "object_key": object_key,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "source_md5": source_md5,
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
    return OsmSnapshot(
        object_key=object_key,
        manifest_key=manifest_key,
        source_md5=source_md5,
        sha256=str(metadata["sha256"]),
        size_bytes=int(metadata["size_bytes"]),
        source_run_id=run_id,
        retrieved_at=str(manifest["retrieved_at"]),
        resolved_url=str(metadata["resolved_url"]),
        last_modified=str(metadata["last_modified"]),
        etag=str(metadata["etag"]),
        content_type=str(metadata["content_type"]),
        downloaded=downloaded,
    )


def fetch_source_md5(
    session: requests.Session,
    *,
    timeout_seconds: int,
) -> str:
    response = session.get(tables.SOURCE_MD5_URL, timeout=timeout_seconds)
    response.raise_for_status()
    match = re.fullmatch(
        r"\s*([0-9a-fA-F]{32})\s+\*?sweden-latest\.osm\.pbf\s*",
        response.text,
    )
    if match is None:
        raise ValueError("Geofabrik Sweden MD5 response has an unexpected format")
    return match.group(1).lower()


def latest_snapshot_manifest(object_store: ObjectStoreResource) -> dict[str, Any]:
    manifests = _snapshot_manifests(object_store)
    if not manifests:
        raise ValueError(
            f"No OpenStreetMap manifests under s3://{tables.S3_BUCKET}/"
            f"{tables.S3_MANIFEST_PREFIX}; materialize sweden_osm_pbf_s3 first"
        )
    return max(manifests, key=lambda manifest: str(manifest["retrieved_at"]))


def _manifest_for_object_key(
    object_store: ObjectStoreResource,
    object_key: str,
) -> dict[str, Any] | None:
    matching = [
        manifest
        for manifest in _snapshot_manifests(object_store)
        if manifest.get("object_key") == object_key
    ]
    if not matching:
        return None
    return max(matching, key=lambda manifest: str(manifest["retrieved_at"]))


def _snapshot_manifests(object_store: ObjectStoreResource) -> list[dict[str, Any]]:
    keys = sorted(
        key
        for key in object_store.list_keys(
            tables.S3_MANIFEST_PREFIX,
            bucket=tables.S3_BUCKET,
        )
        if key.endswith(".json")
    )
    return [
        json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        for key in keys
    ]


def _download_snapshot(
    *,
    target_path: Path,
    session: requests.Session,
    expected_md5: str,
    minimum_size_bytes: int,
    download_attempts: int,
    timeout_seconds: int,
) -> dict[str, str | int]:
    last_error: Exception | None = None
    for attempt in range(1, download_attempts + 1):
        try:
            return _stream_snapshot(
                target_path=target_path,
                session=session,
                expected_md5=expected_md5,
                minimum_size_bytes=minimum_size_bytes,
                timeout_seconds=timeout_seconds,
            )
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt < download_attempts:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise last_error


def _upload_snapshot(
    *,
    object_store: ObjectStoreResource,
    object_key: str,
    source_path: Path,
    upload_attempts: int,
) -> None:
    """Upload one part at a time and retain the verified file across retries."""
    if upload_attempts < 1:
        raise ValueError("upload_attempts must be at least 1")

    last_error: Exception | None = None
    for attempt in range(1, upload_attempts + 1):
        try:
            object_store.upload_file(
                object_key,
                source_path,
                bucket=tables.S3_BUCKET,
                transfer_config=SNAPSHOT_TRANSFER_CONFIG,
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt < upload_attempts:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise last_error


def _stream_snapshot(
    *,
    target_path: Path,
    session: requests.Session,
    expected_md5: str,
    minimum_size_bytes: int,
    timeout_seconds: int,
) -> dict[str, str | int]:
    with session.get(
        tables.SOURCE_URL,
        timeout=timeout_seconds,
        stream=True,
    ) as response:
        response.raise_for_status()
        source_md5 = hashlib.md5(usedforsecurity=False)
        source_sha256 = hashlib.sha256()
        size_bytes = 0
        with target_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                handle.write(chunk)
                source_md5.update(chunk)
                source_sha256.update(chunk)
                size_bytes += len(chunk)

        expected_length = response.headers.get("Content-Length")
        if (
            expected_length is not None
            and expected_length.isdigit()
            and size_bytes != int(expected_length)
        ):
            raise requests.exceptions.ChunkedEncodingError(
                f"incomplete Sweden OSM download: {size_bytes}/{expected_length} bytes"
            )
        if size_bytes < minimum_size_bytes:
            raise ValueError(
                f"Sweden OSM snapshot is materially truncated: {size_bytes} bytes, "
                f"expected at least {minimum_size_bytes}"
            )
        actual_md5 = source_md5.hexdigest()
        if actual_md5 != expected_md5:
            raise ValueError(
                f"Sweden OSM checksum mismatch: expected {expected_md5}, "
                f"received {actual_md5}"
            )
        return {
            "sha256": source_sha256.hexdigest(),
            "size_bytes": size_bytes,
            "resolved_url": str(response.url),
            "content_type": response.headers.get("Content-Type", ""),
            "last_modified": response.headers.get("Last-Modified", ""),
            "etag": response.headers.get("ETag", ""),
        }
