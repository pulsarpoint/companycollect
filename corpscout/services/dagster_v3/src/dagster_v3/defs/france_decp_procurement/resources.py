import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.france_decp_procurement import tables

DEFAULT_TIMEOUT_SECONDS = 600
MINIMUM_SNAPSHOT_BYTES = 50_000_000
MINIMUM_DATA_ROWS = 500_000
USER_AGENT = "Corpscout/1.0 (France DECP open-data ingestion)"


@dataclass(frozen=True)
class DecpSnapshot:
    object_key: str
    manifest_key: str
    sha256: str
    size_bytes: int
    row_count: int
    source_run_id: str
    retrieved_at: str
    downloaded: bool


def decp_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=4,
        request_backoff_factor=5.0,
        respect_retry_after_header=True,
    )
    client.session.headers.update(
        {"Accept-Encoding": "identity", "User-Agent": USER_AGENT}
    )
    return client.session


def sync_decp_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
    minimum_size_bytes: int = MINIMUM_SNAPSHOT_BYTES,
    minimum_data_rows: int = MINIMUM_DATA_ROWS,
) -> DecpSnapshot:
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or decp_http_session()
    try:
        with tempfile.TemporaryDirectory(prefix="france_decp_") as temp_dir:
            target_path = Path(temp_dir) / "decp.csv"
            metadata = _download_snapshot(
                session=http_session,
                target_path=target_path,
                minimum_size_bytes=minimum_size_bytes,
                minimum_data_rows=minimum_data_rows,
            )
            object_key = (
                f"{tables.S3_RAW_PREFIX}/retrieved_date={retrieved_at.date().isoformat()}/"
                f"sha256={metadata['sha256']}/decp.csv"
            )
            downloaded = not object_store.exists(object_key, bucket=tables.S3_BUCKET)
            if downloaded:
                object_store.upload_file(
                    object_key, target_path, bucket=tables.S3_BUCKET
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
        "catalog_url": tables.CATALOG_URL,
        "licence": tables.SOURCE_LICENCE,
        "object_key": object_key,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "sha256": metadata["sha256"],
        "size_bytes": metadata["size_bytes"],
        "row_count": metadata["row_count"],
        "downloaded": downloaded,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    return DecpSnapshot(
        object_key=object_key,
        manifest_key=manifest_key,
        sha256=str(metadata["sha256"]),
        size_bytes=int(metadata["size_bytes"]),
        row_count=int(metadata["row_count"]),
        source_run_id=run_id,
        retrieved_at=str(manifest["retrieved_at"]),
        downloaded=downloaded,
    )


def latest_snapshot_manifest(object_store: ObjectStoreResource) -> dict[str, Any]:
    keys = sorted(
        key
        for key in object_store.list_keys(
            tables.S3_MANIFEST_PREFIX, bucket=tables.S3_BUCKET
        )
        if key.endswith(".json")
    )
    if not keys:
        raise ValueError(
            f"No DECP manifests under s3://{tables.S3_BUCKET}/"
            f"{tables.S3_MANIFEST_PREFIX}; materialize the raw snapshot first"
        )
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        for key in keys
    ]
    return max(manifests, key=lambda manifest: str(manifest["retrieved_at"]))


def _download_snapshot(
    *,
    session: requests.Session,
    target_path: Path,
    minimum_size_bytes: int,
    minimum_data_rows: int,
) -> dict[str, str | int]:
    response = session.get(
        tables.SOURCE_URL, timeout=DEFAULT_TIMEOUT_SECONDS, stream=True
    )
    response.raise_for_status()
    digest = sha256()
    size_bytes = 0
    newline_count = 0
    with target_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            handle.write(chunk)
            digest.update(chunk)
            size_bytes += len(chunk)
            newline_count += chunk.count(b"\n")
    row_count = max(newline_count - 1, 0)
    if size_bytes < minimum_size_bytes:
        raise ValueError(
            f"DECP snapshot is materially truncated: {size_bytes} bytes, "
            f"expected at least {minimum_size_bytes}"
        )
    if row_count < minimum_data_rows:
        raise ValueError(
            f"DECP snapshot has {row_count} data rows, expected at least "
            f"{minimum_data_rows}"
        )
    return {
        "sha256": digest.hexdigest(),
        "size_bytes": size_bytes,
        "row_count": row_count,
    }
