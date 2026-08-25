import json
import tempfile
import time
from collections.abc import Callable, Mapping
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
APR_INTERMEDIATE_CA_PATH = (
    Path(__file__).with_name("certificates")
    / "ssl2buy-emea-rsa-domain-validation-secure-server-ca.pem"
)


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
    # APR serves only its leaf certificate. Pin its issuer intermediate so
    # requests can build the chain while keeping hostname/TLS verification on.
    client.session.verify = str(APR_INTERMEDIATE_CA_PATH)
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


def latest_snapshot_manifest(
    object_store: ObjectStoreResource,
) -> dict[str, object]:
    """Return the newest valid APR snapshot, not merely the newest S3 key."""
    manifest_keys = object_store.list_keys(
        f"{tables.S3_MANIFEST_PREFIX}/",
        bucket=tables.S3_BUCKET,
    )
    if not manifest_keys:
        raise ValueError(
            "no Serbia APR companies snapshot manifest exists; materialize "
            "serbia_apr_companies_raw_snapshot_s3 first"
        )

    manifests: list[dict[str, object]] = []
    for manifest_key in manifest_keys:
        try:
            raw_manifest = json.loads(
                object_store.read_bytes(
                    manifest_key,
                    bucket=tables.S3_BUCKET,
                )
            )
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(
                f"Serbia APR companies manifest is invalid JSON: {manifest_key}"
            ) from exc
        if not isinstance(raw_manifest, dict):
            raise ValueError(
                f"Serbia APR companies manifest is not an object: {manifest_key}"
            )
        manifests.append(validate_snapshot_manifest(raw_manifest))

    return max(
        manifests,
        key=lambda manifest: (
            date.fromisoformat(str(manifest["snapshot_date"])),
            _manifest_retrieved_at(manifest),
        ),
    )


def validate_snapshot_manifest(
    manifest: Mapping[str, object],
) -> dict[str, object]:
    required_fields = {
        "bucket",
        "content_type",
        "downloaded",
        "object_key",
        "record_count",
        "retrieved_at",
        "sha256",
        "size_bytes",
        "snapshot_date",
        "source_license",
        "source_run_id",
        "source_slug",
        "source_url",
    }
    missing_fields = sorted(required_fields - manifest.keys())
    if missing_fields:
        raise ValueError(
            "Serbia APR companies manifest is missing fields: "
            + ", ".join(missing_fields)
        )

    expected_values = {
        "bucket": tables.S3_BUCKET,
        "source_license": tables.SOURCE_LICENSE,
        "source_slug": tables.SOURCE_SLUG,
        "source_url": tables.SOURCE_URL,
    }
    for field_name, expected_value in expected_values.items():
        if manifest[field_name] != expected_value:
            raise ValueError(
                f"Serbia APR companies manifest has unexpected {field_name}: "
                f"{manifest[field_name]!r}"
            )

    object_key = manifest["object_key"]
    if not isinstance(object_key, str) or not object_key.startswith(
        f"{tables.S3_RAW_PREFIX}/"
    ):
        raise ValueError("Serbia APR companies manifest has an invalid raw object key")
    source_run_id = manifest["source_run_id"]
    if not isinstance(source_run_id, str) or source_run_id.strip() == "":
        raise ValueError("Serbia APR companies manifest has an empty source_run_id")

    payload_sha256 = manifest["sha256"]
    if (
        not isinstance(payload_sha256, str)
        or len(payload_sha256) != 64
        or any(character not in "0123456789abcdef" for character in payload_sha256)
    ):
        raise ValueError("Serbia APR companies manifest has an invalid SHA-256")
    for field_name in ("record_count", "size_bytes"):
        value = manifest[field_name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(
                f"Serbia APR companies manifest has an invalid {field_name}"
            )

    try:
        date.fromisoformat(str(manifest["snapshot_date"]))
    except ValueError as exc:
        raise ValueError(
            "Serbia APR companies manifest has an invalid snapshot_date"
        ) from exc
    _manifest_retrieved_at(manifest)
    return dict(manifest)


def _manifest_retrieved_at(manifest: Mapping[str, object]) -> datetime:
    try:
        retrieved_at = datetime.fromisoformat(
            str(manifest["retrieved_at"]).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError(
            "Serbia APR companies manifest has an invalid retrieved_at"
        ) from exc
    if retrieved_at.tzinfo is None:
        raise ValueError(
            "Serbia APR companies manifest retrieved_at must include a timezone"
        )
    return retrieved_at.astimezone(UTC)


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
