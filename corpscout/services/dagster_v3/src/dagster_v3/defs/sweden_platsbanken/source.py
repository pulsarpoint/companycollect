import json
import shutil
import tempfile
import time
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_platsbanken import tables

DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_DOWNLOAD_ATTEMPTS = 4
DEFAULT_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
EVENT_CURSOR_OVERLAP = timedelta(minutes=5)
EVENT_SOURCE_LAG = timedelta(minutes=2)
USER_AGENT = "Corpscout/1.0 (Sweden Platsbanken history ingestion)"


@dataclass(frozen=True)
class JobTechObject:
    object_key: str
    manifest_key: str
    sha256: str
    size_bytes: int
    record_count: int
    downloaded: bool


def discover_historical_archive_urls(
    html: str,
    *,
    catalog_url: str,
) -> tuple[str, ...]:
    """Return the complete ZIP archives linked by JobTech's catalog page."""
    marker = 'href="'
    urls: set[str] = set()
    for fragment in html.split(marker)[1:]:
        href = fragment.split('"', maxsplit=1)[0]
        if "/historiska/berikade/kompletta/" not in href:
            continue
        if not href.endswith("_jsonl.zip"):
            continue
        urls.add(urljoin(catalog_url, href))
    return tuple(sorted(urls))


def count_jobstream_jsonl_records(source_path: Path) -> int:
    """Validate a JobStream JSONL response and return its non-empty row count."""
    row_count = 0
    with source_path.open(encoding="utf-8") as source:
        for line in source:
            if line.strip() == "":
                continue
            json.loads(line)
            row_count += 1
    return row_count


def jobtech_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
        request_backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
        request_max_retry_delay=120.0,
        respect_retry_after_header=True,
    )
    client.session.headers.update(
        {
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        }
    )
    return client.session


def sync_historical_archives(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    refresh_existing: bool,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Store every complete JobTech historical ZIP and a replay manifest."""
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or jobtech_http_session()
    try:
        response = http_session.get(
            tables.HISTORICAL_CATALOG_URL,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        archive_urls = discover_historical_archive_urls(
            response.text,
            catalog_url=tables.HISTORICAL_CATALOG_URL,
        )
        if not archive_urls:
            raise ValueError(
                "JobTech historical catalog contains no complete ZIP archives"
            )

        previous = _optional_latest_manifest(
            object_store,
            f"{tables.MANIFEST_PREFIX}/historical",
        )
        previous_by_url = {
            str(entry["source_url"]): entry for entry in previous.get("archives", [])
        }
        entries: list[dict[str, Any]] = []
        for archive_url in archive_urls:
            previous_entry = previous_by_url.get(archive_url)
            if (
                not refresh_existing
                and previous_entry is not None
                and object_store.exists(
                    str(previous_entry["object_key"]),
                    bucket=tables.S3_BUCKET,
                )
            ):
                entries.append({**previous_entry, "downloaded": False})
                continue

            entries.append(
                _sync_historical_archive(
                    object_store=object_store,
                    archive_url=archive_url,
                    session=http_session,
                )
            )
    finally:
        if owns_session:
            http_session.close()

    manifest_key = _manifest_key("historical", run_id, retrieved_at)
    manifest = {
        "source_slug": tables.SOURCE_SLUG,
        "source_run_id": run_id,
        "source_url": tables.HISTORICAL_CATALOG_URL,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "archives": entries,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    return {**manifest, "manifest_key": manifest_key}


def sync_jobstream_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
) -> JobTechObject:
    return _sync_jobstream_jsonl(
        object_store=object_store,
        run_id=run_id,
        retrieved_at=retrieved_at,
        source_url=tables.JOBSTREAM_SNAPSHOT_URL,
        manifest_kind="jobstream/snapshot",
        object_prefix=f"{tables.JOBSTREAM_PREFIX}/snapshot",
        allow_empty=False,
        extra_manifest={},
        session=session,
    )


def sync_jobstream_events(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    updated_after: datetime,
    updated_before: datetime,
    session: requests.Session | None = None,
) -> JobTechObject:
    if updated_after >= updated_before:
        raise ValueError("JobStream updated-after must be earlier than updated-before")
    query = urlencode(
        {
            "updated-after": _format_utc(updated_after),
            "updated-before": _format_utc(updated_before),
        }
    )
    return _sync_jobstream_jsonl(
        object_store=object_store,
        run_id=run_id,
        retrieved_at=retrieved_at,
        source_url=f"{tables.JOBSTREAM_STREAM_URL}?{query}",
        manifest_kind="jobstream/events",
        object_prefix=f"{tables.JOBSTREAM_PREFIX}/events",
        allow_empty=True,
        extra_manifest={
            "updated_after": _format_utc(updated_after),
            "updated_before": _format_utc(updated_before),
        },
        session=session,
    )


def resolve_jobstream_event_window(
    *,
    object_store: ObjectStoreResource,
    now: datetime,
    configured_after: str,
    configured_before: str,
) -> tuple[datetime, datetime]:
    """Resolve a replay-safe event window from config or durable manifests."""
    if configured_after.strip() != "":
        updated_after = parse_utc_datetime(configured_after)
    else:
        previous_events = _optional_latest_manifest(
            object_store,
            f"{tables.MANIFEST_PREFIX}/jobstream/events",
        )
        if "updated_before" in previous_events:
            updated_after = (
                parse_utc_datetime(str(previous_events["updated_before"]))
                - EVENT_CURSOR_OVERLAP
            )
        else:
            snapshot = latest_jobstream_snapshot_manifest(object_store)
            updated_after = (
                parse_utc_datetime(str(snapshot["retrieved_at"])) - EVENT_CURSOR_OVERLAP
            )

    updated_before = (
        parse_utc_datetime(configured_before)
        if configured_before.strip() != ""
        else now.astimezone(UTC) - EVENT_SOURCE_LAG
    )
    if updated_after >= updated_before:
        raise ValueError(
            "Resolved JobStream event window is empty: "
            f"{_format_utc(updated_after)} >= {_format_utc(updated_before)}"
        )
    return updated_after, updated_before


def latest_historical_manifest(
    object_store: ObjectStoreResource,
) -> dict[str, Any]:
    return _latest_manifest(
        object_store,
        f"{tables.MANIFEST_PREFIX}/historical",
        "historical archives",
    )


def latest_jobstream_snapshot_manifest(
    object_store: ObjectStoreResource,
) -> dict[str, Any]:
    return _latest_manifest(
        object_store,
        f"{tables.MANIFEST_PREFIX}/jobstream/snapshot",
        "JobStream snapshot",
    )


def latest_jobstream_event_manifest(
    object_store: ObjectStoreResource,
) -> dict[str, Any]:
    return _latest_manifest(
        object_store,
        f"{tables.MANIFEST_PREFIX}/jobstream/events",
        "JobStream event batch",
    )


def extract_single_jsonl_archive(archive_path: Path, target_path: Path) -> None:
    """Extract the single JSONL member without trusting archive paths."""
    with zipfile.ZipFile(archive_path) as archive:
        members = [
            member
            for member in archive.infolist()
            if not member.is_dir() and member.filename.lower().endswith(".jsonl")
        ]
        if len(members) != 1:
            raise ValueError(
                f"Expected one JSONL member in {archive_path.name}, found {len(members)}"
            )
        with archive.open(members[0]) as source, target_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=DOWNLOAD_CHUNK_BYTES)


def parse_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a UTC offset: {value}")
    return parsed.astimezone(UTC)


def _sync_historical_archive(
    *,
    object_store: ObjectStoreResource,
    archive_url: str,
    session: requests.Session,
) -> dict[str, Any]:
    source_file = Path(urlparse(archive_url).path).name
    with tempfile.TemporaryDirectory(prefix="sweden_platsbanken_historical_") as temp:
        target_path = Path(temp) / source_file
        metadata = _download_to_path(
            session=session,
            source_url=archive_url,
            target_path=target_path,
            accept="application/zip",
        )
        object_key = (
            f"{tables.HISTORICAL_PREFIX}/source_file={source_file}/"
            f"sha256={metadata['sha256']}/{source_file}"
        )
        downloaded = not object_store.exists(object_key, bucket=tables.S3_BUCKET)
        if downloaded:
            object_store.upload_file(
                object_key,
                target_path,
                bucket=tables.S3_BUCKET,
            )
    return {
        "source_url": archive_url,
        "source_file": source_file,
        "object_key": object_key,
        "sha256": metadata["sha256"],
        "size_bytes": metadata["size_bytes"],
        "etag": metadata["etag"],
        "last_modified": metadata["last_modified"],
        "downloaded": downloaded,
    }


def _sync_jobstream_jsonl(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    source_url: str,
    manifest_kind: str,
    object_prefix: str,
    allow_empty: bool,
    extra_manifest: dict[str, str],
    session: requests.Session | None,
) -> JobTechObject:
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or jobtech_http_session()
    try:
        with tempfile.TemporaryDirectory(
            prefix="sweden_platsbanken_jobstream_"
        ) as temp:
            raw_path = Path(temp) / "source.jsonl"
            _download_to_path(
                session=http_session,
                source_url=source_url,
                target_path=raw_path,
                accept="application/jsonl",
            )
            record_count = count_jobstream_jsonl_records(raw_path)
            if record_count == 0 and not allow_empty:
                raise ValueError(f"{source_url} returned zero JobStream records")
            digest = _sha256_file(raw_path)
            size_bytes = raw_path.stat().st_size
            object_key = (
                f"{object_prefix}/retrieved_date={retrieved_at.date().isoformat()}/"
                f"sha256={digest}/records.jsonl"
            )
            downloaded = not object_store.exists(object_key, bucket=tables.S3_BUCKET)
            if downloaded:
                object_store.upload_file(
                    object_key,
                    raw_path,
                    bucket=tables.S3_BUCKET,
                )
    finally:
        if owns_session:
            http_session.close()

    manifest_key = _manifest_key(manifest_kind, run_id, retrieved_at)
    manifest = {
        "source_slug": tables.SOURCE_SLUG,
        "source_run_id": run_id,
        "source_url": source_url,
        "object_key": object_key,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "sha256": digest,
        "size_bytes": size_bytes,
        "record_count": record_count,
        "downloaded": downloaded,
        **extra_manifest,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    return JobTechObject(
        object_key=object_key,
        manifest_key=manifest_key,
        sha256=digest,
        size_bytes=size_bytes,
        record_count=record_count,
        downloaded=downloaded,
    )


def _download_to_path(
    *,
    session: requests.Session,
    source_url: str,
    target_path: Path,
    accept: str,
) -> dict[str, str | int]:
    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_DOWNLOAD_ATTEMPTS + 1):
        try:
            response = session.get(
                source_url,
                timeout=DEFAULT_TIMEOUT_SECONDS,
                stream=True,
                headers={"Accept": accept},
            )
            response.raise_for_status()
            digest = sha256()
            size_bytes = 0
            with target_path.open("wb") as target:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    if not chunk:
                        continue
                    target.write(chunk)
                    digest.update(chunk)
                    size_bytes += len(chunk)
            expected_length = response.headers.get("Content-Length")
            if (
                expected_length is not None
                and expected_length.isdigit()
                and size_bytes != int(expected_length)
            ):
                raise requests.exceptions.ChunkedEncodingError(
                    f"Incomplete JobTech download: {size_bytes}/{expected_length} bytes"
                )
            return {
                "sha256": digest.hexdigest(),
                "size_bytes": size_bytes,
                "etag": response.headers.get("ETag", ""),
                "last_modified": response.headers.get("Last-Modified", ""),
            }
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt < DEFAULT_DOWNLOAD_ATTEMPTS:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise last_error


def _latest_manifest(
    object_store: ObjectStoreResource,
    prefix: str,
    description: str,
) -> dict[str, Any]:
    manifest = _optional_latest_manifest(object_store, prefix)
    if not manifest:
        raise ValueError(
            f"No {description} manifest under s3://{tables.S3_BUCKET}/{prefix}; "
            "materialize the raw asset first"
        )
    return manifest


def _optional_latest_manifest(
    object_store: ObjectStoreResource,
    prefix: str,
) -> dict[str, Any]:
    keys = sorted(
        key
        for key in object_store.list_keys(prefix, bucket=tables.S3_BUCKET)
        if key.endswith(".json")
    )
    if not keys:
        return {}
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        for key in keys
    ]
    return max(manifests, key=lambda manifest: str(manifest["retrieved_at"]))


def _manifest_key(kind: str, run_id: str, retrieved_at: datetime) -> str:
    timestamp = retrieved_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    return (
        f"{tables.MANIFEST_PREFIX}/{kind}/retrieved_at={timestamp}/run_id={run_id}.json"
    )


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(DOWNLOAD_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_utc(value: datetime) -> str:
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )
