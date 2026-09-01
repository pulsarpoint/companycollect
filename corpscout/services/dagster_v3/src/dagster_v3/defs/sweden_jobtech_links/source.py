import json
import tarfile
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
from re import compile as compile_pattern
from urllib.parse import urljoin, urlparse

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_jobtech_links import tables
from dagster_v3.defs.sweden_jobtech_links.partitions import (
    PartitionKind,
    archive_window,
)

DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_DOWNLOAD_ATTEMPTS = 4
DEFAULT_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
PROGRESS_LOG_INTERVAL = 25
USER_AGENT = "Corpscout/1.0 (Sweden JobTech Links snapshot ingestion)"

_ARCHIVE_FILE_PATTERN = compile_pattern(r"^(\d{4}-\d{2}-\d{2})\.tar\.gz$")


@dataclass(frozen=True)
class SnapshotArchive:
    snapshot_date: date
    source_url: str
    source_file: str


@dataclass(frozen=True)
class StoredSnapshot:
    snapshot_uid: str
    snapshot_date: date
    source_url: str
    archive_object_key: str
    metadata_object_key: str
    archive_sha256: str
    archive_size_bytes: int
    raw_member_path: str
    raw_member_size_bytes: int
    source_etag: str
    source_last_modified: str
    downloaded: bool


@dataclass(frozen=True)
class StoredSnapshotPartition:
    partition_key: str
    manifest_key: str
    snapshots: tuple[StoredSnapshot, ...]
    skipped_existing: bool = False

    @property
    def selected_count(self) -> int:
        return len(self.snapshots)

    @property
    def downloaded_count(self) -> int:
        return sum(snapshot.downloaded for snapshot in self.snapshots)

    @property
    def reused_count(self) -> int:
        return self.selected_count - self.downloaded_count

    @property
    def total_archive_size_bytes(self) -> int:
        return sum(snapshot.archive_size_bytes for snapshot in self.snapshots)

    @property
    def total_raw_member_size_bytes(self) -> int:
        return sum(snapshot.raw_member_size_bytes for snapshot in self.snapshots)


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.casefold() != "a":
            return
        for name, value in attrs:
            if name.casefold() == "href" and value is not None:
                self.hrefs.append(value)


def discover_snapshot_archives(
    html: str,
    *,
    catalog_url: str,
) -> tuple[SnapshotArchive, ...]:
    """Return valid dated JobTech Links archives in chronological order."""
    parser = _LinkParser()
    parser.feed(html)
    snapshots: set[SnapshotArchive] = set()
    for href in parser.hrefs:
        source_url = urljoin(catalog_url, href)
        source_file = Path(urlparse(source_url).path).name
        match = _ARCHIVE_FILE_PATTERN.fullmatch(source_file)
        if match is None:
            continue
        try:
            snapshot_date = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        snapshots.add(
            SnapshotArchive(
                snapshot_date=snapshot_date,
                source_url=source_url,
                source_file=source_file,
            )
        )
    return tuple(
        sorted(
            snapshots,
            key=lambda snapshot: (snapshot.snapshot_date, snapshot.source_url),
        )
    )


def jobtech_links_http_session() -> requests.Session:
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


def fetch_snapshot_catalog(
    session: requests.Session | None = None,
) -> tuple[SnapshotArchive, ...]:
    owns_session = session is None
    http_session = session or jobtech_links_http_session()
    try:
        return _fetch_snapshot_catalog(http_session)
    finally:
        if owns_session:
            http_session.close()


def sync_snapshot_partition(
    *,
    object_store: ObjectStoreResource,
    partition_kind: PartitionKind,
    partition_key: str,
    run_id: str,
    retrieved_at: datetime,
    refresh_existing: bool,
    session: requests.Session | None = None,
    log: Callable[[str], None] | None = None,
) -> StoredSnapshotPartition:
    """Preserve every available source archive in one fixed partition window."""
    window = archive_window(partition_kind, partition_key)
    object_store.ensure_bucket(tables.S3_BUCKET)
    if window.kind == "month" and retrieved_at.astimezone(UTC).date() >= (
        window.end_exclusive
    ):
        completed_partition = _completed_month_partition(
            object_store=object_store,
            partition_key=partition_key,
        )
        if completed_partition is not None:
            (log or (lambda _message: None))(
                f"JobTech Links month {partition_key} already has a complete S3 "
                f"manifest at {completed_partition.manifest_key}; skipping retry"
            )
            return completed_partition

    owns_session = session is None
    http_session = session or jobtech_links_http_session()
    logger = log or (lambda _message: None)
    try:
        catalog_archives = _fetch_snapshot_catalog(http_session)
        selected_archives = tuple(
            archive
            for archive in catalog_archives
            if window.start <= archive.snapshot_date < window.end_exclusive
        )
        if not selected_archives:
            raise ValueError(
                f"JobTech Links partition {partition_key!r} contains no source archives"
            )

        snapshots: list[StoredSnapshot] = []
        for index, archive in enumerate(selected_archives, start=1):
            stored = (
                None
                if refresh_existing
                else _stored_snapshot_for_archive(
                    object_store=object_store,
                    archive=archive,
                )
            )
            if stored is None:
                stored = _store_snapshot_archive(
                    object_store=object_store,
                    archive=archive,
                    run_id=run_id,
                    retrieved_at=retrieved_at,
                    session=http_session,
                )
            snapshots.append(stored)
            if (
                index == 1
                or index % PROGRESS_LOG_INTERVAL == 0
                or index == len(selected_archives)
            ):
                logger(
                    f"JobTech Links partition {partition_key}: processed "
                    f"{index}/{len(selected_archives)} archives "
                    f"({archive.snapshot_date.isoformat()})"
                )
    finally:
        if owns_session:
            http_session.close()

    manifest_key = _partition_manifest_key(
        partition_kind=partition_kind,
        partition_key=partition_key,
        run_id=run_id,
        retrieved_at=retrieved_at,
    )
    manifest = {
        "source_slug": tables.SOURCE_SLUG,
        "source_run_id": run_id,
        "catalog_url": tables.CATALOG_URL,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "partition_kind": partition_kind,
        "partition_key": partition_key,
        "window_start": window.start.isoformat(),
        "window_end_exclusive": window.end_exclusive.isoformat(),
        "window_complete": (
            retrieved_at.astimezone(UTC).date() >= window.end_exclusive
        ),
        "archive_count": len(snapshots),
        "archives": [
            {
                "snapshot_uid": snapshot.snapshot_uid,
                "snapshot_date": snapshot.snapshot_date.isoformat(),
                "source_url": snapshot.source_url,
                "archive_object_key": snapshot.archive_object_key,
                "metadata_object_key": snapshot.metadata_object_key,
                "archive_sha256": snapshot.archive_sha256,
                "archive_size_bytes": snapshot.archive_size_bytes,
                "raw_member_path": snapshot.raw_member_path,
                "raw_member_size_bytes": snapshot.raw_member_size_bytes,
                "source_etag": snapshot.source_etag,
                "source_last_modified": snapshot.source_last_modified,
                "downloaded": snapshot.downloaded,
            }
            for snapshot in snapshots
        ],
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    return StoredSnapshotPartition(
        partition_key=partition_key,
        manifest_key=manifest_key,
        snapshots=tuple(snapshots),
    )


def _completed_month_partition(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
) -> StoredSnapshotPartition | None:
    manifest_prefix = f"{tables.MANIFEST_PREFIX}/month={partition_key}/"
    manifest_keys = reversed(
        object_store.list_keys(manifest_prefix, bucket=tables.S3_BUCKET)
    )
    for manifest_key in manifest_keys:
        if not manifest_key.endswith(".json"):
            continue
        try:
            manifest = json.loads(
                object_store.read_bytes(manifest_key, bucket=tables.S3_BUCKET)
            )
            if (
                manifest.get("partition_kind") != "month"
                or manifest.get("partition_key") != partition_key
                or manifest.get("window_complete") is not True
            ):
                continue
            snapshots = tuple(
                _stored_snapshot_from_metadata(item, downloaded=False)
                for item in manifest["archives"]
            )
            if not snapshots or len(snapshots) != int(manifest["archive_count"]):
                continue
            if not all(
                object_store.exists(
                    snapshot.archive_object_key,
                    bucket=tables.S3_BUCKET,
                )
                and object_store.exists(
                    snapshot.metadata_object_key,
                    bucket=tables.S3_BUCKET,
                )
                for snapshot in snapshots
            ):
                continue
        except json.JSONDecodeError, KeyError, TypeError, ValueError:
            continue
        return StoredSnapshotPartition(
            partition_key=partition_key,
            manifest_key=manifest_key,
            snapshots=snapshots,
            skipped_existing=True,
        )
    return None


def _fetch_snapshot_catalog(session: requests.Session) -> tuple[SnapshotArchive, ...]:
    response = session.get(
        tables.CATALOG_URL,
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    archives = discover_snapshot_archives(
        response.text,
        catalog_url=tables.CATALOG_URL,
    )
    if not archives:
        raise ValueError(
            f"JobTech Links catalog {tables.CATALOG_URL} contains no dated archives"
        )
    return archives


def _store_snapshot_archive(
    *,
    object_store: ObjectStoreResource,
    archive: SnapshotArchive,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session,
) -> StoredSnapshot:
    with tempfile.TemporaryDirectory(prefix="sweden_jobtech_links_") as temp:
        archive_path = Path(temp) / archive.source_file
        download = _download_to_path(
            session=session,
            source_url=archive.source_url,
            target_path=archive_path,
        )
        raw_member_path, raw_member_size_bytes = _validate_archive(archive_path)
        archive_sha256 = str(download["sha256"])
        snapshot_uid = sha256(
            f"{archive.snapshot_date.isoformat()}\0{archive_sha256}".encode()
        ).hexdigest()
        object_prefix = (
            f"{tables.SNAPSHOT_PREFIX}/"
            f"snapshot_date={archive.snapshot_date.isoformat()}/"
            f"sha256={archive_sha256}"
        )
        archive_object_key = f"{object_prefix}/{archive.source_file}"
        metadata_object_key = f"{object_prefix}/metadata.json"
        if not object_store.exists(archive_object_key, bucket=tables.S3_BUCKET):
            object_store.upload_file(
                archive_object_key,
                archive_path,
                bucket=tables.S3_BUCKET,
            )

    metadata = {
        "source_slug": tables.SOURCE_SLUG,
        "snapshot_uid": snapshot_uid,
        "snapshot_date": archive.snapshot_date.isoformat(),
        "catalog_url": tables.CATALOG_URL,
        "source_url": archive.source_url,
        "archive_object_key": archive_object_key,
        "metadata_object_key": metadata_object_key,
        "archive_sha256": archive_sha256,
        "archive_size_bytes": int(download["size_bytes"]),
        "raw_member_path": raw_member_path,
        "raw_member_size_bytes": raw_member_size_bytes,
        "source_etag": str(download["etag"]),
        "source_last_modified": str(download["last_modified"]),
        "first_retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "first_source_run_id": run_id,
    }
    if not object_store.exists(metadata_object_key, bucket=tables.S3_BUCKET):
        object_store.write_json(
            metadata_object_key,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            bucket=tables.S3_BUCKET,
        )
    return _stored_snapshot_from_metadata(metadata, downloaded=True)


def _stored_snapshot_for_archive(
    *,
    object_store: ObjectStoreResource,
    archive: SnapshotArchive,
) -> StoredSnapshot | None:
    prefix = (
        f"{tables.SNAPSHOT_PREFIX}/snapshot_date={archive.snapshot_date.isoformat()}/"
    )
    candidates: list[dict[str, object]] = []
    for key in object_store.list_keys(prefix, bucket=tables.S3_BUCKET):
        if not key.endswith("/metadata.json"):
            continue
        metadata = json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        if metadata.get("source_url") != archive.source_url:
            continue
        archive_object_key = str(metadata["archive_object_key"])
        if object_store.exists(archive_object_key, bucket=tables.S3_BUCKET):
            candidates.append(metadata)
    if not candidates:
        return None
    latest = max(
        candidates,
        key=lambda metadata: str(metadata["first_retrieved_at"]),
    )
    return _stored_snapshot_from_metadata(latest, downloaded=False)


def _stored_snapshot_from_metadata(
    metadata: dict[str, object],
    *,
    downloaded: bool,
) -> StoredSnapshot:
    return StoredSnapshot(
        snapshot_uid=str(metadata["snapshot_uid"]),
        snapshot_date=date.fromisoformat(str(metadata["snapshot_date"])),
        source_url=str(metadata["source_url"]),
        archive_object_key=str(metadata["archive_object_key"]),
        metadata_object_key=str(metadata["metadata_object_key"]),
        archive_sha256=str(metadata["archive_sha256"]),
        archive_size_bytes=int(metadata["archive_size_bytes"]),
        raw_member_path=str(metadata["raw_member_path"]),
        raw_member_size_bytes=int(metadata["raw_member_size_bytes"]),
        source_etag=str(metadata["source_etag"]),
        source_last_modified=str(metadata["source_last_modified"]),
        downloaded=downloaded,
    )


def _download_to_path(
    *,
    session: requests.Session,
    source_url: str,
    target_path: Path,
) -> dict[str, str | int]:
    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_DOWNLOAD_ATTEMPTS + 1):
        try:
            response = session.get(
                source_url,
                timeout=DEFAULT_TIMEOUT_SECONDS,
                stream=True,
                headers={"Accept": "application/gzip"},
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
                    "Incomplete JobTech Links download: "
                    f"{size_bytes}/{expected_length} bytes"
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


def _validate_archive(archive_path: Path) -> tuple[str, int]:
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            members = [
                member
                for member in archive.getmembers()
                if member.isfile() and PurePosixPath(member.name).name == "output.json"
            ]
    except (tarfile.TarError, OSError) as exc:
        raise ValueError(
            f"JobTech Links archive {archive_path.name} is not a valid tar.gz"
        ) from exc

    if len(members) != 1:
        raise ValueError(
            f"Expected exactly one output.json member in {archive_path.name}, "
            f"found {len(members)}"
        )
    member = members[0]
    member_path = PurePosixPath(member.name)
    if member_path.is_absolute() or ".." in member_path.parts:
        raise ValueError(f"Unsafe JobTech Links archive member path: {member.name!r}")
    if member.size <= 0:
        raise ValueError(f"JobTech Links archive member {member.name!r} is empty")
    return member.name, member.size


def _partition_manifest_key(
    *,
    partition_kind: PartitionKind,
    partition_key: str,
    run_id: str,
    retrieved_at: datetime,
) -> str:
    window = archive_window(partition_kind, partition_key)
    timestamp = retrieved_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    return (
        f"{tables.MANIFEST_PREFIX}/{window.kind}={window.value}/"
        f"retrieved_at={timestamp}/run_id={run_id}.json"
    )
