import json
import tarfile
import tempfile
import time
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

DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_DOWNLOAD_ATTEMPTS = 4
DEFAULT_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
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


def sync_latest_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
) -> StoredSnapshot:
    """Download and preserve the newest dated JobTech Links archive."""
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or jobtech_links_http_session()
    try:
        catalog_response = http_session.get(
            tables.CATALOG_URL,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        catalog_response.raise_for_status()
        archives = discover_snapshot_archives(
            catalog_response.text,
            catalog_url=tables.CATALOG_URL,
        )
        if not archives:
            raise ValueError(
                f"JobTech Links catalog {tables.CATALOG_URL} contains no dated archives"
            )

        latest_date = archives[-1].snapshot_date
        latest_archives = tuple(
            archive for archive in archives if archive.snapshot_date == latest_date
        )
        if len(latest_archives) != 1:
            raise ValueError(
                "JobTech Links catalog contains multiple archives for latest date "
                f"{latest_date.isoformat()}"
            )
        latest = latest_archives[0]

        with tempfile.TemporaryDirectory(prefix="sweden_jobtech_links_") as temp:
            archive_path = Path(temp) / latest.source_file
            download = _download_to_path(
                session=http_session,
                source_url=latest.source_url,
                target_path=archive_path,
            )
            raw_member_path, raw_member_size_bytes = _validate_archive(archive_path)
            archive_sha256 = str(download["sha256"])
            snapshot_uid = sha256(
                f"{latest.snapshot_date.isoformat()}\0{archive_sha256}".encode()
            ).hexdigest()
            object_prefix = (
                f"{tables.SNAPSHOT_PREFIX}/"
                f"snapshot_date={latest.snapshot_date.isoformat()}/"
                f"sha256={archive_sha256}"
            )
            archive_object_key = f"{object_prefix}/{latest.source_file}"
            metadata_object_key = f"{object_prefix}/metadata.json"
            downloaded = not object_store.exists(
                archive_object_key,
                bucket=tables.S3_BUCKET,
            )
            if downloaded:
                object_store.upload_file(
                    archive_object_key,
                    archive_path,
                    bucket=tables.S3_BUCKET,
                )

        metadata = {
            "source_slug": tables.SOURCE_SLUG,
            "snapshot_uid": snapshot_uid,
            "snapshot_date": latest.snapshot_date.isoformat(),
            "catalog_url": tables.CATALOG_URL,
            "source_url": latest.source_url,
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

        return StoredSnapshot(
            snapshot_uid=snapshot_uid,
            snapshot_date=latest.snapshot_date,
            source_url=latest.source_url,
            archive_object_key=archive_object_key,
            metadata_object_key=metadata_object_key,
            archive_sha256=archive_sha256,
            archive_size_bytes=int(download["size_bytes"]),
            raw_member_path=raw_member_path,
            raw_member_size_bytes=raw_member_size_bytes,
            source_etag=str(download["etag"]),
            source_last_modified=str(download["last_modified"]),
            downloaded=downloaded,
        )
    finally:
        if owns_session:
            http_session.close()


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
