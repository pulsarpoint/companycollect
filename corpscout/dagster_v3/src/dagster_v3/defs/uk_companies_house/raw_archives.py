import json
import re
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.uk_companies_house import resources, tables

UK_COMPANIES_HOUSE_RAW_BUCKET = "source-uk-companies-house"
REGISTER_KIND = "register"
ACCOUNTS_KIND = "accounts"
_KINDS = {REGISTER_KIND, ACCOUNTS_KIND}
_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class StoredArchive:
    kind: str
    published_date: str
    source_url: str
    filename: str
    object_key: str
    metadata_key: str
    size_bytes: int
    sha256: str
    synced_at: str


@dataclass(frozen=True)
class ArchiveSyncResult:
    archive: StoredArchive
    reused_existing: bool

    def metadata(self) -> dict[str, object]:
        return {
            "s3_bucket": UK_COMPANIES_HOUSE_RAW_BUCKET,
            "s3_key": self.archive.object_key,
            "metadata_key": self.archive.metadata_key,
            "published_date": self.archive.published_date,
            "source_url": self.archive.source_url,
            "size_bytes": self.archive.size_bytes,
            "sha256": self.archive.sha256,
            "reused_existing": self.reused_existing,
        }


@dataclass(frozen=True)
class ArchiveBatchSyncResult:
    archives: tuple[ArchiveSyncResult, ...]
    published_count: int
    stored_count: int

    def metadata(self) -> dict[str, object]:
        downloaded = sum(not item.reused_existing for item in self.archives)
        return {
            "published_archive_count": self.published_count,
            "stored_archive_count": self.stored_count,
            "selected_archive_count": len(self.archives),
            "downloaded_archive_count": downloaded,
            "reused_archive_count": len(self.archives) - downloaded,
            "selected_archive_dates": [
                item.archive.published_date for item in self.archives
            ],
            "s3_bucket": UK_COMPANIES_HOUSE_RAW_BUCKET,
        }


def sync_register_archive(
    *,
    object_store: ObjectStoreResource,
    session: resources.HttpSession | None = None,
    synced_at: datetime | None = None,
    log: Callable[..., object] | None = None,
) -> ArchiveSyncResult:
    source_url = resources.resolve_basic_company_data_url(session=session)
    return sync_archive(
        object_store=object_store,
        kind=REGISTER_KIND,
        published_date=_published_date(source_url),
        source_url=source_url,
        session=session,
        synced_at=synced_at,
        log=log,
    )


def sync_accounts_archives(
    *,
    object_store: ObjectStoreResource,
    max_archives: int,
    session: resources.HttpSession | None = None,
    synced_at: datetime | None = None,
    log: Callable[..., object] | None = None,
) -> ArchiveBatchSyncResult:
    published = list_accounts_archives(session=session)
    object_store.ensure_bucket(UK_COMPANIES_HOUSE_RAW_BUCKET)
    stored = list_stored_archives(object_store, kind=ACCOUNTS_KIND)
    selected = select_accounts_archives_to_sync(
        published,
        stored_dates={archive.published_date for archive in stored},
        max_archives=max_archives,
    )
    results = tuple(
        sync_archive(
            object_store=object_store,
            kind=ACCOUNTS_KIND,
            published_date=published_date,
            source_url=source_url,
            session=session,
            synced_at=synced_at,
            log=log,
        )
        for published_date, source_url in selected
    )
    return ArchiveBatchSyncResult(
        archives=results,
        published_count=len(published),
        stored_count=len(stored) + sum(not item.reused_existing for item in results),
    )


def sync_archive(
    *,
    object_store: ObjectStoreResource,
    kind: str,
    published_date: str,
    source_url: str,
    session: resources.HttpSession | None = None,
    synced_at: datetime | None = None,
    timeout_seconds: int = resources.DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> ArchiveSyncResult:
    _validate_kind(kind)
    _validate_published_date(published_date)
    filename = Path(urlparse(source_url).path).name
    if not filename:
        raise ValueError(f"archive URL has no filename: {source_url}")

    with tempfile.TemporaryDirectory(prefix=f"uk_companies_house_{kind}_") as tmpdir:
        archive_path = Path(tmpdir) / filename
        resources._download_to_path(
            url=source_url,
            dest=archive_path,
            timeout_seconds=timeout_seconds,
            session=session,
            log=log if callable(log) else None,
        )
        digest = _sha256_file(archive_path)
        size_bytes = archive_path.stat().st_size
        object_key = archive_object_key(
            kind=kind,
            published_date=published_date,
            digest=digest,
            filename=filename,
        )
        metadata_key = archive_metadata_key(object_key)
        object_store.ensure_bucket(UK_COMPANIES_HOUSE_RAW_BUCKET)
        reused_existing = object_store.exists(
            object_key,
            bucket=UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
        if not reused_existing:
            object_store.upload_file(
                object_key,
                archive_path,
                bucket=UK_COMPANIES_HOUSE_RAW_BUCKET,
            )

    candidate = StoredArchive(
        kind=kind,
        published_date=published_date,
        source_url=source_url,
        filename=filename,
        object_key=object_key,
        metadata_key=metadata_key,
        size_bytes=size_bytes,
        sha256=digest,
        synced_at=(synced_at or datetime.now(UTC)).isoformat(),
    )
    metadata_exists = object_store.exists(
        metadata_key,
        bucket=UK_COMPANIES_HOUSE_RAW_BUCKET,
    )
    if metadata_exists:
        archive = _read_archive_metadata(object_store, metadata_key)
    else:
        archive = candidate
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(archive), sort_keys=True),
            bucket=UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
    if log is not None:
        log(
            "Stored UK Companies House archive: kind=%s date=%s key=%s reused=%s",
            kind,
            published_date,
            object_key,
            reused_existing,
        )
    return ArchiveSyncResult(archive=archive, reused_existing=reused_existing)


def list_accounts_archives(
    *,
    session: resources.HttpSession | None = None,
    index_url: str = tables.ACCOUNTS_INDEX_URL,
    base_url: str = tables.DOWNLOAD_BASE_URL,
    timeout_seconds: int = 60,
) -> list[tuple[str, str]]:
    from dlt.sources.helpers import requests as dlt_requests

    http_session = session or dlt_requests.Session()
    response = http_session.get(index_url, timeout=timeout_seconds)
    response.raise_for_status()
    pairs: dict[str, str] = {}
    for filename in re.findall(tables.ACCOUNTS_FILENAME_RE, response.text):
        pairs[_published_date(filename)] = base_url + filename
    return sorted(pairs.items())


def select_accounts_archives_to_sync(
    published: list[tuple[str, str]],
    *,
    stored_dates: set[str],
    max_archives: int,
) -> list[tuple[str, str]]:
    if max_archives <= 0:
        raise ValueError("max_archives must be greater than zero")
    if not published:
        return []
    if not stored_dates:
        return published[-1:]
    latest_stored = max(stored_dates)
    return [item for item in published if item[0] > latest_stored][:max_archives]


def list_stored_archives(
    object_store: ObjectStoreResource,
    *,
    kind: str,
) -> list[StoredArchive]:
    _validate_kind(kind)
    metadata_keys = [
        key
        for key in object_store.list_keys(
            f"raw/{kind}/",
            bucket=UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
        if key.endswith("/metadata.json")
    ]
    archives = [_read_archive_metadata(object_store, key) for key in metadata_keys]
    return sorted(
        archives,
        key=lambda archive: (
            archive.published_date,
            archive.synced_at,
            archive.sha256,
        ),
    )


def preferred_stored_archives(
    object_store: ObjectStoreResource,
    *,
    kind: str,
) -> list[StoredArchive]:
    by_date: dict[str, StoredArchive] = {}
    for archive in list_stored_archives(object_store, kind=kind):
        by_date[archive.published_date] = archive
    return [by_date[published_date] for published_date in sorted(by_date)]


def latest_stored_archive(
    object_store: ObjectStoreResource,
    *,
    kind: str,
) -> StoredArchive:
    archives = preferred_stored_archives(object_store, kind=kind)
    if not archives:
        raise ValueError(
            f"No UK Companies House {kind} archives found in object storage; "
            "materialize the upstream S3 archive asset first"
        )
    return archives[-1]


def archive_object_key(
    *,
    kind: str,
    published_date: str,
    digest: str,
    filename: str,
) -> str:
    _validate_kind(kind)
    _validate_published_date(published_date)
    return f"raw/{kind}/published_date={published_date}/sha256={digest}/{filename}"


def archive_metadata_key(object_key: str) -> str:
    return str(PurePosixPath(object_key).parent / "metadata.json")


def _read_archive_metadata(
    object_store: ObjectStoreResource,
    metadata_key: str,
) -> StoredArchive:
    payload = json.loads(
        object_store.read_bytes(
            metadata_key,
            bucket=UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
    )
    return StoredArchive(**payload)


def _published_date(value: str) -> str:
    match = _DATE_RE.search(value)
    if match is None:
        raise ValueError(f"archive name has no YYYY-MM-DD date: {value}")
    return match.group(1)


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_kind(kind: str) -> None:
    if kind not in _KINDS:
        raise ValueError(f"unknown UK Companies House archive kind: {kind}")


def _validate_published_date(published_date: str) -> None:
    if _DATE_RE.fullmatch(published_date) is None:
        raise ValueError(f"invalid published_date: {published_date}")
