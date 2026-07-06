import json
import re
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import requests

from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_COMP_CGU_GROUP_NAME = "brazil_comp_cgu"
BRAZIL_CGU_RAW_BUCKET = "source-brazil-cgu"
BRAZIL_CGU_BASE_URL = "https://portaldatransparencia.gov.br"
SOURCE_SLUG = "brazil_cgu"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 600
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 3.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-brazil-cgu/0.1"

DATASETS = ("ceis", "cnep", "cepim", "leniency_agreements")
DATASET_PORTAL_SLUGS = {
    "ceis": "ceis",
    "cnep": "cnep",
    "cepim": "cepim",
    "leniency_agreements": "acordos-leniencia",
}
DATASET_PORTAL_ORIGINS = {
    "ceis": "CEIS",
    "cnep": "CNEP",
    "cepim": "CEPIM",
    "leniency_agreements": "AcordosLeniencia",
}

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


@dataclass(frozen=True)
class BrazilCguSourceFile:
    dataset: str
    portal_slug: str
    portal_origin: str
    snapshot_date: str
    url: str


@dataclass(frozen=True)
class BrazilCguArchiveSyncResult:
    dataset: str
    snapshot_date: str
    source_url: str
    archive_key: str
    metadata_key: str
    downloaded: bool
    reused_existing_archive: bool
    checked_existing_archive: bool
    source_hash_changed: bool
    previous_sha256: str | None
    size_bytes: int | None
    sha256: str | None
    content_type: str
    source_last_modified: str
    synced_at: str

    def metadata(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "snapshot_date": self.snapshot_date,
            "source_url": self.source_url,
            "s3_bucket": BRAZIL_CGU_RAW_BUCKET,
            "archive_key": self.archive_key,
            "metadata_key": self.metadata_key,
            "downloaded": self.downloaded,
            "reused_existing_archive": self.reused_existing_archive,
            "checked_existing_archive": self.checked_existing_archive,
            "source_hash_changed": self.source_hash_changed,
            "previous_sha256": self.previous_sha256,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "content_type": self.content_type,
            "source_last_modified": self.source_last_modified,
            "synced_at": self.synced_at,
        }


@dataclass(frozen=True)
class BrazilCguSyncResult:
    archives: tuple[BrazilCguArchiveSyncResult, ...]

    def metadata(self) -> dict[str, object]:
        return {
            "archive_count": len(self.archives),
            "downloaded_archive_count": sum(
                archive.downloaded for archive in self.archives
            ),
            "reused_existing_archive_count": sum(
                archive.reused_existing_archive for archive in self.archives
            ),
            "checked_existing_archive_count": sum(
                archive.checked_existing_archive for archive in self.archives
            ),
            "source_hash_changed_count": sum(
                archive.source_hash_changed for archive in self.archives
            ),
            "datasets": [archive.dataset for archive in self.archives],
            "snapshot_dates": {
                archive.dataset: archive.snapshot_date for archive in self.archives
            },
            "archive_keys": [archive.archive_key for archive in self.archives],
            "source_urls": [archive.source_url for archive in self.archives],
        }


def normalize_dataset(dataset: str) -> str:
    clean_value = dataset.strip().lower().replace("-", "_")
    if clean_value not in DATASETS:
        raise ValueError(f"Unsupported Brazil CGU dataset: {dataset}")
    return clean_value


def normalize_snapshot_date(snapshot_date: str) -> str:
    clean_value = snapshot_date.strip()
    parsed_date = date.fromisoformat(clean_value)
    return parsed_date.isoformat()


def cgu_archive_object_key(dataset: str, snapshot_date: str) -> str:
    normalized_dataset = normalize_dataset(dataset)
    normalized_date = normalize_snapshot_date(snapshot_date)
    return (
        f"brazil_cgu/sanctions/raw_archives/dataset={normalized_dataset}/"
        f"snapshot_date={normalized_date}/archive.zip"
    )


def cgu_metadata_object_key(dataset: str, snapshot_date: str) -> str:
    normalized_dataset = normalize_dataset(dataset)
    normalized_date = normalize_snapshot_date(snapshot_date)
    return (
        f"brazil_cgu/sanctions/raw_archives/dataset={normalized_dataset}/"
        f"snapshot_date={normalized_date}/metadata.json"
    )


def cgu_archive_prefix(dataset: str) -> str:
    return f"brazil_cgu/sanctions/raw_archives/dataset={normalize_dataset(dataset)}/"


def cgu_source_file(dataset: str, snapshot_date: str) -> BrazilCguSourceFile:
    normalized_dataset = normalize_dataset(dataset)
    normalized_date = normalize_snapshot_date(snapshot_date)
    compact_date = normalized_date.replace("-", "")
    portal_slug = DATASET_PORTAL_SLUGS[normalized_dataset]
    portal_origin = DATASET_PORTAL_ORIGINS[normalized_dataset]
    return BrazilCguSourceFile(
        dataset=normalized_dataset,
        portal_slug=portal_slug,
        portal_origin=portal_origin,
        snapshot_date=normalized_date,
        url=f"{BRAZIL_CGU_BASE_URL}/download-de-dados/{portal_slug}/{compact_date}",
    )


def cgu_source_files_from_pages(
    pages_by_dataset: Mapping[str, str],
) -> tuple[BrazilCguSourceFile, ...]:
    files: list[BrazilCguSourceFile] = []
    for dataset in DATASETS:
        page = pages_by_dataset[dataset]
        files.append(cgu_source_file(dataset, _snapshot_date_from_page(dataset, page)))
    return tuple(files)


def cgu_source_file_from_archive_key(archive_key: str) -> BrazilCguSourceFile:
    match = re.search(
        r"/dataset=([^/]+)/snapshot_date=(\d{4}-\d{2}-\d{2})/archive\.zip$",
        "/" + archive_key,
    )
    if match is None:
        raise ValueError(f"Invalid Brazil CGU archive key: {archive_key}")
    return cgu_source_file(match.group(1), match.group(2))


class BrazilCguResource(dg.ConfigurableResource):
    """Downloads Portal da Transparencia sanctions ZIP archives into object storage."""

    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def source_page_url(self, dataset: str) -> str:
        return (
            f"{BRAZIL_CGU_BASE_URL}/download-de-dados/"
            f"{DATASET_PORTAL_SLUGS[normalize_dataset(dataset)]}"
        )

    def sync_latest_archives(
        self,
        *,
        object_store: ObjectStoreResource,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> BrazilCguSyncResult:
        object_store.ensure_bucket(BRAZIL_CGU_RAW_BUCKET)
        session = session or self._session()
        pages = {
            dataset: self._get_page(dataset=dataset, session=session)
            for dataset in DATASETS
        }
        source_files = cgu_source_files_from_pages(pages)
        archives = tuple(
            self._sync_source_archive(
                source_file=source_file,
                object_store=object_store,
                session=session,
                log_info=log_info,
            )
            for source_file in source_files
        )
        return BrazilCguSyncResult(archives=archives)

    def _get_page(self, *, dataset: str, session: Any) -> str:
        response = session.get(
            self.source_page_url(dataset),
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()
        return str(response.text)

    def _sync_source_archive(
        self,
        *,
        source_file: BrazilCguSourceFile,
        object_store: ObjectStoreResource,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> BrazilCguArchiveSyncResult:
        archive_key = cgu_archive_object_key(
            source_file.dataset, source_file.snapshot_date
        )
        metadata_key = cgu_metadata_object_key(
            source_file.dataset, source_file.snapshot_date
        )
        synced_at = datetime.now(UTC).isoformat()
        archive_exists = object_store.exists(archive_key, bucket=BRAZIL_CGU_RAW_BUCKET)
        stored_metadata = (
            _read_stored_metadata(object_store, metadata_key) if archive_exists else {}
        )
        previous_digest = _metadata_str(stored_metadata, "sha256") or None

        with tempfile.TemporaryDirectory(prefix="brazil_comp_cgu_") as tmpdir:
            target_path = Path(tmpdir) / f"{source_file.dataset}.zip"
            if log_info is not None:
                log_info(
                    "Downloading Brazil CGU archive for hash check: "
                    "dataset=%s snapshot_date=%s url=%s existing=%s",
                    source_file.dataset,
                    source_file.snapshot_date,
                    source_file.url,
                    archive_exists,
                )
            size_bytes, digest, content_type, source_last_modified = (
                self._download_to_path(
                    url=source_file.url,
                    target_path=target_path,
                    session=session,
                    log_info=log_info,
                )
            )
            if archive_exists and previous_digest == digest:
                if log_info is not None:
                    log_info(
                        "Brazil CGU archive unchanged after hash check: "
                        "bucket=%s key=%s sha256=%s",
                        BRAZIL_CGU_RAW_BUCKET,
                        archive_key,
                        digest,
                    )
                result = BrazilCguArchiveSyncResult(
                    dataset=source_file.dataset,
                    snapshot_date=source_file.snapshot_date,
                    source_url=source_file.url,
                    archive_key=archive_key,
                    metadata_key=metadata_key,
                    downloaded=False,
                    reused_existing_archive=True,
                    checked_existing_archive=True,
                    source_hash_changed=False,
                    previous_sha256=previous_digest,
                    size_bytes=size_bytes,
                    sha256=digest,
                    content_type=content_type,
                    source_last_modified=source_last_modified,
                    synced_at=synced_at,
                )
                object_store.write_json(
                    metadata_key,
                    json.dumps(asdict(result), indent=2, sort_keys=True),
                    bucket=BRAZIL_CGU_RAW_BUCKET,
                )
                return result

            object_store.upload_file(
                archive_key,
                target_path,
                bucket=BRAZIL_CGU_RAW_BUCKET,
            )

        result = BrazilCguArchiveSyncResult(
            dataset=source_file.dataset,
            snapshot_date=source_file.snapshot_date,
            source_url=source_file.url,
            archive_key=archive_key,
            metadata_key=metadata_key,
            downloaded=True,
            reused_existing_archive=False,
            checked_existing_archive=archive_exists,
            source_hash_changed=archive_exists,
            previous_sha256=previous_digest,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=content_type,
            source_last_modified=source_last_modified,
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), indent=2, sort_keys=True),
            bucket=BRAZIL_CGU_RAW_BUCKET,
        )
        return result

    def _session(self) -> Any:
        session = requests.Session()
        session.headers.update({"User-Agent": self.user_agent})
        return session

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
                if attempt >= self.download_max_attempts:
                    break
                sleep_seconds = self.download_retry_base_seconds * attempt
                if log_info is not None:
                    log_info(
                        "Retrying Brazil CGU archive download after stream error: "
                        "url=%s attempt=%s sleep_seconds=%s error=%s",
                        url,
                        attempt,
                        sleep_seconds,
                        exc,
                    )
                time.sleep(sleep_seconds)
        assert last_error is not None
        raise last_error

    def _stream_download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
    ) -> tuple[int, str, str, str]:
        response = session.get(
            url,
            timeout=self.request_timeout_seconds,
            stream=True,
        )
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        source_last_modified = response.headers.get("Last-Modified", "")
        expected_size = _optional_int(response.headers.get("Content-Length"))
        digest = sha256()
        size_bytes = 0

        with target_path.open("wb") as file_obj:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                file_obj.write(chunk)
                digest.update(chunk)
                size_bytes += len(chunk)

        if expected_size is not None and size_bytes != expected_size:
            raise requests.exceptions.ChunkedEncodingError(
                f"Brazil CGU archive size mismatch for {url}: "
                f"expected {expected_size}, got {size_bytes}"
            )
        return size_bytes, digest.hexdigest(), content_type, source_last_modified


def _snapshot_date_from_page(dataset: str, page: str) -> str:
    origin = re.escape(DATASET_PORTAL_ORIGINS[normalize_dataset(dataset)])
    match = re.search(
        r'arquivos\.push\(\{"ano"\s*:\s*"(?P<year>\d{4})",\s*'
        r'"mes"\s*:\s*"(?P<month>\d{2})",\s*'
        r'"dia"\s*:\s*"(?P<day>\d{2})",\s*'
        r'"origem"\s*:\s*"' + origin + r'"\}\);',
        page,
    )
    if match is None:
        raise ValueError(
            f"Could not find current Brazil CGU snapshot date for dataset {dataset}"
        )
    return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"


def _read_stored_metadata(
    object_store: ObjectStoreResource,
    metadata_key: str,
) -> dict[str, object]:
    try:
        return json.loads(
            object_store.read_bytes(metadata_key, bucket=BRAZIL_CGU_RAW_BUCKET)
        )
    except Exception:
        return {}


def _metadata_int(metadata: dict[str, object], key: str) -> int | None:
    value = metadata.get(key)
    if value is None:
        return None
    return int(value)


def _metadata_str(metadata: dict[str, object], key: str) -> str:
    value = metadata.get(key)
    return "" if value is None else str(value)


def _optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
