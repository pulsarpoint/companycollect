import json
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import requests

from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_COMP_PGFN_GROUP_NAME = "brazil_comp_pgfn"
BRAZIL_PGFN_RAW_BUCKET = "source-brazil-pgfn"
BRAZIL_PGFN_BASE_URL = "https://dadosabertos.pgfn.gov.br"
BRAZIL_PGFN_SOURCE_PAGE_URL = (
    "https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/"
    "transparencia-fiscal-1/dados-abertos"
)
SOURCE_SLUG = "brazil_pgfn"
START_YEAR = 2020
END_YEAR = 2026
END_QUARTER = 1
DEFAULT_REQUEST_TIMEOUT_SECONDS = 1_800
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-brazil-pgfn/0.1"

SOURCE_SYSTEMS = {
    "nao_previdenciario": "Nao_Previdenciario",
    "fgts": "FGTS",
    "previdenciario": "Previdenciario",
}
SPECIAL_ARCHIVE_STEMS = {
    ("2022-Q4", "nao_previdenciario"): "Nao_Previdenciariozip",
}

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


@dataclass(frozen=True)
class BrazilPgfnSourceFile:
    snapshot_quarter: str
    source_system: str
    official_file_stem: str
    url: str


@dataclass(frozen=True)
class BrazilPgfnArchiveSyncResult:
    snapshot_quarter: str
    source_system: str
    source_url: str
    archive_key: str
    metadata_key: str
    downloaded: bool
    reused_existing_archive: bool
    size_bytes: int | None
    sha256: str | None
    content_type: str
    source_last_modified: str
    synced_at: str

    def metadata(self) -> dict[str, object]:
        return {
            "snapshot_quarter": self.snapshot_quarter,
            "source_system": self.source_system,
            "source_url": self.source_url,
            "s3_bucket": BRAZIL_PGFN_RAW_BUCKET,
            "archive_key": self.archive_key,
            "metadata_key": self.metadata_key,
            "downloaded": self.downloaded,
            "reused_existing_archive": self.reused_existing_archive,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "content_type": self.content_type,
            "source_last_modified": self.source_last_modified,
            "synced_at": self.synced_at,
        }


@dataclass(frozen=True)
class BrazilPgfnSnapshotSyncResult:
    snapshot_quarter: str
    archives: tuple[BrazilPgfnArchiveSyncResult, ...]

    def metadata(self) -> dict[str, object]:
        return {
            "snapshot_quarter": self.snapshot_quarter,
            "archive_count": len(self.archives),
            "downloaded_archive_count": sum(
                archive.downloaded for archive in self.archives
            ),
            "reused_existing_archive_count": sum(
                archive.reused_existing_archive for archive in self.archives
            ),
            "source_systems": [archive.source_system for archive in self.archives],
            "archive_keys": [archive.archive_key for archive in self.archives],
            "source_urls": [archive.source_url for archive in self.archives],
        }


def pgfn_partition_keys() -> list[str]:
    keys: list[str] = []
    for year in range(START_YEAR, END_YEAR + 1):
        last_quarter = END_QUARTER if year == END_YEAR else 4
        for quarter in range(1, last_quarter + 1):
            keys.append(f"{year}-Q{quarter}")
    return keys


def normalize_snapshot_quarter(snapshot_quarter: str) -> str:
    clean_value = snapshot_quarter.strip().upper()
    if "-Q" not in clean_value:
        raise ValueError(f"PGFN snapshot quarter must use YYYY-QN: {snapshot_quarter}")
    year_text, quarter_text = clean_value.split("-Q", 1)
    year = int(year_text)
    quarter = int(quarter_text)
    if quarter < 1 or quarter > 4:
        raise ValueError(
            f"PGFN snapshot quarter must be between Q1 and Q4: {snapshot_quarter}"
        )
    normalized = f"{year}-Q{quarter}"
    if normalized not in pgfn_partition_keys():
        raise ValueError(f"Unsupported PGFN snapshot quarter: {snapshot_quarter}")
    return normalized


def pgfn_snapshot_source_files(
    snapshot_quarter: str,
) -> tuple[BrazilPgfnSourceFile, ...]:
    normalized_quarter = normalize_snapshot_quarter(snapshot_quarter)
    year, quarter = _snapshot_year_quarter(normalized_quarter)
    files: list[BrazilPgfnSourceFile] = []
    for source_system, default_file_stem in SOURCE_SYSTEMS.items():
        file_stem = SPECIAL_ARCHIVE_STEMS.get(
            (normalized_quarter, source_system), default_file_stem
        )
        files.append(
            BrazilPgfnSourceFile(
                snapshot_quarter=normalized_quarter,
                source_system=source_system,
                official_file_stem=default_file_stem,
                url=(
                    f"{BRAZIL_PGFN_BASE_URL}/{year}_trimestre_{quarter:02d}/"
                    f"Dados_abertos_{file_stem}.zip"
                ),
            )
        )
    return tuple(files)


def pgfn_archive_object_key(snapshot_quarter: str, source_system: str) -> str:
    return (
        f"brazil_pgfn/raw_archives/snapshot={normalize_snapshot_quarter(snapshot_quarter)}"
        f"/source_system={normalize_source_system(source_system)}/archive.zip"
    )


def pgfn_metadata_object_key(snapshot_quarter: str, source_system: str) -> str:
    return (
        f"brazil_pgfn/raw_archives/snapshot={normalize_snapshot_quarter(snapshot_quarter)}"
        f"/source_system={normalize_source_system(source_system)}/metadata.json"
    )


def _snapshot_year_quarter(snapshot_quarter: str) -> tuple[int, int]:
    year_text, quarter_text = snapshot_quarter.split("-Q", 1)
    return int(year_text), int(quarter_text)


def normalize_source_system(source_system: str) -> str:
    clean_value = source_system.strip().lower()
    if clean_value not in SOURCE_SYSTEMS:
        raise ValueError(f"Unsupported PGFN source system: {source_system}")
    return clean_value


class BrazilPgfnResource(dg.ConfigurableResource):
    """Downloads PGFN quarterly open-data ZIP archives into object storage."""

    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def sync_snapshot_archives(
        self,
        *,
        snapshot_quarter: str,
        object_store: ObjectStoreResource,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> BrazilPgfnSnapshotSyncResult:
        normalized_quarter = normalize_snapshot_quarter(snapshot_quarter)
        object_store.ensure_bucket(BRAZIL_PGFN_RAW_BUCKET)
        session = session or self._session()
        archives = tuple(
            self._sync_source_archive(
                source_file=source_file,
                object_store=object_store,
                session=session,
                log_info=log_info,
            )
            for source_file in pgfn_snapshot_source_files(normalized_quarter)
        )
        return BrazilPgfnSnapshotSyncResult(
            snapshot_quarter=normalized_quarter,
            archives=archives,
        )

    def _sync_source_archive(
        self,
        *,
        source_file: BrazilPgfnSourceFile,
        object_store: ObjectStoreResource,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> BrazilPgfnArchiveSyncResult:
        archive_key = pgfn_archive_object_key(
            source_file.snapshot_quarter, source_file.source_system
        )
        metadata_key = pgfn_metadata_object_key(
            source_file.snapshot_quarter, source_file.source_system
        )
        synced_at = datetime.now(UTC).isoformat()

        if object_store.exists(archive_key, bucket=BRAZIL_PGFN_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Brazil PGFN archive: bucket=%s key=%s",
                    BRAZIL_PGFN_RAW_BUCKET,
                    archive_key,
                )
            return self._reuse_existing_archive(
                source_file=source_file,
                object_store=object_store,
                archive_key=archive_key,
                metadata_key=metadata_key,
                synced_at=synced_at,
            )

        if log_info is not None:
            log_info(
                "Downloading Brazil PGFN archive: snapshot=%s source_system=%s url=%s",
                source_file.snapshot_quarter,
                source_file.source_system,
                source_file.url,
            )
        with tempfile.TemporaryDirectory(prefix="brazil_comp_pgfn_") as tmpdir:
            target_path = Path(tmpdir) / f"{source_file.source_system}.zip"
            size_bytes, digest, content_type, source_last_modified = (
                self._download_to_path(
                    url=source_file.url,
                    target_path=target_path,
                    session=session,
                    log_info=log_info,
                )
            )
            object_store.upload_file(
                archive_key,
                target_path,
                bucket=BRAZIL_PGFN_RAW_BUCKET,
            )

        result = BrazilPgfnArchiveSyncResult(
            snapshot_quarter=source_file.snapshot_quarter,
            source_system=source_file.source_system,
            source_url=source_file.url,
            archive_key=archive_key,
            metadata_key=metadata_key,
            downloaded=True,
            reused_existing_archive=False,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=content_type,
            source_last_modified=source_last_modified,
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), indent=2, sort_keys=True),
            bucket=BRAZIL_PGFN_RAW_BUCKET,
        )
        return result

    def _reuse_existing_archive(
        self,
        *,
        source_file: BrazilPgfnSourceFile,
        object_store: ObjectStoreResource,
        archive_key: str,
        metadata_key: str,
        synced_at: str,
    ) -> BrazilPgfnArchiveSyncResult:
        stored_metadata = _read_stored_metadata(object_store, metadata_key)
        size_bytes = _metadata_int(stored_metadata, "size_bytes")
        digest = _metadata_str(stored_metadata, "sha256")

        result = BrazilPgfnArchiveSyncResult(
            snapshot_quarter=source_file.snapshot_quarter,
            source_system=source_file.source_system,
            source_url=source_file.url,
            archive_key=archive_key,
            metadata_key=metadata_key,
            downloaded=False,
            reused_existing_archive=True,
            size_bytes=size_bytes,
            sha256=digest or None,
            content_type=_metadata_str(stored_metadata, "content_type"),
            source_last_modified=_metadata_str(stored_metadata, "source_last_modified"),
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), indent=2, sort_keys=True),
            bucket=BRAZIL_PGFN_RAW_BUCKET,
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
                        "Retrying Brazil PGFN archive download after stream error: "
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
                f"Brazil PGFN archive size mismatch for {url}: "
                f"expected {expected_size}, got {size_bytes}"
            )
        return size_bytes, digest.hexdigest(), content_type, source_last_modified


def _read_stored_metadata(
    object_store: ObjectStoreResource,
    metadata_key: str,
) -> dict[str, object]:
    try:
        return json.loads(
            object_store.read_bytes(metadata_key, bucket=BRAZIL_PGFN_RAW_BUCKET)
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
