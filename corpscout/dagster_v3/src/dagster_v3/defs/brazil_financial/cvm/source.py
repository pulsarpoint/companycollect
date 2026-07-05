import json
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_FIN_CVM_GROUP_NAME = "brazil_fin_cvm"
BRAZIL_CVM_RAW_BUCKET = "source-brazil-cvm"
BRAZIL_CVM_DFP_BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS"
BRAZIL_CVM_ITR_BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS"
BRAZIL_CVM_FRE_BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS"
BRAZIL_CVM_DFP_START_YEAR = 2010
BRAZIL_CVM_DFP_END_YEAR = 2026
BRAZIL_CVM_ITR_START_YEAR = 2011
BRAZIL_CVM_ITR_END_YEAR = 2026
BRAZIL_CVM_FRE_START_YEAR = 2010
BRAZIL_CVM_FRE_END_YEAR = 2026

DEFAULT_REQUEST_TIMEOUT_SECONDS = 1_800
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-brazil-cvm-dfp/0.1"

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


@dataclass(frozen=True)
class BrazilCvmDfpArchiveSyncResult:
    year: str
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
            "year": self.year,
            "source_url": self.source_url,
            "s3_bucket": BRAZIL_CVM_RAW_BUCKET,
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
class BrazilCvmItrArchiveSyncResult:
    year: str
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
            "year": self.year,
            "source_url": self.source_url,
            "s3_bucket": BRAZIL_CVM_RAW_BUCKET,
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
class BrazilCvmFreArchiveSyncResult:
    year: str
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
            "year": self.year,
            "source_url": self.source_url,
            "s3_bucket": BRAZIL_CVM_RAW_BUCKET,
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


def normalize_dfp_year(year: str | int) -> str:
    year_int = int(str(year))
    if year_int < BRAZIL_CVM_DFP_START_YEAR or year_int > BRAZIL_CVM_DFP_END_YEAR:
        raise ValueError(
            f"Brazil CVM DFP year must be between {BRAZIL_CVM_DFP_START_YEAR} "
            f"and {BRAZIL_CVM_DFP_END_YEAR}: {year}"
        )
    return str(year_int)


def normalize_itr_year(year: str | int) -> str:
    year_int = int(str(year))
    if year_int < BRAZIL_CVM_ITR_START_YEAR or year_int > BRAZIL_CVM_ITR_END_YEAR:
        raise ValueError(
            f"Brazil CVM ITR year must be between {BRAZIL_CVM_ITR_START_YEAR} "
            f"and {BRAZIL_CVM_ITR_END_YEAR}: {year}"
        )
    return str(year_int)


def normalize_fre_year(year: str | int) -> str:
    year_int = int(str(year))
    if year_int < BRAZIL_CVM_FRE_START_YEAR or year_int > BRAZIL_CVM_FRE_END_YEAR:
        raise ValueError(
            f"Brazil CVM FRE year must be between {BRAZIL_CVM_FRE_START_YEAR} "
            f"and {BRAZIL_CVM_FRE_END_YEAR}: {year}"
        )
    return str(year_int)


def dfp_archive_name(year: str | int) -> str:
    return f"dfp_cia_aberta_{normalize_dfp_year(year)}.zip"


def dfp_source_url(year: str | int) -> str:
    return f"{BRAZIL_CVM_DFP_BASE_URL}/{dfp_archive_name(year)}"


def dfp_archive_object_key(year: str | int) -> str:
    return f"brazil_cvm/dfp/raw_archives/year={normalize_dfp_year(year)}/archive.zip"


def dfp_metadata_object_key(year: str | int) -> str:
    return f"brazil_cvm/dfp/raw_archives/year={normalize_dfp_year(year)}/metadata.json"


def itr_archive_name(year: str | int) -> str:
    return f"itr_cia_aberta_{normalize_itr_year(year)}.zip"


def itr_source_url(year: str | int) -> str:
    return f"{BRAZIL_CVM_ITR_BASE_URL}/{itr_archive_name(year)}"


def itr_archive_object_key(year: str | int) -> str:
    return f"brazil_cvm/itr/raw_archives/year={normalize_itr_year(year)}/archive.zip"


def itr_metadata_object_key(year: str | int) -> str:
    return f"brazil_cvm/itr/raw_archives/year={normalize_itr_year(year)}/metadata.json"


def fre_archive_name(year: str | int) -> str:
    return f"fre_cia_aberta_{normalize_fre_year(year)}.zip"


def fre_source_url(year: str | int) -> str:
    return f"{BRAZIL_CVM_FRE_BASE_URL}/{fre_archive_name(year)}"


def fre_archive_object_key(year: str | int) -> str:
    return f"brazil_cvm/fre/raw_archives/year={normalize_fre_year(year)}/archive.zip"


def fre_metadata_object_key(year: str | int) -> str:
    return f"brazil_cvm/fre/raw_archives/year={normalize_fre_year(year)}/metadata.json"


class BrazilCvmDfpResource(dg.ConfigurableResource):
    """Downloads CVM DFP annual ZIP archives into object storage."""

    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def sync_year_archive(
        self,
        *,
        year: str | int,
        object_store: ObjectStoreResource,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> BrazilCvmDfpArchiveSyncResult:
        normalized_year = normalize_dfp_year(year)
        archive_key = dfp_archive_object_key(normalized_year)
        metadata_key = dfp_metadata_object_key(normalized_year)
        source_url = dfp_source_url(normalized_year)
        synced_at = datetime.now(UTC).isoformat()

        object_store.ensure_bucket(BRAZIL_CVM_RAW_BUCKET)
        if object_store.exists(archive_key, bucket=BRAZIL_CVM_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Brazil CVM DFP archive: bucket=%s key=%s",
                    BRAZIL_CVM_RAW_BUCKET,
                    archive_key,
                )
            return self._reuse_existing_archive(
                object_store=object_store,
                year=normalized_year,
                source_url=source_url,
                archive_key=archive_key,
                metadata_key=metadata_key,
                synced_at=synced_at,
            )

        if log_info is not None:
            log_info(
                "Downloading Brazil CVM DFP archive: year=%s url=%s bucket=%s key=%s",
                normalized_year,
                source_url,
                BRAZIL_CVM_RAW_BUCKET,
                archive_key,
            )
        with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_dfp_") as tmpdir:
            target_path = Path(tmpdir) / dfp_archive_name(normalized_year)
            size_bytes, digest, content_type, source_last_modified = (
                self._download_to_path(
                    url=source_url,
                    target_path=target_path,
                    session=session or self._session(),
                    log_info=log_info,
                )
            )
            object_store.upload_file(
                archive_key,
                target_path,
                bucket=BRAZIL_CVM_RAW_BUCKET,
            )

        result = BrazilCvmDfpArchiveSyncResult(
            year=normalized_year,
            source_url=source_url,
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
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return result

    def _reuse_existing_archive(
        self,
        *,
        object_store: ObjectStoreResource,
        year: str,
        source_url: str,
        archive_key: str,
        metadata_key: str,
        synced_at: str,
    ) -> BrazilCvmDfpArchiveSyncResult:
        stored_metadata = _read_stored_metadata(object_store, metadata_key)
        size_bytes = _metadata_int(stored_metadata, "size_bytes")
        digest = _metadata_str(stored_metadata, "sha256")
        if size_bytes is None or digest == "":
            archive_body = object_store.read_bytes(
                archive_key, bucket=BRAZIL_CVM_RAW_BUCKET
            )
            size_bytes = len(archive_body)
            digest = sha256(archive_body).hexdigest()

        result = BrazilCvmDfpArchiveSyncResult(
            year=year,
            source_url=source_url,
            archive_key=archive_key,
            metadata_key=metadata_key,
            downloaded=False,
            reused_existing_archive=True,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=_metadata_str(stored_metadata, "content_type"),
            source_last_modified=_metadata_str(stored_metadata, "source_last_modified"),
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), indent=2, sort_keys=True),
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return result

    def _session(self) -> Any:
        session = dlt_requests.Session()
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
                        "Retrying Brazil CVM archive download after stream error: "
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
                f"Brazil CVM archive size mismatch for {url}: "
                f"expected {expected_size}, got {size_bytes}"
            )
        return size_bytes, digest.hexdigest(), content_type, source_last_modified


class BrazilCvmItrResource(BrazilCvmDfpResource):
    """Downloads CVM ITR quarterly/interim ZIP archives into object storage."""

    user_agent: str = "corpscout-dagster-v3-brazil-cvm-itr/0.1"

    def sync_year_archive(
        self,
        *,
        year: str | int,
        object_store: ObjectStoreResource,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> BrazilCvmItrArchiveSyncResult:
        normalized_year = normalize_itr_year(year)
        archive_key = itr_archive_object_key(normalized_year)
        metadata_key = itr_metadata_object_key(normalized_year)
        source_url = itr_source_url(normalized_year)
        synced_at = datetime.now(UTC).isoformat()

        object_store.ensure_bucket(BRAZIL_CVM_RAW_BUCKET)
        if object_store.exists(archive_key, bucket=BRAZIL_CVM_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Brazil CVM ITR archive: bucket=%s key=%s",
                    BRAZIL_CVM_RAW_BUCKET,
                    archive_key,
                )
            return self._reuse_existing_itr_archive(
                object_store=object_store,
                year=normalized_year,
                source_url=source_url,
                archive_key=archive_key,
                metadata_key=metadata_key,
                synced_at=synced_at,
            )

        if log_info is not None:
            log_info(
                "Downloading Brazil CVM ITR archive: year=%s url=%s bucket=%s key=%s",
                normalized_year,
                source_url,
                BRAZIL_CVM_RAW_BUCKET,
                archive_key,
            )
        with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_itr_") as tmpdir:
            target_path = Path(tmpdir) / itr_archive_name(normalized_year)
            size_bytes, digest, content_type, source_last_modified = (
                self._download_to_path(
                    url=source_url,
                    target_path=target_path,
                    session=session or self._session(),
                    log_info=log_info,
                )
            )
            object_store.upload_file(
                archive_key,
                target_path,
                bucket=BRAZIL_CVM_RAW_BUCKET,
            )

        result = BrazilCvmItrArchiveSyncResult(
            year=normalized_year,
            source_url=source_url,
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
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return result

    def _reuse_existing_itr_archive(
        self,
        *,
        object_store: ObjectStoreResource,
        year: str,
        source_url: str,
        archive_key: str,
        metadata_key: str,
        synced_at: str,
    ) -> BrazilCvmItrArchiveSyncResult:
        stored_metadata = _read_stored_metadata(object_store, metadata_key)
        size_bytes = _metadata_int(stored_metadata, "size_bytes")
        digest = _metadata_str(stored_metadata, "sha256")
        if size_bytes is None or digest == "":
            archive_body = object_store.read_bytes(
                archive_key, bucket=BRAZIL_CVM_RAW_BUCKET
            )
            size_bytes = len(archive_body)
            digest = sha256(archive_body).hexdigest()

        result = BrazilCvmItrArchiveSyncResult(
            year=year,
            source_url=source_url,
            archive_key=archive_key,
            metadata_key=metadata_key,
            downloaded=False,
            reused_existing_archive=True,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=_metadata_str(stored_metadata, "content_type"),
            source_last_modified=_metadata_str(stored_metadata, "source_last_modified"),
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), indent=2, sort_keys=True),
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return result


class BrazilCvmFreResource(BrazilCvmDfpResource):
    """Downloads CVM FRE reference-form ZIP archives into object storage."""

    user_agent: str = "corpscout-dagster-v3-brazil-cvm-fre/0.1"

    def sync_year_archive(
        self,
        *,
        year: str | int,
        object_store: ObjectStoreResource,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> BrazilCvmFreArchiveSyncResult:
        normalized_year = normalize_fre_year(year)
        archive_key = fre_archive_object_key(normalized_year)
        metadata_key = fre_metadata_object_key(normalized_year)
        source_url = fre_source_url(normalized_year)
        synced_at = datetime.now(UTC).isoformat()

        object_store.ensure_bucket(BRAZIL_CVM_RAW_BUCKET)
        if object_store.exists(archive_key, bucket=BRAZIL_CVM_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Brazil CVM FRE archive: bucket=%s key=%s",
                    BRAZIL_CVM_RAW_BUCKET,
                    archive_key,
                )
            return self._reuse_existing_fre_archive(
                object_store=object_store,
                year=normalized_year,
                source_url=source_url,
                archive_key=archive_key,
                metadata_key=metadata_key,
                synced_at=synced_at,
            )

        if log_info is not None:
            log_info(
                "Downloading Brazil CVM FRE archive: year=%s url=%s bucket=%s key=%s",
                normalized_year,
                source_url,
                BRAZIL_CVM_RAW_BUCKET,
                archive_key,
            )
        with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_fre_") as tmpdir:
            target_path = Path(tmpdir) / fre_archive_name(normalized_year)
            size_bytes, digest, content_type, source_last_modified = (
                self._download_to_path(
                    url=source_url,
                    target_path=target_path,
                    session=session or self._session(),
                    log_info=log_info,
                )
            )
            object_store.upload_file(
                archive_key,
                target_path,
                bucket=BRAZIL_CVM_RAW_BUCKET,
            )

        result = BrazilCvmFreArchiveSyncResult(
            year=normalized_year,
            source_url=source_url,
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
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return result

    def _reuse_existing_fre_archive(
        self,
        *,
        object_store: ObjectStoreResource,
        year: str,
        source_url: str,
        archive_key: str,
        metadata_key: str,
        synced_at: str,
    ) -> BrazilCvmFreArchiveSyncResult:
        stored_metadata = _read_stored_metadata(object_store, metadata_key)
        size_bytes = _metadata_int(stored_metadata, "size_bytes")
        digest = _metadata_str(stored_metadata, "sha256")
        if size_bytes is None or digest == "":
            archive_body = object_store.read_bytes(
                archive_key, bucket=BRAZIL_CVM_RAW_BUCKET
            )
            size_bytes = len(archive_body)
            digest = sha256(archive_body).hexdigest()

        result = BrazilCvmFreArchiveSyncResult(
            year=year,
            source_url=source_url,
            archive_key=archive_key,
            metadata_key=metadata_key,
            downloaded=False,
            reused_existing_archive=True,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=_metadata_str(stored_metadata, "content_type"),
            source_last_modified=_metadata_str(stored_metadata, "source_last_modified"),
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), indent=2, sort_keys=True),
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return result


def _optional_int(value: str | None) -> int | None:
    if value is None or value.strip() == "":
        return None
    return int(value)


def _read_stored_metadata(
    object_store: ObjectStoreResource,
    metadata_key: str,
) -> Mapping[str, object]:
    if not object_store.exists(metadata_key, bucket=BRAZIL_CVM_RAW_BUCKET):
        return {}
    metadata = json.loads(
        object_store.read_bytes(metadata_key, bucket=BRAZIL_CVM_RAW_BUCKET).decode(
            "utf-8"
        )
    )
    return metadata if isinstance(metadata, dict) else {}


def _metadata_int(metadata: Mapping[str, object], key: str) -> int | None:
    value = metadata.get(key)
    if value is None or value == "":
        return None
    return int(value)


def _metadata_str(metadata: Mapping[str, object], key: str) -> str:
    value = metadata.get(key)
    return value if isinstance(value, str) else ""
