import json
import re
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from hashlib import md5, sha256
from pathlib import Path
from typing import Any

import dagster as dg
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esma_firds import tables

SOLR_FILES_URL = (
    "https://registers.esma.europa.eu/solr/"
    "esma_registers_firds_files/select"
)
SOLR_PAGE_SIZE = 1_000
DEFAULT_REQUEST_TIMEOUT_SECONDS = 120
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 3.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024

_FULL_FILE_RE = re.compile(
    r"^FULINS_([A-Z])_(\d{8})_(\d+)of(\d+)\.zip$", re.IGNORECASE
)
_DAILY_FILE_RE = re.compile(
    r"^(DLTINS|FULCAN)_(\d{8})_(\d+)of(\d+)\.zip$", re.IGNORECASE
)
_DOWNLOAD_RETRYABLE_ERRORS = (
    dlt_requests.ChunkedEncodingError,
    dlt_requests.ConnectionError,
    dlt_requests.Timeout,
)


@dataclass(frozen=True)
class FirdsSourceFile:
    source_file_id: str
    file_name: str
    file_type: str
    publication_date: date
    download_url: str
    checksum: str
    part_number: int
    part_count: int
    cfi_category: str

    @classmethod
    def from_solr_doc(cls, document: Mapping[str, Any]) -> "FirdsSourceFile":
        file_name = str(document.get("file_name", "") or "").strip()
        file_type = str(document.get("file_type", "") or "").strip().upper()
        if file_type not in tables.SUPPORTED_FILE_TYPES:
            raise ValueError(f"Unsupported FIRDS file type: {file_type!r}")
        (
            filename_type,
            filename_date,
            part_number,
            part_count,
            cfi_category,
        ) = _parse_file_name(file_name)
        if filename_type != file_type:
            raise ValueError(
                f"FIRDS file type mismatch: document={file_type}, filename={filename_type}"
            )
        publication_text = str(document.get("publication_date", "") or "")
        publication_date = date.fromisoformat(publication_text[:10])
        if publication_date.strftime("%Y%m%d") != filename_date:
            raise ValueError(
                "FIRDS publication date does not match filename: "
                f"{publication_date} != {filename_date}"
            )
        download_url = str(document.get("download_link", "") or "").strip()
        if not download_url.startswith("https://"):
            raise ValueError(f"Invalid FIRDS download URL for {file_name}")
        checksum = str(document.get("checksum", "") or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{32}", checksum):
            raise ValueError(f"Invalid FIRDS MD5 checksum for {file_name}")
        source_file_id = str(
            document.get("published_instrument_file_id")
            or document.get("id")
            or ""
        ).strip()
        if source_file_id == "":
            raise ValueError(f"Missing FIRDS source file id for {file_name}")
        return cls(
            source_file_id=source_file_id,
            file_name=file_name,
            file_type=file_type,
            publication_date=publication_date,
            download_url=download_url,
            checksum=checksum,
            part_number=part_number,
            part_count=part_count,
            cfi_category=cfi_category,
        )


@dataclass(frozen=True)
class FirdsFileSet:
    file_type: str
    publication_date: date
    files: tuple[FirdsSourceFile, ...]
    is_complete: bool
    missing_categories: tuple[str, ...] = ()
    missing_parts: tuple[int, ...] = ()
    duplicate_parts: tuple[int, ...] = ()


@dataclass(frozen=True)
class FirdsDownloadPlan:
    full: FirdsFileSet
    deltas: tuple[FirdsFileSet, ...]
    cancellations: FirdsFileSet | None

    @property
    def files(self) -> tuple[FirdsSourceFile, ...]:
        selected = list(self.full.files)
        for delta in self.deltas:
            selected.extend(delta.files)
        if self.cancellations is not None:
            selected.extend(self.cancellations.files)
        return tuple(selected)


@dataclass(frozen=True)
class FirdsArchiveResult:
    source_file_id: str
    file_name: str
    file_type: str
    publication_date: str
    download_url: str
    source_checksum_md5: str
    archive_sha256: str
    archive_size_bytes: int
    archive_key: str
    metadata_key: str
    retrieved_at: str
    downloaded: bool

    def metadata(self) -> dict[str, object]:
        return asdict(self)


def parse_solr_response(payload: Mapping[str, Any]) -> tuple[FirdsSourceFile, ...]:
    response = payload.get("response")
    if not isinstance(response, Mapping):
        raise ValueError("FIRDS Solr response has no response object")
    documents = response.get("docs")
    if not isinstance(documents, list):
        raise ValueError("FIRDS Solr response has no docs array")
    files: list[FirdsSourceFile] = []
    for document in documents:
        if not isinstance(document, Mapping):
            continue
        if str(document.get("file_name", "") or "").strip() == "":
            continue
        files.append(FirdsSourceFile.from_solr_doc(document))
    return tuple(files)


def complete_file_sets(
    files: Iterable[FirdsSourceFile],
    *,
    file_type: str,
) -> tuple[FirdsFileSet, ...]:
    normalized_type = file_type.strip().upper()
    if normalized_type not in tables.SUPPORTED_FILE_TYPES:
        raise ValueError(f"Unsupported FIRDS file type: {file_type}")
    grouped: dict[date, list[FirdsSourceFile]] = defaultdict(list)
    for source_file in files:
        if source_file.file_type == normalized_type:
            grouped[source_file.publication_date].append(source_file)

    return tuple(
        _build_file_set(normalized_type, publication_date, grouped_files)
        for publication_date, grouped_files in sorted(grouped.items())
    )


def build_download_plan(files: Sequence[FirdsSourceFile]) -> FirdsDownloadPlan:
    full_sets = complete_file_sets(files, file_type="FULINS")
    complete_full = [file_set for file_set in full_sets if file_set.is_complete]
    if not complete_full:
        raise ValueError("No complete FULINS snapshot in the discovery window")
    full = complete_full[-1]

    delta_sets = complete_file_sets(files, file_type="DLTINS")
    post_baseline_deltas = tuple(
        file_set
        for file_set in delta_sets
        if file_set.publication_date >= full.publication_date
    )
    incomplete_deltas = [
        file_set.publication_date.isoformat()
        for file_set in post_baseline_deltas
        if not file_set.is_complete
    ]
    if incomplete_deltas:
        raise ValueError(
            "Incomplete DLTINS file sets after FIRDS baseline: "
            + ", ".join(incomplete_deltas)
        )

    cancellation_sets = complete_file_sets(files, file_type="FULCAN")
    complete_cancellations = [
        file_set for file_set in cancellation_sets if file_set.is_complete
    ]
    cancellations = complete_cancellations[-1] if complete_cancellations else None
    return FirdsDownloadPlan(
        full=full,
        deltas=post_baseline_deltas,
        cancellations=cancellations,
    )


def archive_object_key(source_file: FirdsSourceFile) -> str:
    return (
        f"{tables.S3_RAW_PREFIX}/file_type={source_file.file_type}/"
        f"publication_date={source_file.publication_date.isoformat()}/"
        f"checksum={source_file.checksum}/{source_file.file_name}"
    )


def metadata_object_key(source_file: FirdsSourceFile) -> str:
    return f"{archive_object_key(source_file).removesuffix('.zip')}/metadata.json"


def source_file_from_archive_metadata(
    metadata: Mapping[str, Any],
) -> FirdsSourceFile:
    return FirdsSourceFile.from_solr_doc(
        {
            "published_instrument_file_id": metadata.get("source_file_id"),
            "file_name": metadata.get("file_name"),
            "file_type": metadata.get("file_type"),
            "publication_date": metadata.get("publication_date"),
            "download_link": metadata.get("download_url"),
            "checksum": metadata.get("source_checksum_md5"),
        }
    )


class FirdsResource(dg.ConfigurableResource):
    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS

    def discover_files(
        self,
        *,
        publication_from: date,
        publication_to: date,
        session: Any | None = None,
    ) -> tuple[FirdsSourceFile, ...]:
        if publication_from > publication_to:
            raise ValueError("FIRDS discovery start must not be after end")
        http = session or self._session()
        start = 0
        files: list[FirdsSourceFile] = []
        while True:
            response = http.get(
                SOLR_FILES_URL,
                params={
                    "q": "*:*",
                    "fq": (
                        "publication_date:["
                        f"{publication_from.isoformat()}T00:00:00Z TO "
                        f"{publication_to.isoformat()}T23:59:59Z]"
                    ),
                    "wt": "json",
                    "rows": SOLR_PAGE_SIZE,
                    "start": start,
                    "sort": "publication_date asc,file_name asc",
                },
                timeout=self.request_timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            page_files = parse_solr_response(payload)
            files.extend(page_files)
            response_data = payload.get("response", {})
            num_found = int(response_data.get("numFound", 0))
            start += len(response_data.get("docs", []))
            if start >= num_found:
                break
            if not response_data.get("docs"):
                raise ValueError(
                    "FIRDS Solr pagination stopped before the reported total"
                )
        return tuple(files)

    def sync_files(
        self,
        *,
        files: Sequence[FirdsSourceFile],
        object_store: ObjectStoreResource,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> tuple[FirdsArchiveResult, ...]:
        object_store.ensure_bucket(tables.S3_BUCKET)
        http = session or self._session()
        return tuple(
            self._sync_file(
                source_file=source_file,
                object_store=object_store,
                session=http,
                log_info=log_info,
            )
            for source_file in files
        )

    def _sync_file(
        self,
        *,
        source_file: FirdsSourceFile,
        object_store: ObjectStoreResource,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> FirdsArchiveResult:
        archive_key = archive_object_key(source_file)
        metadata_key = metadata_object_key(source_file)
        if object_store.exists(
            archive_key, bucket=tables.S3_BUCKET
        ) and object_store.exists(metadata_key, bucket=tables.S3_BUCKET):
            stored = json.loads(
                object_store.read_bytes(
                    metadata_key, bucket=tables.S3_BUCKET
                ).decode("utf-8")
            )
            stored_source_file = source_file_from_archive_metadata(stored)
            if _immutable_file_identity(stored_source_file) != (
                _immutable_file_identity(source_file)
            ):
                raise ValueError(
                    f"Stored FIRDS metadata does not match {source_file.file_name}"
                )
            if str(stored.get("archive_key", "")) != archive_key:
                raise ValueError(
                    f"Stored FIRDS archive key does not match {source_file.file_name}"
                )
            if str(stored.get("metadata_key", "")) != metadata_key:
                raise ValueError(
                    f"Stored FIRDS metadata key does not match {source_file.file_name}"
                )
            return FirdsArchiveResult(
                source_file_id=stored_source_file.source_file_id,
                file_name=stored_source_file.file_name,
                file_type=stored_source_file.file_type,
                publication_date=(
                    stored_source_file.publication_date.isoformat()
                ),
                download_url=stored_source_file.download_url,
                source_checksum_md5=stored_source_file.checksum,
                archive_sha256=str(stored["archive_sha256"]),
                archive_size_bytes=int(stored["archive_size_bytes"]),
                archive_key=archive_key,
                metadata_key=metadata_key,
                retrieved_at=str(stored["retrieved_at"]),
                downloaded=False,
            )

        with tempfile.TemporaryDirectory(prefix="esma_firds_download_") as tmpdir:
            target = Path(tmpdir) / source_file.file_name
            size_bytes, digest_md5, digest_sha256 = self._download_to_path(
                source_file.download_url,
                target,
                session=session,
                log_info=log_info,
            )
            if digest_md5 != source_file.checksum:
                raise ValueError(
                    f"FIRDS checksum mismatch for {source_file.file_name}: "
                    f"expected {source_file.checksum}, got {digest_md5}"
                )
            object_store.upload_file(
                archive_key,
                target,
                bucket=tables.S3_BUCKET,
            )

        result = FirdsArchiveResult(
            source_file_id=source_file.source_file_id,
            file_name=source_file.file_name,
            file_type=source_file.file_type,
            publication_date=source_file.publication_date.isoformat(),
            download_url=source_file.download_url,
            source_checksum_md5=source_file.checksum,
            archive_sha256=digest_sha256,
            archive_size_bytes=size_bytes,
            archive_key=archive_key,
            metadata_key=metadata_key,
            retrieved_at=datetime.now(UTC).isoformat(),
            downloaded=True,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(result.metadata(), indent=2, sort_keys=True),
            bucket=tables.S3_BUCKET,
        )
        return result

    def _download_to_path(
        self,
        url: str,
        target: Path,
        *,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> tuple[int, str, str]:
        last_error: Exception | None = None
        for attempt in range(1, self.download_max_attempts + 1):
            try:
                return self._stream_download(url, target, session=session)
            except _DOWNLOAD_RETRYABLE_ERRORS as exc:
                last_error = exc
                if attempt >= self.download_max_attempts:
                    break
                if log_info is not None:
                    log_info(
                        "Retrying FIRDS archive download after transient error: "
                        "url=%s attempt=%s/%s error=%s",
                        url,
                        attempt,
                        self.download_max_attempts,
                        exc,
                    )
                time.sleep(self.download_retry_base_seconds * attempt)
        assert last_error is not None
        raise last_error

    def _stream_download(
        self,
        url: str,
        target: Path,
        *,
        session: Any,
    ) -> tuple[int, str, str]:
        response = session.get(
            url,
            timeout=self.request_timeout_seconds,
            stream=True,
        )
        response.raise_for_status()
        expected_header = response.headers.get("Content-Length")
        expected_size = (
            int(expected_header)
            if expected_header is not None and str(expected_header).isdigit()
            else None
        )
        md5_digest = md5(usedforsecurity=False)
        sha256_digest = sha256()
        size_bytes = 0
        with target.open("wb") as output:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                output.write(chunk)
                md5_digest.update(chunk)
                sha256_digest.update(chunk)
                size_bytes += len(chunk)
        if expected_size is not None and size_bytes != expected_size:
            raise dlt_requests.ChunkedEncodingError(
                f"incomplete FIRDS download: {size_bytes}/{expected_size} bytes from {url}"
            )
        return size_bytes, md5_digest.hexdigest(), sha256_digest.hexdigest()

    @staticmethod
    def _session() -> Any:
        return dlt_requests.Client(
            request_timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS,
            request_max_attempts=5,
        ).session


def _parse_file_name(file_name: str) -> tuple[str, str, int, int, str]:
    full_match = _FULL_FILE_RE.fullmatch(file_name)
    if full_match is not None:
        category, filename_date, part_number, part_count = full_match.groups()
        return (
            "FULINS",
            filename_date,
            int(part_number),
            int(part_count),
            category.upper(),
        )
    daily_match = _DAILY_FILE_RE.fullmatch(file_name)
    if daily_match is not None:
        file_type, filename_date, part_number, part_count = daily_match.groups()
        return (
            file_type.upper(),
            filename_date,
            int(part_number),
            int(part_count),
            "",
        )
    raise ValueError(f"Unsupported FIRDS filename: {file_name}")


def _immutable_file_identity(
    source_file: FirdsSourceFile,
) -> tuple[str, str, date, str, int, int, str]:
    return (
        source_file.file_name,
        source_file.file_type,
        source_file.publication_date,
        source_file.checksum,
        source_file.part_number,
        source_file.part_count,
        source_file.cfi_category,
    )


def _build_file_set(
    file_type: str,
    publication_date: date,
    files: Sequence[FirdsSourceFile],
) -> FirdsFileSet:
    ordered = tuple(
        sorted(files, key=lambda item: (item.cfi_category, item.part_number))
    )
    missing_categories: tuple[str, ...] = ()
    missing_parts: set[int] = set()
    duplicate_parts: set[int] = set()
    if file_type == "FULINS":
        categories = {item.cfi_category for item in ordered}
        missing_categories = tuple(
            sorted(tables.EXPECTED_FULL_CFI_CATEGORIES - categories)
        )
        grouped_by_category: dict[str, list[FirdsSourceFile]] = defaultdict(list)
        for item in ordered:
            grouped_by_category[item.cfi_category].append(item)
        for category_files in grouped_by_category.values():
            missing_parts.update(_missing_parts(category_files))
            duplicate_parts.update(_duplicate_parts(category_files))
    else:
        missing_parts.update(_missing_parts(ordered))
        duplicate_parts.update(_duplicate_parts(ordered))
    return FirdsFileSet(
        file_type=file_type,
        publication_date=publication_date,
        files=ordered,
        is_complete=(
            not missing_categories
            and not missing_parts
            and not duplicate_parts
        ),
        missing_categories=missing_categories,
        missing_parts=tuple(sorted(missing_parts)),
        duplicate_parts=tuple(sorted(duplicate_parts)),
    )


def _missing_parts(files: Sequence[FirdsSourceFile]) -> set[int]:
    if not files:
        return set()
    totals = {item.part_count for item in files}
    if len(totals) != 1:
        return set(range(1, max(totals) + 1))
    expected = set(range(1, next(iter(totals)) + 1))
    observed = {item.part_number for item in files}
    return expected - observed


def _duplicate_parts(files: Sequence[FirdsSourceFile]) -> set[int]:
    observed: set[int] = set()
    duplicates: set[int] = set()
    for source_file in files:
        if source_file.part_number in observed:
            duplicates.add(source_file.part_number)
        observed.add(source_file.part_number)
    return duplicates
