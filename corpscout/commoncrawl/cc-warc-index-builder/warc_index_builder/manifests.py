"""Download immutable Common Crawl manifest snapshots."""

import gzip
import hashlib
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import duckdb
import httpx

from ._identity import decode_sha256, new_identity_digest, update_text


COMMON_CRAWL_DATA_URL = "https://data.commoncrawl.org"
WARC_MANIFEST_FILENAME = "warc.paths.gz"
INDEX_MANIFEST_FILENAME = "cc-index-table.paths.gz"
_TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_REQUIRED_STRING_COLUMNS = (
    "url",
    "url_host_name",
    "url_host_registered_domain",
    "url_path",
    "content_mime_type",
    "warc_filename",
)
_REQUIRED_INTEGER_COLUMNS = (
    "fetch_status",
    "warc_record_offset",
    "warc_record_length",
)
_OPTIONAL_STRING_COLUMNS = (
    "content_mime_detected",
    "content_languages",
)
_INTEGER_TYPES = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
    }
)


class ManifestDownloadError(RuntimeError):
    pass


class ManifestChangedError(RuntimeError):
    def __init__(self, mismatches: Sequence[tuple[str, str, str]]) -> None:
        self.mismatches = tuple(mismatches)
        details = ", ".join(
            f"{name} expected={expected} actual={actual}"
            for name, expected, actual in self.mismatches
        )
        super().__init__(f"crawl manifests changed during catalog build: {details}")


class ManifestParseError(RuntimeError):
    pass


class SourceSchemaError(RuntimeError):
    pass


class _TransientDownloadError(RuntimeError):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True, slots=True)
class ManifestSnapshot:
    url: str
    path: Path
    sha256: str
    byte_count: int
    reused: bool


@dataclass(frozen=True, slots=True)
class ManifestDigest:
    url: str
    sha256: str
    byte_count: int


@dataclass(frozen=True, slots=True)
class ManifestRecheckResult:
    warc: ManifestDigest
    index: ManifestDigest


@dataclass(frozen=True, slots=True)
class WarcObject:
    warc_index: int
    warc_filename: str


@dataclass(frozen=True, slots=True)
class IndexSource:
    source_index: int
    path: str
    url: str


@dataclass(frozen=True, slots=True)
class SourceSchema:
    source_index: int
    column_types: tuple[tuple[str, str], ...]

    @property
    def has_content_mime_detected(self) -> bool:
        return self.type_for("content_mime_detected") is not None

    @property
    def has_content_languages(self) -> bool:
        return self.type_for("content_languages") is not None

    def type_for(self, column: str) -> str | None:
        return next((column_type for name, column_type in self.column_types if name == column), None)

    @property
    def normalized_descriptor(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (column, (self.type_for(column) or "MISSING").upper())
            for column in (
                *_REQUIRED_STRING_COLUMNS,
                *_REQUIRED_INTEGER_COLUMNS,
                *_OPTIONAL_STRING_COLUMNS,
            )
        )


def source_schema_sha256(schema: SourceSchema) -> str:
    """Hash one normalized capability descriptor without its path or shard index."""
    digest = new_identity_digest("source-schema")
    _update_source_schema_descriptor(digest, schema)
    return digest.hexdigest()


def source_schemas_sha256(schemas: Sequence[SourceSchema]) -> str:
    """Hash source-index-ordered normalized schemas without source locations."""
    if not schemas:
        raise ValueError("source schemas must not be empty")
    digest = new_identity_digest("source-schemas")
    digest.update(len(schemas).to_bytes(4, byteorder="big"))
    for expected_index, schema in enumerate(schemas):
        if schema.source_index != expected_index:
            raise ValueError(
                "source schemas must be in contiguous source_index order starting at 0"
            )
        digest.update(schema.source_index.to_bytes(4, byteorder="big"))
        _update_source_schema_descriptor(digest, schema)
    return digest.hexdigest()


def _update_source_schema_descriptor(digest, schema: SourceSchema) -> None:
    descriptor = schema.normalized_descriptor
    digest.update(len(descriptor).to_bytes(4, byteorder="big"))
    for column, column_type in descriptor:
        update_text(digest, column)
        update_text(digest, column_type)


def crawl_manifest_url(crawl: str, filename: str) -> str:
    return f"{COMMON_CRAWL_DATA_URL}/crawl-data/{crawl}/{filename}"


def _read_manifest_lines(path: Path) -> list[str]:
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline=None) as manifest:
            return [line.rstrip("\n") for line in manifest]
    except (OSError, EOFError, UnicodeDecodeError) as error:
        raise ManifestParseError(f"read gzip manifest {path}: {error}") from error


def _validate_manifest_line(path: Path, line_number: int, value: str) -> None:
    if not value:
        raise ManifestParseError(f"{path}: line {line_number} is blank")
    if value != value.strip():
        raise ManifestParseError(f"{path}: line {line_number} has surrounding whitespace")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ManifestParseError(f"{path}: line {line_number} has an invalid object path: {value}")


def read_warc_inventory(path: Path, crawl: str) -> tuple[WarcObject, ...]:
    inventory: list[WarcObject] = []
    seen: set[str] = set()
    crawl_prefix = f"crawl-data/{crawl}/"
    for line_number, warc_filename in enumerate(_read_manifest_lines(path), start=1):
        _validate_manifest_line(path, line_number, warc_filename)
        if not warc_filename.startswith(crawl_prefix):
            raise ManifestParseError(
                f"{path}: line {line_number} is outside crawl {crawl}: {warc_filename}"
            )
        if "/warc/" not in warc_filename or not warc_filename.endswith(".warc.gz"):
            raise ManifestParseError(
                f"{path}: line {line_number} is not a WARC object: {warc_filename}"
            )
        if warc_filename in seen:
            raise ManifestParseError(
                f"{path}: line {line_number} duplicates WARC path: {warc_filename}"
            )
        seen.add(warc_filename)
        inventory.append(WarcObject(len(inventory), warc_filename))
    if not inventory:
        raise ManifestParseError(f"{path}: WARC inventory is empty")
    return tuple(inventory)


def read_index_sources(path: Path, crawl: str) -> tuple[IndexSource, ...]:
    sources: list[IndexSource] = []
    seen: set[str] = set()
    expected_partition = f"/crawl={crawl}/subset=warc/"
    for line_number, source_path in enumerate(_read_manifest_lines(path), start=1):
        _validate_manifest_line(path, line_number, source_path)
        if source_path in seen:
            raise ManifestParseError(
                f"{path}: line {line_number} duplicates index path: {source_path}"
            )
        seen.add(source_path)
        if "/subset=warc/" not in source_path:
            continue
        if expected_partition not in source_path:
            raise ManifestParseError(
                f"{path}: line {line_number} is outside crawl {crawl}: {source_path}"
            )
        if not source_path.startswith("cc-index/table/cc-main/warc/"):
            raise ManifestParseError(
                f"{path}: line {line_number} has an invalid index prefix: {source_path}"
            )
        if not source_path.endswith(".parquet"):
            raise ManifestParseError(
                f"{path}: line {line_number} is not a Parquet source: {source_path}"
            )
        sources.append(
            IndexSource(
                source_index=len(sources),
                path=source_path,
                url=f"{COMMON_CRAWL_DATA_URL}/{source_path}",
            )
        )
    if not sources:
        raise ManifestParseError(f"{path}: URL-index WARC source inventory is empty")
    return tuple(sources)


def inspect_source_schema(
    connection: duckdb.DuckDBPyConnection, source: IndexSource
) -> SourceSchema:
    try:
        rows = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [source.url]
        ).fetchall()
    except duckdb.Error as error:
        raise SourceSchemaError(f"inspect source schema {source.url}: {error}") from error

    available = {str(row[0]): str(row[1]).upper() for row in rows}
    missing = [
        column
        for column in (*_REQUIRED_STRING_COLUMNS, *_REQUIRED_INTEGER_COLUMNS)
        if column not in available
    ]
    if missing:
        raise SourceSchemaError(
            f"source {source.url} is missing required columns: {', '.join(missing)}"
        )

    incompatible: list[str] = []
    for column in _REQUIRED_STRING_COLUMNS:
        if available[column] != "VARCHAR":
            incompatible.append(f"{column}={available[column]} (expected VARCHAR)")
    for column in _REQUIRED_INTEGER_COLUMNS:
        if available[column] not in _INTEGER_TYPES:
            incompatible.append(f"{column}={available[column]} (expected integral)")
    for column in _OPTIONAL_STRING_COLUMNS:
        if column in available and available[column] != "VARCHAR":
            incompatible.append(f"{column}={available[column]} (expected VARCHAR)")
    if incompatible:
        raise SourceSchemaError(
            f"source {source.url} has incompatible columns: {', '.join(incompatible)}"
        )

    relevant_columns = (
        *_REQUIRED_STRING_COLUMNS,
        *_REQUIRED_INTEGER_COLUMNS,
        *(
            column
            for column in _OPTIONAL_STRING_COLUMNS
            if column in available
        ),
    )
    return SourceSchema(
        source_index=source.source_index,
        column_types=tuple((column, available[column]) for column in relevant_columns),
    )


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    with path.open("rb") as manifest:
        while chunk := manifest.read(1024 * 1024):
            digest.update(chunk)
            byte_count += len(chunk)
    return digest.hexdigest(), byte_count


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    value = value.strip()
    if value.isdecimal():
        try:
            return float(int(value))
        except (ValueError, OverflowError):
            return None
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _download_once(
    client: httpx.Client,
    url: str,
    partial_path: Path | None,
    timeout_seconds: float,
) -> tuple[str, int]:
    with client.stream(
        "GET",
        url,
        headers={"Accept-Encoding": "identity"},
        timeout=timeout_seconds,
        follow_redirects=True,
    ) as response:
        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise _TransientDownloadError(
                f"HTTP {response.status_code}", retry_after=_retry_after_seconds(response)
            )
        if response.status_code != 200:
            raise ManifestDownloadError(f"download manifest {url}: HTTP {response.status_code}")
        digest = hashlib.sha256()
        byte_count = 0
        manifest = partial_path.open("wb") if partial_path is not None else None
        try:
            for chunk in response.iter_raw():
                if not chunk:
                    continue
                if manifest is not None:
                    manifest.write(chunk)
                digest.update(chunk)
                byte_count += len(chunk)
            if manifest is not None:
                manifest.flush()
                os.fsync(manifest.fileno())
        finally:
            if manifest is not None:
                manifest.close()
        return digest.hexdigest(), byte_count


def _download_with_retries(
    client: httpx.Client,
    url: str,
    partial_path: Path | None,
    *,
    attempts: int,
    timeout_seconds: float,
) -> tuple[str, int]:
    for attempt in range(1, attempts + 1):
        retry_after: float | None = None
        try:
            return _download_once(client, url, partial_path, timeout_seconds)
        except _TransientDownloadError as error:
            retry_after = error.retry_after
            last_error: Exception = error
        except httpx.TransportError as error:
            last_error = error
        except ManifestDownloadError:
            if partial_path is not None:
                partial_path.unlink(missing_ok=True)
            raise
        except OSError as error:
            if partial_path is not None:
                partial_path.unlink(missing_ok=True)
            operation = (
                f"download manifest {url}"
                if partial_path is None
                else f"write manifest snapshot {partial_path}"
            )
            raise ManifestDownloadError(
                f"{operation}: {error}"
            ) from error

        if partial_path is not None:
            partial_path.unlink(missing_ok=True)
        if attempt == attempts:
            raise ManifestDownloadError(
                f"download manifest {url} failed after {attempts} attempts: {last_error}"
            ) from last_error
        time.sleep(retry_after if retry_after is not None else float(2 ** (attempt - 1)))

    raise AssertionError("unreachable")


def download_manifest_digest(
    client: httpx.Client,
    url: str,
    *,
    attempts: int,
    timeout_seconds: float = 60.0,
) -> ManifestDigest:
    """Always download a manifest and return its exact compressed-byte digest."""
    if attempts <= 0:
        raise ValueError("attempts must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout must be positive")
    digest, byte_count = _download_with_retries(
        client,
        url,
        None,
        attempts=attempts,
        timeout_seconds=timeout_seconds,
    )
    return ManifestDigest(url=url, sha256=digest, byte_count=byte_count)


def recheck_crawl_manifests(
    client: httpx.Client,
    crawl: str,
    *,
    expected_warc_sha256: str,
    expected_index_sha256: str,
    attempts: int,
    timeout_seconds: float = 60.0,
) -> ManifestRecheckResult:
    """Freshly download both crawl manifests and reject any build-time change."""
    decode_sha256(expected_warc_sha256, "expected_warc_sha256")
    decode_sha256(expected_index_sha256, "expected_index_sha256")
    warc = download_manifest_digest(
        client,
        crawl_manifest_url(crawl, WARC_MANIFEST_FILENAME),
        attempts=attempts,
        timeout_seconds=timeout_seconds,
    )
    index = download_manifest_digest(
        client,
        crawl_manifest_url(crawl, INDEX_MANIFEST_FILENAME),
        attempts=attempts,
        timeout_seconds=timeout_seconds,
    )
    mismatches = [
        (name, expected, digest.sha256)
        for name, expected, digest in (
            ("warc.paths.gz", expected_warc_sha256, warc),
            ("cc-index-table.paths.gz", expected_index_sha256, index),
        )
        if digest.sha256 != expected
    ]
    if mismatches:
        raise ManifestChangedError(mismatches)
    return ManifestRecheckResult(warc=warc, index=index)


def download_manifest_snapshot(
    client: httpx.Client,
    url: str,
    destination: Path,
    *,
    attempts: int,
    timeout_seconds: float = 60.0,
) -> ManifestSnapshot:
    if attempts <= 0:
        raise ValueError("attempts must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout must be positive")
    if destination.is_symlink():
        raise ManifestDownloadError(f"manifest destination must not be a symlink: {destination}")
    if destination.exists():
        if not destination.is_file():
            raise ManifestDownloadError(f"manifest destination is not a file: {destination}")
        digest, byte_count = _hash_file(destination)
        return ManifestSnapshot(url, destination, digest, byte_count, reused=True)

    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ManifestDownloadError(f"create manifest directory {destination.parent}: {error}") from error

    partial_path = destination.with_name(f"{destination.name}.partial")
    if partial_path.is_dir():
        raise ManifestDownloadError(f"manifest partial path is a directory: {partial_path}")
    partial_path.unlink(missing_ok=True)

    digest, byte_count = _download_with_retries(
        client,
        url,
        partial_path,
        attempts=attempts,
        timeout_seconds=timeout_seconds,
    )
    try:
        os.replace(partial_path, destination)
        _fsync_directory(destination.parent)
    except OSError as error:
        partial_path.unlink(missing_ok=True)
        raise ManifestDownloadError(
            f"write manifest snapshot {destination}: {error}"
        ) from error
    return ManifestSnapshot(url, destination, digest, byte_count, reused=False)
