"""Download immutable Common Crawl manifest snapshots."""

import gzip
import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path

import httpx


COMMON_CRAWL_DATA_URL = "https://data.commoncrawl.org"
WARC_MANIFEST_FILENAME = "warc.paths.gz"
INDEX_MANIFEST_FILENAME = "cc-index-table.paths.gz"
_TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class ManifestDownloadError(RuntimeError):
    pass


class ManifestParseError(RuntimeError):
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
class WarcObject:
    warc_index: int
    warc_filename: str


@dataclass(frozen=True, slots=True)
class IndexSource:
    source_index: int
    path: str
    url: str


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
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _download_once(
    client: httpx.Client,
    url: str,
    partial_path: Path,
    timeout_seconds: float,
) -> tuple[str, int]:
    with client.stream("GET", url, timeout=timeout_seconds, follow_redirects=True) as response:
        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise _TransientDownloadError(
                f"HTTP {response.status_code}", retry_after=_retry_after_seconds(response)
            )
        if response.status_code != 200:
            raise ManifestDownloadError(f"download manifest {url}: HTTP {response.status_code}")

        digest = hashlib.sha256()
        byte_count = 0
        with partial_path.open("wb") as manifest:
            for chunk in response.iter_raw():
                if not chunk:
                    continue
                manifest.write(chunk)
                digest.update(chunk)
                byte_count += len(chunk)
            manifest.flush()
            os.fsync(manifest.fileno())
        return digest.hexdigest(), byte_count


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

    for attempt in range(1, attempts + 1):
        retry_after: float | None = None
        try:
            digest, byte_count = _download_once(client, url, partial_path, timeout_seconds)
            os.replace(partial_path, destination)
            _fsync_directory(destination.parent)
            return ManifestSnapshot(url, destination, digest, byte_count, reused=False)
        except _TransientDownloadError as error:
            retry_after = error.retry_after
            last_error: Exception = error
        except httpx.TransportError as error:
            last_error = error
        except ManifestDownloadError:
            partial_path.unlink(missing_ok=True)
            raise
        except OSError as error:
            partial_path.unlink(missing_ok=True)
            raise ManifestDownloadError(f"write manifest snapshot {destination}: {error}") from error

        partial_path.unlink(missing_ok=True)
        if attempt == attempts:
            raise ManifestDownloadError(
                f"download manifest {url} failed after {attempts} attempts: {last_error}"
            ) from last_error
        time.sleep(retry_after if retry_after is not None else float(2 ** (attempt - 1)))

    raise AssertionError("unreachable")
