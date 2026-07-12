"""Download immutable Common Crawl manifest snapshots."""

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


def crawl_manifest_url(crawl: str, filename: str) -> str:
    return f"{COMMON_CRAWL_DATA_URL}/crawl-data/{crawl}/{filename}"


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
