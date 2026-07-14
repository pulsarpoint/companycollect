"""Common Crawl manifest discovery and deterministic WARC size sampling."""

import gzip
import hashlib
import json
import os
import re
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import httpx

COMMON_CRAWL_DATA_URL = "https://data.commoncrawl.org"
WARC_MANIFEST_FILENAME = "warc.paths.gz"
INDEX_MANIFEST_FILENAME = "cc-index-table.paths.gz"
_CRAWL_ID = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")
_TRANSIENT_STATUS = frozenset({429, 500, 502, 503, 504})
_SAMPLE_VERSION = 1


class ManifestError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CrawlManifests:
    index_path: Path
    warc_path: Path


@dataclass(frozen=True, slots=True)
class IndexSource:
    source_index: int
    path: str
    url: str


@dataclass(frozen=True, slots=True)
class WarcObject:
    warc_index: int
    warc_filename: str


@dataclass(frozen=True, slots=True)
class WarcSize:
    warc_index: int
    warc_filename: str
    object_bytes: int


@dataclass(frozen=True, slots=True)
class WarcSizeSample:
    sizes: tuple[WarcSize, ...]
    reused: bool

    @property
    def average_bytes(self) -> float:
        return sum(size.object_bytes for size in self.sizes) / len(self.sizes)


def _require_crawl(crawl: str) -> None:
    if _CRAWL_ID.fullmatch(crawl) is None:
        raise ValueError("crawl must match CC-MAIN-YYYY-NN")


def _manifest_lines(path: Path) -> list[str]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as manifest:
            return [line.removesuffix("\n") for line in manifest]
    except (OSError, EOFError, UnicodeDecodeError) as error:
        raise ManifestError(f"read manifest {path}: {error}") from error


def _validate_object_path(path: Path, line_number: int, value: str) -> None:
    if not value or value != value.strip():
        raise ManifestError(f"{path}: invalid whitespace on line {line_number}")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ManifestError(
            f"{path}: invalid object path on line {line_number}: {value}"
        )


def _replace_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.partial")
    if path.is_symlink() or partial.is_symlink() or partial.is_dir():
        raise ManifestError(f"unsafe cache path: {path}")
    with partial.open("wb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    os.replace(partial, path)


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    retry_after = "" if response is None else response.headers.get("Retry-After", "")
    return (
        min(60.0, float(retry_after))
        if retry_after.isdecimal()
        else float(2 ** min(attempt, 5))
    )


def _download_manifest(
    client: httpx.Client, url: str, destination: Path, attempts: int, timeout: float
) -> None:
    last_error: Exception | None = None
    for attempt in range(attempts):
        response: httpx.Response | None = None
        try:
            response = client.get(
                url,
                headers={"Accept-Encoding": "identity"},
                follow_redirects=True,
                timeout=timeout,
            )
        except httpx.TransportError as error:
            last_error = error
        else:
            if response.status_code == 200:
                try:
                    _replace_bytes(destination, response.content)
                    _manifest_lines(destination)
                    return
                except ManifestError as error:
                    last_error = error
            elif response.status_code in _TRANSIENT_STATUS:
                last_error = ManifestError(f"HTTP {response.status_code}")
            else:
                raise ManifestError(f"download {url}: HTTP {response.status_code}")
        destination.unlink(missing_ok=True)
        if attempt + 1 < attempts:
            time.sleep(_retry_delay(response, attempt))
    raise ManifestError(
        f"download {url} failed after {attempts} attempts: {last_error}"
    )


def sync_manifests(
    client: httpx.Client,
    crawl: str,
    directory: Path,
    *,
    attempts: int = 5,
    timeout: float = 60.0,
) -> CrawlManifests:
    """Cache the two small crawl manifests, replacing unreadable local copies."""
    _require_crawl(crawl)
    if attempts < 1 or timeout <= 0:
        raise ValueError("attempts and timeout must be positive")
    paths: dict[str, Path] = {}
    for filename in (INDEX_MANIFEST_FILENAME, WARC_MANIFEST_FILENAME):
        validator = (
            read_index_sources
            if filename == INDEX_MANIFEST_FILENAME
            else read_warc_inventory
        )
        path = directory / filename
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ManifestError(f"manifest cache path is not a regular file: {path}")
        try:
            if path.is_file():
                validator(path, crawl)
                paths[filename] = path
                continue
        except ManifestError:
            path.unlink(missing_ok=True)
        url = f"{COMMON_CRAWL_DATA_URL}/crawl-data/{crawl}/{filename}"
        _download_manifest(client, url, path, attempts, timeout)
        validator(path, crawl)
        paths[filename] = path
    return CrawlManifests(paths[INDEX_MANIFEST_FILENAME], paths[WARC_MANIFEST_FILENAME])


def read_index_sources(path: Path, crawl: str) -> tuple[IndexSource, ...]:
    _require_crawl(crawl)
    sources: list[IndexSource] = []
    seen: set[str] = set()
    partition = f"/crawl={crawl}/subset=warc/"
    for line_number, source_path in enumerate(_manifest_lines(path), 1):
        _validate_object_path(path, line_number, source_path)
        if "/subset=warc/" not in source_path:
            continue
        valid = (
            partition in source_path
            and source_path.startswith("cc-index/table/cc-main/warc/")
            and source_path.endswith(".parquet")
        )
        if not valid or source_path in seen:
            reason = "duplicate" if source_path in seen else "invalid"
            raise ManifestError(f"{path}: {reason} WARC index source: {source_path}")
        seen.add(source_path)
        sources.append(
            IndexSource(
                len(sources), source_path, f"{COMMON_CRAWL_DATA_URL}/{source_path}"
            )
        )
    if not sources:
        raise ManifestError(f"{path}: no subset=warc Parquet sources")
    return tuple(sources)


def read_warc_inventory(path: Path, crawl: str) -> tuple[WarcObject, ...]:
    _require_crawl(crawl)
    warcs: list[WarcObject] = []
    seen: set[str] = set()
    prefix = f"crawl-data/{crawl}/"
    for line_number, filename in enumerate(_manifest_lines(path), 1):
        _validate_object_path(path, line_number, filename)
        valid = (
            filename.startswith(prefix)
            and "/warc/" in filename
            and filename.endswith(".warc.gz")
        )
        if not valid or filename in seen:
            reason = "duplicate" if filename in seen else "invalid"
            raise ManifestError(f"{path}: {reason} WARC object: {filename}")
        seen.add(filename)
        warcs.append(WarcObject(len(warcs), filename))
    if not warcs:
        raise ManifestError(f"{path}: empty WARC inventory")
    return tuple(warcs)


def _sample_objects(
    crawl: str, warcs: Sequence[WarcObject], count: int
) -> tuple[WarcObject, ...]:
    def sample_key(warc: WarcObject) -> bytes:
        value = f"warc-size-sample-v{_SAMPLE_VERSION}\0{crawl}\0{warc.warc_filename}"
        return hashlib.sha256(value.encode()).digest()

    selected = sorted(warcs, key=sample_key)[: min(count, len(warcs))]
    return tuple(sorted(selected, key=lambda warc: warc.warc_index))


def _head_warc_size(
    client: httpx.Client, warc: WarcObject, attempts: int, timeout: float
) -> WarcSize:
    url = f"{COMMON_CRAWL_DATA_URL}/{warc.warc_filename}"
    last_error: Exception | None = None
    for attempt in range(attempts):
        response: httpx.Response | None = None
        try:
            response = client.head(
                url,
                headers={"Accept-Encoding": "identity"},
                follow_redirects=True,
                timeout=timeout,
            )
        except httpx.TransportError as error:
            last_error = error
        else:
            if response.status_code == 200:
                try:
                    object_bytes = int(response.headers.get("Content-Length", "0"))
                except ValueError as error:
                    raise ManifestError(
                        f"HEAD {url}: invalid Content-Length"
                    ) from error
                if object_bytes <= 0:
                    raise ManifestError(f"HEAD {url}: missing positive Content-Length")
                return WarcSize(warc.warc_index, warc.warc_filename, object_bytes)
            if response.status_code not in _TRANSIENT_STATUS:
                raise ManifestError(f"HEAD {url}: HTTP {response.status_code}")
            last_error = ManifestError(f"HTTP {response.status_code}")
        if attempt + 1 < attempts:
            time.sleep(_retry_delay(response, attempt))
    raise ManifestError(f"HEAD {url} failed after {attempts} attempts: {last_error}")


def _read_sample_cache(
    path: Path, crawl: str, selected: Sequence[WarcObject]
) -> tuple[WarcSize, ...] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("version") != _SAMPLE_VERSION or payload.get("crawl") != crawl:
            return None
        sizes = tuple(WarcSize(**row) for row in payload["sizes"])
    except OSError, ValueError, TypeError, KeyError:
        return None
    expected = [(warc.warc_index, warc.warc_filename) for warc in selected]
    actual = [(size.warc_index, size.warc_filename) for size in sizes]
    return (
        sizes
        if actual == expected and all(size.object_bytes > 0 for size in sizes)
        else None
    )


def sample_warc_sizes(
    client: httpx.Client,
    crawl: str,
    warcs: Sequence[WarcObject],
    cache_path: Path,
    *,
    count: int = 256,
    workers: int = 32,
    attempts: int = 5,
    timeout: float = 30.0,
) -> WarcSizeSample:
    """HEAD a stable hash-selected sample and atomically cache every exact size."""
    _require_crawl(crawl)
    if not warcs or min(count, workers, attempts) < 1 or timeout <= 0:
        raise ValueError(
            "warcs, count, workers, attempts, and timeout must be positive"
        )
    selected = _sample_objects(crawl, warcs, count)
    if cache_path.is_symlink() or (cache_path.exists() and not cache_path.is_file()):
        raise ManifestError(f"sample cache path is not a regular file: {cache_path}")
    if cache_path.is_file() and (
        cached := _read_sample_cache(cache_path, crawl, selected)
    ):
        return WarcSizeSample(cached, reused=True)
    with ThreadPoolExecutor(max_workers=min(workers, len(selected))) as pool:
        sizes = tuple(
            pool.map(
                lambda warc: _head_warc_size(client, warc, attempts, timeout), selected
            )
        )
    payload = {
        "version": _SAMPLE_VERSION,
        "crawl": crawl,
        "sizes": [
            {
                "warc_index": size.warc_index,
                "warc_filename": size.warc_filename,
                "object_bytes": size.object_bytes,
            }
            for size in sizes
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    _replace_bytes(cache_path, encoded)
    return WarcSizeSample(sizes, reused=False)
