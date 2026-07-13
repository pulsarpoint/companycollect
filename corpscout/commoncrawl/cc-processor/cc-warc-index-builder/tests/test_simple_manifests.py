import gzip
import hashlib
import threading
from pathlib import Path

import httpx
import pytest

from warc_index_builder import manifests


CRAWL = "CC-MAIN-2026-25"


def _gzip_lines(lines: list[str]) -> bytes:
    return gzip.compress(("\n".join(lines) + "\n").encode())


def _write_manifest(path: Path, lines: list[str]) -> None:
    path.write_bytes(_gzip_lines(lines))


def _index_source(number: int) -> str:
    return (
        "cc-index/table/cc-main/warc/"
        f"crawl={CRAWL}/subset=warc/part-{number:05d}.parquet"
    )


def _warc(number: int) -> str:
    return f"crawl-data/{CRAWL}/segments/segment/warc/file-{number}.warc.gz"


def test_sync_manifests_retries_then_reuses_valid_cache(tmp_path: Path) -> None:
    index_lines = [_index_source(number) for number in range(300)]
    warc_lines = [_warc(number) for number in range(4)]
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path.endswith(manifests.INDEX_MANIFEST_FILENAME):
            if requests.count(request.url.path) == 1:
                return httpx.Response(
                    503, headers={"Retry-After": "0"}, request=request
                )
            body = _gzip_lines(index_lines)
        else:
            body = _gzip_lines(warc_lines)
        return httpx.Response(200, content=body, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        first = manifests.sync_manifests(client, CRAWL, tmp_path, attempts=2, timeout=1)
        request_count = len(requests)
        second = manifests.sync_manifests(
            client, CRAWL, tmp_path, attempts=2, timeout=1
        )

    assert first == second
    assert len(requests) == request_count == 3
    assert len(manifests.read_index_sources(first.index_path, CRAWL)) == 300
    assert len(manifests.read_warc_inventory(first.warc_path, CRAWL)) == 4
    assert not list(tmp_path.glob("*.partial"))


def test_parsers_preserve_manifest_order_and_reject_duplicates(
    tmp_path: Path,
) -> None:
    index_path = tmp_path / "index.gz"
    expected_sources = [_index_source(number) for number in range(300)]
    _write_manifest(
        index_path,
        ["cc-index/table/cc-main/indexes/cdx.paths.gz", *expected_sources],
    )
    sources = manifests.read_index_sources(index_path, CRAWL)

    assert [source.source_index for source in sources] == list(range(300))
    assert [source.path for source in sources] == expected_sources
    assert (
        sources[17].url == f"{manifests.COMMON_CRAWL_DATA_URL}/{expected_sources[17]}"
    )

    warc_path = tmp_path / "warcs.gz"
    expected_warcs = [_warc(number) for number in (9, 2, 7)]
    _write_manifest(warc_path, expected_warcs)
    inventory = manifests.read_warc_inventory(warc_path, CRAWL)
    assert [(warc.warc_index, warc.warc_filename) for warc in inventory] == [
        (index, filename) for index, filename in enumerate(expected_warcs)
    ]

    _write_manifest(warc_path, [expected_warcs[0], expected_warcs[0]])
    with pytest.raises(manifests.ManifestError, match="duplicate WARC object"):
        manifests.read_warc_inventory(warc_path, CRAWL)


def test_head_sample_is_deterministic_bounded_and_cached(tmp_path: Path) -> None:
    warcs = tuple(manifests.WarcObject(index, _warc(index)) for index in range(20))
    lock = threading.Lock()
    release = threading.Event()
    active = 0
    peak = 0
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            requests.append((request.method, request.headers["Accept-Encoding"]))
            if active == 3:
                release.set()
        assert release.wait(timeout=1)
        number = int(Path(request.url.path).name.split("-")[1].split(".")[0])
        with lock:
            active -= 1
        return httpx.Response(
            200, headers={"Content-Length": str(1_000 + number)}, request=request
        )

    cache_path = tmp_path / "warc-size-sample.json"
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        sample = manifests.sample_warc_sizes(
            client,
            CRAWL,
            warcs,
            cache_path,
            count=8,
            workers=3,
            attempts=1,
            timeout=1,
        )

    def sample_key(warc: manifests.WarcObject) -> bytes:
        value = f"warc-size-sample-v1\0{CRAWL}\0{warc.warc_filename}"
        return hashlib.sha256(value.encode()).digest()

    expected = sorted(
        sorted(warcs, key=sample_key)[:8], key=lambda warc: warc.warc_index
    )
    assert [size.warc_index for size in sample.sizes] == [
        warc.warc_index for warc in expected
    ]
    assert sample.average_bytes == sum(1_000 + warc.warc_index for warc in expected) / 8
    assert not sample.reused
    assert peak == 3
    assert requests == [("HEAD", "identity")] * 8

    def reject_network(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("valid sample cache must avoid the network")

    with httpx.Client(transport=httpx.MockTransport(reject_network)) as client:
        cached = manifests.sample_warc_sizes(
            client,
            CRAWL,
            tuple(reversed(warcs)),
            cache_path,
            count=8,
            workers=3,
            attempts=1,
            timeout=1,
        )
    assert cached.reused
    assert cached.sizes == sample.sizes


def test_permanent_manifest_and_exhausted_head_fail_cleanly(
    tmp_path: Path,
) -> None:
    get_requests = 0

    def missing_manifest(request: httpx.Request) -> httpx.Response:
        nonlocal get_requests
        get_requests += 1
        return httpx.Response(404, request=request)

    with httpx.Client(transport=httpx.MockTransport(missing_manifest)) as client:
        with pytest.raises(manifests.ManifestError, match="HTTP 404"):
            manifests.sync_manifests(client, CRAWL, tmp_path / "manifests", attempts=5)
    assert get_requests == 1
    assert not (tmp_path / "manifests" / manifests.INDEX_MANIFEST_FILENAME).exists()

    head_requests = 0

    def unavailable_warc(request: httpx.Request) -> httpx.Response:
        nonlocal head_requests
        head_requests += 1
        return httpx.Response(503, headers={"Retry-After": "0"}, request=request)

    cache_path = tmp_path / "sample.json"
    with httpx.Client(transport=httpx.MockTransport(unavailable_warc)) as client:
        with pytest.raises(manifests.ManifestError, match="failed after 2 attempts"):
            manifests.sample_warc_sizes(
                client,
                CRAWL,
                (manifests.WarcObject(0, _warc(0)),),
                cache_path,
                count=1,
                workers=1,
                attempts=2,
                timeout=1,
            )
    assert head_requests == 2
    assert not cache_path.exists()
