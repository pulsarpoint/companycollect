import gzip
import hashlib
from pathlib import Path

import httpx
import pytest

import warc_index_builder.manifests as manifests
from warc_index_builder.manifests import (
    ManifestDownloadError,
    ManifestParseError,
    crawl_manifest_url,
    download_manifest_snapshot,
    read_index_sources,
    read_warc_inventory,
)


def _streaming_response(
    body: bytes, *, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(200, stream=httpx.ByteStream(body), headers=headers)


def _write_gzip_lines(path: Path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as manifest:
        for line in lines:
            manifest.write(f"{line}\n")


def test_crawl_manifest_url() -> None:
    assert crawl_manifest_url("CC-MAIN-2026-25", "warc.paths.gz") == (
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-25/warc.paths.gz"
    )


def test_download_snapshot_preserves_exact_response_bytes(tmp_path: Path) -> None:
    payload = gzip.compress(b"crawl-data/example.warc.gz\n")

    def respond(_request: httpx.Request) -> httpx.Response:
        return _streaming_response(payload, headers={"Content-Encoding": "gzip"})

    destination = tmp_path / "manifests/warc.paths.gz"
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        snapshot = download_manifest_snapshot(client, "https://example/warc.paths.gz", destination, attempts=1)

    assert destination.read_bytes() == payload
    assert snapshot.sha256 == hashlib.sha256(payload).hexdigest()
    assert snapshot.byte_count == len(payload)
    assert snapshot.reused is False
    assert destination.with_name("warc.paths.gz.partial").exists() is False


def test_existing_snapshot_is_reused_without_http(tmp_path: Path) -> None:
    destination = tmp_path / "warc.paths.gz"
    destination.write_bytes(b"existing")

    def reject_request(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP must not be called for a reusable snapshot")

    with httpx.Client(transport=httpx.MockTransport(reject_request)) as client:
        snapshot = download_manifest_snapshot(client, "https://example/warc.paths.gz", destination, attempts=1)

    assert snapshot.reused is True
    assert snapshot.sha256 == hashlib.sha256(b"existing").hexdigest()


def test_transient_status_retries_and_honors_retry_after(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_count = 0
    delays: list[float] = []

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            return httpx.Response(503, headers={"Retry-After": "0.25"})
        return _streaming_response(b"complete")

    monkeypatch.setattr(manifests.time, "sleep", delays.append)
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        snapshot = download_manifest_snapshot(
            client, "https://example/index.paths.gz", tmp_path / "index.paths.gz", attempts=2
        )

    assert snapshot.byte_count == len(b"complete")
    assert request_count == 2
    assert delays == [0.25]


def test_transport_error_retries_with_exponential_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_count = 0
    delays: list[float] = []

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            raise httpx.ConnectTimeout("simulated timeout", request=request)
        return _streaming_response(b"complete")

    monkeypatch.setattr(manifests.time, "sleep", delays.append)
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        download_manifest_snapshot(
            client, "https://example/index.paths.gz", tmp_path / "index.paths.gz", attempts=2
        )

    assert request_count == 2
    assert delays == [1.0]


def test_permanent_http_error_is_not_retried(tmp_path: Path) -> None:
    request_count = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ManifestDownloadError, match="HTTP 404"):
            download_manifest_snapshot(
                client, "https://example/missing.gz", tmp_path / "missing.gz", attempts=3
            )

    assert request_count == 1


def test_retry_exhaustion_preserves_no_partial_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "index.paths.gz"
    monkeypatch.setattr(manifests.time, "sleep", lambda _delay: None)

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ManifestDownloadError, match="failed after 2 attempts"):
            download_manifest_snapshot(client, "https://example/index.paths.gz", destination, attempts=2)

    assert destination.exists() is False
    assert destination.with_name("index.paths.gz.partial").exists() is False


class _InterruptedStream(httpx.SyncByteStream):
    def __iter__(self):
        yield b"partial"
        raise httpx.ReadError("simulated interrupted body")


def test_interrupted_body_removes_partial_file(tmp_path: Path) -> None:
    destination = tmp_path / "index.paths.gz"

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_InterruptedStream())

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ManifestDownloadError, match="failed after 1 attempts"):
            download_manifest_snapshot(client, "https://example/index.paths.gz", destination, attempts=1)

    assert destination.exists() is False
    assert destination.with_name("index.paths.gz.partial").exists() is False


def test_stale_partial_is_replaced(tmp_path: Path) -> None:
    destination = tmp_path / "index.paths.gz"
    partial = destination.with_name("index.paths.gz.partial")
    partial.write_bytes(b"stale")

    def respond(_request: httpx.Request) -> httpx.Response:
        return _streaming_response(b"fresh")

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        download_manifest_snapshot(client, "https://example/index.paths.gz", destination, attempts=1)

    assert destination.read_bytes() == b"fresh"
    assert partial.exists() is False


@pytest.mark.parametrize("attempts, timeout", [(0, 60.0), (1, 0.0)])
def test_invalid_download_limits_are_rejected(
    tmp_path: Path, attempts: int, timeout: float
) -> None:
    with httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(200))) as client:
        with pytest.raises(ValueError):
            download_manifest_snapshot(
                client,
                "https://example/index.paths.gz",
                tmp_path / "index.paths.gz",
                attempts=attempts,
                timeout_seconds=timeout,
            )


def test_warc_inventory_preserves_published_order(tmp_path: Path) -> None:
    path = tmp_path / "warc.paths.gz"
    filenames = [
        "crawl-data/CC-MAIN-2016-22/segments/one/warc/CC-MAIN-one-00002.warc.gz",
        "crawl-data/CC-MAIN-2016-22/segments/one/warc/CC-MAIN-one-00000.warc.gz",
        "crawl-data/CC-MAIN-2016-22/segments/two/warc/CC-MAIN-two-00001.warc.gz",
    ]
    _write_gzip_lines(path, filenames)

    inventory = read_warc_inventory(path, "CC-MAIN-2016-22")

    assert [item.warc_index for item in inventory] == [0, 1, 2]
    assert [item.warc_filename for item in inventory] == filenames


@pytest.mark.parametrize(
    "lines, error",
    [
        ([], "inventory is empty"),
        ([""], "is blank"),
        ([" crawl-data/CC-MAIN-2016-22/a/warc/a.warc.gz"], "surrounding whitespace"),
        (["crawl-data/CC-MAIN-2016-22/../warc/a.warc.gz"], "invalid object path"),
        (["crawl-data/CC-MAIN-2026-25/a/warc/a.warc.gz"], "outside crawl"),
        (["crawl-data/CC-MAIN-2016-22/a/wet/a.warc.gz"], "not a WARC object"),
        (["crawl-data/CC-MAIN-2016-22/a/warc/a.txt"], "not a WARC object"),
        (
            [
                "crawl-data/CC-MAIN-2016-22/a/warc/a.warc.gz",
                "crawl-data/CC-MAIN-2016-22/a/warc/a.warc.gz",
            ],
            "duplicates WARC path",
        ),
    ],
)
def test_invalid_warc_inventory_is_rejected(
    tmp_path: Path, lines: list[str], error: str
) -> None:
    path = tmp_path / "warc.paths.gz"
    _write_gzip_lines(path, lines)

    with pytest.raises(ManifestParseError, match=error):
        read_warc_inventory(path, "CC-MAIN-2016-22")


def test_index_sources_filter_subset_and_preserve_manifest_order(tmp_path: Path) -> None:
    path = tmp_path / "cc-index-table.paths.gz"
    prefix = "cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25"
    second = f"{prefix}/subset=warc/part-00002-id.c000.gz.parquet"
    first = f"{prefix}/subset=warc/part-00000-id.c000.gz.parquet"
    same_part = f"{prefix}/subset=warc/part-00000-id.c001.gz.parquet"
    _write_gzip_lines(
        path,
        [
            f"{prefix}/subset=crawldiagnostics/part-00000.parquet",
            second,
            f"{prefix}/subset=robotstxt/part-00000.parquet",
            first,
            same_part,
        ],
    )

    sources = read_index_sources(path, "CC-MAIN-2026-25")

    assert [source.source_index for source in sources] == [0, 1, 2]
    assert [source.path for source in sources] == [second, first, same_part]
    assert sources[0].url == f"https://data.commoncrawl.org/{second}"


@pytest.mark.parametrize(
    "lines, error",
    [
        ([], "source inventory is empty"),
        ([""], "is blank"),
        (
            ["cc-index/table/cc-main/warc/../crawl=CC-MAIN-2026-25/subset=warc/a.parquet"],
            "invalid object path",
        ),
        (
            [
                "cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/a.parquet",
                "cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/a.parquet",
            ],
            "duplicates index path",
        ),
        (
            ["cc-index/table/cc-main/warc/crawl=CC-MAIN-2016-22/subset=warc/a.parquet"],
            "outside crawl",
        ),
        (
            ["wrong/crawl=CC-MAIN-2026-25/subset=warc/a.parquet"],
            "invalid index prefix",
        ),
        (
            ["cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/a.orc"],
            "not a Parquet source",
        ),
        (
            ["cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=robotstxt/a.parquet"],
            "source inventory is empty",
        ),
    ],
)
def test_invalid_index_source_inventory_is_rejected(
    tmp_path: Path, lines: list[str], error: str
) -> None:
    path = tmp_path / "cc-index-table.paths.gz"
    _write_gzip_lines(path, lines)

    with pytest.raises(ManifestParseError, match=error):
        read_index_sources(path, "CC-MAIN-2026-25")


@pytest.mark.parametrize("reader", [read_warc_inventory, read_index_sources])
def test_corrupt_gzip_manifest_is_rejected(tmp_path: Path, reader) -> None:
    path = tmp_path / "corrupt.paths.gz"
    path.write_bytes(b"not gzip")

    with pytest.raises(ManifestParseError, match="read gzip manifest"):
        reader(path, "CC-MAIN-2026-25")
