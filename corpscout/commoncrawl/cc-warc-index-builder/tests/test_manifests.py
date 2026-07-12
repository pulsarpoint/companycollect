import gzip
import hashlib
from pathlib import Path

import httpx
import pytest

import warc_index_builder.manifests as manifests
from warc_index_builder.manifests import (
    ManifestDownloadError,
    crawl_manifest_url,
    download_manifest_snapshot,
)


def _streaming_response(
    body: bytes, *, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(200, stream=httpx.ByteStream(body), headers=headers)


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
