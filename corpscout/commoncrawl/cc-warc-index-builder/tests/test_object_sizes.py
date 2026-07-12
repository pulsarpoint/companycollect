from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from email.utils import format_datetime

import httpx
import pytest

from warc_index_builder.manifests import WarcObject
from warc_index_builder.object_sizes import (
    PermanentWarcSizeError,
    TransientWarcSizeError,
    parse_retry_after,
    probe_warc_size_once,
    retry_delay_seconds,
    warc_object_url,
)


_WARC = WarcObject(
    17,
    "crawl-data/CC-MAIN-2026-25/segments/example/warc/example.warc.gz",
)
_RANGE_HEADERS = {
    "Content-Range": "bytes 0-0/987654",
    "Content-Length": "1",
}


def _response(
    status_code: int,
    body: bytes = b"",
    *,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers=headers,
        stream=httpx.ByteStream(body),
    )


def _range_success(body: bytes = b"x", **headers: str) -> httpx.Response:
    return _response(206, body, headers={**_RANGE_HEADERS, **headers})


def test_probe_accepts_positive_head_content_length_without_range() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Accept-Encoding"] == "identity"
        return _response(200, headers={"Content-Length": "123456"})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = probe_warc_size_once(client, _WARC)

    assert result.warc_index == 17
    assert result.warc_filename == _WARC.warc_filename
    assert result.object_bytes == 123456
    assert result.used_range_fallback is False
    assert [request.method for request in requests] == ["HEAD"]
    assert result.metrics.head_requests == 1
    assert result.metrics.range_requests == 0
    assert result.metrics.http_requests == 1


def test_probe_falls_back_when_head_reports_encoded_representation() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "HEAD":
            return _response(
                200,
                headers={"Content-Length": "123", "Content-Encoding": "gzip"},
            )
        return _range_success()

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = probe_warc_size_once(client, _WARC)

    assert result.object_bytes == 987654
    assert result.used_range_fallback is True
    assert [request.method for request in requests] == ["HEAD", "GET"]


@pytest.mark.parametrize(
    "content_length",
    [None, "0", "not-a-number", str(2**64), "+1", " 1", "9" * 5000],
)
def test_probe_falls_back_to_range_for_unusable_head_length(
    content_length: str | None,
) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "HEAD":
            headers = {} if content_length is None else {"Content-Length": content_length}
            return _response(200, headers=headers)
        assert request.headers["Range"] == "bytes=0-0"
        assert request.headers["Accept-Encoding"] == "identity"
        return _range_success()

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = probe_warc_size_once(client, _WARC)

    assert result.object_bytes == 987654
    assert result.used_range_fallback is True
    assert [request.method for request in requests] == ["HEAD", "GET"]
    assert result.metrics.head_requests == 1
    assert result.metrics.range_requests == 1


@pytest.mark.parametrize("status_code", [405, 501])
def test_probe_falls_back_when_head_cannot_supply_metadata(status_code: int) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return _response(status_code) if request.method == "HEAD" else _range_success()

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = probe_warc_size_once(client, _WARC)

    assert result.object_bytes == 987654
    assert result.used_range_fallback is True


@pytest.mark.parametrize("status_code", [400, 403, 404, 416])
def test_probe_classifies_permanent_head_status_without_range(status_code: int) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(status_code)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(PermanentWarcSizeError) as caught:
            probe_warc_size_once(client, _WARC)

    assert [request.method for request in requests] == ["HEAD"]
    assert caught.value.method == "HEAD"
    assert caught.value.status_code == status_code
    assert caught.value.metrics.http_requests == 1


@pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
def test_probe_classifies_transient_head_status_without_range(status_code: int) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _response(status_code, headers={"Retry-After": "7"})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(TransientWarcSizeError) as caught:
            probe_warc_size_once(client, _WARC)

    assert [request.method for request in requests] == ["HEAD"]
    assert caught.value.status_code == status_code
    assert caught.value.retry_after_seconds == 7.0
    assert caught.value.throttled is (status_code in {429, 503})
    assert caught.value.metrics.http_429 == (1 if status_code == 429 else 0)
    assert caught.value.metrics.http_503 == (1 if status_code == 503 else 0)


@pytest.mark.parametrize(
    "content_range",
    [
        "",
        "bytes 0-1/100",
        "bytes 1-1/100",
        "bytes 0-0/*",
        "bytes 0-0/0",
        f"bytes 0-0/{2**64}",
        "bytes 0-0/100 extra",
        "Bytes 0-0/100",
    ],
)
def test_probe_rejects_non_exact_content_range(content_range: str) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return _response(200)
        return _range_success(**{"Content-Range": content_range})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(PermanentWarcSizeError, match="invalid Content-Range"):
            probe_warc_size_once(client, _WARC)


@pytest.mark.parametrize("content_length", [None, "0", "2", "01", "not-a-number"])
def test_probe_requires_exact_one_byte_range_content_length(
    content_length: str | None,
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return _response(200)
        headers = {"Content-Range": _RANGE_HEADERS["Content-Range"]}
        if content_length is not None:
            headers["Content-Length"] = content_length
        return _response(206, b"x", headers=headers)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(PermanentWarcSizeError, match="invalid Content-Length"):
            probe_warc_size_once(client, _WARC)


@pytest.mark.parametrize("content_encoding", ["gzip", "br", "gzip, identity"])
def test_probe_rejects_encoded_range_response(content_encoding: str) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return _response(200)
        return _range_success(**{"Content-Encoding": content_encoding})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(PermanentWarcSizeError, match="invalid Content-Encoding"):
            probe_warc_size_once(client, _WARC)


@pytest.mark.parametrize("body", [b"", b"xy"])
def test_probe_requires_exact_one_byte_range_body(body: bytes) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return _response(200) if request.method == "HEAD" else _range_success(body)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(PermanentWarcSizeError, match="body bytes"):
            probe_warc_size_once(client, _WARC)


def test_probe_never_reads_body_when_server_ignores_range() -> None:
    class RejectBodyRead(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            raise AssertionError("a non-206 WARC body must not be read")

    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return _response(200)
        return httpx.Response(200, stream=RejectBodyRead())

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(PermanentWarcSizeError, match="GET returned HTTP 200"):
            probe_warc_size_once(client, _WARC)


def test_probe_follows_redirects_for_head_and_range() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "data.commoncrawl.org":
            return _response(307, headers={"Location": "https://storage.example/object"})
        if request.method == "HEAD":
            return _response(405)
        return _range_success()

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = probe_warc_size_once(client, _WARC)

    assert result.object_bytes == 987654
    assert [(request.method, request.url.host) for request in requests] == [
        ("HEAD", "data.commoncrawl.org"),
        ("HEAD", "storage.example"),
        ("GET", "data.commoncrawl.org"),
        ("GET", "storage.example"),
    ]


@pytest.mark.parametrize(
    "error_factory",
    [
        lambda request: httpx.ConnectTimeout("timeout", request=request),
        lambda request: httpx.ConnectError("reset", request=request),
        lambda request: httpx.RemoteProtocolError("unexpected EOF", request=request),
    ],
)
def test_probe_classifies_transport_interruptions_as_transient(
    error_factory: Callable[[httpx.Request], httpx.HTTPError],
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise error_factory(request)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(TransientWarcSizeError) as caught:
            probe_warc_size_once(client, _WARC)

    assert caught.value.method == "HEAD"
    assert caught.value.status_code is None
    assert caught.value.retry_after_seconds is None
    assert caught.value.metrics.head_requests == 1


class _InterruptedRangeBody(httpx.SyncByteStream):
    def __iter__(self) -> Iterator[bytes]:
        raise httpx.ReadError("simulated interrupted body")


def test_probe_classifies_interrupted_range_body_as_transient() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return _response(200)
        return httpx.Response(206, headers=_RANGE_HEADERS, stream=_InterruptedRangeBody())

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(TransientWarcSizeError) as caught:
            probe_warc_size_once(client, _WARC)

    assert caught.value.method == "GET"
    assert caught.value.metrics.head_requests == 1
    assert caught.value.metrics.range_requests == 1


def test_probe_range_transient_status_carries_both_request_metrics() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return _response(405) if request.method == "HEAD" else _response(503)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(TransientWarcSizeError) as caught:
            probe_warc_size_once(client, _WARC)

    assert caught.value.method == "GET"
    assert caught.value.throttled is True
    assert caught.value.metrics.head_requests == 1
    assert caught.value.metrics.range_requests == 1
    assert caught.value.metrics.http_503 == 1


def test_probe_closes_head_and_range_streams() -> None:
    class TrackingStream(httpx.SyncByteStream):
        def __init__(self, body: bytes) -> None:
            self.body = body
            self.closed = False

        def __iter__(self) -> Iterator[bytes]:
            yield self.body

        def close(self) -> None:
            self.closed = True

    streams: list[TrackingStream] = []

    def respond(request: httpx.Request) -> httpx.Response:
        stream = TrackingStream(b"do-not-read" if request.method == "HEAD" else b"x")
        streams.append(stream)
        headers = {} if request.method == "HEAD" else _RANGE_HEADERS
        return httpx.Response(200 if request.method == "HEAD" else 206, headers=headers, stream=stream)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        probe_warc_size_once(client, _WARC)

    assert [stream.closed for stream in streams] == [True, True]


def test_parse_retry_after_accepts_delta_seconds_and_http_dates() -> None:
    now = datetime(2026, 7, 12, 8, 0, tzinfo=timezone.utc)
    assert parse_retry_after("17", now=now) == 17.0
    assert parse_retry_after(format_datetime(now.replace(minute=1)), now=now) == 60.0
    assert parse_retry_after(format_datetime(now.replace(minute=0, second=0)), now=now) == 0.0


@pytest.mark.parametrize(
    "value", [None, "", "-1", "0.25", "tomorrow", "9" * 5000]
)
def test_parse_retry_after_rejects_malformed_values(value: str | None) -> None:
    now = datetime(2026, 7, 12, 8, 0, tzinfo=timezone.utc)
    assert parse_retry_after(value, now=now) is None


@pytest.mark.parametrize(
    "attempt,fraction,expected",
    [
        (1, 0.0, 0.5),
        (1, 1.0, 1.0),
        (2, 0.0, 1.0),
        (6, 1.0, 30.0),
        (20, 1.0, 30.0),
        (1_000_000, 1.0, 30.0),
    ],
)
def test_retry_delay_uses_capped_equal_jitter(
    attempt: int, fraction: float, expected: float
) -> None:
    assert retry_delay_seconds(
        attempt,
        retry_after_seconds=None,
        random_fraction=lambda: fraction,
    ) == expected


def test_retry_after_dominates_local_jitter() -> None:
    assert retry_delay_seconds(
        1,
        retry_after_seconds=45.0,
        random_fraction=lambda: 0.0,
    ) == 45.0


@pytest.mark.parametrize(
    "attempt,retry_after,fraction",
    [(0, None, 0.5), (1, -1.0, 0.5), (1, None, -0.1), (1, None, 1.1)],
)
def test_retry_delay_rejects_invalid_inputs(
    attempt: int, retry_after: float | None, fraction: float
) -> None:
    with pytest.raises(ValueError):
        retry_delay_seconds(
            attempt,
            retry_after_seconds=retry_after,
            random_fraction=lambda: fraction,
        )


def test_warc_object_url_uses_common_crawl_data_endpoint() -> None:
    assert warc_object_url(_WARC.warc_filename) == (
        "https://data.commoncrawl.org/"
        "crawl-data/CC-MAIN-2026-25/segments/example/warc/example.warc.gz"
    )


@pytest.mark.parametrize("filename", ["", "/absolute.warc.gz"])
def test_warc_object_url_rejects_invalid_relative_paths(filename: str) -> None:
    with pytest.raises(ValueError):
        warc_object_url(filename)
