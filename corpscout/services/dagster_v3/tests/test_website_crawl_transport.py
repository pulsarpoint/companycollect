import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest
import requests
from requests.exceptions import ConnectionError, InvalidURL, ReadTimeout, SSLError

from dagster_v3.defs.website_crawl.matching_batches import (
    CrawlerUnavailable,
    process_matching_batch,
    service_request,
)


def response(status=202, **state):
    return SimpleNamespace(status_code=status, json=lambda: state)


def item(**request):
    return {
        "request_id": "crawl-request",
        "request_json": json.dumps(
            {"request_id": "crawl-request", "url": "https://example.se/", **request}
        ),
        "crawl_type": "site_info",
        "input_revision": 1,
        "work_key": "work",
        "run_id": "execution",
    }


def test_admission_replays_exact_payload_with_longer_timeout_and_safe_diagnostics():
    payload = {"batch_id": "crawl-batch-stable", "entries": [{"secret": "payload-secret"}]}
    failure = ReadTimeout("https://user:password@crawler:8090/path?token=query-secret")
    failure.__cause__ = TimeoutError("Bearer bearer-secret\nforged log")
    http, log = Mock(), Mock()
    http.request.side_effect = [failure, response(state="running")]
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep") as sleep:
        assert service_request(
            http, "http://user:password@crawler:8090/private?token=query-secret",
            "POST", "/v1/crawl-batches", payload, log=log,
        ) == {"state": "running"}
    assert http.request.call_count == 2
    for attempt in http.request.call_args_list:
        assert attempt.kwargs["json"] is payload
        assert attempt.kwargs["timeout"] == (5, 180)
        assert attempt.kwargs["allow_redirects"] is False
    sleep.assert_called_once_with(2)
    detail = log.warning.call_args.args[1]
    assert "ReadTimeout <- TimeoutError" in detail
    assert "endpoint='http://crawler:8090' path='/v1/crawl-batches'" in detail
    assert "attempt=1/3" in detail
    for secret in ("password", "query-secret", "payload-secret", "bearer-secret", "forged log"):
        assert secret not in detail


def test_admission_retries_transient_statuses_with_bounded_backoff():
    http = Mock()
    http.request.side_effect = [response(503), response(408), response(state="running")]
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep") as sleep:
        assert service_request(
            http, "http://crawler", "POST", "/v1/crawl-batches", {"batch_id": "stable"}
        ) == {"state": "running"}
    assert sleep.call_args_list == [call(2), call(4)]
    assert http.request.call_count == 3


def test_invalid_admission_response_retains_ambiguous_batch():
    http = Mock()
    http.request.return_value = SimpleNamespace(status_code=202, json=Mock(side_effect=ValueError("secret")))
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep"):
        with pytest.raises(CrawlerUnavailable, match="InvalidJSONResponse.*batch retained"):
            process_matching_batch(SimpleNamespace(log=Mock()), Mock(), http, "http://crawler", [item()])
    assert [attempt.args[0] for attempt in http.request.call_args_list] == ["POST"] * 3


def test_dropped_admission_response_replays_identical_wire_payload():
    bodies = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            bodies.append(self.rfile.read(int(self.headers["Content-Length"])))
            if len(bodies) == 1:
                # The server accepted the first request but lost its response.
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            body = b'{"state": "running"}'
            self.send_response(202)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    payload = {"batch_id": "stable", "entries": [item()]}
    try:
        with (
            requests.Session() as http,
            patch("dagster_v3.defs.website_crawl.matching_batches.sleep"),
        ):
            http.trust_env = False
            state = service_request(http, f"http://127.0.0.1:{server.server_port}", "POST", "/v1/crawl-batches", payload)
        assert state == {"state": "running"}
        assert len(bodies) == 2
        assert bodies[0] == bodies[1]
        assert json.loads(bodies[0]) == payload
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


def test_ambiguous_admission_does_not_cancel_accepted_batch():
    http = Mock()
    http.request.side_effect = ReadTimeout("response lost after server accepted the batch")
    saved = item()
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep"):
        with pytest.raises(CrawlerUnavailable, match="ReadTimeout.*attempt=3/3.*batch retained"):
            process_matching_batch(SimpleNamespace(log=Mock()), Mock(), http, "http://crawler", [saved])
    assert [attempt.args[0] for attempt in http.request.call_args_list] == ["POST"] * 3
    submitted = [attempt.kwargs["json"] for attempt in http.request.call_args_list]
    assert all(payload is submitted[0] for payload in submitted)
    assert submitted[0]["entries"][0]["request"] == json.loads(saved["request_json"])
    assert submitted[0]["run_id"] == "execution"


def test_lost_poll_does_not_cancel_running_batch():
    http = Mock()
    http.request.side_effect = [
        response(state="running", processed=0, total=1, matched=0, failed=0),
        *[ConnectionError("transport-secret") for _ in range(6)],
    ]
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep"):
        with pytest.raises(CrawlerUnavailable, match="GET.*ConnectionError.*attempt=6/6"):
            process_matching_batch(SimpleNamespace(log=Mock()), Mock(), http, "http://crawler", [item()])
    assert [attempt.args[0] for attempt in http.request.call_args_list] == ["POST"] + ["GET"] * 6
    assert all(attempt.kwargs["timeout"] == (5, 30) for attempt in http.request.call_args_list[1:])


@pytest.mark.parametrize("method,path,payload", [
    ("DELETE", "/v1/crawl-batches/stable", None),
    ("POST", "/v1/crawls", {"request_id": "stable"}),
    ("POST", "/v1/crawl-batches", {"entries": []}),
])
def test_other_mutations_are_not_retried(method, path, payload):
    http = Mock()
    http.request.side_effect = ReadTimeout("response lost")
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep") as sleep:
        with pytest.raises(RuntimeError, match="ReadTimeout.*attempt=1/1"):
            service_request(http, "http://crawler", method, path, payload)
    http.request.assert_called_once()
    sleep.assert_not_called()


@pytest.mark.parametrize("failure", [
    InvalidURL("private URL"), SSLError("private certificate"), response(401), response(409),
])
def test_permanent_admission_failure_is_not_retried(failure):
    http = Mock()
    http.request.side_effect = [failure]
    with patch("dagster_v3.defs.website_crawl.matching_batches.sleep") as sleep:
        with pytest.raises(RuntimeError) as caught:
            service_request(http, "http://crawler", "POST", "/v1/crawl-batches", {"batch_id": "stable"})
    assert not isinstance(caught.value, CrawlerUnavailable)
    assert "private" not in str(caught.value)
    http.request.assert_called_once()
    sleep.assert_not_called()


def test_revoked_admission_still_cancels_running_batch_once():
    http = Mock()
    http.request.side_effect = [
        response(state="running", processed=0, total=1, matched=0, failed=0),
        ReadTimeout("cancellation response lost"),
    ]
    context = SimpleNamespace(log=Mock())
    with (
        patch("dagster_v3.defs.website_crawl.matching_batches.sleep") as sleep,
        patch("dagster_v3.defs.website_crawl.matching_batches.check_admission", side_effect=[None, RuntimeError("profile revoked")]),
    ):
        with pytest.raises(RuntimeError, match="profile revoked"):
            process_matching_batch(context, Mock(), http, "http://crawler", [item(llm={"provider": "test"})])
    assert [attempt.args[0] for attempt in http.request.call_args_list] == ["POST", "DELETE"]
    context.log.warning.assert_called_once()
    sleep.assert_not_called()
