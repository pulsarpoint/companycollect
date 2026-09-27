"""ProviderReconResource against a real local HTTP server."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from dagster_v3.defs.provider_recon.resource import ProviderReconError, ProviderReconResource


class _API(BaseHTTPRequestHandler):
    state: dict = {}

    def _send(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        self.state.setdefault("bodies", []).append(json.loads(self.rfile.read(length) or b"{}"))
        if self.state.get("busy"):
            self._send(409, {"error": "an operation is already running", "run_id": "r0"})
        else:
            self._send(202, {"run_id": "r1", "status": "running"})

    def do_GET(self) -> None:
        polls = self.state["polls"] = self.state.get("polls", 0) + 1
        if self.state.get("missing"):
            self._send(404, {"error": "no such run"})
            return
        if polls <= self.state.get("fail_first", 0):
            self._send(503, {"error": "restarting"})
            return
        status = "running" if polls < 3 else self.state.get("final", "succeeded")
        self._send(200, {"run_id": "r1", "status": status, "changed": ["aws"], "unchanged_count": 36, "issues": []})

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def api():
    _API.state = {}
    server = HTTPServer(("127.0.0.1", 0), _API)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield ProviderReconResource(api_url=f"http://127.0.0.1:{server.server_port}"), _API.state
    server.shutdown()


def test_start_and_wait_until_succeeded(api) -> None:
    resource, state = api
    started = resource.start_collect(["aws"])
    assert started["run_id"] == "r1"
    assert state["bodies"] == [{"providers": ["aws"]}]
    run = resource.wait_for_run("r1", poll_s=0, sleep=lambda _: None)
    assert run["status"] == "succeeded" and state["polls"] == 3


def test_busy_service_raises(api) -> None:
    resource, state = api
    state["busy"] = True
    with pytest.raises(ProviderReconError, match="busy with run r0"):
        resource.start_collect()


def test_wait_times_out(api) -> None:
    resource, _ = api
    ticks = iter([0.0, 0.0, 10.0, 20.0])
    with pytest.raises(ProviderReconError, match="still running"):
        resource.wait_for_run("r1", timeout_s=5, poll_s=0, sleep=lambda _: None, clock=lambda: next(ticks))



def test_wait_tolerates_transient_poll_failures(api) -> None:
    resource, state = api
    state["fail_first"] = 2
    run = resource.wait_for_run("r1", poll_s=0, sleep=lambda _: None)
    assert run["status"] == "succeeded"


def test_wait_gives_up_after_consecutive_poll_failures(api) -> None:
    resource, state = api
    state["fail_first"] = 100
    with pytest.raises(ProviderReconError, match="failed 5 times in a row"):
        resource.wait_for_run("r1", poll_s=0, sleep=lambda _: None)


def test_unreachable_service_counts_as_transient() -> None:
    resource = ProviderReconResource(api_url="http://127.0.0.1:9", request_timeout_s=1)
    with pytest.raises(ProviderReconError, match="failed 5 times in a row"):
        resource.wait_for_run("r1", poll_s=0, sleep=lambda _: None)


def test_lost_run_is_explained(api) -> None:
    resource, state = api
    state["missing"] = True
    with pytest.raises(ProviderReconError, match="restarted or evicted"):
        resource.wait_for_run("r1", poll_s=0, sleep=lambda _: None)
