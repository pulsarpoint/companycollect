import asyncio
import json
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from browser_http_fixture import install_browser_api
from test_crawl import browser_responses

from crawler_service.debug_trace import CURRENT_TRACE, CrawlTrace, read_trace, trace_event, trace_http_hooks
from crawler_service.service import CrawlRequest, CrawlService
from crawler_service.service_api import create_app


class DebugTraceTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_payload_timing_and_secrets_are_saved_before_paging(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            trace = CrawlTrace(root, ['private-key', 'secret"escaped'])
            token = CURRENT_TRACE.set(trace)
            try:
                async def reply(request):
                    await asyncio.sleep(0.01)
                    return httpx.Response(200, json={"text": "answer" * 2000, "echo": "private-key",
                        "headers": {"set-cookie": "unknown-cookie"}, "accessToken": "unknown-token",
                        "nested": [{"api_key_encrypted": "encrypted-value"}], "usage": {"prompt_tokens": 20}})
                async with httpx.AsyncClient(transport=httpx.MockTransport(reply), event_hooks=trace_http_hooks("model_http")) as client:
                    await client.post("https://provider.test/v1?token=query-secret", headers={"Authorization": "Bearer header-secret"}, json={"prompt": "test private-key", "password": "unknown-password"})
            finally:
                CURRENT_TRACE.reset(token)
            first = read_trace(root, 0, 1)
            self.assertTrue(first["has_more"])
            second = read_trace(root, first["cursor"], 1)
            self.assertEqual(second["events"][0]["id"], 2)
            self.assertGreater(second["events"][0]["duration_ms"], 0)
            self.assertEqual(first["events"][0]["operation"], second["events"][0]["operation"])
            saved = "".join(path.read_text() for path in (root / "debug").iterdir())
            for secret in ["private-key", "header-secret", "query-secret", "unknown-password", "unknown-cookie", "unknown-token", "encrypted-value"]:
                self.assertNotIn(secret, saved)
            self.assertIn("answer" * 2000, saved)
            self.assertIn('"prompt_tokens": 20', saved)
            self.assertEqual(read_trace(root, second["cursor"], 100)["events"], [])
            with self.assertRaises(ValueError):
                read_trace(root, 1, 100)
            with (root / "debug/events.jsonl").open("ab") as output:
                output.write(b'{"incomplete":')
            partial = read_trace(root, second["cursor"], 100)
            self.assertFalse(partial["has_more"])
            self.assertEqual(partial["cursor"], second["cursor"])

    async def test_concurrent_jobs_do_not_mix_logs(self):
        with TemporaryDirectory() as temporary:
            async def run(name):
                trace = CrawlTrace(Path(temporary) / name, [])
                token = CURRENT_TRACE.set(trace)
                logging.getLogger().addHandler(trace)
                try:
                    await asyncio.sleep(0)
                    logging.getLogger("crawler_service.test").warning("only %s", name)
                    trace_event("decision", name, details={"name": name})
                finally:
                    CURRENT_TRACE.reset(token)
                    logging.getLogger().removeHandler(trace)
            await asyncio.gather(run("first"), run("second"))
            for name, other in [("first", "second"), ("second", "first")]:
                saved = (Path(temporary) / name / "debug/events.jsonl").read_text()
                self.assertIn(name, saved)
                self.assertNotIn(other, saved)


class DebugApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        install_browser_api(self)

    async def test_real_crawl_trace_auth_stream_resume_download_and_restart(self):
        url = "https://example.test/jobs"
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = CrawlService(root, {}, concurrency=1, max_pending=2)
            app = create_app(service, api_token="admin-token")
            with patch("crawler_service.crawl.open_browser", lambda *_args, **_: browser_responses({url: ("<h1>Jobs</h1>", [], 200, None)}, [])):
                async with app.router.lifespan_context(app), httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://service") as client:
                    self.assertEqual((await client.get("/v1/crawls/debug-one/debug")).status_code, 401)
                    client.headers["Authorization"] = "Bearer admin-token"
                    response = await client.post("/v1/crawls", json={"request_id": "debug-one", "url": url, "pages": [url], "save_artifacts": False, "debug": True})
                    self.assertEqual(response.status_code, 202)
                    job = await asyncio.wait_for(service.wait("debug-one"), 5)
                    self.assertEqual(job.state, "completed", (root / job.result_file).read_text())
                    index = (await client.get("/v1/crawls/debug-one/debug?limit=2")).json()
                    self.assertTrue(index["enabled"])
                    self.assertEqual(index["attempt"], 1)
                    self.assertTrue(index["has_more"])
                    event = index["events"][0]
                    details = (await client.get(f"/v1/crawls/debug-one/debug?attempt=1&event={event['id']}")).json()
                    self.assertTrue(details["request"]["debug"])
                    stream = await client.get("/v1/crawls/debug-one/debug?stream=true", headers={"Last-Event-ID": f"1:{index['cursor']}"})
                    self.assertIn("crawl-debug-complete", stream.text)
                    batches = [json.loads(line[6:]) for line in stream.text.splitlines() if line.startswith("data: ")]
                    events = [event for batch in batches for event in batch["events"]]
                    self.assertGreater(min(event["id"] for event in events), 2)
                    self.assertIn("result", [event["stage"] for event in events])
                    self.assertIn("navigation", [event["stage"] for event in events])
                    export = await client.get("/v1/crawls/debug-one/debug?download=true")
                    exported = [json.loads(line) for line in export.text.splitlines()]
                    self.assertIn("attachment", export.headers["content-disposition"])
                    self.assertTrue(any(event["stage"] == "progress" for event in exported))
                    self.assertTrue(any("details" in event for event in exported))
                    self.assertFalse((await client.get("/v1/crawls/debug-one/debug?after=1")).is_success)
                    self.assertEqual((await client.get("/v1/crawls/debug-one/debug?attempt=2")).status_code, 404)
                    service.submit(CrawlRequest(request_id="normal", url=url, pages=[url], save_artifacts=False), source="rest")
                    normal = await service.wait("normal")
                    self.assertFalse((root / normal.result_file).parent.joinpath("debug").exists())
                    service.submit(CrawlRequest(request_id="debug-artifacts", url=url, pages=[url], debug=True), source="rest")
                    with_artifacts = await service.wait("debug-artifacts")
                    self.assertEqual(with_artifacts.state, "completed")
                    self.assertTrue((root / with_artifacts.result_file).parent.joinpath("debug/events.jsonl").is_file())
            restarted = CrawlService(root, {}, concurrency=1, max_pending=2)
            restored_app = create_app(restarted, api_token=None)
            async with restored_app.router.lifespan_context(restored_app), httpx.AsyncClient(transport=httpx.ASGITransport(restored_app), base_url="http://service") as client:
                saved = (await client.get("/v1/crawls/debug-one/debug?download=true")).text
                self.assertEqual(saved, export.text)

    async def test_failure_remains_debuggable_and_resets_context(self):
        with TemporaryDirectory() as temporary:
            service = CrawlService(Path(temporary), {"BROWSER_API_TOKEN": "secret-value"}, concurrency=1, max_pending=2)
            async def fail(*_args):
                trace_event("navigation", "Before failure")
                raise RuntimeError("provider echoed secret-value")
            with patch.object(service, "run_scan", fail):
                await service.start()
                try:
                    service.submit(CrawlRequest(request_id="failure", url="example.test", pages=["/"], debug=True), source="rest")
                    job = await service.wait("failure")
                    self.assertEqual(job.state, "failed")
                    root = Path(temporary) / "jobs/failure/attempts/0001/debug"
                    data = "".join(path.read_text() for path in root.iterdir())
                    self.assertIn("RuntimeError", data)
                    self.assertIn("Traceback", data)
                    self.assertNotIn("secret-value", data)
                    self.assertIsNone(CURRENT_TRACE.get())
                finally:
                    await service.close()

    async def test_inflight_trace_is_readable_and_cancellation_is_retained(self):
        with TemporaryDirectory() as temporary:
            service = CrawlService(Path(temporary), {}, concurrency=1, max_pending=2)
            entered = asyncio.Event()
            async def waiting(*_args):
                trace_event("navigation", "Request sent; waiting for response")
                entered.set()
                await asyncio.Event().wait()
            with patch.object(service, "run_scan", waiting):
                app = create_app(service, api_token=None)
                async with app.router.lifespan_context(app), httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://service") as client:
                    service.submit(CrawlRequest(request_id="waiting", url="example.test", pages=["/"], debug=True), source="rest")
                    await asyncio.wait_for(entered.wait(), 2)
                    before = (await client.get("/v1/crawls/waiting/debug")).json()
                    self.assertEqual(before["job"]["state"], "running")
                    self.assertTrue(any("waiting for response" in event["message"] for event in before["events"]))
                    service.cancel("waiting")
                    job = await service.wait("waiting")
                    self.assertEqual(job.state, "cancelled")
                    after = (await client.get(f"/v1/crawls/waiting/debug?after={before['cursor']}")).json()
                    self.assertEqual(after["job"]["state"], "cancelled")
                    self.assertTrue(any("cancelled" in event["message"] for event in after["events"]))


if __name__ == "__main__":
    unittest.main()
