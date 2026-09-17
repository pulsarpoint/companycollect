"""Check REST and local job durability using the real crawler at a browser boundary."""

import asyncio
import io
import json
import unittest
from contextlib import asynccontextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from test_crawl import browser_responses
from test_package import response
from test_selection_instructions import requested_assessment
from test_site_info import COMPANY, SITE
from test_site_info import HTML as COMPANY_HTML

from company_research.crawl import main
from company_research.service import CrawlRequest, CrawlService, ServiceUnavailable
from company_research.service_api import create_app
from company_research.storage import write_json

HTML = "<h1>Example jobs</h1><p>Embedded software engineer</p>"
URL = "https://example.test/jobs"


class CrawlServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "results"
        self.requested = []
        self.hold = asyncio.Event()
        self.hold.set()

        @asynccontextmanager
        async def browser():
            await self.hold.wait()
            async with browser_responses(
                {URL: (HTML, [], 200, None)}, self.requested
            ) as value:
                yield value

        self.browser_patch = patch("company_research.crawl.open_browser", browser)
        self.browser_patch.start()
        self.addCleanup(self.browser_patch.stop)
        self.service = CrawlService(self.root, {}, concurrency=1, max_pending=1)

    async def test_rest_result_and_idempotency_survive_restart(self):
        app = create_app(self.service, api_token="test-token")
        payload = {
            "request_id": "example-1",
            "url": "https://example.test",
            "pages": ["/jobs"],
        }
        async with app.router.lifespan_context(app):
            with self.assertRaisesRegex(ServiceUnavailable, "already running"):
                await self.service.start()
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://service"
            ) as client:
                denied = await client.post("/v1/crawls", json=payload)
                self.assertEqual(denied.status_code, 401)
                self.assertEqual(self.service.jobs, {})
                client.headers["Authorization"] = "Bearer test-token"
                accepted = await client.post("/v1/crawls", json=payload)
                self.assertEqual(accepted.status_code, 202)
                self.assertEqual(accepted.headers["Location"], "/v1/crawls/example-1")
                self.assertTrue((self.root / "jobs/example-1/request.json").is_file())
                job = await asyncio.wait_for(self.service.wait("example-1"), timeout=5)
                self.assertEqual(job.state, "completed")
                self.assertEqual(job.crawl_status, "finished")
                result = await client.get("/v1/crawls/example-1/result")
                self.assertEqual(result.status_code, 200)
                document = result.json()
                self.assertEqual(document["documents"][0]["html"], HTML)
                self.assertEqual(document["crawl"]["usage"]["calls"], 0)
                duplicate = await client.post("/v1/crawls", json=payload)
                self.assertEqual(duplicate.json()["attempt"], 1)
                conflict = await client.post(
                    "/v1/crawls", json=payload | {"site_info": True}
                )
                self.assertEqual(conflict.status_code, 409)
                self.assertEqual(
                    (await client.get("/v1/crawls/missing")).status_code, 404
                )
        restarted = CrawlService(self.root, {}, concurrency=1, max_pending=1)
        await restarted.start()
        try:
            restored = restarted.submit(
                CrawlRequest.model_validate(payload), source="jetstream"
            )
            self.assertEqual(restored.attempt, 1)
            self.assertEqual(restored.state, "completed")
            self.assertEqual(self.requested, [URL])
            self.assertEqual(
                json.loads((self.root / restored.result_file).read_text()), document
            )
        finally:
            await restarted.close()

    async def test_capacity_backpressure_and_interrupted_job_recovery(self):
        self.hold.clear()
        app = create_app(self.service, api_token=None)
        payload = {"request_id": "resume", "url": URL, "pages": [URL]}
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://service"
            ) as client:
                self.assertEqual(
                    (await client.post("/v1/crawls", json=payload)).status_code, 202
                )
                async with asyncio.timeout(5):
                    while self.service.jobs["resume"].state != "running":
                        await asyncio.sleep(0.01)
                duplicate = await client.post("/v1/crawls", json=payload)
                self.assertEqual(duplicate.status_code, 202)
                busy = await client.post(
                    "/v1/crawls", json=payload | {"request_id": "another"}
                )
                self.assertEqual(busy.status_code, 503)
                self.assertEqual(
                    (await client.get("/v1/crawls/resume/result")).status_code, 409
                )
                competing = CrawlService(self.root, {}, concurrency=1, max_pending=1)
                with self.assertRaises(ServiceUnavailable):
                    await competing.start()
        first_attempt = self.root / "jobs/resume/attempts/0001/crawl-manifest.json"
        preserved = first_attempt.read_bytes()
        self.hold.set()
        restarted = CrawlService(self.root, {}, concurrency=1, max_pending=1)
        await restarted.start()
        try:
            job = await asyncio.wait_for(restarted.wait("resume"), 5)
            self.assertEqual(job.attempt, 2)
            self.assertEqual(job.crawl_status, "finished")
            self.assertEqual(first_attempt.read_bytes(), preserved)
            self.assertEqual(self.requested, [URL])
        finally:
            await restarted.close()

    async def test_result_written_before_crash_is_adopted_without_recrawling(self):
        await self.service.start()
        request = CrawlRequest(request_id="saved", url=URL, pages=[URL])
        self.service.submit(request, source="rest")
        saved = await asyncio.wait_for(self.service.wait("saved"), 5)
        await self.service.close()
        saved.state, saved.result_file, saved.finished_at = "running", None, None
        write_json(self.root / "jobs/saved/job.json", saved.model_dump())
        restarted = CrawlService(self.root, {}, concurrency=1, max_pending=1)
        await restarted.start()
        try:
            adopted = await restarted.wait("saved")
            self.assertEqual(adopted.state, "completed")
            self.assertEqual(adopted.attempt, 1)
            self.assertEqual(self.requested, [URL])
        finally:
            await restarted.close()

    async def test_validation_and_missing_credentials_do_not_enqueue(self):
        app = create_app(self.service, api_token=None)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://service"
            ) as client:
                for invalid in [
                    {"url": URL, "request_id": "../escape"},
                    {"url": URL, "output_dir": "/tmp/escape"},
                    {"url": URL, "pages": []},
                    {"url": "file:///etc/passwd"},
                    {"url": URL, "crawl": False},
                    {"url": URL, "instructions": "  "},
                ]:
                    self.assertEqual(
                        (await client.post("/v1/crawls", json=invalid)).status_code, 422
                    )
                missing_key = await client.post(
                    "/v1/crawls", json={"url": URL, "site_info": True}
                )
                self.assertEqual(missing_key.status_code, 503)
                self.assertEqual(self.service.jobs, {})
                self.assertFalse((self.root / "jobs").exists())

    async def test_execution_error_produces_terminal_local_json(self):
        await self.service.start()
        try:
            with patch(
                "company_research.service.crawl_company",
                side_effect=ValueError("private-secret"),
            ):
                self.service.submit(
                    CrawlRequest(request_id="failure", url=URL, pages=[URL]),
                    source="rest",
                )
                job = await asyncio.wait_for(self.service.wait("failure"), 5)
            self.assertEqual(job.state, "failed")
            result = (self.root / job.result_file).read_text()
            self.assertNotIn("private-secret", result)
            self.assertIn("company-crawl-error/1.0", result)
        finally:
            await self.service.close()

    async def test_rest_preserves_site_info_only_and_instruction_selection(self):
        self.service.environment["DEEPSEEK"] = "model-secret"

        def browser():
            return browser_responses(
                {SITE: (COMPANY_HTML, [], 200, None), URL: (HTML, [], 200, None)},
                self.requested,
            )

        original_send = httpx.AsyncClient.send
        model_requests = []

        async def send(client, request, **kwargs):
            if request.url.host != "api.deepseek.com":
                return await original_send(client, request, **kwargs)
            data = json.loads(request.content)
            self.assertEqual(data["model"], "deepseek-flash")
            prompt = data["messages"][1]["content"]
            source = json.loads(prompt.split("INPUT DATA:\n")[1])
            model_requests.append(source)
            document = (
                COMPANY
                if source.get("task") == "site_classification"
                else {
                    "assessments": [
                        requested_assessment(c, "direct") for c in source["candidates"]
                    ]
                }
            )
            return httpx.Response(200, json=response(document), request=request)

        app = create_app(self.service, api_token=None)
        with (
            patch("company_research.crawl.open_browser", browser),
            patch.object(httpx.AsyncClient, "send", send),
        ):
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="http://service"
                ) as client:
                    info = await client.post(
                        "/v1/crawls",
                        json={
                            "request_id": "info",
                            "url": SITE,
                            "site_info": True,
                            "config": {"max_model_calls": 1},
                        },
                    )
                    self.assertEqual(info.status_code, 202)
                    await asyncio.wait_for(self.service.wait("info"), 5)
                    result = (await client.get("/v1/crawls/info/result")).json()
                    self.assertEqual(result["crawl"]["mode"], "site_info")
                    self.assertEqual(
                        result["crawl"]["config"]["model"], "deepseek-flash"
                    )
                    stored_request = json.loads(
                        (self.root / "jobs/info/request.json").read_text()
                    )
                    self.assertEqual(stored_request["config"], {"max_model_calls": 1})
                    self.assertEqual(
                        result["crawl"]["stop_reason"], "site_info_complete"
                    )
                    self.assertEqual(len(result["documents"]), 1)
                    self.assertEqual(self.requested, [SITE])
                    jobs = await client.post(
                        "/v1/crawls",
                        json={
                            "request_id": "jobs",
                            "url": SITE,
                            "pages": [URL],
                            "instructions": "Get current jobs",
                        },
                    )
                    self.assertEqual(jobs.status_code, 202)
                    await asyncio.wait_for(self.service.wait("jobs"), 5)
                    result = (await client.get("/v1/crawls/jobs/result")).json()
                    self.assertEqual(result["documents"][0]["html"], HTML)
                    self.assertEqual(
                        model_requests[-1]["selection_instructions"], "Get current jobs"
                    )
                    self.assertEqual(self.requested, [SITE, URL])
        for path in self.root.rglob("*.json"):
            self.assertNotIn("model-secret", path.read_text())


class CrawlCliResultTests(unittest.TestCase):
    def test_existing_cli_preserves_stdout_and_writes_portable_json(self):
        with TemporaryDirectory() as directory:
            output = Path(directory) / "crawl"
            requested = []
            stdout = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "company-research-crawl",
                        URL,
                        "--pages",
                        URL,
                        "--output-dir",
                        str(output),
                    ],
                ),
                patch(
                    "company_research.crawl.open_browser",
                    lambda: browser_responses({URL: (HTML, [], 200, None)}, requested),
                ),
                redirect_stdout(stdout),
                redirect_stderr(io.StringIO()),
            ):
                main()
            manifest = json.loads(stdout.getvalue())
            result = json.loads((output / "result.json").read_text())
            self.assertEqual(result["crawl"], manifest)
            self.assertEqual(result["documents"][0]["html"], HTML)
            self.assertEqual(manifest["status"], "finished")


if __name__ == "__main__":
    unittest.main()
