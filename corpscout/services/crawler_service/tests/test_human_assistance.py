"""Verify deadlines, retained failures and independent manual retries."""

import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from browser_http_fixture import install_browser_api
from test_service_results import S3Server

from crawler_service.browser import PageCapture
from crawler_service.human_control import (
    HumanAssistanceExpired,
    HumanSession,
    access_problem,
)
from crawler_service.service import CrawlRequest, CrawlService
from crawler_service.service_api import create_app
from crawler_service.storage import utc_now, write_json


class BrowserPage:
    url = "https://example.test/"
    html = "<html><title>Just a moment...</title>Verify you are human</html>"

    def is_closed(self):
        return False

    async def content(self):
        return self.html

    async def screenshot(self, *, path, timeout):
        Path(path).write_bytes(b"test-screenshot")


class HumanAssistanceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        install_browser_api(self)

    async def test_failed_upload_survives_restart_without_recrawling(self):
        storage_server = S3Server()
        self.addCleanup(storage_server.close)
        storage_server.fail = True
        crawls = []

        async def fail(url, **kwargs):
            crawls.append(url)
            raise OSError("test browser unavailable")

        with (
            TemporaryDirectory() as directory,
            patch("crawler_service.service.crawl_company", fail),
        ):
            service = CrawlService(Path(directory), {}, concurrency=1, max_pending=1)
            service.results = storage_server.storage()
            await service.start()
            try:
                service.submit(
                    CrawlRequest(
                        request_id="upload-failure",
                        url="https://example.test/",
                        pages=["/"],
                    ),
                    source="rest",
                )
                await asyncio.wait_for(service.wait("upload-failure"), 2)
                async with asyncio.timeout(8):
                    while service.jobs["upload-failure"].s3_error is None:
                        await asyncio.sleep(0.02)
                self.assertEqual(
                    service.history.get("upload-failure", 1)["s3_state"], "pending"
                )
            finally:
                await service.close()
            storage_server.fail = False
            restarted = CrawlService(Path(directory), {}, concurrency=1, max_pending=1)
            restarted.results = storage_server.storage()
            await restarted.start()
            try:
                async with asyncio.timeout(5):
                    while restarted.jobs["upload-failure"].s3_state != "uploaded":
                        await asyncio.sleep(0.02)
                self.assertEqual(crawls, ["https://example.test/"])
                self.assertEqual(
                    restarted.history.get("upload-failure", 1)["state"], "failed"
                )
                self.assertEqual(len(storage_server.objects), 1)
            finally:
                await restarted.close()

    async def test_deadline_records_evidence_and_rejects_false_confirmation(self):
        events = []
        session = HumanSession(
            headed=False,
            interactive=False,
            timeout=0.05,
            notify=lambda state, values: events.append((state, values)),
        )
        browser = SimpleNamespace(page=BrowserPage(), document_status=403)
        response = PageCapture(
            url=browser.page.url,
            html=browser.page.html,
            cleaned_html="",
            status_code=403,
            headers={},
            metadata={},
            links=[],
            error=None,
        )
        browser.capture = AsyncMock(return_value=response)
        browser.screenshot = lambda path: browser.page.screenshot(
            path=path, timeout=5000
        )
        with TemporaryDirectory() as directory:
            task = asyncio.create_task(
                session.check_result(
                    browser, response, browser.page.url, Path(directory), "p0001"
                )
            )
            await asyncio.sleep(0.01)
            self.assertTrue(session.waiting)
            session.resume_requested.set()
            with self.assertRaises(HumanAssistanceExpired):
                await task
            self.assertFalse(session.waiting)
            self.assertEqual([state for state, _ in events], ["captcha", "captcha"])
            self.assertTrue(
                (Path(directory) / "human-assistance/p0001/blocked.html").is_file()
            )

    async def test_sqlite_failure_and_manual_retry_bypass_busy_normal_worker(self):
        normal_hold, manual_hold = asyncio.Event(), asyncio.Event()

        async def crawl(url, *, output_dir, human=None, **kwargs):
            self.assertIsNotNone(human)
            self.assertEqual(human.headed, human.interactive)
            if url.endswith("busy/"):
                self.assertEqual(human.headed, human.interactive)
                self.assertEqual(human.timeout, 10)
                human.waiting = True
                await normal_hold.wait()
            if human is not None and human.interactive:
                self.assertEqual(human.headed, human.interactive)
                self.assertEqual(human.timeout, 900)
                await manual_hold.wait()
            else:
                self.assertEqual(human.headed, human.interactive)
            result = {
                "status": "failed",
                "stop_reason": "human_assistance_timeout",
                "finished_at": utc_now(),
            }
            write_json(output_dir / "result.json", {"crawl": result, "documents": []})
            return result

        with (
            TemporaryDirectory() as directory,
            patch("crawler_service.service.crawl_company", crawl),
        ):
            service = CrawlService(
                Path(directory),
                {"CRAWL_HUMAN_ENABLED": "true"},
                concurrency=1,
                max_pending=1,
            )
            app = create_app(service, api_token="secret")
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app),
                    base_url="http://crawler",
                    headers={"Authorization": "Bearer secret"},
                ) as client,
            ):
                failed = CrawlRequest(
                    request_id="failure", url="https://example.test/", pages=["/"]
                )
                service.submit(failed, source="jetstream")
                first = await asyncio.wait_for(service.wait("failure"), 2)
                self.assertEqual(first.state, "failed")
                self.assertEqual(first.error, "human_assistance_timeout")
                original_session = first.browser_lease_id
                service.submit(
                    CrawlRequest(
                        request_id="busy",
                        url="https://example.test/busy/",
                        pages=["/busy/"],
                    ),
                    source="rest",
                )
                payload = {"attempt": 1, "request_id": "manual-first"}
                retry = await client.post("/v1/crawls/failure/retry", json=payload)
                self.assertEqual(retry.status_code, 202, retry.text)
                async with asyncio.timeout(2):
                    while service.jobs["manual-first"].state != "running":
                        await asyncio.sleep(0.01)
                self.assertFalse(normal_hold.is_set())
                self.assertEqual(
                    service.jobs["manual-first"].browser_lease_id, original_session
                )
                self.assertEqual(
                    (await client.post("/v1/crawls/busy/resume")).status_code,
                    409,
                )
                self.assertFalse(
                    service.human_sessions["busy"].resume_requested.is_set()
                )
                self.assertEqual(
                    (
                        await client.post("/v1/crawls/failure/retry", json=payload)
                    ).status_code,
                    202,
                )
                self.assertEqual(
                    (
                        await client.post(
                            "/v1/crawls/failure/retry",
                            json=payload | {"request_id": "duplicate"},
                        )
                    ).status_code,
                    409,
                )
                history = (
                    await client.get(
                        "/v1/crawls/status?state=failed&domain=example&source=jetstream"
                    )
                ).json()
                self.assertEqual(history["total"], 1)
                self.assertEqual(history["attempts"][0]["request_id"], "failure")
                self.assertEqual(
                    (
                        await client.post("/v1/crawls/manual-first/browser-ticket")
                    ).status_code,
                    409,
                )
                self.assertEqual(
                    (await client.post("/v1/crawls/manual-first/cancel")).status_code,
                    202,
                )
                cancelled = await asyncio.wait_for(service.wait("manual-first"), 2)
                self.assertEqual(cancelled.state, "cancelled")
                self.assertEqual(service.history.get("failure", 1)["state"], "failed")
                normal_hold.set()
                await asyncio.wait_for(service.wait("busy"), 2)
            restarted = CrawlService(Path(directory), {}, concurrency=1, max_pending=1)
            await restarted.start()
            try:
                self.assertEqual(restarted.history.get("failure", 1)["state"], "failed")
                self.assertEqual(
                    restarted.history.get("manual-first", 1)["retry_of"], "failure"
                )
                self.assertTrue((Path(directory) / "crawl-history.sqlite3").is_file())
                self.assertEqual(
                    json.loads(
                        (
                            Path(directory) / "jobs/failure/attempts/0001/result.json"
                        ).read_text()
                    )["crawl"]["status"],
                    "failed",
                )
            finally:
                await restarted.close()

    def test_block_detection_does_not_treat_normal_turnstile_form_as_challenge(self):
        self.assertIsNone(
            access_problem(200, '<form><div class="cf-turnstile"></div></form>')
        )
        self.assertEqual(access_problem(403, "Forbidden"), "blocked")
        self.assertEqual(
            access_problem(200, "<title>Just a moment...</title>"), "captcha"
        )


if __name__ == "__main__":
    unittest.main()
