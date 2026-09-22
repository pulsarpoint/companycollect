"""Brave requests stop across jobs until explicit verification of the same query."""

import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from browser_http_fixture import install_browser_api

from company_research.brave_browser import (
    BraveSearch,
    BraveSearchBlocked,
    brave_access_problem,
)
from company_research.browser import PageCapture
from company_research.human_control import HumanAssistanceExpired, HumanSession
from company_research.models import ResearchConfig
from company_research.service import CrawlRequest, CrawlService
from company_research.service_api import create_app
from company_research.storage import utc_now, write_json

URL = "https://search.brave.com/search?q=Example+reports&source=web"
SECOND = "https://search.brave.com/search?q=Other+reports&source=web"
RESULTS = """<html><title>Example reports - Brave Search</title><div class="snippet" data-type="web">
<a href="https://example.test/reports"><span class="search-snippet-title">Reports</span></a>
<div class="generic-snippet">Annual financial reports</div></div></html>"""


def capture(url=URL, html=RESULTS, status=200, error=None):
    return PageCapture(
        url=url,
        html=html,
        cleaned_html=html,
        status_code=status,
        error=error,
        headers={},
        metadata={},
        links=[],
    )


class SearchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        install_browser_api(self)

    async def until(self, predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.001)

    def human(self):
        events = []
        session = HumanSession(
            headed=False,
            interactive=False,
            timeout=0.01,
            notify=lambda state, values: events.append((state, values)),
        )
        return session, events

    def test_detection_including_http_200_and_result_snippets(self):
        for status, html, url in [
            (429, "Too many requests", URL),
            (403, "Forbidden", URL),
            (200, "<title>Verify you are human</title>", URL),
            (200, "<h1>We need to confirm you're human</h1>", URL),
            (200, '<form action="/captcha"><button>Continue</button></form>', URL),
            (200, "", "https://search.brave.com/captcha"),
        ]:
            with self.subTest(status=status, html=html):
                self.assertIsNotNone(brave_access_problem(capture(url, html, status)))
        self.assertIsNone(
            brave_access_problem(
                capture(html=RESULTS.replace("Reports", "Verify you are human"))
            )
        )
        self.assertIsNone(
            brave_access_problem(capture(html="<h1>No results found</h1>"))
        )
        self.assertIsNone(
            brave_access_problem(capture(status=403, error="robots denial"))
        )

    async def test_manual_activation_preserves_query_and_blocks_concurrent_jobs(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            search = BraveSearch(root / ".brave")
            human, events = self.human()
            other_human, _ = self.human()
            browser = SimpleNamespace(capture=AsyncMock(return_value=capture()))
            search.browser = browser
            challenge = capture(html="<h1>Verify you are human</h1>", status=429)
            calls = []

            async def navigate(url, _config):
                calls.append(url)
                return challenge if len(calls) == 1 else capture(url)

            async def headed(session):
                self.assertIs(session, human)
                self.assertTrue(session.verification_requested.is_set())
                human.browser_available = True

            with (
                patch.object(search, "navigate", navigate),
                patch.object(search, "open", headed),
            ):
                task = asyncio.create_task(
                    search.fetch(URL, ResearchConfig(), human, root / "one", "s0001")
                )
                await self.until(lambda: human.verification_available)
                other = asyncio.create_task(
                    search.fetch(
                        SECOND, ResearchConfig(), other_human, root / "two", "s0001"
                    )
                )
                await asyncio.sleep(
                    0.03
                )  # Longer than the automatic-page assistance timeout.
                self.assertEqual(calls, [URL])
                self.assertFalse(task.done())
                self.assertTrue(search.block_file.exists())
                self.assertEqual(events[-1][1]["assistance_deadline"], None)
                human.verification_requested.set()
                await self.until(lambda: human.verifying_search and human.waiting)
                self.assertEqual(calls, [URL, URL])
                browser.capture.return_value = challenge
                human.resume_requested.set()
                await self.until(
                    lambda: browser.capture.await_count == 1 and human.waiting
                )
                self.assertTrue(search.block_file.exists())
                self.assertFalse(other.done())
                browser.capture.return_value = capture(SECOND)
                human.resume_requested.set()
                await self.until(lambda: browser.capture.await_count == 2)
                self.assertTrue(search.block_file.exists())
                browser.capture.return_value = capture()
                human.resume_requested.set()
                result, next_result = await asyncio.wait_for(
                    asyncio.gather(task, other), 2
                )
                self.assertEqual(result.url, URL)
                self.assertEqual(next_result.url, SECOND)
                self.assertEqual(calls, [URL, URL, SECOND])
                self.assertFalse(search.block_file.exists())
                self.assertFalse(human.waiting)
                self.assertFalse(human.verification_available)
                self.assertFalse(human.browser_available)
                self.assertTrue(
                    (root / "one/human-assistance/s0001/resolved.json").exists()
                )

    async def test_cancellation_and_restart_do_not_clear_search_block(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            search = BraveSearch(root / ".brave")
            search.browser = SimpleNamespace()
            search.open = AsyncMock()
            human, _ = self.human()
            with patch.object(
                search, "navigate", AsyncMock(return_value=capture(status=429))
            ):
                task = asyncio.create_task(
                    search.fetch(URL, ResearchConfig(), human, root, "s0001")
                )
                await self.until(lambda: human.verification_available)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            restarted = BraveSearch(root / ".brave")
            with patch.object(restarted, "navigate", AsyncMock()) as navigate:
                with self.assertRaises(BraveSearchBlocked):
                    await restarted.fetch(SECOND, ResearchConfig(), None, root, "s0002")
                navigate.assert_not_awaited()
                self.assertTrue(restarted.block_file.exists())

    async def test_verification_timeout_keeps_latch(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            search = BraveSearch(root / ".brave")
            write_json(search.block_file, {"url": URL, "reason": "captcha"})
            human, _ = self.human()
            search.browser = SimpleNamespace()
            search.open = AsyncMock()
            human.browser_available = True
            with (
                patch.object(
                    search, "navigate", AsyncMock(return_value=capture(status=429))
                ),
                patch(
                    "company_research.brave_browser.asyncio.timeout_at",
                    side_effect=lambda _: asyncio.timeout(0.01),
                ),
            ):
                task = asyncio.create_task(
                    search.fetch(URL, ResearchConfig(), human, root, "s0001")
                )
                await self.until(lambda: human.verification_available)
                human.verification_requested.set()
                with self.assertRaises(HumanAssistanceExpired):
                    await task
                self.assertTrue(search.block_file.exists())
                self.assertFalse(human.waiting)

    async def test_api_verifies_same_attempt_without_retry_and_requires_auth(self):
        with TemporaryDirectory() as directory:
            service = CrawlService(
                Path(directory),
                {"CRAWL_HUMAN_ENABLED": "true"},
                concurrency=1,
                max_pending=2,
            )

            async def crawl(url, *, output_dir, search, human, **kwargs):
                await service.search.fetch(
                    URL, ResearchConfig(), human, output_dir, "s0001"
                )
                result = {
                    "status": "finished",
                    "stop_reason": "page_budget",
                    "finished_at": utc_now(),
                }
                write_json(output_dir / "result.json", {"crawl": result})
                return result

            async def headed(human):
                human.browser_available = True

            service.search.browser = SimpleNamespace(
                capture=AsyncMock(return_value=capture())
            )
            with (
                patch("company_research.service.crawl_company", crawl),
                patch.object(
                    service.search,
                    "navigate",
                    AsyncMock(return_value=capture(status=429)),
                ),
                patch.object(service.search, "open", headed),
            ):
                app = create_app(service, api_token="test-secret")
                async with (
                    app.router.lifespan_context(app),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app),
                        base_url="http://crawler",
                        headers={"Authorization": "Bearer test-secret"},
                    ) as client,
                ):
                    service.submit(
                        CrawlRequest(
                            request_id="paused",
                            url="https://example.test/",
                            pages=["/"],
                        ),
                        source="rest",
                    )
                    await self.until(
                        lambda: service.jobs["paused"].verification_available
                    )
                    session = service.human_sessions["paused"]
                    self.assertEqual(
                        (await client.post("/v1/crawls/paused/resume")).status_code, 409
                    )
                    self.assertEqual(
                        (
                            await client.post("/v1/crawls/paused/browser-ticket")
                        ).status_code,
                        409,
                    )
                    self.assertEqual(
                        (
                            await client.post(
                                "/v1/crawls/paused/verify-search",
                                headers={"Authorization": ""},
                            )
                        ).status_code,
                        401,
                    )
                    self.assertEqual(
                        (
                            await client.post("/v1/crawls/paused/verify-search")
                        ).status_code,
                        202,
                    )
                    await self.until(lambda: session.verifying_search)
                    self.assertEqual(
                        (await client.post("/v1/crawls/paused/resume")).status_code, 202
                    )
                    job = await asyncio.wait_for(service.wait("paused"), 2)
                    self.assertEqual(job.state, "completed")
                    self.assertEqual(job.attempt, 1)
                    self.assertEqual(list(service.jobs), ["paused"])
                    self.assertFalse(job.verification_available)
                    self.assertFalse(job.browser_available)


if __name__ == "__main__":
    unittest.main()
