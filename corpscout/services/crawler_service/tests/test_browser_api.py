"""Exercise sticky HTTP routing and lifecycle at the browser-driver boundary."""

import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from browser_service.api import create_app
from browser_service.capture import PageCapture
from browser_service.runtime import BrowserRuntimeSettings, BrowserService

from crawler_service.browser import BrowserUnavailable
from crawler_service.browser_client import BrowserLeaseClient
from crawler_service.human_control import HumanAssistanceExpired, HumanSession


async def until(predicate):
    async with asyncio.timeout(4):
        while not predicate():
            await asyncio.sleep(0.01)


class FixturePage:
    def __init__(self):
        self.closed = False
        self.url = "about:blank"

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True

    async def goto(self, url, **_):
        self.url = url

    async def bring_to_front(self):
        pass

    async def screenshot(self, *, timeout):
        return b"fixture-image"


class FixtureTab:
    def __init__(self, context, page):
        self.context, self.page = context, page
        self.inflight = 0
        self.max_inflight = 0

    async def navigate(self, url, **_):
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            await asyncio.sleep(0.02)
            self.page.url = url
            return await self.capture()
        finally:
            self.inflight -= 1

    async def capture(self):
        return PageCapture(
            url=self.page.url,
            html="<h1>Fixture</h1>",
            status_code=200,
            headers={"set-cookie": "private", "content-type": "text/html"},
            error=None,
        )


class BrowserAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.service = BrowserService(
            Path(self.temporary.name),
            settings=BrowserRuntimeSettings(
                max_browsers=2, idle_timeout_seconds=120, session_retention_days=7
            ),
        )

        async def start(profile, **kwargs):
            profile.starts = getattr(profile, "starts", 0) + 1
            profile.state = "running"
            profile.context = SimpleNamespace(
                browser=None, pages=[], new_page=AsyncMock(side_effect=FixturePage)
            )
            profile.generation = uuid4().hex

        for target, replacement in (
            ("browser_service.browser_sessions.PersistentBrowserSession.start", start),
            (
                "browser_service.browser_sessions.PersistentBrowserSession.save",
                AsyncMock(),
            ),
        ):
            patcher = patch(target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.driver = patch("browser_service.runtime.BrowserSession", FixtureTab)
        self.driver.start()
        self.addCleanup(self.driver.stop)
        await self.service.start()
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(
                create_app(self.service, api_token="fixture-token")
            ),
            base_url="http://test",
            headers={"Authorization": "Bearer fixture-token"},
        )

    async def asyncTearDown(self):
        await self.service.close()
        await self.http.aclose()

    async def reserve(self, name):
        import hashlib

        response = await self.http.post(
            "/v1/browser/sessions",
            json={
                "id": hashlib.md5(name.encode()).hexdigest(),
                "requestId": name,
                "domain": name + ".test",
            },
        )
        self.assertEqual(response.status_code, 201)
        return response.json()["id"]

    async def test_busy_claim_retries_same_id_without_a_browser_queue(self):
        first, second = [await self.reserve(name) for name in ("one", "two")]
        identifier = uuid4().hex
        entered, finish = asyncio.Event(), asyncio.Event()

        async def crawl():
            async with BrowserLeaseClient(self.http).lease(
                identifier=identifier, request_id="waiting", domain="waiting.test"
            ):
                entered.set()
                await finish.wait()

        task = asyncio.create_task(crawl())
        try:
            await asyncio.sleep(0.05)
            self.assertIsNone(self.service.store.get(identifier))
            self.assertFalse(entered.is_set())
            await self.service.release(first)
            await asyncio.wait_for(entered.wait(), 2)
            self.assertEqual(self.service.store.get(identifier)["state"], "ready")
            self.assertEqual(self.service.store.get(second)["state"], "ready")
            self.assertEqual(len(self.service.store.recent()), 3)
        finally:
            finish.set()
            await task
        self.assertEqual(self.service.store.get(identifier)["state"], "closed")

    async def test_cancelled_busy_claim_does_not_release_other_sessions(self):
        first, second = [await self.reserve(name) for name in ("one", "two")]
        identifier = uuid4().hex

        async def crawl():
            async with BrowserLeaseClient(self.http).lease(
                identifier=identifier, request_id="waiting", domain="waiting.test"
            ):
                self.fail("Busy claim acquired a browser")

        task = asyncio.create_task(crawl())
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertIsNone(self.service.store.get(identifier))
        self.assertEqual(
            [self.service.store.get(i)["state"] for i in (first, second)],
            ["ready", "ready"],
        )

    async def test_client_cancellation_releases_assignment_before_returning(self):
        ready = asyncio.Event()
        ids = []

        async def crawl():
            async with BrowserLeaseClient(self.http).lease(
                identifier=uuid4().hex, request_id="cancelled", domain="cancelled.test"
            ) as client:
                ids.append(client.id)
                await client.open_tab("site")
                ready.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(crawl())
        await ready.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertNotIn(ids[0], self.service.active)
        self.assertTrue(not self.service.active)

    async def test_closed_browser_recovers_during_manual_wait_without_resuming(self):
        events = []
        human = HumanSession(
            headed=True,
            interactive=True,
            timeout=5,
            notify=lambda state, values: events.append((state, values)),
        )

        def ready(snapshot):
            human.session_id = snapshot["generation"]
            human.browser_available = True

        async with BrowserLeaseClient(self.http, on_ready=ready).lease(
            identifier=uuid4().hex, request_id="manual", domain="example.test"
        ) as client:
            page = await client.open_tab("site")
            result = await page.navigate(
                "https://example.test/verification",
                timeout_seconds=10,
                check_robots_txt=False,
            )
            reservation = self.service.active[client.id]
            profile = reservation.profile
            generation = profile.generation
            task = asyncio.create_task(
                human.check_result(
                    page, result, result.url, Path(self.temporary.name), "p0001"
                )
            )
            try:
                await until(lambda: human.waiting)
                deadline = events[0][1]["assistance_deadline"]
                await reservation.tabs["site"].page.close()
                profile.state, profile.generation = "error", None
                # A Resume arriving for the dead browser must not confirm its replacement.
                human.resume_requested.set()
                await until(
                    lambda: any(
                        "Browser restored" in values.get("reason", "")
                        for _, values in events
                    )
                )
                self.assertFalse(task.done())
                self.assertTrue(human.waiting)
                self.assertFalse(human.resume_requested.is_set())
                self.assertIs(reservation.profile, profile)
                self.assertEqual(
                    profile.store.for_profile(profile.id)["session_id"], client.id
                )
                self.assertEqual(
                    profile.store.for_profile(profile.id)["request_id"], "manual"
                )
                self.assertNotEqual(profile.generation, generation)
                self.assertEqual(human.session_id, profile.generation)
                self.assertEqual(reservation.tabs["site"].page.url, result.url)
                self.assertEqual(len(self.service.active), 1)
                self.assertEqual(profile.starts, 2)
                self.assertEqual(
                    [
                        values["assistance_deadline"]
                        for _, values in events
                        if "assistance_deadline" in values
                    ],
                    [deadline],
                )
                self.assertNotIn("running", [state for state, _ in events])
                human.resume_requested.set()
                self.assertEqual((await task).url, result.url)
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_recovery_preserves_live_operator_page_and_closed_tab(
        self,
    ):
        async with BrowserLeaseClient(self.http).lease(
            identifier=uuid4().hex, request_id="manual", domain="example.test"
        ) as client:
            page = await client.open_tab("site")
            reservation = self.service.active[client.id]
            profile = reservation.profile
            tab = reservation.tabs["site"]
            tab.page.url = "https://example.test/login-step"
            self.assertFalse(await page.recover("https://example.test/pending"))
            self.assertEqual(tab.page.url, "https://example.test/login-step")
            self.assertEqual(profile.starts, 1)
            await tab.page.close()
            for _ in range(2):
                self.assertFalse(await page.recover("https://example.test/pending"))
                self.assertFalse(page.available)
                self.assertTrue(tab.page.is_closed())
                self.assertIs(reservation.tabs["site"], tab)
            profile.state, profile.generation = "error", None
            self.assertTrue(await page.recover("https://example.test/pending"))
            self.assertEqual(profile.starts, 2)
            self.assertEqual(
                reservation.tabs["site"].page.url, "https://example.test/pending"
            )

    async def test_closed_verification_tab_stays_closed_until_operator_reopens_it(self):
        events = []
        human = HumanSession(
            headed=True,
            interactive=True,
            timeout=5,
            notify=lambda state, values: events.append((state, values)),
        )
        human.browser_available = True
        async with BrowserLeaseClient(self.http).lease(
            identifier=uuid4().hex, request_id="manual", domain="example.test"
        ) as client:
            page = await client.open_tab("site")
            result = await page.navigate(
                "https://example.test/pending",
                timeout_seconds=10,
                check_robots_txt=False,
            )
            reservation = self.service.active[client.id]
            profile = reservation.profile
            generation = profile.generation
            closed_tab = reservation.tabs["site"]
            task = asyncio.create_task(
                human.check_result(
                    page, result, result.url, Path(self.temporary.name), "p0001"
                )
            )
            try:
                await until(lambda: human.waiting)
                await closed_tab.page.close()
                await until(lambda: not page.available)
                for _ in range(2):
                    self.assertFalse(await page.recover(result.url))
                    self.assertIs(reservation.tabs["site"], closed_tab)
                self.assertFalse(task.done())
                self.assertTrue(human.waiting)
                self.assertEqual(profile.starts, 1)
                self.assertEqual(profile.generation, generation)
                self.assertTrue(
                    any(
                        "Verification tab closed" in values.get("reason", "")
                        and values["browser_available"]
                        for _, values in events
                    )
                )
                human.resume_requested.set()
                await until(lambda: reservation.tabs["site"] is not closed_tab)
                await until(lambda: not human.resume_requested.is_set())
                self.assertFalse(task.done())
                self.assertTrue(human.waiting)
                self.assertEqual(reservation.tabs["site"].page.url, result.url)
                self.assertEqual(profile.generation, generation)
                self.assertEqual(
                    profile.store.for_profile(profile.id)["session_id"], client.id
                )
                human.resume_requested.set()
                self.assertEqual((await task).url, result.url)
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_failed_recovery_does_not_loop_or_extend_manual_deadline(self):
        events = []
        human = HumanSession(
            headed=True,
            interactive=True,
            timeout=2.2,
            notify=lambda state, values: events.append((state, values)),
        )
        async with BrowserLeaseClient(self.http).lease(
            identifier=uuid4().hex, request_id="manual", domain="example.test"
        ) as client:
            page = await client.open_tab("site")
            result = await page.navigate(
                "https://example.test/", timeout_seconds=10, check_robots_txt=False
            )
            task = asyncio.create_task(
                human.check_result(
                    page, result, result.url, Path(self.temporary.name), "p0001"
                )
            )
            await until(lambda: human.waiting)
            profile = self.service.active[client.id].profile
            profile.state, profile.generation = "error", None
            with patch.object(
                profile, "start", AsyncMock(side_effect=OSError("fixture"))
            ) as start:
                with self.assertRaises(HumanAssistanceExpired):
                    await task
                self.assertEqual(start.await_count, 1)
            self.assertFalse(human.waiting)
            self.assertEqual(
                sum("assistance_deadline" in values for _, values in events), 1
            )
            self.assertNotIn("running", [state for state, _ in events])

    async def test_conflicting_reservation_does_not_release_existing_crawl(self):
        identifier = uuid4().hex
        async with BrowserLeaseClient(self.http).lease(
            identifier=identifier, request_id="owner", domain="one.test"
        ):
            with self.assertRaises(BrowserUnavailable):
                async with BrowserLeaseClient(self.http).lease(
                    identifier=identifier, request_id="other", domain="two.test"
                ):
                    self.fail("Conflicting session was accepted")
            self.assertEqual(self.service.snapshot(identifier)["requestId"], "owner")
            self.assertEqual(self.service.snapshot(identifier)["state"], "ready")
