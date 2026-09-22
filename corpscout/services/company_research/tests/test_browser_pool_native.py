"""Real saved-profile scans and Brave verification against a local HTTP fixture."""

import asyncio
import json
import os
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import uvicorn
from browser_service.api import create_app
from browser_service.runtime import BrowserRuntimeSettings, BrowserService
from browser_service.virtual_desktop import ACTIVE_DESKTOPS

from company_research.brave_browser import BraveSearch
from company_research.browser_client import BrowserLeaseClient
from company_research.fetch import open_browser
from company_research.human_control import HumanSession
from company_research.models import ResearchConfig
from company_research.service import CrawlRequest, CrawlService

RESULTS = """<html><title>Fixture search results</title><div class="snippet" data-type="web">
<a href="https://example.test/reports"><span class="search-snippet-title">Reports</span></a>
<div class="generic-snippet">Annual reports</div></div></html>"""


async def until(predicate):
    async with asyncio.timeout(45):
        while not predicate():
            await asyncio.sleep(0.02)


@unittest.skipUnless(
    os.environ.get("COMPANY_RESEARCH_XVFB_TEST") == "1", "opt-in Linux/Xvfb test"
)
class BrowserPoolNativeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        requests = self.requests

        class Fixture(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                path = urlsplit(self.path).path
                cookies = self.headers.get("Cookie", "")
                if path == "/search":
                    requests.append((self.path, "login=yes" in cookies))
                blocked = path == "/search" and "verified=yes" not in cookies
                self.send_response(429 if blocked else 200)
                self.send_header("Content-Type", "text/html")
                if path == "/login":
                    self.send_header("Set-Cookie", "login=yes; Path=/; HttpOnly")
                if path == "/allow":
                    self.send_header("Set-Cookie", "verified=yes; Path=/; HttpOnly")
                self.end_headers()
                if path == "/search":
                    html = (
                        "<h1>Local verification fixture</h1><p>Not a real CAPTCHA.</p>"
                        if blocked
                        else RESULTS
                    )
                else:
                    html = (
                        "<html><title>Domain fixture</title><h1>Domain fixture</h1><p>"
                        + ("Authenticated" if "login=yes" in cookies else "Anonymous")
                        + "</p></html>"
                    )
                self.wfile.write(html.encode())

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.browsers = BrowserService(
            self.root / "browser-state",
            settings=BrowserRuntimeSettings(
                max_browsers=2, idle_timeout_seconds=120, session_retention_days=7
            ),
        )
        await self.browsers.start()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.api = uvicorn.Server(
            uvicorn.Config(
                create_app(self.browsers, api_token=None),
                log_level="error",
                access_log=False,
                lifespan="off",
                timeout_graceful_shutdown=5,
            )
        )
        self.api_task = asyncio.create_task(self.api.serve(sockets=[self.listener]))
        await until(lambda: self.api.started)
        self.browser_url = f"http://127.0.0.1:{self.listener.getsockname()[1]}"

    async def asyncTearDown(self):
        self.api.should_exit = True
        await self.api_task
        self.listener.close()
        await self.browsers.close()
        await asyncio.to_thread(self.server.shutdown)
        self.server.server_close()
        self.thread.join()
        self.temporary.cleanup()
        self.assertEqual(ACTIVE_DESKTOPS, {})

    async def test_service_reuses_requested_profile_and_closes_browser_after_scan(self):
        service = CrawlService(
            self.root,
            {"BROWSER_API_URL": self.browser_url},
            concurrency=1,
            max_pending=2,
        )
        await service.start()
        try:
            first_id, second_id = uuid4().hex, uuid4().hex
            seed = await self.browsers.claim(
                identifier=first_id, request_id="seed", domain="fixture", headless=True
            )
            first = seed.profile
            tab = await first.open_tab(self.url + "/login")
            await first.tabs[tab].page.wait_for_load_state("domcontentloaded")
            await first.tabs[tab].page.evaluate(
                "localStorage.setItem('fixture', 'retained')"
            )
            await self.browsers.release(first_id)
            for number, saved_id in enumerate((first_id, second_id, first_id)):
                identifier = f"fixture-{number}"
                service.submit(
                    CrawlRequest(
                        request_id=identifier,
                        session_id=saved_id,
                        url=self.url,
                        pages=["/company"],
                        config=ResearchConfig(check_robots_txt=False),
                    ),
                    source="rest",
                )
                job = await asyncio.wait_for(service.wait(identifier), 45)
                self.assertEqual(job.state, "completed", job.error)
                self.assertEqual(job.browser_lease_id, saved_id)
                self.assertEqual(
                    job.browser_execution_id, self.browsers.store.get(saved_id)["id"]
                )
                document = json.loads((self.root / job.result_file).read_text())
                self.assertIn(
                    "Authenticated" if saved_id == first_id else "Anonymous",
                    document["documents"][0]["html"],
                )
                self.assertFalse(self.browsers.active)
                self.assertEqual(len(ACTIVE_DESKTOPS), 0)
            saved = await self.browsers.claim(
                identifier=first_id,
                request_id="check",
                domain="fixture",
                headless=False,
            )
            page = await saved.profile.context.new_page()
            await page.goto(self.url + "/company")
            self.assertEqual(
                await page.evaluate("localStorage.getItem('fixture')"), "retained"
            )
            await self.browsers.release(first_id)
        finally:
            await service.close()

    async def test_site_and_search_share_profile_global_gate_and_verification_pause(
        self,
    ):
        runtime = self.browsers
        http = httpx.AsyncClient(base_url=self.browser_url, timeout=60)
        tasks = []
        try:
            first_id = uuid4().hex
            seed = await self.browsers.claim(
                identifier=first_id, request_id="seed", domain="fixture", headless=False
            )
            tab = await seed.profile.open_tab(self.url + "/login")
            await seed.profile.tabs[tab].page.wait_for_load_state("domcontentloaded")
            await self.browsers.release(first_id)
            lock = asyncio.Lock()
            events = []
            human = HumanSession(
                headed=True,
                interactive=False,
                timeout=10,
                notify=lambda state, values: events.append((state, values)),
            )

            def browser_ready(snapshot):
                if not human.verification_available:
                    human.session_id = snapshot["generation"]
                    human.browser_available = True

            async with (
                BrowserLeaseClient(http, on_ready=browser_ready).lease(
                    identifier=first_id,
                    request_id="one",
                    domain="one.test",
                    headless=False,
                ) as assigned,
                BrowserLeaseClient(http).lease(
                    identifier=uuid4().hex,
                    request_id="two",
                    domain="two.test",
                    headless=False,
                ) as other,
            ):
                first = self.browsers.get(assigned.id).profile
                generation = first.generation
                self.assertEqual(assigned.profile_id, first.id)
                human.browser_available = True
                search = BraveSearch(
                    self.root / ".brave", browser_client=assigned, lock=lock
                )
                second_search = BraveSearch(
                    self.root / ".brave", browser_client=other, lock=lock
                )
                try:
                    async with open_browser(human, browser_client=assigned) as site:
                        await site.navigate(
                            self.url + "/company",
                            timeout_seconds=10,
                            check_robots_txt=False,
                        )
                        query = self.url + "/search?q=Fixture"
                        task = asyncio.create_task(
                            search.fetch(
                                query,
                                ResearchConfig(check_robots_txt=False),
                                human,
                                self.root / "one",
                                "s0001",
                            )
                        )
                        tasks.append(task)
                        await until(lambda: human.verification_available)
                        second_task = asyncio.create_task(
                            second_search.fetch(
                                self.url + "/search?q=Other",
                                ResearchConfig(check_robots_txt=False),
                                None,
                                self.root / "two",
                                "s0001",
                            )
                        )
                        tasks.append(second_task)
                        await asyncio.sleep(0.2)
                        self.assertEqual(self.requests, [("/search?q=Fixture", True)])
                        self.assertEqual(first.generation, generation)
                        self.assertEqual(
                            first.store.for_profile(first.id)["request_id"], "one"
                        )
                        tabs = runtime.active[assigned.id].tabs
                        self.assertIs(tabs["search"].context, tabs["site"].context)
                        self.assertIsNone(second_search.browser)
                        # Cancel the other waiter, keeping the challenged query unchanged.
                        second_task.cancel()
                        await asyncio.gather(second_task, return_exceptions=True)
                        human.verification_requested.set()
                        await until(lambda: human.verifying_search and human.waiting)
                        self.assertTrue(human.browser_available)
                        self.assertEqual(first.generation, generation)
                        deadline = next(
                            values["assistance_deadline"]
                            for _, values in events
                            if values.get("assistance_deadline") is not None
                        )
                        closed_tab = runtime.active[assigned.id].tabs["search"]
                        await closed_tab.page.close()
                        await until(lambda: not search.browser.available)
                        requests_before = len(self.requests)
                        for _ in range(2):
                            self.assertFalse(await search.browser.recover(query))
                        self.assertEqual(len(self.requests), requests_before)
                        self.assertTrue(closed_tab.page.is_closed())
                        self.assertEqual(first.generation, generation)
                        self.assertTrue(human.waiting)
                        self.assertTrue(search.block_file.exists())
                        human.resume_requested.set()
                        await until(
                            lambda: any(
                                "Browser restored" in values.get("reason", "")
                                for _, values in events
                            )
                        )
                        self.assertFalse(task.done())
                        self.assertFalse(human.resume_requested.is_set())
                        self.assertEqual(first.generation, generation)
                        self.assertEqual(
                            first.store.for_profile(first.id)["session_id"], assigned.id
                        )
                        self.assertEqual(
                            runtime.active[assigned.id].tabs["search"].page.url, query
                        )
                        restored_events = sum(
                            "Browser restored" in values.get("reason", "")
                            for _, values in events
                        )
                        await first.save()
                        cdp = await first.context.browser.new_browser_cdp_session()
                        await cdp.send("Browser.close")
                        await until(lambda: first.state == "error")
                        human.resume_requested.set()
                        await until(
                            lambda: (
                                sum(
                                    "Browser restored" in values.get("reason", "")
                                    for _, values in events
                                )
                                > restored_events
                            )
                        )
                        self.assertFalse(task.done())
                        self.assertTrue(human.waiting)
                        self.assertFalse(human.resume_requested.is_set())
                        self.assertTrue(search.block_file.exists())
                        self.assertNotEqual(first.generation, generation)
                        self.assertEqual(
                            first.store.for_profile(first.id)["session_id"], assigned.id
                        )
                        self.assertEqual(
                            first.store.for_profile(first.id)["request_id"], "one"
                        )
                        self.assertEqual(human.session_id, first.generation)
                        self.assertTrue(human.browser_available)
                        self.assertEqual(len(ACTIVE_DESKTOPS), 2)
                        self.assertEqual(
                            runtime.active[assigned.id].tabs["search"].page.url, query
                        )
                        self.assertEqual(
                            {
                                values["assistance_deadline"]
                                for _, values in events
                                if values.get("assistance_deadline") is not None
                            },
                            {deadline},
                        )
                        # This endpoint belongs to the local fixture, not a CAPTCHA solver.
                        await search.browser.navigate(
                            self.url + "/allow",
                            timeout_seconds=10,
                            check_robots_txt=False,
                        )
                        await search.browser.navigate(
                            query, timeout_seconds=10, check_robots_txt=False
                        )
                        human.resume_requested.set()
                        result = await asyncio.wait_for(task, 10)
                        self.assertEqual(result.status_code, 200)
                        self.assertFalse(search.block_file.exists())
                        self.assertTrue(
                            all(authenticated for _, authenticated in self.requests)
                        )
                        await search.close()
                        # The site tab belongs to the same recovered reservation.
                        site = await assigned.open_tab("site")
                        await site.navigate(
                            self.url + "/company",
                            timeout_seconds=10,
                            check_robots_txt=False,
                        )
                        self.assertEqual((await site.capture()).status_code, 200)
                        self.assertEqual(first.state, "running")
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    await search.close()
                    await second_search.close()
            self.assertEqual(first.state, "stopped")
            self.assertIsNone(first.context)
            self.assertFalse(self.browsers.active)
        finally:
            await http.aclose()

    async def test_manual_queue_opens_two_browsers_and_waits_for_capacity(self):
        service = CrawlService(
            self.root,
            {"BROWSER_API_URL": self.browser_url, "CRAWL_HUMAN_ENABLED": "true"},
            concurrency=1,
            max_pending=3,
        )
        await service.start()
        try:
            for name in ("one", "two", "three"):
                service.submit(
                    CrawlRequest(
                        request_id=name,
                        interactive=True,
                        url=self.url,
                        pages=["/company"],
                        config=ResearchConfig(check_robots_txt=False),
                    ),
                    source="manual",
                )
            await until(
                lambda: all(
                    service.jobs[name].state == "awaiting_human"
                    for name in ("one", "two")
                )
            )
            self.assertEqual(service.jobs["three"].state, "queued")
            profiles = {
                profile.store.for_profile(profile.id)["request_id"]: profile
                for profile in (s.profile for s in self.browsers.active.values())
            }
            self.assertEqual(set(profiles), {"one", "two"})
            for profile in profiles.values():
                self.assertTrue(
                    any(
                        page.url == self.url + "/company"
                        for page in profile.context.pages
                    )
                )
            self.assertEqual(len(ACTIVE_DESKTOPS), 2)
            service.cancel("one")
            await asyncio.wait_for(service.wait("one"), 20)
            await until(lambda: service.jobs["three"].state == "awaiting_human")
            self.assertIsNone(profiles["one"].store.for_profile(profiles["one"].id))
            self.assertEqual(profiles["one"].state, "stopped")
            self.assertEqual(service.jobs["two"].state, "awaiting_human")
            self.assertEqual(len(ACTIVE_DESKTOPS), 2)
        finally:
            await service.close()
