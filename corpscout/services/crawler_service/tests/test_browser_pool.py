"""Crawler integration with a separately listening browser HTTP service."""

import asyncio
import socket
from pathlib import Path
from unittest.mock import patch

import uvicorn
from browser_service.api import create_app
from test_browser_api import BrowserAPITests, until

from crawler_service.service import CrawlRequest, CrawlService


class ExternalBrowserPoolTests(BrowserAPITests):
    async def test_automatic_crawl_prefers_headless_and_interactive_requires_headed(
        self,
    ):
        service = CrawlService(
            self.root / "mixed-service",
            {"BROWSER_API_URL": self.browser_url, "CRAWL_HUMAN_ENABLED": "true"},
            concurrency=1,
            max_pending=2,
        )
        observed = []

        async def crawl(_url, **kwargs):
            observed.append(
                (
                    self.service.get(kwargs["browser_client"].id).profile.headless,
                    kwargs["human"].headed,
                )
            )
            return {"status": "finished", "stop_reason": "completed"}

        with patch("crawler_service.service.crawl_company", crawl):
            await service.start()
            try:
                for interactive in (False, True):
                    request_id = f"mode-{interactive}"
                    service.submit(
                        CrawlRequest(
                            request_id=request_id,
                            url="https://example.test",
                            pages=["/"],
                            interactive=interactive,
                        ),
                        source="rest",
                    )
                    job = await asyncio.wait_for(service.wait(request_id), 5)
                    self.assertEqual(job.state, "completed", job.error)
                    self.assertEqual(
                        job.browser_execution_id,
                        self.service.store.get(job.browser_lease_id)["id"],
                    )
                self.assertEqual(observed, [(True, False), (False, True)])
            finally:
                await service.close()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.root = Path(self.temporary.name)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        self.browser_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        self.server = uvicorn.Server(
            uvicorn.Config(
                create_app(self.service, api_token=None),
                log_level="critical",
                lifespan="off",
                access_log=False,
            )
        )
        self.server_task = asyncio.create_task(self.server.serve(sockets=[listener]))
        await until(lambda: self.server.started)

    async def asyncTearDown(self):
        self.server.should_exit = True
        await self.server_task
        await super().asyncTearDown()

    async def test_service_assigns_site_and_search_to_same_profile_and_shared_gate(
        self,
    ):
        service = CrawlService(
            self.root / "service",
            {"BROWSER_API_URL": self.browser_url},
            concurrency=2,
            max_pending=4,
        )
        profiles, searches = [], []
        both_started, release = asyncio.Event(), asyncio.Event()

        async def crawl(_url, **kwargs):
            client, search = kwargs["browser_client"], kwargs["search"]
            profile = self.service.get(client.id).profile
            self.assertIs(search.browser_client, client)
            self.assertIs(search.lock, service.search.lock)
            self.assertEqual(search.block_file, service.search.block_file)
            profiles.append(profile)
            searches.append(search)
            if len(profiles) == 2:
                both_started.set()
            await release.wait()
            return {"status": "finished", "stop_reason": "completed"}

        with patch("crawler_service.service.crawl_company", crawl):
            await service.start()
            try:
                for request in ("one", "two", "three"):
                    service.submit(
                        CrawlRequest(
                            request_id=request,
                            url=f"https://{request}.test",
                            pages=["/"],
                        ),
                        source="rest",
                    )
                await asyncio.wait_for(both_started.wait(), 3)
                self.assertIsNot(profiles[0], profiles[1])
                self.assertIsNot(searches[0], searches[1])
                release.set()
                for request in ("one", "two", "three"):
                    job = await asyncio.wait_for(service.wait(request), 4)
                    self.assertEqual(job.state, "completed")
                    self.assertFalse(
                        any(
                            s.store.for_profile(s.id) is not None
                            and s.store.for_profile(s.id)["request_id"] == request
                            for s in (
                                active.profile
                                for active in self.service.active.values()
                            )
                        )
                    )
            finally:
                await service.close()

    async def test_crawler_shutdown_releases_leases_but_leaves_browser_service_running(
        self,
    ):
        service = CrawlService(
            self.root / "service",
            {"BROWSER_API_URL": self.browser_url},
            concurrency=3,
            max_pending=4,
        )
        active = set()

        async def crawl(_url, **kwargs):
            active.add(
                self.service.store.for_profile(kwargs["browser_client"].profile_id)[
                    "request_id"
                ]
            )
            await asyncio.Event().wait()

        with patch("crawler_service.service.crawl_company", crawl):
            await service.start()
            try:
                for request in ("one", "two", "three"):
                    service.submit(
                        CrawlRequest(
                            request_id=request,
                            url=f"https://{request}.test",
                            pages=["/"],
                        ),
                        source="rest",
                    )
                await until(lambda: len(active) == 2 and "three" in service.executions)
                service.cancel("three")
                job = await asyncio.wait_for(service.wait("three"), 3)
                self.assertEqual(job.state, "cancelled")
                self.assertEqual(active, {"one", "two"})
            finally:
                await service.close()
            self.assertEqual(self.service.active, {})
            self.assertTrue(self.service.accepting)

    async def test_paused_manual_crawl_does_not_leave_second_profile_unused(self):
        service = CrawlService(
            self.root / "manual-service",
            {"CRAWL_HUMAN_ENABLED": "true", "BROWSER_API_URL": self.browser_url},
            concurrency=1,
            max_pending=3,
        )
        assigned = {}

        async def crawl(_url, **kwargs):
            client, human = kwargs["browser_client"], kwargs["human"]
            profile = self.service.get(client.id).profile
            assigned[profile.store.for_profile(profile.id)["request_id"]] = profile.id
            human.waiting = True
            human.notify("awaiting_human", {"reason": "Manual fixture pause"})
            await asyncio.Event().wait()

        with patch("crawler_service.service.crawl_company", crawl):
            await service.start()
            try:
                for request in ("one", "two", "three"):
                    service.submit(
                        CrawlRequest(
                            request_id=request,
                            url=f"https://{request}.test",
                            pages=["/"],
                        ),
                        source="manual",
                    )
                await until(lambda: len(assigned) == 2)
                self.assertEqual(set(assigned), {"one", "two"})
                self.assertNotEqual(assigned["one"], assigned["two"])
                self.assertEqual(service.jobs["one"].state, "awaiting_human")
                self.assertEqual(service.jobs["two"].state, "awaiting_human")
                self.assertEqual(service.jobs["three"].state, "queued")
                self.assertEqual(len(self.service.active), 2)
                service.cancel("one")
                self.assertEqual((await service.wait("one")).state, "cancelled")
                await until(lambda: "three" in assigned)
                self.assertNotEqual(assigned["three"], assigned["one"])
                self.assertEqual(service.jobs["two"].state, "awaiting_human")
                self.assertNotIn(assigned["one"], self.service.active)
                self.assertEqual(self.service.get(assigned["two"]).profile.starts, 1)
            finally:
                await service.close()
            self.assertEqual(service.executions, {})
            self.assertEqual(service.human_sessions, {})
