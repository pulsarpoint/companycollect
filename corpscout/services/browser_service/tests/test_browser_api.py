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
from browser_service.runtime import BrowserService


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
            Path(self.temporary.name), count=2, max_pending=5, idle_timeout=120
        )
        self.pool, self.mux = (
            self.service.pool,
            self.service.multiplexer,
        )
        for profile in self.pool.sessions.values():

            async def start(*, restore_tabs=True, profile=profile):
                profile.wanted_running = True
                profile.state = "running"
                profile.context = SimpleNamespace(
                    browser=None, pages=[], new_page=AsyncMock(side_effect=FixturePage)
                )
                profile.generation = uuid4().hex

            profile.start = AsyncMock(side_effect=start)
            profile.save = AsyncMock()
        self.driver = patch(
            "browser_service.browser_multiplexer.BrowserSession", FixtureTab
        )
        self.driver.start()
        self.addCleanup(self.driver.stop)
        await self.pool.start()
        await self.mux.start()
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(
                create_app(self.service, api_token="fixture-token")
            ),
            base_url="http://test",
            headers={"Authorization": "Bearer fixture-token"},
        )

    async def asyncTearDown(self):
        self.pool.closing = True
        await self.mux.close()
        await self.pool.close()
        await self.http.aclose()
        self.service.store.close()

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

    async def test_two_profiles_queue_third_session_and_keep_affinity_until_released(
        self,
    ):
        first, second, third = [
            await self.reserve(name) for name in ("one", "two", "three")
        ]
        await until(
            lambda: (
                self.mux.reservations[first].state
                == self.mux.reservations[second].state
                == "ready"
            )
        )
        self.assertEqual(self.mux.reservations[third].state, "queued")
        self.assertNotEqual(
            self.mux.reservations[first].profile.id,
            self.mux.reservations[second].profile.id,
        )
        self.assertEqual(
            await self.reserve("one"), first
        )  # Retry cannot consume another profile.
        assigned = self.mux.reservations[first].profile
        generation = assigned.generation
        self.assertEqual((await assigned.snapshot())["lease_id"], first)
        for name in ("site", "search", "site"):
            response = await self.http.post(
                "/v1/browser/extract",
                json={
                    "session": {"id": first},
                    "tab": name,
                    "url": "https://example.test/",
                },
            )
            self.assertEqual(response.status_code, 200)
            document = response.json()
            self.assertEqual(document["session"]["id"], first)
            self.assertEqual(document["session"]["profileId"], assigned.id)
            self.assertNotIn("set-cookie", document["headers"])
        self.assertEqual(assigned.generation, generation)
        tabs = self.mux.reservations[first].tabs
        self.assertIs(tabs["site"].context, tabs["search"].context)
        queued = await self.http.post(
            "/v1/browser/extract",
            json={"session": {"id": third}, "url": "https://three.test/"},
        )
        self.assertEqual(queued.status_code, 409)
        released = await self.http.delete(f"/v1/browser/sessions/{first}")
        self.assertEqual(released.status_code, 200)
        await until(lambda: self.mux.reservations[third].state == "ready")
        self.assertIs(self.mux.reservations[third].profile, assigned)
        self.assertNotEqual(assigned.generation, generation)
        self.assertEqual(
            (await self.http.get(f"/v1/browser/sessions/{first}")).status_code, 410
        )
        self.assertEqual(
            sum(p.start.await_count for p in self.pool.sessions.values()), 3
        )

    async def test_unknown_ids_missing_id_disabled_pool_and_auth_never_launch(self):
        before = sum(p.start.await_count for p in self.pool.sessions.values())
        for payload, status in [
            ({"url": "https://example.test"}, 422),
            ({"session": {"id": "0" * 32}}, 404),
        ]:
            response = await self.http.post("/v1/browser/extract", json=payload)
            self.assertEqual(response.status_code, status)
        denied = await self.http.post(
            "/v1/browser/sessions",
            headers={"Authorization": "Bearer wrong"},
            json={"id": "1" * 32, "requestId": "one", "domain": "one.test"},
        )
        self.assertEqual(denied.status_code, 401)
        self.mux.accepting = False
        response = await self.http.post(
            "/v1/browser/sessions",
            json={"id": "1" * 32, "requestId": "one", "domain": "one.test"},
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            sum(p.start.await_count for p in self.pool.sessions.values()), before
        )
        self.assertEqual(self.mux.reservations, {})

    async def test_first_extract_allocates_once_and_serializes_concurrent_requests(
        self,
    ):
        identifier = uuid4().hex
        responses = await asyncio.gather(
            *(
                self.http.post(
                    "/v1/browser/extract",
                    json={
                        "session": {"id": identifier},
                        "url": f"https://example.test/{index}",
                    },
                )
                for index in range(3)
            )
        )
        self.assertEqual([r.status_code for r in responses], [200, 200, 200])
        assigned = {r.json()["session"]["profileId"] for r in responses}
        self.assertEqual(len(assigned), 1)
        self.assertEqual(len(self.service.store.recent()), 1)
        self.assertEqual(
            self.service.store.get(identifier)["profile_id"], assigned.pop()
        )
        self.assertEqual(self.mux.reservations[identifier].tabs["site"].max_inflight, 1)
        self.assertEqual(
            sum(p.start.await_count for p in self.pool.sessions.values()), 2
        )
        captured = await self.http.post(
            "/v1/browser/extract", json={"session": {"id": identifier}}
        )
        self.assertEqual(captured.status_code, 200)
        self.assertEqual(captured.json()["url"], "https://example.test/2")

    async def test_optional_browser_is_sticky_and_cannot_be_changed(self):
        identifier = uuid4().hex
        payload = {
            "session": {"id": identifier},
            "browserId": "browser-2",
            "url": "https://example.test/",
        }
        first = await self.http.post("/v1/browser/extract", json=payload)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json()["session"]["profileId"], "browser-2")
        search = await self.http.post(
            "/v1/browser/extract",
            json={
                "session": {"id": identifier},
                "tab": "search",
                "url": "https://search.test/",
            },
        )
        self.assertEqual(search.status_code, 200)
        self.assertEqual(search.json()["session"]["profileId"], "browser-2")
        mismatch = await self.http.post(
            "/v1/browser/extract", json=payload | {"browserId": "browser-1"}
        )
        self.assertEqual(mismatch.status_code, 409)
        self.assertEqual(self.service.store.get(identifier)["profile_id"], "browser-2")
        self.assertEqual(self.service.store.get(identifier)["domain"], "example.test")
        self.assertEqual(len(self.service.store.recent()), 1)

    async def test_busy_browser_does_not_fall_back_or_create_a_waiting_session(self):
        first, second, third = [uuid4().hex for _ in range(3)]
        payload = {
            "session": {"id": first},
            "browserId": "browser-2",
            "url": "https://example.test/",
        }
        self.assertEqual(
            (await self.http.post("/v1/browser/extract", json=payload)).status_code, 200
        )
        waiting_payload = payload | {"session": {"id": second}}
        busy = await self.http.post("/v1/browser/extract", json=waiting_payload)
        self.assertEqual(busy.status_code, 503)
        self.assertIsNone(self.service.store.get(second))
        automatic = await self.http.post(
            "/v1/browser/extract",
            json={
                "session": {"id": third},
                "url": "https://third.test/",
            },
        )
        self.assertEqual(automatic.status_code, 200)
        self.assertEqual(automatic.json()["session"]["profileId"], "browser-1")
        overflow = uuid4().hex
        self.assertEqual(
            (
                await self.http.post(
                    "/v1/browser/extract",
                    json={
                        "session": {"id": overflow},
                        "url": "https://overflow.test/",
                    },
                )
            ).status_code,
            503,
        )
        self.assertIsNone(self.service.store.get(overflow))
        await self.http.delete(f"/v1/browser/sessions/{first}")
        retried = await self.http.post("/v1/browser/extract", json=waiting_payload)
        self.assertEqual(retried.status_code, 200)
        self.assertEqual(retried.json()["session"]["profileId"], "browser-2")

    async def test_simultaneous_first_requests_use_distinct_existing_browsers(self):
        identifiers = [uuid4().hex, uuid4().hex]
        responses = await asyncio.gather(
            *(
                self.http.post(
                    "/v1/browser/extract",
                    json={
                        "session": {"id": identifier},
                        "url": "https://example.test/",
                    },
                )
                for identifier in identifiers
            )
        )
        self.assertEqual([r.status_code for r in responses], [200, 200])
        self.assertEqual(
            {r.json()["session"]["profileId"] for r in responses},
            {"browser-1", "browser-2"},
        )
        self.assertEqual(
            sum(p.start.await_count for p in self.pool.sessions.values()), 2
        )

    async def test_first_request_validation_and_terminal_ids_never_allocate(self):
        for options in [
            {"browserId": "browser-99"},
            {"url": "https://name:password@example.test/"},
            {"unknownOption": True},
        ]:
            identifier = uuid4().hex
            result = await self.http.post(
                "/v1/browser/extract",
                json={
                    "session": {"id": identifier},
                    "url": "https://example.test/",
                    **options,
                },
            )
            self.assertEqual(result.status_code, 422)
            self.assertIsNone(self.service.store.get(identifier))
        identifier = uuid4().hex
        payload = {"session": {"id": identifier}, "url": "https://example.test/"}
        await self.http.post("/v1/browser/extract", json=payload)
        self.mux.reservations[identifier].touched = 0
        self.assertEqual(
            (await self.http.post("/v1/browser/extract", json=payload)).status_code, 410
        )
        await self.http.delete(f"/v1/browser/sessions/{identifier}")
        self.assertEqual(
            (await self.http.post("/v1/browser/extract", json=payload)).status_code, 410
        )

    async def test_parallel_requests_on_one_session_are_serialized_and_capture_does_not_navigate(
        self,
    ):
        identifier = await self.reserve("one")
        await until(lambda: self.mux.reservations[identifier].state == "ready")
        requests = [
            {"session": {"id": identifier}, "url": f"https://example.test/{i}"}
            for i in range(3)
        ]
        responses = await asyncio.gather(
            *(
                self.http.post("/v1/browser/extract", json=payload)
                for payload in requests
            )
        )
        self.assertEqual(
            [r.json()["url"] for r in responses], [p["url"] for p in requests]
        )
        tab = self.mux.reservations[identifier].tabs["site"]
        self.assertEqual(tab.max_inflight, 1)
        result = await self.http.post(
            "/v1/browser/extract",
            json={"session": {"id": identifier}, "screenshot": True},
        )
        self.assertEqual(result.json()["url"], requests[-1]["url"])
        self.assertEqual(result.json()["screenshot"], "Zml4dHVyZS1pbWFnZQ==")

    async def test_idle_reservations_expire_while_heartbeats_keep_manual_waits_alive(
        self,
    ):
        first, second, queued = [
            await self.reserve(name) for name in ("one", "two", "three")
        ]
        await until(lambda: self.mux.reservations[first].state == "ready")
        self.mux.reservations[first].touched = 0
        await self.http.post(f"/v1/browser/sessions/{second}/heartbeat")
        await until(
            lambda: (
                first not in self.mux.reservations
                and self.mux.reservations[queued].state == "ready"
            )
        )
        self.assertEqual(self.mux.reservations[second].state, "ready")
        self.assertEqual(
            (await self.http.get(f"/v1/browser/sessions/{first}")).status_code, 410
        )
