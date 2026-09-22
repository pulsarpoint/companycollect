"""Exercise sticky HTTP routing and lifecycle at the browser-driver boundary."""

import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx

from browser_service.api import create_app
from browser_service.browser_sessions import PersistentBrowserSession
from browser_service.capture import PageCapture
from browser_service.runtime import BrowserRuntimeSettings, BrowserService


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

        async def start(profile, *, restore_tabs=False):
            profile.state = "running"
            profile.context = SimpleNamespace(
                browser=None, pages=[], new_page=AsyncMock(side_effect=FixturePage)
            )
            profile.generation = uuid4().hex

        self.starts = patch.object(PersistentBrowserSession, "start", start)
        self.starts.start()
        self.addCleanup(self.starts.stop)
        self.saves = patch.object(PersistentBrowserSession, "save", AsyncMock())
        self.saves.start()
        self.addCleanup(self.saves.stop)
        self.driver = patch("browser_service.runtime.BrowserSession", FixtureTab)
        self.driver.start()
        self.addCleanup(self.driver.stop)
        await self.service.start()
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(
                create_app(
                    self.service,
                    api_token="fixture-token",
                    deepseek_api_key="fixture-key",
                    openrouter_api_key="fixture-openrouter-key",
                )
            ),
            base_url="http://test",
            headers={"Authorization": "Bearer fixture-token"},
        )

    async def asyncTearDown(self):
        await self.service.close()
        await self.http.aclose()

    def execution_headers(self, identifier):
        return {"X-Browser-Execution-Id": self.service.active[identifier].execution_id}

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

    async def test_agent_requires_approval_and_matching_live_page(self):
        identifier = await self.reserve("agent-guard")
        session = self.service.get(identifier)
        tab = await self.service.open_tab(session, "site")
        tab.page.url = "https://example.test/"
        path = f"/v1/browser/sessions/{identifier}/tabs/site/challenge-agent"
        body = {
            "confirm": True,
            "expectedUrl": tab.page.url,
            "expectedGeneration": session.profile.generation,
        }
        unauthorized = await self.http.post(
            path, json=body, headers={"Authorization": "Bearer wrong"}
        )
        self.assertEqual(unauthorized.status_code, 401)
        for changed in (
            {"confirm": False},
            {"confirm": "true"},
            {"maxSteps": 21},
            {"timeoutSeconds": 181},
        ):
            self.assertEqual(
                (await self.http.post(path, json=body | changed)).status_code, 422
            )
        for changed in (
            {"expectedGeneration": "stale"},
            {"expectedUrl": "https://other.test/"},
        ):
            self.assertEqual(
                (await self.http.post(path, json=body | changed)).status_code, 409
            )
        async with session.lock:
            self.assertEqual((await self.http.post(path, json=body)).status_code, 409)
        await self.service.release(identifier)
        self.assertEqual((await self.http.post(path, json=body)).status_code, 410)
        self.assertFalse((self.service.root / "challenge-runs").exists())

    async def test_agent_uses_selected_cdp_tab_and_keeps_lease_after_finishing(self):
        identifier = await self.reserve("agent")
        session = self.service.get(identifier)
        tab = await self.service.open_tab(session, "site")
        tab.page.url = "https://example.test/"
        tab.page.context = session.profile.context
        cdp = SimpleNamespace(send=AsyncMock(), detach=AsyncMock())
        tab.page.context.new_cdp_session = AsyncMock(return_value=cdp)
        tab.page.evaluate = AsyncMock(return_value={"width": 800, "height": 600})
        tab.page.screenshot = AsyncMock(return_value=b"fixture-image")
        calls = []
        original_send = httpx.AsyncClient.send

        async def model(client, request, **kwargs):
            if request.url.host != "api.deepseek.com":
                return await original_send(client, request, **kwargs)
            payload = json.loads(request.content)
            calls.append(payload)
            action = (
                {
                    "action": "click",
                    "x": 100,
                    "y": 120,
                    "reason": "Select visible challenge",
                }
                if len(calls) == 1
                else {
                    "action": "finish",
                    "outcome": "appears_clear",
                    "reason": "Company content is visible",
                }
            )
            return httpx.Response(
                200,
                request=request,
                json={
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": json.dumps(action)},
                        }
                    ],
                    "usage": {"prompt_tokens": 20, "completion_tokens": 10},
                },
            )

        with patch("httpx.AsyncClient.send", model):
            response = await self.http.post(
                f"/v1/browser/sessions/{identifier}/tabs/site/challenge-agent",
                json={
                    "confirm": True,
                    "expectedUrl": tab.page.url,
                    "expectedGeneration": session.profile.generation,
                },
            )
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["state"], "appears_clear")
        self.assertEqual(
            result["usage"], {"prompt_tokens": 40, "completion_tokens": 20}
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["model"], "deepseek-flash")
        self.assertEqual(calls[0]["messages"][1]["content"][1]["type"], "image_url")
        cdp.send.assert_any_await(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseReleased",
                "x": 100,
                "y": 120,
                "button": "left",
                "clickCount": 1,
            },
        )
        cdp.detach.assert_awaited_once()
        self.assertEqual(self.service.snapshot(identifier)["state"], "ready")
        self.assertFalse(session.lock.locked())
        saved = self.service.root / "challenge-runs" / result["runId"]
        self.assertEqual(
            json.loads((saved / "result.json").read_text())["state"], "appears_clear"
        )
        self.assertTrue((saved / "02.png").exists())

    async def test_glm_uses_openrouter_and_saves_actual_model(self):
        identifier = await self.reserve("agent")
        session = self.service.get(identifier)
        tab = await self.service.open_tab(session, "site")
        tab.page.url = "https://example.test/"
        tab.page.context = session.profile.context
        cdp = SimpleNamespace(send=AsyncMock(), detach=AsyncMock())
        tab.page.context.new_cdp_session = AsyncMock(return_value=cdp)
        tab.page.evaluate = AsyncMock(return_value={"width": 800, "height": 600})
        tab.page.screenshot = AsyncMock(return_value=b"fixture-image")
        calls = []
        original_send = httpx.AsyncClient.send

        async def model(client, request, **kwargs):
            if request.url.host != "openrouter.ai":
                return await original_send(client, request, **kwargs)
            self.assertEqual(request.url.path, "/api/v1/chat/completions")
            self.assertEqual(
                request.headers["authorization"], "Bearer fixture-openrouter-key"
            )
            payload = json.loads(request.content)
            self.assertEqual(payload["reasoning"], {"effort": "low"})
            self.assertNotIn("thinking", payload)
            calls.append(payload)
            action = (
                {
                    "action": "click",
                    "x": 100,
                    "y": 120,
                    "reason": "Select visible challenge",
                }
                if len(calls) == 1
                else {
                    "action": "finish",
                    "outcome": "appears_clear",
                    "reason": "Company content is visible",
                }
            )
            return httpx.Response(
                200,
                request=request,
                json={
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": json.dumps(action)},
                        }
                    ],
                    "usage": {"prompt_tokens": 20, "completion_tokens": 10},
                },
            )

        with patch("httpx.AsyncClient.send", model):
            response = await self.http.post(
                f"/v1/browser/sessions/{identifier}/tabs/site/challenge-agent",
                json={
                    "confirm": True,
                    "model": "z-ai/glm-5.3-flash",
                    "expectedUrl": tab.page.url,
                    "expectedGeneration": session.profile.generation,
                },
            )
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["state"], "appears_clear")
        self.assertEqual(result["model"], "z-ai/glm-5.3-flash")
        self.assertEqual(
            result["usage"], {"prompt_tokens": 40, "completion_tokens": 20}
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["model"], "z-ai/glm-5.3-flash")
        self.assertEqual(calls[0]["messages"][1]["content"][1]["type"], "image_url")
        cdp.send.assert_any_await(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseReleased",
                "x": 100,
                "y": 120,
                "button": "left",
                "clickCount": 1,
            },
        )
        cdp.detach.assert_awaited_once()
        self.assertEqual(self.service.snapshot(identifier)["state"], "ready")
        self.assertFalse(session.lock.locked())
        saved = self.service.root / "challenge-runs" / result["runId"]
        self.assertEqual(
            json.loads((saved / "result.json").read_text())["state"], "appears_clear"
        )
        self.assertTrue((saved / "02.png").exists())

    async def test_on_demand_capacity_reopen_and_mode_change(self):
        self.assertFalse(self.service.active)
        first = await self.reserve("one")
        second = await self.reserve("two")
        response = await self.http.post(
            "/v1/browser/sessions",
            json={"requestId": "three", "domain": "three.test", "headless": False},
        )
        self.assertEqual(response.status_code, 503)
        saved_id = response.json()["detail"]["sessionId"]
        self.assertIsNotNone(self.service.store.session(saved_id))
        before = self.service.snapshot(first)
        await self.service.release(first, execution_id=before["executionId"])
        self.assertEqual(self.service.snapshot(first)["state"], "closed")
        self.assertEqual(len(self.service.active), 1)
        reopened = await self.http.post(
            "/v1/browser/sessions",
            json={
                "id": first,
                "requestId": "new-request",
                "domain": "one.test",
                "headless": False,
            },
        )
        self.assertEqual(reopened.status_code, 201)
        self.assertFalse(reopened.json()["headless"])
        self.assertNotEqual(before["executionId"], reopened.json()["executionId"])
        stale = await self.http.delete(
            "/v1/browser/sessions/" + first,
            headers={"X-Browser-Execution-Id": before["executionId"]},
        )
        self.assertEqual(stale.status_code, 409)
        self.assertIn(first, self.service.active)
        await self.service.release(second)

    async def test_another_request_cannot_share_a_profile(self):
        identifier = await self.reserve("owner")
        response = await self.http.post(
            "/v1/browser/sessions",
            json={
                "id": identifier,
                "requestId": "someone-else",
                "domain": "owner.test",
            },
        )
        self.assertEqual(response.status_code, 409)
        original = self.service.snapshot(identifier)
        response = await self.http.post(
            "/v1/browser/sessions",
            json={"id": identifier, "requestId": "owner", "domain": "owner.test"},
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["executionId"], original["executionId"])

    async def test_extract_allocates_and_preserves_session_identity(self):
        response = await self.http.post(
            "/v1/browser/extract", json={"url": "https://example.test/"}
        )
        self.assertEqual(response.status_code, 200, response.text)
        session = response.json()["session"]
        results = await asyncio.gather(
            *[
                self.http.post(
                    "/v1/browser/extract",
                    json={
                        "session": {
                            "id": session["id"],
                            "executionId": session["executionId"],
                        },
                        "url": "https://example.test/" + str(n),
                    },
                )
                for n in range(3)
            ]
        )
        self.assertTrue(all(r.status_code == 200 for r in results))
        self.assertEqual(self.service.get(session["id"]).tabs["site"].max_inflight, 1)
        self.assertEqual(len(self.service.active), 1)

    async def test_closed_session_status_is_visible_without_restarting(self):
        identifier = await self.reserve("closed")
        await self.service.release(identifier)
        response = await self.http.get("/v1/browser/sessions/" + identifier)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["state"], "closed")
        self.assertFalse(self.service.active)

    async def test_proxy_identity_cannot_change_when_reopened(self):
        self.service.proxy_routes["proxy-a"] = "http://private:test@localhost:9000"
        identifier = uuid4().hex
        response = await self.http.post(
            "/v1/browser/sessions",
            json={
                "id": identifier,
                "requestId": "proxy",
                "domain": "example.test",
                "route": "proxy-a",
            },
        )
        self.assertEqual(response.status_code, 201)
        await self.service.release(identifier)
        response = await self.http.post(
            "/v1/browser/sessions",
            json={
                "id": identifier,
                "requestId": "proxy-next",
                "domain": "example.test",
                "route": "direct",
            },
        )
        self.assertEqual(response.status_code, 409)
        self.assertNotIn("private", (await self.http.get("/v1/server")).text)
