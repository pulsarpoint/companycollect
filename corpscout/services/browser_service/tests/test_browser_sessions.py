import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from test_browser_api import BrowserAPITests

from browser_service.api import create_app
from browser_service.browser_sessions import PersistentBrowserSession
from browser_service.runtime import BrowserRuntimeSettings, BrowserService
from browser_service.session_store import SessionStore
from browser_service.virtual_desktop import ACTIVE_DESKTOPS, VirtualDesktop


class SavedBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_saved_session_cookies_are_private_and_not_in_status(self):
        with TemporaryDirectory() as directory:
            store = SessionStore(Path(directory) / "sessions.sqlite3")
            self.addCleanup(store.close)
            session = PersistentBrowserSession(
                "browser-1",
                Path(directory),
                store,
                headless=True,
                route="direct",
                proxy=None,
            )
            session.context = SimpleNamespace(
                cookies=AsyncMock(
                    return_value=[{"name": "session", "value": "private-cookie"}]
                )
            )
            session.state = "running"
            await session.save()
            saved = Path(directory) / "session.json"
            self.assertEqual(saved.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                json.loads(saved.read_text())["cookies"][0]["value"], "private-cookie"
            )
            self.assertNotIn("private-cookie", json.dumps(await session.snapshot()))
            self.assertNotIn("cookies", await session.snapshot())


class ManualSessionTests(BrowserAPITests):
    async def test_manual_start_stop_pin_and_url_validation(self):
        response = await self.http.post(
            "/v1/browser-sessions", json={"headless": False}
        )
        self.assertEqual(response.status_code, 201, response.text)
        identifier = response.json()["id"]
        execution = self.service.get(identifier).execution_id
        profile = self.service.get(identifier).profile
        profile.open_tab = AsyncMock(return_value="tab-one")
        for url in (
            "file:///etc/passwd",
            "javascript:alert(1)",
            "https://user:pass@example.test",
        ):
            self.assertEqual(
                (
                    await self.http.post(
                        f"/v1/browser-sessions/{identifier}/tabs", json={"url": url}
                    )
                ).status_code,
                422,
            )
        profile.open_tab.assert_not_called()
        response = await self.http.post(
            f"/v1/browser-sessions/{identifier}/tabs",
            json={"url": "https://example.test"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        for value, status in (("true", 422), (True, 200)):
            response = await self.http.post(
                f"/v1/browser-sessions/{identifier}/settings", json={"pinned": value}
            )
            self.assertEqual(response.status_code, status)
        self.assertTrue(self.service.store.session(identifier)["pinned"])
        self.assertEqual(
            (
                await self.http.post(f"/v1/browser-sessions/{identifier}/stop", json={})
            ).status_code,
            409,
        )
        response = await self.http.post(
            f"/v1/browser-sessions/{identifier}/stop", json={"executionId": execution}
        )
        self.assertEqual(response.status_code, 200, response.text)
        response = await self.http.post(
            f"/v1/browser-sessions/{identifier}/start", json={"headless": True}
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(self.service.get(identifier).profile.headless)
        self.assertNotEqual(self.service.get(identifier).execution_id, execution)


class SavedBrowserTickets(unittest.TestCase):
    def test_management_does_not_expose_private_ports_and_stale_tickets_fail(self):
        with (
            TemporaryDirectory() as directory,
            patch.dict(ACTIVE_DESKTOPS, {}, clear=True),
        ):
            service = BrowserService(
                Path(directory),
                settings=BrowserRuntimeSettings(
                    max_browsers=1, idle_timeout_seconds=120, session_retention_days=7
                ),
            )
            self.addCleanup(service.store.close)
            desktop = VirtualDesktop(101, 201, "saved", "browser-1")
            ACTIVE_DESKTOPS[desktop.id] = desktop
            client = TestClient(create_app(service, api_token="private-token"))
            self.assertEqual(client.get("/v1/server").status_code, 401)
            client.headers["Authorization"] = "Bearer private-token"
            data = client.get("/v1/server").json()
            self.assertEqual(data["sessions"], [])
            for private in ("vnc_port", "cdp_port", "private-token", "cookies"):
                self.assertNotIn(private, json.dumps(data))
            endpoint = f"/v1/desktops/{desktop.id}/browser-ticket"
            ticket = client.post(endpoint).json()
            ACTIVE_DESKTOPS.pop(desktop.id)
            self.assertEqual(client.post(endpoint).status_code, 404)
            with self.assertRaises(WebSocketDisconnect):
                with client.websocket_connect(ticket["websocket_path"]):
                    self.fail("Closed desktop ticket connected")
            ACTIVE_DESKTOPS[desktop.id] = desktop
            with self.assertRaises(WebSocketDisconnect):
                with client.websocket_connect(ticket["websocket_path"]):
                    self.fail("Consumed ticket connected")
