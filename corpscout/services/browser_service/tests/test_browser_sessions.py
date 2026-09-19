import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from browser_service.api import create_app
from browser_service.browser_sessions import BrowserSessions, PersistentBrowserSession
from browser_service.runtime import BrowserService
from browser_service.virtual_desktop import ACTIVE_DESKTOPS, VirtualDesktop


class SavedBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_auto_restart_setting_is_validated_and_persisted_per_profile(self):
        with TemporaryDirectory() as directory:
            service = BrowserService(
                Path(directory), count=2, max_pending=5, idle_timeout=120
            )
            app = create_app(service, api_token="private")
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                endpoint = "/v1/browser-sessions/browser-1/settings"
                self.assertEqual(
                    (
                        await client.post(endpoint, json={"auto_restart": False})
                    ).status_code,
                    401,
                )
                client.headers["Authorization"] = "Bearer private"
                self.assertEqual(
                    (
                        await client.post(endpoint, json={"auto_restart": "false"})
                    ).status_code,
                    422,
                )
                response = await client.post(endpoint, json={"auto_restart": False})
                self.assertEqual(response.status_code, 200)
                self.assertFalse(response.json()["auto_restart"])
                self.assertTrue(service.pool.sessions["browser-2"].auto_restart)
                first = service.pool.sessions["browser-1"]
                restored = PersistentBrowserSession("browser-1", first.root)
                self.assertFalse(restored.auto_restart)
                self.assertEqual(
                    (first.root / "settings.json").stat().st_mode & 0o777, 0o600
                )

    async def test_explicit_stop_and_disabled_setting_cancel_pending_restarts(self):
        with TemporaryDirectory() as directory:
            session = PersistentBrowserSession("browser-1", Path(directory))
            session.start = AsyncMock()
            session.wanted_running = True
            session.schedule_restart()
            original = session.restart_task
            session.schedule_restart()
            self.assertIs(session.restart_task, original)
            await session.stop()
            await asyncio.sleep(0)
            session.start.assert_not_called()
            self.assertEqual(session.state, "stopped")
            self.assertFalse(session.wanted_running)
            session.wanted_running = True
            session.schedule_restart()
            await session.set_auto_restart(False)
            self.assertIsNone(session.restart_task)
            session.start.assert_not_called()
            await session.stop()

    async def test_failed_automatic_restart_is_not_retried_in_a_loop(self):
        with TemporaryDirectory() as directory:
            session = PersistentBrowserSession("browser-1", Path(directory))
            session.wanted_running = True
            session.start = AsyncMock(
                side_effect=RuntimeError("fixture startup failure")
            )
            session.schedule_restart()
            with self.assertLogs("browser_service.browser_sessions", level="WARNING"):
                await session.restart_task
            self.assertIsNone(session.restart_task)
            session.start.assert_awaited_once_with(restore_tabs=False)
            self.assertEqual(session.state, "error")
            await session.stop()

    async def test_saved_session_cookies_are_private_and_not_in_status(self):
        with TemporaryDirectory() as directory:
            session = PersistentBrowserSession("browser-1", Path(directory))
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

    async def test_profile_roots_and_configuration_are_independent(self):
        with TemporaryDirectory() as directory:
            pool = BrowserSessions(Path(directory), 2)
            self.assertEqual(list(pool.sessions), ["browser-1", "browser-2"])
            self.assertNotEqual(
                pool.sessions["browser-1"].root, pool.sessions["browser-2"].root
            )
            for count in (-1, 17):
                with self.assertRaises(ValueError):
                    BrowserSessions(Path(directory), count)

    async def test_api_authentication_and_tab_url_validation(self):
        with TemporaryDirectory() as directory:
            service = BrowserService(
                Path(directory), count=2, max_pending=5, idle_timeout=120
            )
            session = service.pool.sessions["browser-1"]
            session.open_tab = AsyncMock(return_value="tab-one")
            app = create_app(service, api_token="secret-token")
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                self.assertEqual(
                    (await client.get("/v1/browser-sessions")).status_code, 401
                )
                client.headers["Authorization"] = "Bearer secret-token"
                sessions = (await client.get("/v1/browser-sessions")).json()["sessions"]
                self.assertEqual(len(sessions), 2)
                self.assertEqual(
                    (
                        await client.post("/v1/browser-sessions/browser-9/start")
                    ).status_code,
                    404,
                )
                for url in (
                    "file:///etc/passwd",
                    "javascript:alert(1)",
                    "https://user:password@example.com",
                ):
                    response = await client.post(
                        "/v1/browser-sessions/browser-1/tabs", json={"url": url}
                    )
                    self.assertEqual(response.status_code, 422)
                session.open_tab.assert_not_called()
                response = await client.post(
                    "/v1/browser-sessions/browser-1/tabs",
                    json={"url": "https://melexis.com/"},
                )
                self.assertEqual(response.status_code, 200)
                session.open_tab.assert_awaited_once_with("https://melexis.com/")
                self.assertEqual(
                    (
                        await client.post(
                            "/v1/browser-sessions/browser-1/browser-ticket"
                        )
                    ).status_code,
                    409,
                )

    async def test_stopped_session_does_not_accept_new_tabs(self):
        with TemporaryDirectory() as directory:
            session = PersistentBrowserSession("browser-1", Path(directory))
            with self.assertRaisesRegex(ValueError, "Start"):
                await session.open_tab("https://example.com/")


class SavedBrowserTickets(unittest.TestCase):
    def test_management_does_not_expose_private_ports_and_stale_tickets_fail(self):
        with (
            TemporaryDirectory() as directory,
            patch.dict(ACTIVE_DESKTOPS, {}, clear=True),
        ):
            service = BrowserService(
                Path(directory), count=1, max_pending=5, idle_timeout=120
            )
            self.addCleanup(service.store.close)
            desktop = VirtualDesktop(101, 201, "saved", "browser-1")
            ACTIVE_DESKTOPS[desktop.id] = desktop
            client = TestClient(create_app(service, api_token="private-token"))
            self.assertEqual(client.get("/v1/server").status_code, 401)
            client.headers["Authorization"] = "Bearer private-token"
            data = client.get("/v1/server").json()
            self.assertEqual(data["sessions"][0]["name"], "browser-1")
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
