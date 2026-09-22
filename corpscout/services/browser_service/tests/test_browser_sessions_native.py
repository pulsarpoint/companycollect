"""Opt-in real Xvfb profiles, tabs, cookies and browser-restart persistence."""

import asyncio
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from browser_service.runtime import BrowserRuntimeSettings, BrowserService
from browser_service.virtual_desktop import ACTIVE_DESKTOPS


@unittest.skipUnless(
    os.environ.get("COMPANY_RESEARCH_XVFB_TEST") == "1", "opt-in Linux/Xvfb test"
)
class SavedBrowserNativeTests(unittest.IsolatedAsyncioTestCase):
    async def test_profiles_share_tabs_but_isolate_sessions_and_survive_restart(self):
        class Fixture(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                if self.path == "/login":
                    self.send_header(
                        "Set-Cookie", "fixture_session=yes; Path=/; HttpOnly"
                    )
                self.end_headers()
                self.wfile.write(
                    b"<html><title>Saved browser fixture</title><h1>Fixture</h1></html>"
                )

        server = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with TemporaryDirectory() as directory:
                service = BrowserService(
                    Path(directory),
                    settings=BrowserRuntimeSettings(
                        max_browsers=2,
                        idle_timeout_seconds=120,
                        session_retention_days=7,
                    ),
                )
                try:
                    await service.start()
                    self.assertEqual(ACTIVE_DESKTOPS, {})
                    first_id, second_id = uuid4().hex, uuid4().hex
                    first_session = await service.claim(
                        identifier=first_id,
                        request_id="one",
                        domain="fixture",
                        headless=False,
                    )
                    second_session = await service.claim(
                        identifier=second_id,
                        request_id="two",
                        domain="fixture",
                        headless=False,
                    )
                    first, second = first_session.profile, second_session.profile
                    self.assertEqual(len(ACTIVE_DESKTOPS), 2)
                    url = f"http://127.0.0.1:{server.server_port}"
                    tab_id = await first.open_tab(url + "/login")
                    page = first.tabs[tab_id].page
                    await page.wait_for_load_state("domcontentloaded")
                    await page.evaluate("localStorage.setItem('fixture', 'retained')")
                    other_id = await first.open_tab(url + "/second")
                    await first.tabs[other_id].page.wait_for_load_state(
                        "domcontentloaded"
                    )
                    self.assertEqual(
                        await first.tabs[other_id].page.evaluate(
                            "localStorage.getItem('fixture')"
                        ),
                        "retained",
                    )
                    self.assertTrue(
                        any(
                            cookie["name"] == "fixture_session"
                            for cookie in await first.context.cookies()
                        )
                    )
                    await second.open_tab(url + "/isolated")
                    self.assertFalse(
                        any(
                            cookie["name"] == "fixture_session"
                            for cookie in await second.context.cookies()
                        )
                    )
                    reader, writer = await asyncio.open_connection(
                        "127.0.0.1", first.desktop.vnc_port
                    )
                    self.assertEqual(await reader.readexactly(12), b"RFB 003.008\n")
                    writer.close()
                    await writer.wait_closed()
                    previous_generation = first.generation
                    previous_desktop = first.desktop.id
                    await service.release(first_id)
                    self.assertNotIn(previous_desktop, ACTIVE_DESKTOPS)
                    self.assertEqual(len(ACTIVE_DESKTOPS), 1)
                    # The same profile can move from headed to headless and back.
                    first_session = await service.claim(
                        identifier=first_id,
                        request_id="reopened",
                        domain="fixture",
                        headless=True,
                    )
                    first = first_session.profile
                    self.assertNotEqual(first.generation, previous_generation)
                    self.assertIsNone(first.desktop.vnc_port)
                    page = await first.context.new_page()
                    await page.goto(url + "/check")
                    self.assertEqual(
                        await page.evaluate("localStorage.getItem('fixture')"),
                        "retained",
                    )
                    self.assertTrue(
                        any(
                            c["name"] == "fixture_session"
                            for c in await first.context.cookies()
                        )
                    )
                    await service.release(first_id)
                    first_session = await service.claim(
                        identifier=first_id,
                        request_id="headed-again",
                        domain="fixture",
                        headless=False,
                    )
                    self.assertIsNotNone(first_session.profile.desktop.vnc_port)
                    self.assertTrue(
                        any(
                            c["name"] == "fixture_session"
                            for c in await first_session.profile.context.cookies()
                        )
                    )
                finally:
                    await service.close()
                self.assertEqual(ACTIVE_DESKTOPS, {})
        finally:
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            thread.join()
