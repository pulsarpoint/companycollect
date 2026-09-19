"""Opt-in real Xvfb profiles, tabs, cookies and browser-restart persistence."""

import asyncio
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

from browser_service.browser_sessions import BrowserSessions
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
                pool = BrowserSessions(Path(directory), 2)
                try:
                    await pool.start()
                    first, second = pool.sessions.values()
                    self.assertEqual(len(ACTIVE_DESKTOPS), 2)
                    self.assertEqual(
                        {item.owner for item in ACTIVE_DESKTOPS.values()},
                        {"browser-1", "browser-2"},
                    )
                    self.assertEqual(
                        [first.state, second.state], ["running", "running"]
                    )
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
                    await first.stop()
                    self.assertNotIn(previous_desktop, ACTIVE_DESKTOPS)
                    self.assertEqual(len(ACTIVE_DESKTOPS), 1)
                    await first.start()
                    self.assertNotEqual(first.generation, previous_generation)
                    pages = [
                        tab.page
                        for tab in first.tabs.values()
                        if tab.page.url.startswith(url)
                    ]
                    self.assertEqual(len(pages), 2)
                    await pages[0].wait_for_load_state("domcontentloaded")
                    self.assertEqual(
                        await pages[0].evaluate("localStorage.getItem('fixture')"),
                        "retained",
                    )
                    self.assertTrue(
                        any(
                            cookie["name"] == "fixture_session"
                            for cookie in await first.context.cookies()
                        )
                    )
                    # Closing the browser window must leave a restartable session.
                    await first.set_auto_restart(False)
                    await first.save()
                    cdp = await first.context.browser.new_browser_cdp_session()
                    await cdp.send("Browser.close")
                    async with asyncio.timeout(5):
                        while first.state != "error":
                            await asyncio.sleep(0.02)
                    self.assertIsNone(first.generation)
                    await first.start()
                    self.assertEqual(first.state, "running")
                    await first.set_auto_restart(True)
                    generation = first.generation
                    self.assertEqual(len(first.context.pages), 2)
                    await first.context.pages[0].close()
                    await asyncio.sleep(1.2)
                    self.assertEqual(first.state, "running")
                    self.assertEqual(first.generation, generation)
                    await first.context.pages[0].close()
                    async with asyncio.timeout(30):
                        while (
                            first.state != "running" or first.generation == generation
                        ):
                            await asyncio.sleep(0.05)
                    self.assertEqual(len(first.context.pages), 1)
                    self.assertEqual(first.context.pages[0].url, "about:blank")
                    self.assertTrue(
                        any(
                            cookie["name"] == "fixture_session"
                            for cookie in await first.context.cookies()
                        )
                    )
                    page = first.context.pages[0]
                    await page.goto(url + "/second")
                    self.assertEqual(
                        await page.evaluate("localStorage.getItem('fixture')"),
                        "retained",
                    )
                    await page.close()
                    await first.stop()
                    await asyncio.sleep(1.2)
                    self.assertEqual(first.state, "stopped")
                    self.assertIsNone(first.restart_task)
                    self.assertIsNone(first.context)
                finally:
                    await pool.close()
                self.assertEqual(ACTIVE_DESKTOPS, {})
        finally:
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            thread.join()
