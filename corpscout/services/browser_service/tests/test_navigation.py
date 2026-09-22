"""Redirect provenance and bounded recovery from legacy HTTP-only entry sites."""

import asyncio
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from cloakbrowser import launch_async
from playwright.async_api import Error

from browser_service.browser import BrowserSession
from browser_service.capture import PageCapture


class NavigationTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_root_tls_protocol_errors_allow_one_http_attempt(self):
        cases = [
            ("https://old.test/", "ERR_SSL_PROTOCOL_ERROR", True),
            ("https://old.test/", "ERR_SSL_VERSION_OR_CIPHER_MISMATCH", True),
            ("https://old.test/", "ERR_CERT_AUTHORITY_INVALID", False),
            ("https://old.test/", "ERR_CERT_COMMON_NAME_INVALID", False),
            ("https://old.test/", "ERR_NAME_NOT_RESOLVED", False),
            ("https://old.test/account", "ERR_SSL_PROTOCOL_ERROR", False),
            ("https://old.test/?token=private", "ERR_SSL_PROTOCOL_ERROR", False),
        ]
        for url, code, fallback in cases:
            with self.subTest(url=url, code=code):
                session = BrowserSession(SimpleNamespace(), SimpleNamespace(on=Mock()))
                success = PageCapture(
                    url="https://new.test/home",
                    html="Company",
                    status_code=200,
                    headers={},
                    error=None,
                )
                session.navigate_once = AsyncMock(
                    side_effect=[Error(f"Page.goto: net::{code}"), success]
                )
                capture = await session.navigate(
                    url, timeout_seconds=10, check_robots_txt=True
                )
                self.assertEqual(
                    session.navigate_once.await_count, 2 if fallback else 1
                )
                self.assertEqual(
                    capture.navigation_attempts[0], {"url": url, "error": code}
                )
                if fallback:
                    self.assertEqual(
                        session.navigate_once.call_args.args[0], "http://old.test/"
                    )
                    self.assertIsNone(capture.error)
                    self.assertEqual(capture.url, "https://new.test/home")
                else:
                    self.assertIn(code, capture.error)
                    self.assertEqual(capture.html, "")

    async def test_failed_http_fallback_is_terminal_and_tab_closure_still_raises(self):
        session = BrowserSession(SimpleNamespace(), SimpleNamespace(on=Mock()))
        session.navigate_once = AsyncMock(
            side_effect=Error("net::ERR_SSL_PROTOCOL_ERROR")
        )
        capture = await session.navigate(
            "https://old.test/", timeout_seconds=10, check_robots_txt=True
        )
        self.assertEqual(len(capture.navigation_attempts), 2)
        self.assertIsNone(capture.status_code)
        session.navigate_once = AsyncMock(
            side_effect=Error("Target page, context or browser has been closed")
        )
        with self.assertRaisesRegex(Error, "has been closed"):
            await session.navigate(
                "https://old.test/", timeout_seconds=10, check_robots_txt=True
            )


@unittest.skipUnless(
    os.environ.get("BROWSER_NAVIGATION_NATIVE_TEST") == "1",
    "opt-in native navigation fixture",
)
class NativeNavigationTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_fallback_redirects_and_destination_robots(self):
        paths = []

        class Website(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                paths.append((self.server.server_port, self.path))
                if self.path == "/robots.txt":
                    body = b"User-agent: *\nDisallow: /private\n"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                elif self.path == "/":
                    self.send_response(301)
                    self.send_header("Location", target + "/company")
                    self.end_headers()
                    return
                elif self.path == "/denied":
                    self.send_response(302)
                    self.send_header("Location", target + "/private")
                    self.end_headers()
                    return
                else:
                    body = b"<html><h1>Destination Company</h1></html>"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        servers = [ThreadingHTTPServer(("127.0.0.1", 0), Website) for _ in range(2)]
        for server in servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()
        origin, target = [
            f"http://127.0.0.1:{server.server_port}" for server in servers
        ]
        browser = await launch_async(headless=True)
        try:
            context = await browser.new_context()
            session = BrowserSession(context, await context.new_page())
            # TLS sent to this deliberately HTTP-only fixture fails before its 301.
            capture = await session.navigate(
                origin.replace("http:", "https:") + "/",
                timeout_seconds=10,
                check_robots_txt=True,
            )
            self.assertEqual(capture.status_code, 200)
            self.assertEqual(capture.url, target + "/company")
            self.assertEqual(
                capture.redirects,
                [
                    {
                        "url": origin + "/",
                        "location": target + "/company",
                        "status_code": 301,
                    }
                ],
            )
            self.assertEqual(len(capture.navigation_attempts), 2)
            self.assertIn("Destination Company", capture.html)
            self.assertIn((servers[1].server_port, "/robots.txt"), paths)
            self.assertEqual(len(context.pages), 1, "Temporary robots tabs must close")
            denied = await session.navigate(
                origin + "/denied", timeout_seconds=10, check_robots_txt=True
            )
            self.assertEqual(denied.url, target + "/private")
            self.assertEqual(denied.html, "")
            self.assertIn("robots.txt", denied.error)
            self.assertEqual(denied.redirects[0]["status_code"], 302)
            self.assertEqual(len(denied.navigation_attempts), 1)
        finally:
            await browser.close()
            for server in servers:
                await asyncio.to_thread(server.shutdown)
                server.server_close()
