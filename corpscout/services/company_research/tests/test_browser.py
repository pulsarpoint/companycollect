"""HTML cleanup and opt-in native browser checks against a real HTTP server."""

import asyncio
import json
import os
import re
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

from native_browser_fixture import open_test_browser as open_browser

from company_research.browser import simplify_html
from company_research.human_control import HumanSession


class HTMLCleanupTests(unittest.TestCase):
    def test_preserves_content_and_resolves_links_without_executable_markup(self):
        raw = """<html><head><base href="/company/"><title>Company</title></head>
        <body><style>.x{color:red}</style><script>doSomething()</script><!--secret-->
        <h1 class="big" onclick="bad()">Company</h1><p>We build electronics.</p>
        <a href="jobs" aria-label="Open jobs">Careers</a><img src="logo.png" alt="Logo">
        <a href="javascript:bad()">Bad link</a><a href="mailto:hello@example.test">Email</a>
        <a href="http://[invalid">Malformed link</a>
        <table><tr><td colspan="2">Annual revenue</td></tr></table></body></html>"""
        cleaned = simplify_html(raw, "https://example.test/")
        for forbidden in (
            "<script",
            "<style",
            "<!--",
            "onclick",
            "javascript:",
            'class="',
        ):
            self.assertNotIn(forbidden, cleaned)
        for retained in (
            "Company",
            "We build electronics.",
            'aria-label="Open jobs"',
            'href="https://example.test/company/jobs"',
            'alt="Logo"',
            'href="mailto:hello@example.test"',
            'colspan="2"',
        ):
            self.assertIn(retained, cleaned)


@unittest.skipUnless(
    os.environ.get("COMPANY_RESEARCH_BROWSER_TEST") == "1", "opt-in browser test"
)
class NativeBrowserTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.paths = []
        self.identities = []
        self.reported = threading.Event()
        paths, identities, reported = self.paths, self.identities, self.reported

        class Website(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args) -> None:
                pass

            def do_GET(self):
                paths.append(self.path)
                if self.path in {"/redirect", "/allow"}:
                    self.send_response(303)
                    self.send_header("Location", "/company")
                    self.send_header("Set-Cookie", "test_session=yes; Path=/; HttpOnly")
                    self.end_headers()
                    return
                status = 200
                if self.path == "/robots.txt":
                    body = "User-agent: *\nDisallow: /private\n"
                elif self.path == "/company":
                    body = """<html><head><title>Company</title>
                    <script type="application/ld+json">{"@type":"Organization","name":"Example"}</script>
                    </head><body><h1>Company</h1><p id="dynamic"></p><a href="/jobs">Jobs</a>
                    <script>document.getElementById('dynamic').textContent='Loaded with JavaScript';
                    fetch('/identity', {method:'POST',body:JSON.stringify({
                        userAgent:navigator.userAgent,brands:navigator.userAgentData.brands
                    })});</script></body></html>"""
                elif self.path == "/jobs" and "test_session=yes" in self.headers.get(
                    "Cookie", ""
                ):
                    body = "<h1>Embedded Software Engineer</h1>"
                else:
                    status, body = (
                        403,
                        '<h1>Authentication required</h1><a href="/allow">Sign in</a>',
                    )
                encoded = body.encode()
                self.send_response(status)
                self.send_header(
                    "Content-Type",
                    "text/plain" if self.path == "/robots.txt" else "text/html",
                )
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def do_POST(self):
                identities.append(
                    {
                        "javascript": json.loads(
                            self.rfile.read(int(self.headers["Content-Length"]))
                        ),
                        "headers": {
                            key.casefold(): value for key, value in self.headers.items()
                        },
                    }
                )
                self.send_response(204)
                self.end_headers()
                reported.set()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Website)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    async def test_native_identity_rendering_redirects_cookies_and_robots(self):
        modes = (
            [False, True]
            if os.environ.get("COMPANY_RESEARCH_HEADED_TEST") == "1"
            else [False]
        )
        for headed in modes:
            with self.subTest(headed=headed):
                self.reported.clear()
                human = HumanSession(
                    headed=headed,
                    interactive=headed,
                    timeout=10,
                    notify=lambda *_: None,
                )
                async with open_browser(human) as browser:
                    company = await browser.navigate(
                        self.url + "/redirect",
                        timeout_seconds=10,
                        check_robots_txt=True,
                    )
                    self.assertTrue(company.successful)
                    self.assertEqual(company.url, self.url + "/company")
                    self.assertIn("Loaded with JavaScript", company.cleaned_html)
                    self.assertNotIn("<script", company.cleaned_html)
                    self.assertIn("application/ld+json", company.html)
                    self.assertIn(
                        self.url + "/jobs", [link["href"] for link in company.links]
                    )
                    self.assertTrue(await asyncio.to_thread(self.reported.wait, 5))
                    identity = self.identities[-1]
                    self.assertEqual(
                        identity["headers"]["user-agent"],
                        identity["javascript"]["userAgent"],
                    )
                    self.assertEqual(
                        sorted(
                            re.findall(
                                r'"([^"]+)";v="([^"]+)"',
                                identity["headers"]["sec-ch-ua"],
                            )
                        ),
                        sorted(
                            (brand["brand"], brand["version"])
                            for brand in identity["javascript"]["brands"]
                        ),
                    )
                    jobs = await browser.navigate(
                        self.url + "/jobs", timeout_seconds=10, check_robots_txt=True
                    )
                    self.assertTrue(jobs.successful)
                    self.assertIn("Embedded Software Engineer", jobs.cleaned_html)
                    denied = await browser.navigate(
                        self.url + "/private", timeout_seconds=10, check_robots_txt=True
                    )
                    self.assertFalse(denied.successful)
                    self.assertIn("robots.txt", denied.error)
                    self.assertNotIn("/private", self.paths)

    async def test_resume_captures_same_document_and_keeps_authentication(self):
        human = HumanSession(
            headed=False, interactive=True, timeout=10, notify=lambda *_: None
        )
        async with open_browser() as browser:
            blocked = await browser.navigate(
                self.url + "/login", timeout_seconds=10, check_robots_txt=True
            )
            with TemporaryDirectory() as directory:
                waiting = asyncio.create_task(
                    human.check_result(
                        browser, blocked, self.url + "/login", Path(directory), "p0001"
                    )
                )
                async with asyncio.timeout(5):
                    while not human.waiting:
                        await asyncio.sleep(0.01)
                # Controlled test authentication, performed in the existing browser tab.
                await browser.page.goto(self.url + "/allow", wait_until="load")
                visits = self.paths.count("/company")
                human.resume_requested.set()
                captured = await waiting
                self.assertTrue(captured.successful)
                self.assertEqual(captured.url, self.url + "/company")
                self.assertEqual(self.paths.count("/company"), visits)
                jobs = await browser.navigate(
                    self.url + "/jobs", timeout_seconds=10, check_robots_txt=True
                )
                self.assertTrue(jobs.successful)
                self.assertEqual(browser.document_status, 200)


if __name__ == "__main__":
    unittest.main()
