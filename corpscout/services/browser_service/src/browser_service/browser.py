"""Native browser navigation and deterministic HTML capture."""

import asyncio
import re
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser

from playwright.async_api import BrowserContext, Error, Page, Response

from browser_service.capture import PageCapture


class BrowserSession:
    """One tab and browser context, retained through operator interaction."""

    def __init__(self, context: BrowserContext, page: Page):
        self.context = context
        self.page = page
        self.document_status: int | None = None
        self.document_headers: dict[str, str] = {}
        self.robots: dict[str, RobotFileParser] = {}
        self.redirects: list[dict] = []
        self.navigation_attempts: list[dict] = []
        page.on("response", self.track_response)

    def track_response(self, response: Response) -> None:
        if (
            response.request.is_navigation_request()
            and response.frame == self.page.main_frame
        ):
            self.document_status = response.status
            self.document_headers = response.headers
            if 300 <= response.status < 400 and "location" in response.headers:
                self.redirects.append(
                    {
                        "url": response.url,
                        "status_code": response.status,
                        "location": urljoin(response.url, response.headers["location"]),
                    }
                )

    async def robots_denial(self, url: str, timeout_seconds: float) -> str | None:
        origin = urlsplit(url)
        robots_url = f"{origin.scheme}://{origin.netloc}/robots.txt"
        if robots_url not in self.robots:
            # Use Chromium's TLS, proxy and cookie handling for robots as well as
            # content. APIRequestContext uses a separate HTTP/TLS implementation.
            robots_page = await self.context.new_page()
            try:
                response = await robots_page.goto(
                    robots_url,
                    wait_until="domcontentloaded",
                    timeout=min(timeout_seconds, 10) * 1000,
                )
                if response is None:
                    return "robots.txt unavailable (no response)"
                if response.status >= 500 or response.status == 429:
                    return f"robots.txt unavailable (HTTP {response.status})"
                parser = RobotFileParser(robots_url)
                parser.parse(
                    (await response.text()).splitlines() if response.ok else []
                )
                self.robots[robots_url] = parser
            finally:
                await robots_page.close()
        user_agent = await self.page.evaluate("navigator.userAgent")
        if not self.robots[robots_url].can_fetch(user_agent, url):
            return "Access denied by robots.txt"
        return None

    async def navigate(
        self, url: str, *, timeout_seconds: float, check_robots_txt: bool
    ) -> PageCapture:
        self.redirects = []
        self.navigation_attempts = []
        original = urlsplit(url)
        for attempt in range(2):
            try:
                capture = await self.navigate_once(
                    url,
                    timeout_seconds=timeout_seconds,
                    check_robots_txt=check_robots_txt,
                )
                self.navigation_attempts.append({"url": url, "error": capture.error})
                capture.navigation_attempts = list(self.navigation_attempts)
                return capture
            except Error as error:
                network_error = re.search(r"\bnet::(ERR_[A-Z_]+)\b", str(error))
                if network_error is None:
                    raise
                code = network_error[1]
                self.navigation_attempts.append({"url": url, "error": code})
                # A legacy domain may only provide an HTTP redirect. Never
                # downgrade certificate errors, query-bearing URLs, or paths.
                if (
                    attempt == 0
                    and original.scheme == "https"
                    and original.path in {"", "/"}
                    and not original.query
                    and not original.username
                    and not self.redirects
                    and code
                    in {"ERR_SSL_PROTOCOL_ERROR", "ERR_SSL_VERSION_OR_CIPHER_MISMATCH"}
                ):
                    url = original._replace(scheme="http").geturl()
                    continue
                return PageCapture(
                    url=url,
                    html="",
                    status_code=None,
                    headers={},
                    error=f"Browser fetch failed ({code})",
                    redirects=list(self.redirects),
                    navigation_attempts=list(self.navigation_attempts),
                )
        raise AssertionError("Navigation must finish within two attempts")

    async def navigate_once(
        self, url: str, *, timeout_seconds: float, check_robots_txt: bool
    ) -> PageCapture:
        self.document_status = None
        self.document_headers = {}
        if check_robots_txt:
            denied = await self.robots_denial(url, timeout_seconds)
            if denied is not None:
                return PageCapture(
                    url=url, html="", status_code=403, headers={}, error=denied
                )
        await self.page.goto(
            url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000
        )
        await asyncio.sleep(2)
        # Redirects can move to another origin with its own crawl policy.
        if check_robots_txt and self.page.url != url:
            denied = await self.robots_denial(self.page.url, timeout_seconds)
            if denied is not None:
                return PageCapture(
                    url=self.page.url,
                    html="",
                    status_code=403,
                    headers={},
                    error=denied,
                    redirects=list(self.redirects),
                )
        return await self.capture()

    async def capture(self) -> PageCapture:
        """Read the current document without navigation or synthetic status codes."""
        html = await self.page.content()
        url = self.page.url
        return PageCapture(
            url=url,
            html=html,
            status_code=self.document_status,
            headers=self.document_headers.copy(),
            error=None,
            redirects=list(self.redirects),
            navigation_attempts=list(self.navigation_attempts),
        )

    async def screenshot(self, path: Path) -> None:
        await self.page.screenshot(path=str(path), timeout=5000)
