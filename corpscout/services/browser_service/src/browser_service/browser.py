"""Native browser navigation and deterministic HTML capture."""

import asyncio
from pathlib import Path
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

from playwright.async_api import BrowserContext, Page, Response

from browser_service.capture import PageCapture


class BrowserSession:
    """One tab and browser context, retained through operator interaction."""

    def __init__(self, context: BrowserContext, page: Page):
        self.context = context
        self.page = page
        self.document_status: int | None = None
        self.document_headers: dict[str, str] = {}
        self.robots: dict[str, RobotFileParser] = {}
        page.on("response", self.track_response)

    def track_response(self, response: Response) -> None:
        if (
            response.request.is_navigation_request()
            and response.frame == self.page.main_frame
        ):
            self.document_status = response.status
            self.document_headers = response.headers

    async def robots_denial(self, url: str, timeout_seconds: float) -> str | None:
        origin = urlsplit(url)
        robots_url = f"{origin.scheme}://{origin.netloc}/robots.txt"
        if robots_url not in self.robots:
            response = await self.context.request.get(
                robots_url, timeout=min(timeout_seconds, 10) * 1000, max_redirects=5
            )
            try:
                if response.status >= 500 or response.status == 429:
                    return f"robots.txt unavailable (HTTP {response.status})"
                parser = RobotFileParser(robots_url)
                parser.parse(
                    (await response.text()).splitlines() if response.ok else []
                )
                self.robots[robots_url] = parser
            finally:
                await response.dispose()
        user_agent = await self.page.evaluate("navigator.userAgent")
        if not self.robots[robots_url].can_fetch(user_agent, url):
            return "Access denied by robots.txt"
        return None

    async def navigate(
        self, url: str, *, timeout_seconds: float, check_robots_txt: bool
    ) -> PageCapture:
        if check_robots_txt:
            denied = await self.robots_denial(url, timeout_seconds)
            if denied is not None:
                return PageCapture(
                    url=url,
                    html="",
                    status_code=403,
                    headers={},
                    error=denied,
                )
        self.document_status = None
        self.document_headers = {}
        await self.page.goto(
            url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000
        )
        await asyncio.sleep(2)
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
        )

    async def screenshot(self, path: Path) -> None:
        await self.page.screenshot(path=str(path), timeout=5000)
