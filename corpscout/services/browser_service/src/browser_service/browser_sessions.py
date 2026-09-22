"""One temporary Chromium execution using a session-owned persistent profile."""

import asyncio
import json
import logging
from contextlib import AsyncExitStack
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from playwright.async_api import Browser, BrowserContext, Error, Page, async_playwright

from browser_service.browser import BrowserSession
from browser_service.capture import access_problem, utc_now
from browser_service.session_store import SessionStore
from browser_service.virtual_desktop import VirtualDesktop, open_virtual_browser

LOGGER = logging.getLogger(__name__)


class PersistentBrowserSession:
    def __init__(
        self,
        identifier: str,
        root: Path,
        store: SessionStore,
        *,
        headless: bool,
        route: str,
        proxy: str | None,
    ):
        self.store = store
        self.id = identifier
        self.root = root
        self.headless = headless
        self.route = route
        self.proxy = proxy
        self.lock = asyncio.Lock()
        self.stack = AsyncExitStack()
        self.context: BrowserContext | None = None
        self.desktop: VirtualDesktop | None = None
        self.generation: str | None = None
        self.tabs: dict[str, BrowserSession] = {}
        self.state = "stopped"
        self.error: str | None = None
        self.saved_at: str | None = None

    @property
    def browser_data_root(self) -> Path:
        return self.root

    def track_page(self, page: Page) -> None:
        if self.context is None:
            return
        if any(tab.page == page for tab in self.tabs.values()):
            return
        identifier = uuid4().hex
        self.tabs[identifier] = BrowserSession(self.context, page)

        def closed(_: Page) -> None:
            self.tabs.pop(identifier, None)
            if self.state == "running" and not self.tabs:
                LOGGER.warning(
                    "Last browser tab closed in %s (assigned=%s)",
                    self.id,
                    self.store.for_profile(self.id) is not None,
                )
                self.state = "error"
                self.generation = None
                self.error = (
                    "Browser closed. Start it again to restore the saved session."
                )

        page.on("close", closed)

    async def start(self, *, restore_tabs: bool = False) -> None:
        async with self.lock:
            if self.context is not None and self.state == "running" and self.tabs:
                return
            await self.close_browser()
            self.state = "starting"
            self.error = None
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.root.chmod(0o700)
            try:
                self.desktop = await self.stack.enter_async_context(
                    open_virtual_browser(
                        self.browser_data_root / "profile",
                        kind="saved",
                        owner=self.id,
                        headless=self.headless,
                        proxy=self.proxy,
                    )
                )
                playwright = await self.stack.enter_async_context(async_playwright())
                browser = await playwright.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{self.desktop.cdp_port}"
                )
                browser.on("disconnected", self.disconnected)
                self.context = browser.contexts[0]
                self.context.on("page", self.track_page)
                for page in self.context.pages:
                    self.track_page(page)
                saved_file = self.browser_data_root / "session.json"
                if saved_file.exists() or not restore_tabs:
                    saved = (
                        json.loads(saved_file.read_text(encoding="utf-8"))
                        if saved_file.exists()
                        else {}
                    )
                    if saved.get("cookies"):
                        await self.context.add_cookies(saved["cookies"])
                    urls = saved.get("tabs", []) if restore_tabs else []
                    # Restore the recorded tabs once, after restoring cookies.
                    previous = list(self.context.pages)
                    blank = await self.context.new_page()
                    for page in previous:
                        await page.close()
                    for url in urls:
                        page = await self.context.new_page()
                        # Loading continues normally in the visible browser.
                        try:
                            await page.goto(url, wait_until="commit", timeout=15_000)
                        except Error:
                            LOGGER.info("Saved tab could not reload in %s", self.id)
                    if urls:
                        await blank.close()
                    self.saved_at = saved.get("saved_at")
                if not self.context.pages:
                    await self.context.new_page()
                self.generation = uuid4().hex
                self.state = "running"
            except BaseException:
                self.state = "error"
                await self.stack.aclose()
                self.context = None
                self.desktop = None
                self.tabs.clear()
                self.state = "error"
                self.error = "Browser could not start; check service logs."
                raise

    def disconnected(self, _: Browser) -> None:
        was_running = self.state == "running"
        if self.state in {"running", "starting"}:
            self.state = "error"
            self.generation = None
            self.error = "Browser closed. Start it again to restore the saved session."
        if was_running:
            LOGGER.warning("Browser disconnected in session %s", self.id)

    async def save(self) -> None:
        """Preserve session cookies as well as Chromium's persistent profile."""
        if self.context is None or self.state != "running":
            return
        self.saved_at = utc_now()
        data = {
            "cookies": await self.context.cookies(),
            "tabs": [
                tab.page.url
                for tab in self.tabs.values()
                if not tab.page.is_closed()
                and urlsplit(tab.page.url).scheme in {"http", "https"}
            ],
            "saved_at": self.saved_at,
        }
        self.browser_data_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.browser_data_root / "session.json.tmp"
        temporary.write_text(json.dumps(data), encoding="utf-8")
        temporary.chmod(0o600)
        temporary.replace(self.browser_data_root / "session.json")

    async def stop(self) -> None:
        async with self.lock:
            try:
                await self.save()
            finally:
                await self.close_browser()

    async def close_browser(self) -> None:
        self.state = "stopped"
        # Let Chromium flush its profile before the desktop is terminated.
        if self.context is not None and self.context.browser is not None:
            try:
                async with asyncio.timeout(10):
                    cdp = await self.context.browser.new_browser_cdp_session()
                    await cdp.send("Browser.close")
            except (Error, TimeoutError):
                LOGGER.info("Browser already disconnected in %s", self.id)
        await self.stack.aclose()
        self.context = None
        self.desktop = None
        self.generation = None
        self.tabs.clear()
        self.state = "stopped"

    async def open_tab(self, url: str) -> str:
        async with self.lock:
            if self.context is None or self.state != "running":
                raise ValueError("Start this browser session first")
            page = await self.context.new_page()
            self.track_page(page)
            await page.bring_to_front()
            try:
                await page.goto(url, wait_until="commit", timeout=20_000)
            except Error:
                # Keep the visible tab available for the user to inspect/retry.
                LOGGER.info("Navigation did not commit in %s", self.id)
            await self.save()
            return next(key for key, tab in self.tabs.items() if tab.page == page)

    async def snapshot(self) -> dict:
        pages = []
        for identifier, tab in list(self.tabs.items()):
            if tab.page.is_closed():
                continue
            try:
                title = await tab.page.title()
            except Error:
                title = "Loading…"
            pages.append(
                {
                    "id": identifier,
                    "url": tab.page.url,
                    "title": title,
                    "status_code": tab.document_status,
                }
            )
        assignment = self.store.for_profile(self.id)
        return {
            "id": self.id,
            "headless": self.headless,
            "route": self.route,
            "desktop_available": self.desktop is not None
            and self.desktop.vnc_port is not None,
            "state": self.state,
            "generation": self.generation,
            "saved_at": self.saved_at,
            "error": self.error,
            "request_id": assignment["request_id"] if assignment else None,
            "lease_id": self.id if assignment else None,
            "execution_id": assignment["id"] if assignment else None,
            "domain": assignment["domain"] if assignment else None,
            "tabs": pages,
        }

    async def inspect_tab(self, identifier: str) -> dict:
        tab = self.tabs.get(identifier)
        if tab is None or tab.page.is_closed():
            raise ValueError("Unknown browser tab")
        html = await tab.page.content()
        return {
            "url": tab.page.url,
            "title": await tab.page.title(),
            "status_code": tab.document_status,
            "access_problem": access_problem(tab.document_status, html),
            "checked_at": utc_now(),
        }
