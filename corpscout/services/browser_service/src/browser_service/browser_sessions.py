"""Private persistent headed browsers, independent of individual crawl attempts."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from playwright.async_api import Browser, BrowserContext, Error, Page, async_playwright

from browser_service.browser import BrowserSession
from browser_service.capture import access_problem, utc_now
from browser_service.virtual_desktop import VirtualDesktop, open_virtual_browser

LOGGER = logging.getLogger(__name__)


class PersistentBrowserSession:
    def __init__(self, identifier: str, root: Path):
        self.id = identifier
        self.root = root
        self.lock = asyncio.Lock()
        self.stack = AsyncExitStack()
        self.context: BrowserContext | None = None
        self.desktop: VirtualDesktop | None = None
        self.generation: str | None = None
        self.tabs: dict[str, BrowserSession] = {}
        self.state = "stopped"
        self.error: str | None = None
        self.saved_at: str | None = None
        settings_file = self.root / "settings.json"
        settings = (
            json.loads(settings_file.read_text(encoding="utf-8"))
            if settings_file.exists()
            else {}
        )
        self.auto_restart: bool = settings.get("auto_restart", True)
        self.wanted_running = False
        self.restart_task: asyncio.Task | None = None
        self.request_id: str | None = None
        self.lease_id: str | None = None
        self.domain: str | None = None
        self.recycling = False

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
                    self.request_id is not None,
                )
                self.state = "error"
                self.generation = None
                self.error = (
                    "Browser closed. Start it again to restore the saved session."
                )
                self.schedule_restart()

        page.on("close", closed)

    async def start(self, *, restore_tabs: bool = True, manual: bool = False) -> None:
        async with self.lock:
            if manual and self.request_id is not None:
                raise ValueError(
                    "This profile is assigned to a crawl; cancel the crawl first"
                )
            self.wanted_running = True
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
                        self.root / "profile", kind="saved", owner=self.id
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
                saved_file = self.root / "session.json"
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
            LOGGER.warning(
                "Browser disconnected in %s (assigned=%s)",
                self.id,
                self.request_id is not None,
            )
            self.schedule_restart()

    def schedule_restart(self) -> None:
        if (
            not self.auto_restart
            or not self.wanted_running
            or self.restart_task is not None
            or self.request_id is not None
        ):
            return
        self.state = "restarting"
        self.generation = None
        self.error = None
        self.restart_task = asyncio.create_task(self.restart_after_close())

    async def restart_after_close(self) -> None:
        try:
            # Let Chromium finish closing the window before reopening the profile.
            await asyncio.sleep(1)
            if self.auto_restart and self.wanted_running:
                await self.start(restore_tabs=False)
        except Exception as error:
            self.state = "error"
            self.generation = None
            self.error = "Browser could not restart. Use Start browser to retry."
            LOGGER.warning(
                "Could not automatically restart %s (%s)", self.id, type(error).__name__
            )
        finally:
            self.restart_task = None

    async def cancel_restart(self) -> None:
        if self.restart_task is not None:
            self.restart_task.cancel()
            await asyncio.gather(self.restart_task, return_exceptions=True)
            self.restart_task = None
        if self.state == "restarting":
            self.state = "error"
            self.error = "Browser closed. Start it again to restore the saved session."

    async def set_auto_restart(self, enabled: bool) -> None:
        self.auto_restart = enabled
        if not enabled:
            await self.cancel_restart()
        async with self.lock:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = self.root / "settings.json.tmp"
            temporary.write_text(
                json.dumps({"auto_restart": enabled}), encoding="utf-8"
            )
            temporary.chmod(0o600)
            temporary.replace(self.root / "settings.json")
        if enabled and self.wanted_running and self.state == "error":
            self.schedule_restart()

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
        temporary = self.root / "session.json.tmp"
        temporary.write_text(json.dumps(data), encoding="utf-8")
        temporary.chmod(0o600)
        temporary.replace(self.root / "session.json")

    async def stop(self, *, manual: bool = False) -> None:
        if manual and self.request_id is not None:
            raise ValueError(
                "This profile is assigned to a crawl; cancel the crawl first"
            )
        self.wanted_running = False
        await self.cancel_restart()
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
            if self.request_id is not None:
                raise ValueError(
                    "This profile is assigned to a crawl; use its desktop for verification"
                )
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
        return {
            "id": self.id,
            "state": self.state,
            "generation": self.generation,
            "saved_at": self.saved_at,
            "error": self.error,
            "auto_restart": self.auto_restart,
            "request_id": self.request_id,
            "lease_id": self.lease_id,
            "domain": self.domain,
            "recycling": self.recycling,
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


class BrowserSessions:
    def __init__(self, root: Path, count: int):
        if count < 0 or count > 16:
            raise ValueError("CRAWL_BROWSER_SESSIONS must be between 0 and 16")
        self.root = root
        self.sessions = {
            f"browser-{number}": PersistentBrowserSession(
                f"browser-{number}", root / f"browser-{number}"
            )
            for number in range(1, count + 1)
        }
        self.save_task: asyncio.Task | None = None
        self.closing = False
        self.next_session = 0

    async def start(self, *, restore_tabs: bool = True) -> None:
        self.closing = False
        if not self.sessions:
            return
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        for session in self.sessions.values():
            try:
                await session.start(restore_tabs=restore_tabs)
            except Exception:
                LOGGER.exception("Persistent browser failed to start: %s", session.id)
        self.save_task = asyncio.create_task(self.save_periodically())

    @asynccontextmanager
    async def lease(
        self, request_id: str, domain: str, *, profile_id: str | None = None
    ) -> AsyncIterator[PersistentBrowserSession]:
        """Keep one profile exclusive through the scan, human pauses, and recycling."""
        sessions = (
            list(self.sessions.values())
            if profile_id is None
            else [self.sessions[profile_id]]
        )
        if not sessions:
            raise ValueError("No saved browser profiles configured")
        session = None
        while session is None:
            if self.closing:
                raise RuntimeError("Browser pool is shutting down")
            for offset in range(len(sessions)):
                index = (self.next_session + offset) % len(sessions)
                candidate = sessions[index]
                if (
                    candidate.request_id is None
                    and candidate.state == "running"
                    and candidate.wanted_running
                    and not candidate.lock.locked()
                ):
                    session = candidate
                    # Reserve before any await so concurrent workers cannot share it.
                    session.request_id, session.domain = request_id, domain
                    self.next_session = (
                        list(self.sessions).index(candidate.id) + 1
                    ) % len(self.sessions)
                    break
            if session is None:
                await asyncio.sleep(0.2)
        try:
            async with session.lock:
                await session.save()
                assert session.context is not None
                previous = list(session.context.pages)
                await session.context.new_page()
                for page in previous:
                    await page.close()
            yield session
        finally:
            # Cancellation must not free a profile while its old browser still runs.
            cleanup = asyncio.create_task(self.recycle(session))
            cancelled = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    cancelled = True
            await cleanup
            if cancelled:
                raise asyncio.CancelledError

    async def recycle(self, session: PersistentBrowserSession) -> None:
        session.recycling = True
        try:
            async with session.lock:
                try:
                    await session.save()
                finally:
                    await session.close_browser()
            if not self.closing and session.wanted_running:
                await session.start(restore_tabs=False)
                async with session.lock:
                    await session.save()
        except Exception as error:
            # An unhealthy profile stays unavailable until an operator restarts it.
            session.state = "error"
            session.error = "Browser could not recycle. Use Start browser to retry."
            LOGGER.warning(
                "Could not recycle %s (%s)", session.id, type(error).__name__
            )
        finally:
            session.recycling = False
            session.request_id = None
            session.lease_id = None
            session.domain = None

    async def save_periodically(self) -> None:
        while True:
            await asyncio.sleep(10)
            for session in self.sessions.values():
                try:
                    async with session.lock:
                        await session.save()
                except Exception as error:
                    LOGGER.warning(
                        "Could not save %s (%s)", session.id, type(error).__name__
                    )

    async def close(self) -> None:
        self.closing = True
        if self.save_task is not None:
            self.save_task.cancel()
            await asyncio.gather(self.save_task, return_exceptions=True)
            self.save_task = None
        for session in self.sessions.values():
            try:
                await session.stop()
            except Exception as error:
                LOGGER.warning(
                    "Could not close %s (%s)", session.id, type(error).__name__
                )
