"""Persistent identities, ephemeral browser executions, and one global capacity limit."""

import asyncio
import fcntl
import logging
import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from time import time
from typing import Literal, TextIO
from uuid import NAMESPACE_URL, uuid4, uuid5

from playwright.async_api import Error
from pydantic import Field

from browser_service.browser import BrowserSession
from browser_service.browser_sessions import PersistentBrowserSession
from browser_service.capture import StrictModel
from browser_service.session_store import BrowserSessionError, SessionStore

LOGGER = logging.getLogger(__name__)


class BrowserRuntimeSettings(StrictModel):
    max_browsers: int = Field(ge=0, le=64, strict=True)
    idle_timeout_seconds: float = Field(gt=0, strict=True, allow_inf_nan=False)
    session_retention_days: float = Field(
        gt=0, le=3650, strict=True, allow_inf_nan=False
    )


@dataclass(kw_only=True)
class BraveBrowserUsage:
    generation: str | None
    requests_started: int
    restart_after: int


@dataclass(kw_only=True)
class ActiveBrowserSession:
    id: str
    execution_id: str
    profile: PersistentBrowserSession
    tabs: dict[str, BrowserSession] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    cleanup: asyncio.Task | None = None
    recovery_error: str | None = None
    operation: dict | None = None
    brave_usage: BraveBrowserUsage | None = None


def copy_profile(source: Path, target: Path) -> None:
    """Copy a closed profile without Chromium's machine/process lock files."""
    if target.exists():
        return
    if (source / "SingletonLock").exists() or (source / "SingletonLock").is_symlink():
        raise RuntimeError("Close the template browser before copying its profile")
    temporary = target.with_name(target.name + ".copying")
    if temporary.exists():
        shutil.rmtree(temporary)
    shutil.copytree(
        source,
        temporary,
        ignore=shutil.ignore_patterns("Singleton*", "DevToolsActivePort", "lockfile"),
    )
    temporary.chmod(0o700)
    temporary.rename(target)


async def copy_closed_profile(source: Path, target: Path) -> None:
    copy = asyncio.create_task(asyncio.to_thread(copy_profile, source, target))
    try:
        await asyncio.shield(copy)
    except asyncio.CancelledError:
        # Do not release the profile or capacity while a background copy still owns it.
        await copy
        raise


class BrowserService:
    def __init__(
        self,
        root: Path,
        *,
        settings: BrowserRuntimeSettings,
        base_profile: Path | None = None,
        proxy_routes: dict[str, str] | None = None,
    ):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        self.base_profile = base_profile or root / "base-profile"
        self.proxy_routes = {"direct": None, **(proxy_routes or {})}
        self.store = SessionStore(root / "sessions.sqlite3")
        self.startup_settings = settings
        saved = self.store.get_setting("browser_runtime")
        old_pool = self.store.get_setting("browser_pool")
        if old_pool is not None or (saved is not None and "max_browsers" not in saved):
            migrated = settings.model_dump() | (saved or {})
            if old_pool is not None:
                migrated["max_browsers"] = (
                    old_pool["headless_count"] + old_pool["headed_count"]
                )
            saved = BrowserRuntimeSettings.model_validate(migrated).model_dump()
            self.store.set_setting("browser_runtime", saved)
            self.store.delete_setting("browser_pool")
        self.settings = (
            BrowserRuntimeSettings.model_validate(saved)
            if saved is not None
            else settings
        )
        self.active: dict[str, ActiveBrowserSession] = {}
        self.lock: TextIO | None = None
        self.reaper: asyncio.Task | None = None
        self.accepting = False

    @property
    def idle_timeout(self) -> float:
        return self.settings.idle_timeout_seconds

    @property
    def retention(self) -> float:
        return self.settings.session_retention_days * 86400

    def runtime_configuration(self) -> dict:
        occupied = self.store.connection.execute(
            "SELECT count() FROM executions WHERE ended_at IS NULL"
        ).fetchone()[0]
        return {
            "source": "sqlite"
            if self.store.get_setting("browser_runtime") is not None
            else "startup",
            "startup": self.startup_settings.model_dump(),
            "current": self.settings.model_dump(),
            "capacity": {
                "occupied": occupied,
                "available": max(0, self.settings.max_browsers - occupied),
            },
        }

    def configure_runtime(self, settings: BrowserRuntimeSettings | None) -> dict:
        if not self.accepting:
            raise BrowserSessionError(
                503, "Browser service is not accepting settings changes"
            )
        if settings is None:
            self.store.delete_setting("browser_runtime")
        else:
            self.store.set_setting("browser_runtime", settings.model_dump())
        self.settings = settings or self.startup_settings
        return self.runtime_configuration()

    async def start(self) -> None:
        self.lock = (self.root / "service.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            self.store.close()
            raise RuntimeError(
                "Another browser service owns this state directory"
            ) from None
        try:
            self.store.interrupt_previous_process()
            self.base_profile.mkdir(parents=True, exist_ok=True, mode=0o700)
            if (self.base_profile / "SingletonLock").exists() or (
                self.base_profile / "SingletonLock"
            ).is_symlink():
                raise RuntimeError(
                    "Close the base profile browser before starting the service"
                )
            await self.import_saved_profiles()
            await self.clean_expired_profiles()
            self.accepting = True
            self.reaper = asyncio.create_task(self.expire_idle())
        except BaseException:
            await self.close()
            raise

    async def import_saved_profiles(self) -> None:
        if self.store.get_setting("legacy_profiles_imported") is not None:
            return
        legacy = self.root / "profiles"
        if legacy.exists():
            for folder in legacy.iterdir():
                if not folder.is_dir():
                    continue
                sources = [("direct", folder)]
                if (folder / "routes").is_dir():
                    sources.extend(
                        (p.name, p) for p in (folder / "routes").iterdir() if p.is_dir()
                    )
                for route, source in sources:
                    if not (source / "profile").is_dir():
                        continue
                    identifier = uuid5(
                        NAMESPACE_URL, f"legacy:{folder.name}:{route}"
                    ).hex
                    self.store.ensure_session(
                        identifier,
                        route=route,
                        headless=folder.name.startswith("headless-"),
                        retention=self.retention,
                        label=f"Imported {folder.name} · {route}",
                        pinned=True,
                    )
                    destination = self.root / "sessions" / identifier
                    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
                    await copy_closed_profile(
                        source / "profile", destination / "profile"
                    )
                    if (source / "session.json").exists() and not (
                        destination / "session.json"
                    ).exists():
                        shutil.copyfile(
                            source / "session.json", destination / "session.json"
                        )
                        (destination / "session.json").chmod(0o600)
        self.store.set_setting("legacy_profiles_imported", {"completed": True})

    async def claim(
        self,
        *,
        identifier: str,
        request_id: str,
        domain: str,
        headless: bool | None = None,
        route: str | None = None,
    ) -> ActiveBrowserSession:
        if not self.accepting:
            raise BrowserSessionError(503, "Browser service is not accepting requests")
        if route is not None and route not in self.proxy_routes:
            raise BrowserSessionError(422, "Requested browser route is not configured")
        saved = self.store.ensure_session(
            identifier, route=route, headless=headless, retention=self.retention
        )
        if saved["route"] not in self.proxy_routes:
            raise BrowserSessionError(
                422, "Saved session's proxy route is no longer configured"
            )
        row = self.store.claim(
            identifier,
            request_id,
            domain,
            max_browsers=self.settings.max_browsers,
            timeout=self.idle_timeout,
            retention=self.retention,
            headless=headless,
        )
        session = self.active.get(identifier)
        if session is not None:
            async with session.lock:
                return self.touch(identifier, execution_id=row["id"])
        saved = self.store.session(identifier)
        profile = PersistentBrowserSession(
            identifier,
            self.root / "sessions" / identifier,
            self.store,
            headless=bool(saved["headless"]),
            route=saved["route"],
            proxy=self.proxy_routes[saved["route"]],
        )
        session = ActiveBrowserSession(
            id=identifier, execution_id=row["id"], profile=profile
        )
        self.active[identifier] = session
        try:
            async with session.lock:
                profile.root.mkdir(parents=True, exist_ok=True, mode=0o700)
                # Keep copying inside the reservation so starting browsers count toward capacity.
                await copy_closed_profile(self.base_profile, profile.root / "profile")
                await profile.start(restore_tabs=False)
                self.store.ready(session.execution_id, profile.generation)
                return self.touch(identifier)
        except BaseException:
            await self.release(
                identifier, reason="failed", execution_id=session.execution_id
            )
            raise

    async def for_request(
        self,
        identifier: str,
        *,
        domain: str | None,
        headless: bool | None = None,
        route: str | None = None,
        execution_id: str | None = None,
    ) -> ActiveBrowserSession:
        if identifier in self.active:
            if execution_id is None:
                raise BrowserSessionError(
                    409, "Send the current executionId to use an open session"
                )
            session = self.get(identifier, execution_id=execution_id)
            if (headless is not None and headless != session.profile.headless) or (
                route is not None and route != session.profile.route
            ):
                raise BrowserSessionError(
                    409, "Close the execution before changing its browser options"
                )
            return session
        if execution_id is not None:
            raise BrowserSessionError(409, "This browser execution has ended")
        if domain is None:
            raise BrowserSessionError(404, "Send a URL to open this saved session")
        return await self.claim(
            identifier=identifier,
            request_id=uuid4().hex,
            domain=domain,
            headless=headless,
            route=route,
        )

    def get(
        self,
        identifier: str,
        *,
        starting: bool = False,
        execution_id: str | None = None,
    ) -> ActiveBrowserSession:
        session = self.active.get(identifier)
        if session is None:
            if self.store.session(identifier) is not None:
                raise BrowserSessionError(
                    410, "Browser is closed; reopen the saved session"
                )
            raise BrowserSessionError(404, "Unknown saved session")
        if execution_id is not None and session.execution_id != execution_id:
            raise BrowserSessionError(
                409, "Browser execution changed; refresh the session"
            )
        row = self.store.for_profile(identifier)
        if row is None or row["state"] == "stopping":
            raise BrowserSessionError(409, "Browser execution is closing")
        if row["state"] == "starting" and not starting:
            raise BrowserSessionError(409, "Browser execution is starting")
        if not session.lock.locked() and row["expires_at"] <= time():
            raise BrowserSessionError(410, "Browser execution idle timeout reached")
        return session

    def touch(
        self, identifier: str, *, execution_id: str | None = None
    ) -> ActiveBrowserSession:
        session = self.get(identifier, execution_id=execution_id)
        self.store.touch(
            session.execution_id, self.idle_timeout, session.profile.generation
        )
        return session

    def snapshot(self, identifier: str) -> dict:
        saved = self.store.session(identifier)
        if saved is None:
            raise BrowserSessionError(404, "Unknown saved session")
        session = self.active.get(identifier)
        row = self.store.get(identifier)
        return {
            "id": identifier,
            "executionId": session.execution_id if session else None,
            "requestId": row["request_id"] if row else None,
            "domain": row["domain"] if row else None,
            "state": "expired"
            if saved["expired_at"] is not None
            else row["state"]
            if session
            else "closed",
            "profileId": identifier,
            "route": saved["route"],
            "headless": bool(saved["headless"]),
            "desktopAvailable": bool(
                session and session.profile.desktop and session.profile.desktop.vnc_port
            ),
            "generation": session.profile.generation if session else None,
            "idleTimeoutSeconds": self.idle_timeout,
            "retainedUntil": saved["retained_until"],
            "pinned": bool(saved["pinned"]),
            "label": saved["label"],
            "error": session.recovery_error
            if session
            else row["error"]
            if row
            else None,
            "operation": session.operation if session else None,
            "brave_usage": asdict(session.brave_usage)
            if session and session.brave_usage is not None
            else None,
        }

    async def saved_snapshots(self) -> list[dict]:
        snapshots = []
        for saved in self.store.saved():
            active = self.active.get(saved["id"])
            data = (
                await active.profile.snapshot()
                if active
                else {
                    "id": saved["id"],
                    "state": "closed",
                    "headless": bool(saved["headless"]),
                    "route": saved["route"],
                    "desktop_available": False,
                    "generation": None,
                    "saved_at": None,
                    "error": None,
                    "request_id": None,
                    "lease_id": None,
                    "execution_id": None,
                    "domain": None,
                    "tabs": [],
                }
            )
            data.update(
                pinned=bool(saved["pinned"]),
                retained_until=saved["retained_until"],
                label=saved["label"],
            )
            snapshots.append(data)
        return snapshots

    def desktop_active(self, identifier: str, generation: str) -> bool:
        try:
            return self.get(identifier).profile.generation == generation
        except BrowserSessionError:
            return False

    async def open_tab(
        self, session: ActiveBrowserSession, name: str
    ) -> BrowserSession:
        profile = session.profile
        if profile.state != "running":
            # Recover the assigned profile, never choose another or expand the pool.
            await profile.start(restore_tabs=False)
            session.tabs.clear()
        self.store.touch(session.execution_id, self.idle_timeout, profile.generation)
        assert profile.context is not None
        tab = session.tabs.get(name)
        if tab is None or tab.page.is_closed() or tab.context is not profile.context:
            if name not in session.tabs and len(session.tabs) >= 16:
                raise BrowserSessionError(
                    409, "This browser session has reached its tab limit"
                )
            tab = BrowserSession(profile.context, await profile.context.new_page())
            session.tabs[name] = tab
        return tab

    def tab(self, session: ActiveBrowserSession, name: str) -> BrowserSession:
        profile, tab = session.profile, session.tabs.get(name)
        if (
            profile.state != "running"
            or tab is None
            or tab.page.is_closed()
            or tab.context is not profile.context
        ):
            raise BrowserSessionError(
                409, "Browser tab is unavailable; reopen it in the same session"
            )
        return tab

    async def recover_tab(
        self,
        session: ActiveBrowserSession,
        name: str,
        url: str,
        *,
        reopen_closed_tab: bool,
    ) -> Literal["unchanged", "restored", "tab_closed"]:
        """Restore a missing verification tab while retaining its exclusive lease."""
        profile = session.profile
        tab = session.tabs.get(name)
        if (
            profile.state == "running"
            and tab is not None
            and not tab.page.is_closed()
            and tab.context is profile.context
        ):
            return "unchanged"
        if (
            profile.state == "running"
            and (tab is None or tab.context is profile.context)
            and not reopen_closed_tab
        ):
            # Closing a tab is an operator action, not a browser crash.
            return "tab_closed"
        if not self.accepting:
            raise BrowserSessionError(503, "Browser service is stopping")
        if session.recovery_error is not None:
            raise BrowserSessionError(409, session.recovery_error)
        try:
            tab = await self.open_tab(session, name)
            # Reload only the pending page. Never replay form submissions or Resume.
            try:
                await tab.page.goto(url, wait_until="commit", timeout=15_000)
            except Error:
                if tab.page.is_closed() or profile.state != "running":
                    raise
                LOGGER.info("Recovered page is still loading in %s", profile.id)
            await tab.page.bring_to_front()
        except Exception as error:
            session.recovery_error = (
                "Browser recovery failed; cancel this crawl and retry manually"
            )
            LOGGER.warning(
                "Verification browser recovery failed in %s (%s)",
                profile.id,
                type(error).__name__,
            )
            raise BrowserSessionError(409, session.recovery_error) from error
        LOGGER.info("Restored verification browser in %s", profile.id)
        return "restored"

    async def release(
        self,
        identifier: str,
        *,
        reason: str = "closed",
        execution_id: str | None = None,
    ) -> None:
        session = self.active.get(identifier)
        if session is None:
            return
        if execution_id is not None and execution_id != session.execution_id:
            raise BrowserSessionError(
                409, "Browser execution changed; refusing to close it"
            )
        if session.cleanup is None or session.cleanup.done():
            self.store.finish(
                session.execution_id, "stopping", retention=self.retention
            )
            session.cleanup = asyncio.create_task(self.stop_execution(session, reason))
        await asyncio.shield(session.cleanup)

    async def stop_execution(self, session: ActiveBrowserSession, reason: str) -> None:
        async with session.lock:
            await session.profile.stop()
            session.tabs.clear()
            self.store.finish(session.execution_id, reason, retention=self.retention)
            self.active.pop(session.id, None)

    async def clean_expired_profiles(self) -> None:
        for identifier in self.store.expire_profiles():
            folder = self.root / "sessions" / identifier
            if folder.exists():
                await asyncio.to_thread(shutil.rmtree, folder)

    async def expire_idle(self) -> None:
        while True:
            await asyncio.sleep(1)
            for session in list(self.active.values()):
                row = self.store.for_profile(session.id)
                if row and not session.lock.locked() and row["expires_at"] <= time():
                    try:
                        await self.release(
                            session.id,
                            reason="idle_timeout",
                            execution_id=session.execution_id,
                        )
                    except Exception:
                        LOGGER.exception("Could not close idle browser %s", session.id)
            try:
                await self.clean_expired_profiles()
            except OSError:
                LOGGER.warning(
                    "Could not remove expired browser profiles; cleanup will retry"
                )

    async def close(self) -> None:
        self.accepting = False
        if self.reaper is not None:
            self.reaper.cancel()
            await asyncio.gather(self.reaper, return_exceptions=True)
        await asyncio.gather(
            *(
                self.release(
                    session.id, reason="interrupted", execution_id=session.execution_id
                )
                for session in list(self.active.values())
            )
        )
        self.store.close()
        if self.lock is not None:
            fcntl.flock(self.lock, fcntl.LOCK_UN)
            self.lock.close()
            self.lock = None
