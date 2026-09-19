"""Sticky reservations over the configured browser pool; never create profiles."""

import asyncio
import logging
from dataclasses import dataclass, field
from time import monotonic
from typing import Literal

from playwright.async_api import Error

from browser_service.browser import BrowserSession
from browser_service.browser_sessions import BrowserSessions, PersistentBrowserSession
from browser_service.session_store import SessionStore

LOGGER = logging.getLogger(__name__)


class BrowserReservationError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


@dataclass(kw_only=True)
class BrowserReservation:
    id: str
    request_id: str
    domain: str
    idle_timeout: float
    touched: float
    state: str = "queued"
    requested_profile_id: str | None = None
    assigned: asyncio.Event = field(default_factory=asyncio.Event)
    profile: PersistentBrowserSession | None = None
    tabs: dict[str, BrowserSession] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    released: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None
    error: str | None = None
    recovery_error: str | None = None
    release_reason: str | None = None

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "requestId": self.request_id,
            "domain": self.domain,
            "state": self.state,
            "profileId": self.profile.id if self.profile is not None else None,
            "generation": self.profile.generation if self.profile is not None else None,
            "idleTimeoutSeconds": self.idle_timeout,
            "error": self.error or self.recovery_error,
        }


class BrowserMultiplexer:
    def __init__(
        self,
        pool: BrowserSessions,
        store: SessionStore,
        *,
        max_pending: int,
        idle_timeout: float,
    ):
        self.pool = pool
        self.store = store
        self.idle_timeout = idle_timeout
        self.allocation = asyncio.Lock()
        self.max_pending = max_pending
        self.reservations: dict[str, BrowserReservation] = {}
        self.reaper: asyncio.Task | None = None
        self.accepting = False

    async def start(self) -> None:
        self.store.interrupt_previous_process()
        self.accepting = True
        self.reaper = asyncio.create_task(self.expire_idle())

    def reserve(
        self,
        *,
        identifier: str,
        request_id: str,
        domain: str,
        profile_id: str | None = None,
    ) -> BrowserReservation:
        if not self.accepting or self.pool.closing or not self.pool.sessions:
            raise BrowserReservationError(503, "No browser pool is available")
        previous = self.store.get(identifier)
        if previous is not None:
            if previous["request_id"] != request_id or previous["domain"] != domain:
                raise BrowserReservationError(
                    409, "Session ID belongs to a different crawl"
                )
            return self.touch(identifier)
        if len(self.reservations) >= self.max_pending:
            raise BrowserReservationError(503, "Browser reservation queue is full")
        self.store.create(identifier, "crawler", request_id, domain, self.idle_timeout)
        reservation = BrowserReservation(
            id=identifier,
            request_id=request_id,
            domain=domain,
            idle_timeout=self.idle_timeout,
            touched=monotonic(),
            requested_profile_id=profile_id,
        )
        self.reservations[identifier] = reservation
        reservation.task = asyncio.create_task(self.hold(reservation))
        return reservation

    async def for_request(
        self, identifier: str, *, domain: str | None, browser_id: str | None
    ) -> BrowserReservation:
        """Allocate on first navigation, then retain the durable ID's assignment."""
        if self.store.get(identifier) is not None:
            reservation = self.get(identifier)  # Terminal IDs must never be revived.
            assigned_id = (
                reservation.profile.id
                if reservation.profile is not None
                else reservation.requested_profile_id
            )
            if browser_id is not None and browser_id != assigned_id:
                raise BrowserReservationError(
                    409, "Session is pinned to a different browser"
                )
        else:
            if domain is None:
                raise BrowserReservationError(
                    404, "Unknown session; send a URL in the first request"
                )
            if browser_id is not None and browser_id not in self.pool.sessions:
                raise BrowserReservationError(422, "Unknown browser ID")
            # A busy pool must not leave a new reservation behind. The exact same
            # request can be retried later. Honor existing explicit FIFO waiters.
            if not self.accepting or self.pool.closing:
                raise BrowserReservationError(503, "No browser pool is available")
            pending = [r for r in self.reservations.values() if r.state == "queued"]
            if any(r.requested_profile_id is None for r in pending):
                raise BrowserReservationError(
                    503, "Browsers are busy; retry this request later"
                )
            claimed = {r.requested_profile_id for r in pending}
            profiles = list(self.pool.sessions.values())
            available = None
            for offset in range(len(profiles)):
                candidate = profiles[(self.pool.next_session + offset) % len(profiles)]
                if (
                    candidate.id not in claimed
                    and (browser_id is None or candidate.id == browser_id)
                    and candidate.request_id is None
                    and candidate.state == "running"
                    and candidate.wanted_running
                    and not candidate.lock.locked()
                ):
                    available = candidate
                    break
            if available is None:
                raise BrowserReservationError(
                    503,
                    "Requested browser is busy or unavailable; retry this request later",
                )
            # No await before recording the chosen browser: simultaneous first
            # requests cannot claim the same free profile or duplicate an ID.
            reservation = self.reserve(
                identifier=identifier,
                request_id=f"session-{identifier}",
                domain=domain,
                profile_id=available.id,
            )
        if reservation.requested_profile_id is not None:
            await reservation.assigned.wait()
        return self.ready(identifier)

    async def hold(self, reservation: BrowserReservation) -> None:
        lease = self.pool.lease(
            reservation.request_id,
            reservation.domain,
            profile_id=reservation.requested_profile_id,
        )
        entered = False
        try:
            async with self.allocation:
                profile = await lease.__aenter__()
                entered = True
                reservation.profile = profile
                profile.lease_id = reservation.id
                self.store.assign(reservation.id, profile.id, profile.generation)
                reservation.state = "ready"
                reservation.assigned.set()
            await reservation.released.wait()
            async with reservation.lock:
                reservation.tabs.clear()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            reservation.state = "failed"
            reservation.error = "Browser reservation failed"
            self.store.finish(reservation.id, "failed")
            LOGGER.warning("Browser reservation failed (%s)", type(error).__name__)
        finally:
            reservation.assigned.set()
            if entered:
                await lease.__aexit__(None, None, None)

    def get(self, identifier: str) -> BrowserReservation:
        reservation = self.reservations.get(identifier)
        if reservation is None:
            previous = self.store.get(identifier)
            raise BrowserReservationError(
                410 if previous is not None else 404,
                "Browser session has ended; start a new crawl with a new ID"
                if previous
                else "Unknown browser session ID",
            )
        if reservation.state in {"releasing", "failed"}:
            raise BrowserReservationError(410, "Browser session has ended")
        if (
            not reservation.lock.locked()
            and monotonic() - reservation.touched >= reservation.idle_timeout
        ):
            raise BrowserReservationError(410, "Browser session expired")
        return reservation

    def touch(self, identifier: str) -> BrowserReservation:
        reservation = self.get(identifier)
        reservation.touched = monotonic()
        self.store.touch(
            identifier,
            reservation.idle_timeout,
            reservation.profile.generation if reservation.profile else None,
        )
        return reservation

    def ready(self, identifier: str) -> BrowserReservation:
        reservation = self.get(identifier)
        if reservation.state != "ready":
            raise BrowserReservationError(
                409, "Browser session is queued; wait until it is ready"
            )
        return reservation

    async def open_tab(
        self, reservation: BrowserReservation, name: str
    ) -> BrowserSession:
        profile = reservation.profile
        assert profile is not None
        if profile.state != "running":
            # Recover the assigned profile, never choose another or expand the pool.
            await profile.start(restore_tabs=False)
            reservation.tabs.clear()
        assert profile.context is not None
        tab = reservation.tabs.get(name)
        if tab is None or tab.page.is_closed() or tab.context is not profile.context:
            if name not in reservation.tabs and len(reservation.tabs) >= 16:
                raise BrowserReservationError(
                    409, "This browser session has reached its tab limit"
                )
            tab = BrowserSession(profile.context, await profile.context.new_page())
            reservation.tabs[name] = tab
        return tab

    def tab(self, reservation: BrowserReservation, name: str) -> BrowserSession:
        profile, tab = reservation.profile, reservation.tabs.get(name)
        if (
            profile is None
            or profile.state != "running"
            or tab is None
            or tab.page.is_closed()
            or tab.context is not profile.context
        ):
            raise BrowserReservationError(
                409, "Browser tab is unavailable; reopen it in the same session"
            )
        return tab

    async def recover_tab(
        self,
        reservation: BrowserReservation,
        name: str,
        url: str,
        *,
        reopen_closed_tab: bool,
    ) -> Literal["unchanged", "restored", "tab_closed"]:
        """Restore a missing verification tab while retaining its exclusive lease."""
        profile = reservation.profile
        assert profile is not None
        tab = reservation.tabs.get(name)
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
        if (
            (profile.state != "running" and not profile.auto_restart)
            or not profile.wanted_running
            or self.pool.closing
        ):
            raise BrowserReservationError(409, "Automatic browser recovery is disabled")
        if reservation.recovery_error is not None:
            raise BrowserReservationError(409, reservation.recovery_error)
        try:
            tab = await self.open_tab(reservation, name)
            # Reload only the pending page. Never replay form submissions or Resume.
            try:
                await tab.page.goto(url, wait_until="commit", timeout=15_000)
            except Error:
                if tab.page.is_closed() or profile.state != "running":
                    raise
                LOGGER.info("Recovered page is still loading in %s", profile.id)
            await tab.page.bring_to_front()
        except Exception as error:
            reservation.recovery_error = (
                "Browser recovery failed; cancel this crawl and retry manually"
            )
            LOGGER.warning(
                "Verification browser recovery failed in %s (%s)",
                profile.id,
                type(error).__name__,
            )
            raise BrowserReservationError(409, reservation.recovery_error) from error
        LOGGER.info("Restored verification browser in %s", profile.id)
        return "restored"

    async def release(self, identifier: str, *, reason: str = "released") -> None:
        reservation = self.reservations.get(identifier)
        if reservation is None:
            return  # Idempotent release, including after expiration.
        if reservation.release_reason is None:
            reservation.release_reason = reason
        previous = reservation.state
        reservation.state = "releasing"
        self.store.finish(identifier, "releasing")
        reservation.released.set()
        assert reservation.task is not None
        if previous == "queued":
            reservation.task.cancel()
        # A disconnected DELETE caller must not make the profile available early.
        try:
            await asyncio.shield(reservation.task)
        except asyncio.CancelledError:
            if not reservation.task.cancelled():
                raise
        finally:
            if reservation.task.done():
                self.reservations.pop(identifier, None)
                self.store.finish(identifier, reservation.release_reason)

    async def expire_idle(self) -> None:
        while True:
            await asyncio.sleep(1)
            for reservation in list(self.reservations.values()):
                if reservation.state == "releasing" or (
                    not reservation.lock.locked()
                    and monotonic() - reservation.touched >= reservation.idle_timeout
                ):
                    await self.release(reservation.id, reason="expired")

    async def close(self) -> None:
        self.accepting = False
        if self.reaper is not None:
            self.reaper.cancel()
            await asyncio.gather(self.reaper, return_exceptions=True)
            self.reaper = None
        await asyncio.gather(
            *(
                self.release(identifier, reason="interrupted")
                for identifier in list(self.reservations)
            )
        )
