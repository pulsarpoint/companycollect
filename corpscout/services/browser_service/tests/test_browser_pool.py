"""Exclusive domain leases and cleanup, with the desktop boundary replaced."""

import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from browser_service.api import create_app
from browser_service.browser_sessions import BrowserSessions, PersistentBrowserSession
from browser_service.runtime import BrowserService


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.005)


def ready_pool(pool):
    """Replace process launch/persistence while exercising the real lease lifecycle."""
    for session in pool.sessions.values():

        async def start(*, restore_tabs=True, session=session):
            session.state = "running"
            session.wanted_running = True
            session.context = SimpleNamespace(
                pages=[], new_page=AsyncMock(), browser=None
            )

        session.start = AsyncMock(side_effect=start)
        session.save = AsyncMock()
        # close_browser itself still clears the actual session state.


class BrowserPoolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.pool = BrowserSessions(self.root / "profiles", 2)
        ready_pool(self.pool)
        await self.pool.start()
        self.addAsyncCleanup(self.pool.close)

    async def test_exclusive_lease_includes_pause_and_recycle(self):
        release = asyncio.Event()
        cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()
        acquired = {}

        async def scan(request):
            async with self.pool.lease(request, request + ".test") as session:
                acquired[request] = session
                await release.wait()

        first = asyncio.create_task(scan("one"))
        second = asyncio.create_task(scan("two"))
        await until(lambda: len(acquired) == 2)
        self.assertIsNot(acquired["one"], acquired["two"])
        third = asyncio.create_task(scan("three"))
        await asyncio.sleep(0.25)
        self.assertNotIn("three", acquired)
        for session in self.pool.sessions.values():
            self.assertEqual(session.start.await_count, 1)  # No restart during pause.

        async def close_stack():
            cleanup_entered.set()
            await cleanup_release.wait()

        for session in self.pool.sessions.values():
            session.stack.aclose = close_stack
        release.set()
        await cleanup_entered.wait()
        await asyncio.sleep(0.25)
        self.assertNotIn("three", acquired)
        self.assertTrue(
            all(s.recycling and s.request_id for s in self.pool.sessions.values())
        )
        cleanup_release.set()
        await asyncio.wait_for(asyncio.gather(first, second, third), 3)
        self.assertTrue(all(s.request_id is None for s in self.pool.sessions.values()))
        self.assertEqual(
            sum(s.start.await_count for s in self.pool.sessions.values()), 5
        )

    async def test_waiter_cancellation_does_not_touch_profiles(self):
        async with (
            self.pool.lease("one", "one.test"),
            self.pool.lease("two", "two.test"),
        ):

            async def wait_for_profile():
                async with self.pool.lease("cancelled", "cancelled.test"):
                    self.fail("Busy profile was reused")

            waiter = asyncio.create_task(wait_for_profile())
            await asyncio.sleep(0.02)
            waiter.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiter
            self.assertEqual(
                [s.request_id for s in self.pool.sessions.values()], ["one", "two"]
            )

    async def test_failure_and_cancellation_recycle_before_release(self):
        with self.assertRaisesRegex(RuntimeError, "scan failed"):
            async with self.pool.lease("failed", "failed.test") as session:
                raise RuntimeError("scan failed")
        self.assertIsNone(session.request_id)
        self.assertEqual(session.start.await_count, 2)
        entered, cleanup_entered, cleanup_release = (asyncio.Event() for _ in range(3))

        async def scan():
            async with self.pool.lease("cancelled", "cancelled.test") as assigned:

                async def close_stack():
                    cleanup_entered.set()
                    await cleanup_release.wait()

                assigned.stack.aclose = close_stack
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(scan())
        await entered.wait()
        task.cancel()
        await cleanup_entered.wait()
        task.cancel()  # Service shutdown may arrive during operator cancellation.
        await asyncio.sleep(0.02)
        self.assertFalse(task.done())
        self.assertTrue(
            any(s.request_id == "cancelled" for s in self.pool.sessions.values())
        )
        cleanup_release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(all(s.request_id is None for s in self.pool.sessions.values()))

    async def test_failed_save_quarantines_profile_and_shutdown_never_reopens(self):
        with self.assertLogs("browser_service.browser_sessions", level="WARNING"):
            async with self.pool.lease("one", "one.test") as first:
                first.save.side_effect = RuntimeError("disk full")
        self.assertEqual(first.state, "error")
        self.assertIsNone(first.context)
        self.assertEqual(first.start.await_count, 1)
        first.save.side_effect = None
        async with self.pool.lease("two", "two.test") as second:
            self.assertIsNot(first, second)
            self.pool.closing = True
        self.assertEqual(second.state, "stopped")
        self.assertEqual(second.start.await_count, 1)

    async def test_leased_profile_rejects_disruptive_api_controls(self):
        service = BrowserService(
            self.root / "service", count=0, max_pending=5, idle_timeout=120
        )
        service.pool = self.pool
        service.multiplexer.pool = self.pool
        async with self.pool.lease("one", "one.test") as first:
            first.schedule_restart()
            self.assertIsNone(first.restart_task)
            original_start = first.start
            first.start = PersistentBrowserSession.start.__get__(first)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(create_app(service, api_token=None)),
                base_url="http://test",
            ) as client:
                for suffix, payload in [
                    ("start", None),
                    ("stop", None),
                    ("tabs", {"url": "https://example.test"}),
                ]:
                    response = await client.post(
                        f"/v1/browser-sessions/{first.id}/{suffix}", json=payload
                    )
                    self.assertEqual(response.status_code, 409)
                snapshot = (await client.get("/v1/browser-sessions")).json()[
                    "sessions"
                ][0]
                self.assertEqual(snapshot["request_id"], "one")
                self.assertEqual(snapshot["domain"], "one.test")
            first.start = original_start
