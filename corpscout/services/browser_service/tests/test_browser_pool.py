"""Executions retain ownership until their browser cleanup finishes."""

import asyncio
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from test_browser_api import BrowserAPITests, until

from browser_service.runtime import BrowserRuntimeSettings


class BrowserLifecycleTests(BrowserAPITests):
    async def test_cancelled_close_holds_capacity_until_cleanup_finishes(self):
        self.service.configure_runtime(
            BrowserRuntimeSettings(
                max_browsers=1, idle_timeout_seconds=120, session_retention_days=7
            )
        )
        identifier = await self.reserve("one")
        profile = self.service.get(identifier).profile
        entered, finish = asyncio.Event(), asyncio.Event()

        async def close_stack():
            entered.set()
            await finish.wait()

        profile.stack.aclose = close_stack
        closing = asyncio.create_task(
            self.http.delete(
                f"/v1/browser/sessions/{identifier}",
                headers=self.execution_headers(identifier),
            )
        )
        await asyncio.wait_for(entered.wait(), 2)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertEqual(self.service.store.get(identifier)["state"], "stopping")
        response = await self.http.post(
            "/v1/browser/sessions", json={"requestId": "two", "domain": "two.test"}
        )
        self.assertEqual(response.status_code, 503)
        finish.set()
        await until(lambda: identifier not in self.service.active)
        self.assertEqual(self.service.store.get(identifier)["state"], "closed")
        await self.reserve("two")

    async def test_close_waits_for_operation_and_rejects_new_commands(self):
        identifier = await self.reserve("busy")
        session = self.service.get(identifier)
        headers = self.execution_headers(identifier)
        async with session.lock:
            closing = asyncio.create_task(
                self.http.delete(f"/v1/browser/sessions/{identifier}", headers=headers)
            )
            await until(
                lambda: self.service.store.get(identifier)["state"] == "stopping"
            )
            self.assertFalse(closing.done())
            self.assertEqual(
                (
                    await self.http.post(
                        f"/v1/browser/sessions/{identifier}/heartbeat", headers=headers
                    )
                ).status_code,
                409,
            )
        self.assertEqual((await closing).status_code, 200)

    async def test_cleanup_failure_keeps_ownership_until_retry(self):
        identifier = await self.reserve("cleanup")
        profile = self.service.get(identifier).profile
        profile.stack.aclose = AsyncMock(side_effect=RuntimeError("cleanup failed"))
        with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
            await self.service.release(identifier)
        self.assertEqual(self.service.store.get(identifier)["state"], "stopping")
        self.assertIn(identifier, self.service.active)
        profile.stack.aclose.side_effect = None
        await self.service.release(identifier)
        self.assertNotIn(identifier, self.service.active)

    async def test_start_failure_closes_execution_and_preserves_identity(self):
        identifier = uuid4().hex
        with patch(
            "browser_service.browser_sessions.PersistentBrowserSession.start",
            AsyncMock(side_effect=RuntimeError("launch failed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "launch failed"):
                await self.service.claim(
                    identifier=identifier, request_id="one", domain="one.test"
                )
        self.assertEqual(self.service.store.get(identifier)["state"], "failed")
        self.assertIsNotNone(self.service.store.session(identifier))
        self.assertNotIn(identifier, self.service.active)

    async def test_request_owned_browser_rejects_manual_disruptive_controls(self):
        identifier = await self.reserve("crawler")
        for suffix, payload in (
            ("start", {}),
            ("stop", {"executionId": self.service.get(identifier).execution_id}),
            ("tabs", {"url": "https://example.test"}),
        ):
            response = await self.http.post(
                f"/v1/browser-sessions/{identifier}/{suffix}", json=payload
            )
            self.assertEqual(response.status_code, 409, response.text)
