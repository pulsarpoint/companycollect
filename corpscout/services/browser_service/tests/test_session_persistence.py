"""Saved identity outlives execution, while expiry removes only inactive profiles."""

import asyncio
from time import time

from test_browser_api import BrowserAPITests, until

from browser_service.session_store import SessionStore


class PersistenceTests(BrowserAPITests):
    async def test_reads_do_not_extend_deadline(self):
        identifier = await self.reserve("read-only")
        before = self.service.store.get(identifier)
        for path in (
            f"/v1/browser/sessions/{identifier}",
            "/v1/server",
            "/v1/browser-sessions",
        ):
            self.assertEqual((await self.http.get(path)).status_code, 200)
        self.assertEqual(
            self.service.store.get(identifier)["expires_at"], before["expires_at"]
        )
        await self.http.post(
            f"/v1/browser/sessions/{identifier}/heartbeat",
            headers=self.execution_headers(identifier),
        )
        self.assertGreater(
            self.service.store.get(identifier)["expires_at"], before["expires_at"]
        )

    async def test_idle_expiry_closes_browser_but_identity_can_reopen(self):
        identifier = await self.reserve("idle")
        headers = self.execution_headers(identifier)
        with self.service.store.connection:
            self.service.store.connection.execute(
                "UPDATE executions SET expires_at=0 WHERE session_id=?", (identifier,)
            )
        self.assertEqual(
            (
                await self.http.post(
                    f"/v1/browser/sessions/{identifier}/heartbeat", headers=headers
                )
            ).status_code,
            410,
        )
        await until(lambda: identifier not in self.service.active)
        self.assertEqual(self.service.store.get(identifier)["state"], "idle_timeout")
        self.assertEqual(await self.reserve("idle"), identifier)
        self.assertNotEqual(headers, self.execution_headers(identifier))

    async def test_busy_operation_is_not_closed_at_idle_deadline(self):
        identifier = await self.reserve("busy")
        session = self.service.get(identifier)
        async with session.lock:
            with self.service.store.connection:
                self.service.store.connection.execute(
                    "UPDATE executions SET expires_at=0 WHERE session_id=?",
                    (identifier,),
                )
            await asyncio.sleep(1.05)
            self.assertEqual(self.service.store.get(identifier)["state"], "ready")
            self.service.touch(identifier)
        self.assertGreater(
            self.service.store.get(identifier)["last_request_at"], time() - 1
        )

    async def test_restart_retains_profile_and_does_not_launch_browsers(self):
        identifier = await self.reserve("restart")
        profile = self.service.get(identifier).profile.root / "profile"
        (profile / "retained").write_text("state")
        previous = self.service.get(identifier).execution_id
        await self.service.close()
        self.service.store = SessionStore(self.service.root / "sessions.sqlite3")
        await self.service.start()
        self.assertEqual(self.service.active, {})
        self.assertEqual(self.service.snapshot(identifier)["state"], "closed")
        await self.reserve("restart")
        self.assertNotEqual(previous, self.service.get(identifier).execution_id)
        self.assertEqual((profile / "retained").read_text(), "state")

    async def test_template_copied_once_and_retention_respects_pin_and_ownership(self):
        (self.service.base_profile / "seed").write_text("template")
        first = await self.reserve("copy")
        profile = self.service.get(first).profile.root / "profile"
        self.assertEqual((profile / "seed").read_text(), "template")
        (profile / "seed").write_text("changed")
        await self.service.release(first)
        await self.reserve("copy")
        self.assertEqual((profile / "seed").read_text(), "changed")
        self.assertEqual((self.service.base_profile / "seed").read_text(), "template")
        second = await self.reserve("pinned")
        await self.service.release(second)
        self.service.store.set_pinned(second, True, self.service.retention)
        with self.service.store.connection:
            self.service.store.connection.execute(
                "UPDATE sessions SET retained_until=0"
            )
        await self.service.clean_expired_profiles()
        self.assertTrue(profile.exists())
        self.assertIsNone(self.service.store.session(second)["expired_at"])
        await self.service.release(first)
        with self.service.store.connection:
            self.service.store.connection.execute(
                "UPDATE sessions SET retained_until=0 WHERE id=?", (first,)
            )
        await self.service.clean_expired_profiles()
        self.assertFalse(profile.exists())
        response = await self.http.post(
            "/v1/browser/sessions",
            json={"id": first, "requestId": "expired", "domain": "site.test"},
        )
        self.assertEqual(response.status_code, 410)

    async def test_legacy_profile_import_is_preserved_and_pinned(self):
        await self.service.close()
        self.service.store = SessionStore(self.service.root / "sessions.sqlite3")
        self.service.store.delete_setting("legacy_profiles_imported")
        legacy = self.service.root / "profiles" / "headless-1" / "routes" / "proxy1"
        (legacy / "profile").mkdir(parents=True)
        (legacy / "profile" / "Cookies").write_text("private")
        (legacy / "session.json").write_text('{"cookies":[]}')
        await self.service.start()
        saved = self.service.store.saved()[0]
        self.assertTrue(saved["pinned"])
        self.assertEqual(saved["route"], "proxy1")
        self.assertEqual(
            (
                self.service.root / "sessions" / saved["id"] / "profile" / "Cookies"
            ).read_text(),
            "private",
        )
        self.assertTrue((legacy / "profile" / "Cookies").exists())
