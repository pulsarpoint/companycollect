"""Durable affinity and expiry behavior across process and request boundaries."""

import asyncio
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from unittest import TestCase
from uuid import uuid4

from test_browser_api import BrowserAPITests, until

from browser_service.session_store import SessionStore


class StoreTests(TestCase):
    def test_mapping_and_terminal_ids_survive_reopen(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "sessions.sqlite3"
            store = SessionStore(path)
            identifier = uuid4().hex
            store.create(identifier, "crawler", "crawl-1", "example.test", 120)
            store.assign(identifier, "browser-1", "generation-1")
            store.close()
            store = SessionStore(path)
            try:
                self.assertEqual(store.get(identifier)["profile_id"], "browser-1")
                store.interrupt_previous_process()
                record = store.get(identifier)
                self.assertEqual(record["state"], "interrupted")
                self.assertIsNotNone(record["ended_at"])
                with self.assertRaises(sqlite3.IntegrityError):
                    store.create(identifier, "crawler", "new", "other.test", 120)
            finally:
                store.close()

    def test_database_rejects_two_active_assignments_to_same_browser(self):
        with TemporaryDirectory() as directory:
            store = SessionStore(Path(directory) / "sessions.sqlite3")
            try:
                for identifier in ("one", "two"):
                    store.create(identifier, "crawler", identifier, "example.test", 120)
                store.assign("one", "browser-1", "generation-1")
                with self.assertRaises(sqlite3.IntegrityError):
                    store.assign("two", "browser-1", "generation-1")
            finally:
                store.close()


class ExpiryTests(BrowserAPITests):
    async def test_status_and_management_reads_do_not_extend_deadline(self):
        identifier = await self.reserve("read-only")
        await until(lambda: self.mux.reservations[identifier].state == "ready")
        before = self.service.store.get(identifier)
        await self.http.get(f"/v1/browser/sessions/{identifier}")
        await self.http.get("/v1/server")
        await self.http.get("/v1/browser-sessions")
        after = self.service.store.get(identifier)
        self.assertEqual(after["last_request_at"], before["last_request_at"])
        self.assertEqual(after["expires_at"], before["expires_at"])
        await self.http.post(f"/v1/browser/sessions/{identifier}/heartbeat")
        self.assertGreater(
            self.service.store.get(identifier)["expires_at"], before["expires_at"]
        )

    async def test_late_heartbeat_and_reserve_cannot_revive_expired_id(self):
        identifier = await self.reserve("old")
        await until(lambda: self.mux.reservations[identifier].state == "ready")
        self.mux.reservations[identifier].touched = 0
        response = await self.http.post(f"/v1/browser/sessions/{identifier}/heartbeat")
        self.assertEqual(response.status_code, 410)
        await until(lambda: identifier not in self.mux.reservations)
        self.assertEqual(self.service.store.get(identifier)["state"], "expired")
        response = await self.http.post(
            "/v1/browser/sessions",
            json={"id": identifier, "requestId": "old", "domain": "old.test"},
        )
        self.assertEqual(response.status_code, 410)

    async def test_inflight_operation_is_not_recycled_at_idle_deadline(self):
        identifier = await self.reserve("busy")
        await until(lambda: self.mux.reservations[identifier].state == "ready")
        reservation = self.mux.reservations[identifier]
        generation = reservation.profile.generation
        async with reservation.lock:
            reservation.touched = 0
            await asyncio.sleep(1.05)
            self.assertEqual(reservation.state, "ready")
            self.assertEqual(reservation.profile.generation, generation)
            self.mux.touch(identifier)
        self.assertGreater(reservation.touched, monotonic() - 1)

    async def test_restarted_service_rejects_old_id_without_reassignment(self):
        identifier = await self.reserve("restart")
        await until(lambda: self.mux.reservations[identifier].state == "ready")
        await self.mux.close()
        await self.mux.start()
        response = await self.http.get(f"/v1/browser/sessions/{identifier}")
        self.assertEqual(response.status_code, 410)
        self.assertEqual(self.service.store.get(identifier)["state"], "interrupted")
        self.assertEqual(self.mux.reservations, {})
