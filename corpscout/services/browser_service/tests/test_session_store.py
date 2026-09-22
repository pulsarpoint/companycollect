"""Exercise ownership and capacity at the SQLite transaction boundary."""

import sqlite3
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

from browser_service.session_store import BrowserSessionError, SessionStore


class SessionStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "sessions.sqlite3"
        self.store = SessionStore(self.path)
        self.addCleanup(self.store.close)
        for identifier in ("first", "second"):
            self.store.ensure_session(
                identifier, route=None, headless=None, retention=86400
            )

    def claim(self, identifier, store=None, request=None):
        return (store or self.store).claim(
            identifier,
            request or identifier,
            "site.test",
            max_browsers=1,
            timeout=120,
            retention=86400,
            headless=None,
        )

    def test_independent_connections_cannot_exceed_global_capacity(self):
        barrier = threading.Barrier(2)

        def claim(identifier):
            store = SessionStore(self.path)
            try:
                barrier.wait(timeout=5)
                try:
                    return self.claim(identifier, store)
                except BrowserSessionError as error:
                    return error.status
            finally:
                store.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, ["first", "second"]))
        self.assertEqual(sum(isinstance(row, dict) for row in results), 1)
        self.assertIn(503, results)

    def test_starting_ready_stopping_all_occupy_capacity(self):
        first = self.claim("first")
        for state in ("starting", "ready", "stopping"):
            if state == "ready":
                self.store.ready(first["id"], "generation")
            if state == "stopping":
                self.store.finish(first["id"], state, retention=86400)
            with self.assertRaises(BrowserSessionError) as raised:
                self.claim("second")
            self.assertEqual(raised.exception.status, 503)
        self.store.finish(first["id"], "closed", retention=86400)
        self.claim("second")

    def test_reopen_identity_after_restart_with_new_execution(self):
        first = self.claim("first")
        self.store.interrupt_previous_process()
        reopened = self.claim("first", request="new-operation")
        self.assertNotEqual(first["id"], reopened["id"])
        self.assertEqual(reopened["session_id"], "first")
        self.assertEqual(len(self.store.recent()), 2)

    def test_database_rejects_duplicate_profile_owner(self):
        first = self.claim("first")
        with self.assertRaises(BrowserSessionError) as raised:
            self.claim("first", request="other")
        self.assertEqual(raised.exception.status, 409)
        self.store.finish(first["id"], "stopping", retention=86400)
        with self.assertRaises(sqlite3.IntegrityError), self.store.connection:
            self.store.connection.execute("""INSERT INTO executions
                (id,session_id,request_id,domain,state,created_at,last_request_at,expires_at)
                VALUES ('duplicate','first','other','site.test','starting',0,0,120)""")

    def test_migrates_legacy_history_without_deleting_it(self):
        path = self.path.with_name("legacy.sqlite3")
        with sqlite3.connect(path) as old:
            old.execute("CREATE TABLE sessions (id TEXT, profile_id TEXT)")
            old.execute("INSERT INTO sessions VALUES ('lease', 'browser-1')")
        store = SessionStore(path)
        try:
            self.assertEqual(
                tuple(
                    store.connection.execute("SELECT * FROM legacy_sessions").fetchone()
                ),
                ("lease", "browser-1"),
            )
            self.assertEqual(store.saved(), [])
        finally:
            store.close()
