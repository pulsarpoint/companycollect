import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from crawler_service.company_lookup_store import LookupStore
from crawler_service.crawl_history import CrawlHistory

OLD = "2026-09-01T00:00:00+00:00"
CUTOFF = "2026-09-28T00:00:00+00:00"
RECENT = "2026-09-29T00:00:00+00:00"


class SQLiteRetentionTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.lookup_path = Path(self.directory.name) / "lookup ? retained.sqlite3"
        self.lookup = LookupStore(self.lookup_path)
        self.addCleanup(self.lookup.close)
        self.history = CrawlHistory(Path(self.directory.name) / "history.sqlite3")
        self.addCleanup(self.history.close)

    def publication(self, request_id, *, published_at=OLD):
        payload = {"status": "matched", "pages": ["large capture" * 1000]}
        self.lookup.enqueue(request_id, 1, payload)
        with self.lookup.db:
            self.lookup.db.execute(
                "UPDATE publications SET published_at=? WHERE request_id=?",
                (published_at, request_id),
            )
        return payload

    def batch(self, batch_id, request_id, *, published_at=OLD):
        self.lookup.submit(
            batch_id,
            {"domains": ["example.se"]},
            [{"request_id": request_id, "domain": "example.se"}],
        )
        self.lookup.dispatched(request_id)
        with self.lookup.db:
            self.lookup.db.execute(
                "UPDATE batches SET published_at=? WHERE batch_id=?",
                (published_at, batch_id),
            )

    def job(self, request_id, **overrides):
        job = {
            "request_id": request_id,
            "attempt": 1,
            "domain": "example.se",
            "state": "completed",
            "source": "rest",
            "updated_at": OLD,
            "purpose": "crawl",
            "s3_state": "uploaded",
            "large_metadata": "details" * 1000,
        } | overrides
        self.history.save(job)
        return job

    def test_compaction_preserves_receipts_counts_and_restart_idempotency(self):
        self.batch("batch", "published")
        payload = self.publication("published")
        snapshot = self.lookup.snapshot("batch")
        receipt = self.lookup.receipt("published", 1)

        report = self.lookup.prune(CUTOFF)
        self.assertEqual(report["publications"], 1)
        row = self.lookup.db.execute("SELECT * FROM publications").fetchone()
        self.assertEqual(json.loads(row["payload"]), {"status": "matched"})
        self.assertIsNotNone(row["payload_pruned_at"])
        self.assertEqual(self.lookup.snapshot("batch"), snapshot)
        self.assertEqual(self.lookup.receipt("published", 1), receipt)

        # Recovery enqueues every result still present on disk. A retained
        # receipt must suppress both duplicate delivery and payload regrowth.
        reopened = LookupStore(self.lookup_path)
        try:
            reopened.enqueue("published", 1, payload)
            self.assertEqual(reopened.ready(), [])
            self.assertEqual(reopened.prune(CUTOFF)["publications"], 0)
            reopened.submit("batch", {"domains": ["example.se"]}, [])
            self.assertEqual(reopened.snapshot("batch"), snapshot)
            with self.assertRaisesRegex(ValueError, "different inputs"):
                reopened.submit("batch", {"domains": ["changed.se"]}, [])
        finally:
            reopened.close()

    def test_pending_recent_and_shared_batch_payloads_are_preserved(self):
        pending = self.publication("pending", published_at=None)
        recent = self.publication("recent", published_at=RECENT)
        self.batch("old", "shared")
        self.batch("active", "shared", published_at=None)
        shared = self.publication("shared")
        self.batch("recent-batch", "recent-batch", published_at=RECENT)
        recent_batch = self.publication("recent-batch")
        self.publication("eligible")

        self.assertEqual(self.lookup.prune(CUTOFF)["publications"], 1)
        for request_id, expected in [
            ("pending", pending), ("recent", recent),
            ("shared", shared), ("recent-batch", recent_batch),
        ]:
            row = self.lookup.db.execute(
                "SELECT payload,payload_pruned_at FROM publications WHERE request_id=?",
                (request_id,),
            ).fetchone()
            self.assertEqual(json.loads(row["payload"]), expected)
            self.assertIsNone(row["payload_pruned_at"])

        self.lookup.delivered("active", [], None)
        with self.lookup.db:
            self.lookup.db.execute("UPDATE batches SET published_at=? WHERE batch_id='active'", (OLD,))
        self.assertEqual(self.lookup.prune(CUTOFF)["publications"], 1)

    def test_publication_limit_converges_and_legacy_schema_keeps_free_pages(self):
        legacy_path = Path(self.directory.name) / "legacy.sqlite3"
        db = sqlite3.connect(legacy_path)
        db.execute("""CREATE TABLE publications (
            request_id TEXT NOT NULL, attempt INTEGER NOT NULL, payload TEXT NOT NULL,
            published_at TEXT, error TEXT, PRIMARY KEY(request_id,attempt))""")
        for number in range(4):
            db.execute("INSERT INTO publications VALUES (?,1,?,?,NULL)",
                       (str(number), json.dumps({"status": "failed", "pages": ["x" * 65536]}), OLD))
        db.commit()
        db.close()
        store = LookupStore(legacy_path)
        try:
            self.assertEqual(store.db.execute("PRAGMA auto_vacuum").fetchone()[0], 0)
            report = store.prune(CUTOFF, limit=2)
            self.assertEqual(report["publications"], 2)
            self.assertGreater(report["reusable_bytes"], 0)
            self.assertEqual(store.prune(CUTOFF, limit=2)["publications"], 2)
            self.assertEqual(store.prune(CUTOFF, limit=2)["publications"], 0)
        finally:
            store.close()

    def test_history_prunes_only_delivered_terminal_events_and_keeps_retries(self):
        pending_upload = self.job("pending-upload", s3_state="pending")
        unconfigured = self.job("unconfigured", s3_state="not_configured")
        running = self.job("running", state="running")
        pending_lookup = self.job("pending-lookup", purpose="company_lookup", s3_state="not_configured")
        recent_delivery = self.job("recent-delivery", purpose="company_lookup", s3_state="not_configured")
        self.publication("pending-lookup", published_at=None)
        self.publication("recent-delivery", published_at=RECENT)
        failed = self.job("failed-delivered", state="failed")
        lookup = self.job("lookup-delivered", purpose="company_lookup", s3_state="not_configured")
        self.publication("lookup-delivered")
        recent = self.job("recent", updated_at=RECENT)
        revision = self.history.revision()

        # Ineligible oldest rows must not starve later eligible events.
        report = self.history.prune(CUTOFF, lookup_path=self.lookup_path, limit=1)
        self.assertEqual(report["events"], 1)
        self.assertEqual(self.history.prune(CUTOFF, lookup_path=self.lookup_path, limit=1)["events"], 1)
        self.assertEqual(self.history.prune(CUTOFF, lookup_path=self.lookup_path)["events"], 0)
        self.assertEqual(self.history.revision(), revision)
        remaining = {event["job"]["request_id"] for event in self.history.events(0)}
        self.assertEqual(remaining, {"pending-upload", "unconfigured", "running", "pending-lookup", "recent-delivery", "recent"})
        self.assertEqual(self.history.pending_uploads(), [pending_upload])
        for job in [pending_upload, unconfigured, running, pending_lookup, recent_delivery, failed, lookup, recent]:
            self.assertEqual(self.history.get(job["request_id"], 1), job)

    def test_history_keeps_latest_revision_and_monotonic_event_cursor(self):
        self.job("first")
        latest = self.job("latest")
        revision = self.history.revision()
        self.assertEqual(self.history.prune(CUTOFF, lookup_path=self.lookup_path)["events"], 1)
        self.assertEqual(self.history.revision(), revision)
        self.assertEqual(self.history.events(0), [{"id": revision, "job": latest}])
        self.job("next", updated_at=RECENT)
        self.assertGreater(self.history.events(revision)[0]["id"], revision)

    def test_active_attempt_protects_its_old_events(self):
        self.job("reactivated", state="failed")
        self.job("reactivated", state="running", updated_at=RECENT)
        self.job("latest")
        self.assertEqual(self.history.prune(CUTOFF, lookup_path=self.lookup_path)["events"], 0)

    def test_restart_metadata_updates_do_not_reset_terminal_retention(self):
        self.job("recovered", finished_at=OLD)
        current = self.job("recovered", finished_at=OLD, updated_at=RECENT)
        self.assertEqual(self.history.prune(CUTOFF, lookup_path=self.lookup_path)["events"], 1)
        self.assertEqual([event["job"] for event in self.history.events(0)], [current])

    def test_cleanup_does_not_wait_for_a_reader_to_release_its_snapshot(self):
        self.publication("published")
        reader = sqlite3.connect(self.lookup_path)
        try:
            reader.execute("BEGIN")
            original = reader.execute("SELECT payload FROM publications").fetchone()[0]
            self.assertEqual(self.lookup.prune(CUTOFF)["publications"], 1)
            self.assertEqual(reader.execute("SELECT payload FROM publications").fetchone()[0], original)
        finally:
            reader.close()

    def test_missing_lookup_database_is_not_created(self):
        missing = Path(self.directory.name) / "missing.sqlite3"
        self.job("delivered")
        with self.assertRaises(sqlite3.OperationalError):
            self.history.prune(CUTOFF, lookup_path=missing)
        self.assertFalse(missing.exists())
        self.assertEqual(len(self.history.events(0)), 1)

    def test_new_databases_incrementally_vacuum_and_limits_are_positive(self):
        for connection in [self.lookup.db, self.history.connection]:
            self.assertEqual(connection.execute("PRAGMA auto_vacuum").fetchone()[0], 2)
        with self.assertRaises(ValueError):
            self.lookup.prune(CUTOFF, limit=0)
        with self.assertRaises(ValueError):
            self.history.prune(CUTOFF, lookup_path=self.lookup_path, limit=-1)


if __name__ == "__main__":
    unittest.main()
