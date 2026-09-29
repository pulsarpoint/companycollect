import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from crawler_service.company_lookup_store import LookupStore


class PublicationRaceTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name)

    def test_reopened_batch_with_unfinished_member(self):
        self.check_reopened_batch(False)

    def test_reopened_batch_with_pending_member(self):
        self.check_reopened_batch(True)

    def check_reopened_batch(self, remaining_finishes_before_ack: bool):
        tmp_path = self.path
        path = tmp_path / "queue.sqlite3"
        store = LookupStore(path)
        payload = {"domains": ["a.se", "b.se"]}
        items = [{"request_id": name, "domain": f"{name}.se"} for name in ("a", "b")]
        try:
            store.submit("batch", payload, items)
            store.dispatched("a")
            store.enqueue("a", 1, {"status": "not_found"})
            store.cancel("batch")
            batch_id, inflight = store.ready()[0]
            assert [row["request_id"] for row in inflight] == ["a"]

            # The same batch is resumed while its cancelled snapshot is in flight.
            store.submit("batch", payload, items)
            if remaining_finishes_before_ack:
                store.dispatched("b")
                store.enqueue("b", 1, {"status": "not_found"})
            store.delivered(batch_id, inflight, None)
            assert store.receipt("a", 1)["state"] == "published"
            assert store.snapshot("batch")["published_at"] is None
            assert store.snapshot("batch")["state"] == (
                "publishing" if remaining_finishes_before_ack else "running"
            )
            assert store.db.execute("SELECT published_at FROM batches").fetchone()[0] is None
            store.close()
            store = LookupStore(path)

            if not remaining_finishes_before_ack:
                assert store.ready() == []
                store.dispatched("b")
                store.enqueue("b", 1, {"status": "not_found"})
            batch_id, remaining = store.ready()[0]
            assert [row["request_id"] for row in remaining] == ["b"]
            store.delivered(batch_id, remaining, None)
            assert store.snapshot("batch")["state"] == "published"
            assert store.receipt("b", 1)["state"] == "published"
            assert store.ready() == []
        finally:
            store.close()


    def test_legacy_published_marker_recovers_when_remaining_results_arrive(self):
        tmp_path = self.path
        path = tmp_path / "queue.sqlite3"
        store = LookupStore(path)
        payload = {"domains": ["a.se", "b.se"]}
        items = [{"request_id": name, "domain": f"{name}.se"} for name in ("a", "b")]
        try:
            store.submit("batch", payload, items)
            store.dispatched("a")
            store.enqueue("a", 1, {"status": "not_found"})
            store.cancel("batch")
            batch_id, rows = store.ready()[0]
            store.delivered(batch_id, rows, None)
            old_timestamp = store.snapshot("batch")["published_at"]
            assert old_timestamp is not None
            store.submit("batch", payload, items)
            # Reproduce a marker written by the old delivery code after re-admission.
            with store.db:
                store.db.execute("UPDATE batches SET published_at=?", (old_timestamp,))
            store.close()
            store = LookupStore(path)

            assert store.snapshot("batch")["state"] == "running"
            assert store.snapshot("batch")["published_at"] is None
            assert store.ready() == []
            store.dispatched("b")
            store.enqueue("b", 1, {"status": "matched"})
            assert store.snapshot("batch")["state"] == "publishing"
            assert store.snapshot("batch")["published_at"] is None
            batch_id, rows = store.ready()[0]
            assert [row["request_id"] for row in rows] == ["b"]
            store.delivered(batch_id, rows, None)
            assert store.snapshot("batch")["state"] == "published"
            assert store.snapshot("batch")["processed"] == 2
            assert store.ready() == []
        finally:
            store.close()


    def test_pending_cancelled_member_prevents_early_batch_receipt(self):
        tmp_path = self.path
        store = LookupStore(tmp_path / "queue.sqlite3")
        try:
            store.submit("batch", {"domains": ["a.se", "b.se"]}, [
                {"request_id": name, "domain": f"{name}.se"} for name in ("a", "b")
            ])
            store.dispatched("a")
            store.enqueue("a", 1, {"status": "not_found"})
            store.cancel("batch")
            batch_id, inflight = store.ready()[0]
            # Shared work can finish in another batch while this membership is cancelled.
            store.enqueue("b", 1, {"status": "not_found"})
            store.delivered(batch_id, inflight, None)
            assert store.snapshot("batch")["published_at"] is None
            batch_id, remaining = store.ready()[0]
            assert [row["request_id"] for row in remaining] == ["b"]
            store.delivered(batch_id, remaining, None)
            assert store.snapshot("batch")["published_at"] is not None
            assert store.ready() == []
        finally:
            store.close()

    def test_cancel_clears_stale_marker_for_unfinished_batch(self):
        store = LookupStore(self.path / "queue.sqlite3")
        self.addCleanup(store.close)
        payload = {"domains": ["a.se", "b.se"]}
        items = [{"request_id": name, "domain": f"{name}.se"} for name in ("a", "b")]
        store.submit("batch", payload, items)
        store.dispatched("a")
        store.enqueue("a", 1, {"status": "not_found"})
        store.cancel("batch")
        batch_id, rows = store.ready()[0]
        store.delivered(batch_id, rows, None)
        old_timestamp = store.snapshot("batch")["published_at"]
        receipt = store.receipt("a", 1)
        store.submit("batch", payload, items)
        with store.db:
            store.db.execute("UPDATE batches SET published_at=?", (old_timestamp,))

        assert store.snapshot("batch")["state"] == "running"
        store.cancel("batch")
        assert store.snapshot("batch")["state"] == "cancelled"
        assert store.snapshot("batch")["published_at"] is None
        assert tuple(store.db.execute("SELECT cancelled,published_at FROM batches").fetchone()) == (1, None)
        assert store.undispatched(10) == []
        assert store.receipt("a", 1) == receipt
        batch_id, remaining = store.ready()[0]
        assert remaining == []
        store.delivered(batch_id, remaining, None)
        assert store.snapshot("batch")["published_at"] is not None

    def test_cancel_preserves_fully_acknowledged_batch_receipts(self):
        for initially_cancelled in (False, True):
            with self.subTest(initially_cancelled=initially_cancelled):
                store = LookupStore(self.path / f"queue-{initially_cancelled}.sqlite3")
                try:
                    items = [{"request_id": "a", "domain": "a.se"}]
                    if initially_cancelled:
                        items.append({"request_id": "b", "domain": "b.se"})
                    store.submit("batch", {}, items)
                    store.dispatched("a")
                    store.enqueue("a", 1, {"status": "not_found"})
                    if initially_cancelled:
                        store.cancel("batch")
                    batch_id, rows = store.ready()[0]
                    store.delivered(batch_id, rows, None)
                    before = store.snapshot("batch")
                    receipt = store.receipt("a", 1)
                    assert before["published_at"] is not None

                    store.cancel("batch")
                    store.cancel("batch")
                    assert store.snapshot("batch") == before
                    assert store.receipt("a", 1) == receipt
                    assert store.ready() == []
                finally:
                    store.close()
