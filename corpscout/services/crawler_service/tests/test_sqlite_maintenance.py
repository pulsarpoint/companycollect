import asyncio
import json
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from crawler_service.company_lookup_store import LookupStore
from crawler_service.crawl_history import CrawlHistory
from crawler_service.service import CrawlService


class SQLiteMaintenanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_compacts_old_delivered_payload_and_keeps_pending_work(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            old = (datetime.now(UTC) - timedelta(days=2)).isoformat(timespec="microseconds")
            lookups = LookupStore(root / "company-lookups.sqlite3")
            try:
                lookups.enqueue("delivered", 1, {"status": "matched", "details": "x" * 20000})
                lookups.enqueue("pending", 1, {"status": "failed", "details": "y" * 20000})
                with lookups.db:
                    lookups.db.execute(
                        "UPDATE publications SET published_at=? WHERE request_id='delivered'",
                        (old,),
                    )
            finally:
                lookups.close()
            # Both databases exist before the service creates its maintenance worker.
            history = CrawlHistory(root / "crawl-history.sqlite3")
            history.close()
            service = CrawlService(root, {"CRAWL_SQLITE_RETENTION_DAYS": "1"}, concurrency=1, max_pending=1)
            await service.start()
            try:
                async with asyncio.timeout(5):
                    while True:
                        row = service.lookup_store.db.execute(
                            "SELECT payload FROM publications WHERE request_id='delivered'"
                        ).fetchone()
                        if "details" not in json.loads(row[0]):
                            break
                        await asyncio.sleep(0.01)
                self.assertTrue(service.healthy())
                self.assertEqual(service.lookup_store.receipt("delivered", 1)["state"], "published")
                pending = service.lookup_store.db.execute(
                    "SELECT payload FROM publications WHERE request_id='pending'"
                ).fetchone()[0]
                self.assertEqual(json.loads(pending)["details"], "y" * 20000)
            finally:
                await service.close()
            self.assertTrue(service.maintenance_task.done())

    async def test_shutdown_waits_for_cleanup_before_releasing_service_lock(self):
        with TemporaryDirectory() as directory:
            entered, finish = threading.Event(), threading.Event()

            def held_cleanup(root, before):
                entered.set()
                if not finish.wait(5):
                    raise TimeoutError("Test cleanup was not released")
                return {"publications": 0, "events": 0, "reusable_bytes": 0, "free_bytes": 2 * 1024**3}

            service = CrawlService(Path(directory), {}, concurrency=1, max_pending=1)
            with patch("crawler_service.service.prune_sqlite", held_cleanup):
                closing = None
                await service.start()
                try:
                    async with asyncio.timeout(5):
                        while not entered.is_set():
                            await asyncio.sleep(0.01)
                    closing = asyncio.create_task(service.close())
                    await asyncio.sleep(0.05)
                    self.assertFalse(closing.done())
                    self.assertIsNotNone(service.lock)
                finally:
                    finish.set()
                    if closing is None:
                        await service.close()
                    else:
                        await closing
            self.assertIsNone(service.lock)

    async def test_retention_rejects_nonpositive_days(self):
        with TemporaryDirectory() as directory:
            for days in ("0", "-1"):
                with self.subTest(days=days), self.assertRaisesRegex(ValueError, "at least 1"):
                    CrawlService(Path(directory), {"CRAWL_SQLITE_RETENTION_DAYS": days}, concurrency=1, max_pending=1)
