import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import psycopg2
from clickhouse_driver.errors import NetworkError, ServerException

from crawler_service.company_lookup_store import LookupStore
from crawler_service.service import CrawlService


class LookupPublicationLoggingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.service = CrawlService(
            Path(directory),
            {
                "CLICKHOUSE_NATIVE_URL": "clickhouse://registry:9000",
                "CLICKHOUSE_RESULTS_NATIVE_URL": (
                    "clickhouses://db-user:db-password@results:9440/private"
                    "?password=query-secret#fragment-secret"
                ),
            },
            concurrency=1,
            max_pending=1,
        )
        self.store = LookupStore(Path(directory) / "lookups.sqlite3")
        self.addCleanup(self.store.close)
        self.service.lookup_store = self.store
        self.store.submit("batch", {}, [{"request_id": "lookup", "domain": "example.se"}])
        self.store.enqueue("lookup", 1, {"status": "not_found"})
        self.registration = self.enterContext(patch("crawler_service.service.register_results"))
        self.enterContext(
            patch("crawler_service.service.asyncio.sleep", side_effect=asyncio.CancelledError)
        )

    async def test_native_failure_logs_safe_reason_endpoint_and_keeps_retry(self):
        failure = RuntimeError("ClickHouse insert failed for corpscout.website_site_info_results")
        failure.__cause__ = ServerException(
            "ACCESS_DENIED private SQL and payload\npassword=query-secret", code=497
        )
        with (
            patch("crawler_service.service.publish_lookup_results", side_effect=failure),
            self.assertLogs("crawler_service.service", level="WARNING") as logs,
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.service.deliver_lookup_results()

        error = self.store.snapshot("batch")["publication_error"]
        self.assertIn("ServerException code=497 reason=ACCESS_DENIED", error)
        self.assertIn("corpscout.website_site_info_results", error)
        self.assertIn("operation='publish_results'", error)
        self.assertIn("endpoint='clickhouses://results:9440'", error)
        self.assertIn(error, logs.output[0])
        self.assertEqual(self.store.receipt("lookup", 1)["error"], error)
        self.assertEqual(self.store.snapshot("batch")["state"], "publishing")
        self.assertEqual(len(self.store.ready()[0][1]), 1)
        for secret in ("db-user", "db-password", "query-secret", "fragment-secret", "private", "\n"):
            self.assertNotIn(secret, error)

        with (
            patch("crawler_service.service.publish_lookup_results"),
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.service.deliver_lookup_results()
        self.assertEqual(self.store.snapshot("batch")["state"], "published")
        self.assertIsNone(self.store.snapshot("batch")["publication_error"])
        self.assertEqual(self.store.ready(), [])

    async def test_parent_failure_names_registry_endpoint_and_does_not_publish(self):
        self.registration.side_effect = NetworkError("private native connection details")
        with (
            patch("crawler_service.service.publish_lookup_results") as publish,
            self.assertLogs("crawler_service.service", level="WARNING") as logs,
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.service.deliver_lookup_results()
        error = self.store.snapshot("batch")["publication_error"]
        self.assertIn("operation='register_parents'", error)
        self.assertIn("endpoint='clickhouse://registry:9000'", error)
        self.assertIn("reason=NETWORK_ERROR", error)
        self.assertNotIn("private", logs.output[0])
        publish.assert_not_called()

    async def test_invalid_result_connection_keeps_actionable_local_reason(self):
        with (
            patch(
                "crawler_service.service.publish_lookup_results",
                side_effect=ValueError("Expected a native ClickHouse result connection URL"),
            ),
            self.assertLogs("crawler_service.service", level="WARNING") as logs,
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.service.deliver_lookup_results()
        error = self.store.snapshot("batch")["publication_error"]
        self.assertIn("Expected a native ClickHouse result connection URL", error)
        self.assertIn(error, logs.output[0])

    async def test_repeated_failures_warn_on_change_or_after_a_minute_and_count_retries(self):
        network = NetworkError("private network details")
        denied = ServerException("private query", code=497)
        receipts = []
        times = [105, 110, 169.9, 170, 171]

        with (
            patch("crawler_service.service.time") as clock,
            patch("crawler_service.service.publish_lookup_results", side_effect=[
                network, network, denied, denied, denied, None,
            ]) as publish,
            patch.object(self.store, "delivered", wraps=self.store.delivered) as delivered,
            self.assertLogs("crawler_service.service", level="INFO") as logs,
        ):
            clock.monotonic.return_value = 100

            async def next_iteration(seconds):
                self.assertEqual(seconds, 5)
                receipts.append(self.store.receipt("lookup", 1))
                if len(receipts) > len(times):
                    raise asyncio.CancelledError
                clock.monotonic.return_value = times[len(receipts) - 1]

            with (
                patch("crawler_service.service.asyncio.sleep", side_effect=next_iteration),
                self.assertRaises(asyncio.CancelledError),
            ):
                await self.service.deliver_lookup_results()

        warnings = [record.getMessage() for record in logs.records if record.levelname == "WARNING"]
        self.assertEqual(len(warnings), 3)
        for message, retry in zip(warnings, [1, 3, 5], strict=True):
            self.assertIn(f"retry={retry} ", message)
            self.assertIn("results=1 ", message)
            self.assertIn("next_retry_seconds=5", message)
        self.assertIn("previous_failures=5", logs.output[-1])
        self.assertEqual(publish.call_count, 6)
        self.assertEqual(delivered.call_count, 6)
        self.assertTrue(all(receipt["state"] == "pending" and receipt["error"] for receipt in receipts[:5]))
        self.assertEqual(receipts[-1]["state"], "published")
        self.assertIsNone(receipts[-1]["error"])
        self.assertEqual(self.store.ready(), [])

    async def test_missing_native_result_url_reports_pending_work_once_a_minute(self):
        del self.service.environment["CLICKHOUSE_RESULTS_NATIVE_URL"]
        times = [105, 159.9, 160]
        iterations = 0
        with (
            patch("crawler_service.service.time") as clock,
            patch("crawler_service.service.publish_lookup_results") as publish,
            self.assertLogs("crawler_service.service", level="ERROR") as logs,
        ):
            clock.monotonic.return_value = 100

            async def next_iteration(seconds):
                nonlocal iterations
                self.assertEqual(seconds, 5)
                if iterations == len(times):
                    raise asyncio.CancelledError
                clock.monotonic.return_value = times[iterations]
                iterations += 1

            with (
                patch("crawler_service.service.asyncio.sleep", side_effect=next_iteration),
                self.assertRaises(asyncio.CancelledError),
            ):
                await self.service.deliver_lookup_results()

        self.assertEqual(len(logs.records), 2)
        for message in logs.output:
            self.assertIn("configure CLICKHOUSE_RESULTS_NATIVE_URL", message)
            self.assertIn("pending results retained", message)
        self.registration.assert_not_called()
        publish.assert_not_called()
        self.assertEqual(self.store.receipt("lookup", 1)["state"], "pending")
        self.assertEqual(len(self.store.ready()[0][1]), 1)

    async def test_missing_native_result_url_is_quiet_without_pending_publications(self):
        del self.service.environment["CLICKHOUSE_RESULTS_NATIVE_URL"]
        batch, rows = self.store.ready()[0]
        self.store.delivered(batch, rows, None)
        with (
            self.assertNoLogs("crawler_service.service", level="WARNING"),
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.service.deliver_lookup_results()

    async def test_nested_postgres_registration_failure_names_processing_endpoint(self):
        self.service.environment["PROCESSING_PG_URL"] = (
            "postgresql://pg-user:pg-password@coordinator:5432/private"
            "?password=pg-query-secret#pg-fragment-secret"
        )
        failure = RuntimeError("private parent registration details")
        failure.__cause__ = psycopg2.OperationalError("private DSN pg-password")
        self.registration.side_effect = failure
        with (
            patch("crawler_service.service.publish_lookup_results") as publish,
            self.assertLogs("crawler_service.service", level="WARNING") as logs,
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.service.deliver_lookup_results()

        error = self.store.receipt("lookup", 1)["error"]
        self.assertIn("endpoint='postgresql://coordinator:5432'", error)
        self.assertIn("operation='register_parents'", error)
        self.assertIn("OperationalError", error)
        self.assertIn("PostgreSQL connection or execution failed", error)
        self.assertIn(error, logs.output[0])
        self.assertNotIn("registry:9000", error)
        for secret in ("pg-user", "pg-password", "pg-query-secret", "pg-fragment-secret", "private"):
            self.assertNotIn(secret, error)
        publish.assert_not_called()
