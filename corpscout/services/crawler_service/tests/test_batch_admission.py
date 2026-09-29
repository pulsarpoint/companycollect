"""Saved batch admission must not depend on another central database round trip."""

import errno
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from test_llm_profile import KEY, profile_payload

from crawler_service.company_lookup_store import LookupStore
from crawler_service.service import CrawlService
from crawler_service.service_api import create_app


class BatchAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = self.enterContext(TemporaryDirectory())
        self.service = CrawlService(Path(temporary), {
            "CLICKHOUSE_URL": "http://registry",
            "CLICKHOUSE_NATIVE_URL": "clickhouse://reader:secret@identity:9000/private?password=hidden",
            "CLICKHOUSE_RESULTS_NATIVE_URL": "clickhouse://writer:secret@results:9000/corpscout",
            "CRAWLER_LLM_ENCRYPTION_KEY": KEY,
        }, concurrency=1, max_pending=20)
        self.service.lookup_store = LookupStore(Path(temporary) / "lookups.sqlite3")
        self.addCleanup(self.service.lookup_store.db.close)
        self.service.accepting = True
        self.app = create_app(self.service, api_token=None)

    def payload(self, endpoint):
        if endpoint == "/v1/company-lookup-batches":
            return {"batch_id": "batch", "domains": ["example.se"],
                    "country": "SE", "llm": profile_payload()}
        return {"batch_id": "batch", "input_id": "draft", "run_id": "execution", "entries": [{
            "request": {"request_id": "crawl-example", "url": "https://example.se/",
                        "llm": profile_payload(), "site_info": True, "crawl": False,
                        "company_lookup": {"country": "SE", "skip_if_mapped": True}},
            "crawl_type": "site_info", "input_revision": 1, "work_key": "a" * 64,
        }]}

    async def test_saved_crawl_batch_reattaches_during_registration_outage(self):
        await self.check_reattachment("/v1/crawl-batches")

    async def test_saved_lookup_batch_reattaches_during_registration_outage(self):
        await self.check_reattachment("/v1/company-lookup-batches")

    async def check_reattachment(self, endpoint):
        payload = self.payload(endpoint)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(self.app), base_url="http://api") as http:
            with patch("crawler_service.service_api.register_requests") as register:
                first = await http.post(endpoint, json=payload)
                self.assertEqual(first.status_code, 202, first.text)
                register.assert_called_once()
                register.side_effect = RuntimeError("database unavailable")
                repeated = await http.post(endpoint, json=payload)
                self.assertEqual(repeated.status_code, 202, repeated.text)
                self.assertEqual(repeated.json()["total"], 1)
                self.service.lookup_store.cancel("batch")
                resumed = await http.post(endpoint, json=payload)
                self.assertEqual(resumed.status_code, 202, resumed.text)
                self.assertEqual(resumed.json()["state"], "running")
                conflict = await http.post(endpoint, json=payload | {"input_id": "changed"})
                self.assertEqual(conflict.status_code, 409, conflict.text)
                register.assert_called_once()
                self.assertEqual(self.service.lookup_store.db.execute("SELECT count(*) FROM items").fetchone()[0], 1)

    async def test_new_batch_registration_failure_logs_safe_reason_and_retains_no_work(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(self.app), base_url="http://api") as http:
            with (
                patch("crawler_service.service_api.register_requests", side_effect=OSError(errno.ECONNREFUSED, "password=top-secret SQL body")),
                self.assertLogs("crawler_service.service_api", level="WARNING") as logs,
            ):
                response = await http.post("/v1/crawl-batches", json=self.payload("/v1/crawl-batches"))
        self.assertEqual(response.status_code, 503)
        self.assertIsNone(self.service.lookup_store.snapshot("batch"))
        rendered = "\n".join(logs.output)
        self.assertIn("operation='register_admission'", rendered)
        self.assertIn("clickhouse://identity:9000", rendered)
        self.assertIn("ECONNREFUSED", rendered)
        self.assertIn("duration_ms=", rendered)
        for secret in ("top-secret", "reader:", "hidden", "private", "SQL body"):
            self.assertNotIn(secret, rendered)
            self.assertNotIn(secret, response.text)
