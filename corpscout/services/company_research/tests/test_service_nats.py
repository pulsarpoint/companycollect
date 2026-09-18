"""Use an isolated real JetStream broker; replace only the browser boundary."""

import asyncio
import json
import os
import shutil
import socket
import subprocess
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
import nats
from nats.aio.msg import Msg
from nats.errors import Error as NatsError
from test_crawl import browser_responses
from test_package import response
from test_site_info import COMPANY, HTML, SITE

from company_research.service import CrawlService
from company_research.service_nats import JetStreamInput, JetStreamSettings


class JetStreamServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_discovery_request_returns_capture_and_is_acknowledged(self):
        self.service.environment["DEEPSEEK"] = "test-key"

        async def boundary(client, request, **kwargs):
            if request.url.host == "api.deepseek.com":
                return httpx.Response(200, json=response(COMPANY))
            return httpx.Response(404)

        with (
            patch(
                "company_research.crawl.open_browser",
                lambda: browser_responses(
                    {SITE: (HTML, [], 200, None)}, self.requested
                ),
            ),
            patch.object(httpx.AsyncClient, "send", boundary),
        ):
            await self.js.publish(
                self.settings.subject,
                json.dumps(
                    {
                        "request_id": "full",
                        "url": SITE,
                        "crawl": "full",
                        "config": {"max_pages": 40},
                    }
                ).encode(),
            )
            job = await self.wait_job("full")
            await self.wait_acked(1)
        self.assertEqual(job.state, "completed")
        result = json.loads((self.service.root / job.result_file).read_text())
        self.assertEqual(result["crawl"]["mode"], "full")
        self.assertEqual(result["crawl"]["config"]["max_pages"], 40)
        self.assertEqual(result["crawl"]["config"]["max_external_pages"], 30)
        self.assertEqual(result["crawl"]["status"], "finished")
        self.assertEqual(result["documents"][0]["html"], HTML)
        self.assertEqual(result["crawl"]["usage"]["calls"], 1)

    async def asyncSetUp(self):
        binary = os.environ.get("NATS_SERVER") or shutil.which("nats-server")
        if binary is None:
            self.skipTest(
                "Set NATS_SERVER or install nats-server for real JetStream tests"
            )
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        self.server = subprocess.Popen(
            [
                binary,
                "-js",
                "-a",
                "127.0.0.1",
                "-p",
                str(port),
                "-sd",
                str(self.root / "nats"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.addCleanup(self.stop_server)
        self.settings = JetStreamSettings(
            servers=[f"nats://127.0.0.1:{port}"], ack_wait=1, create_stream=True
        )
        async with asyncio.timeout(10):
            while True:
                try:
                    reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    writer.close()
                    await writer.wait_closed()
                    break
                except OSError:
                    await asyncio.sleep(0.02)
        self.client = await nats.connect(servers=self.settings.servers)
        self.addAsyncCleanup(self.client.close)
        self.js = self.client.jetstream()
        self.requested = []
        self.hold = asyncio.Event()
        self.hold.set()
        self.url = "https://example.test/jobs"

        @asynccontextmanager
        async def browser():
            await self.hold.wait()
            async with browser_responses(
                {self.url: ("<h1>Engineer</h1>", [], 200, None)}, self.requested
            ) as value:
                yield value

        boundary = patch("company_research.crawl.open_browser", browser)
        boundary.start()
        self.addCleanup(boundary.stop)
        self.service = CrawlService(
            self.root / "results", {}, concurrency=1, max_pending=2
        )
        await self.service.start()
        self.addAsyncCleanup(self.service.close)
        self.input = JetStreamInput(self.service, self.settings)
        await self.input.start()
        self.addAsyncCleanup(self.input.close)

    def stop_server(self):
        self.server.terminate()
        try:
            self.server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.server.kill()
            self.server.wait(timeout=5)

    async def wait_job(self, request_id):
        async with asyncio.timeout(10):
            while request_id not in self.service.jobs:
                await asyncio.sleep(0.02)
            return await self.service.wait(request_id)

    async def wait_acked(self, delivered):
        async with asyncio.timeout(10):
            while True:
                info = await self.js.consumer_info(
                    self.settings.stream, self.settings.durable
                )
                if (
                    info.delivered.consumer_seq >= delivered
                    and info.num_ack_pending == 0
                ):
                    return info
                await asyncio.sleep(0.02)

    async def test_progress_ack_and_duplicate_request_do_not_repeat_crawl(self):
        self.hold.clear()
        payload = {
            "request_id": "long-job",
            "url": self.url,
            "pages": [self.url],
            "save_artifacts": False,
        }
        await self.js.publish(self.settings.subject, json.dumps(payload).encode())
        async with asyncio.timeout(5):
            while "long-job" not in self.service.jobs:
                await asyncio.sleep(0.02)
        await asyncio.sleep(1.4)
        pending = await self.js.consumer_info(
            self.settings.stream, self.settings.durable
        )
        self.assertEqual(pending.num_ack_pending, 1)
        self.assertEqual(pending.delivered.consumer_seq, 1)
        self.assertIsNone(self.service.jobs["long-job"].result_file)
        self.hold.set()
        job = await self.wait_job("long-job")
        self.assertTrue((self.service.root / job.result_file).is_file())
        self.assertEqual(
            [
                path.name
                for path in (self.service.root / job.result_file).parent.iterdir()
            ],
            ["result.json"],
        )
        await self.wait_acked(1)
        await self.js.publish(self.settings.subject, json.dumps(payload).encode())
        await self.wait_acked(2)
        self.assertEqual(self.requested, [self.url])
        self.assertEqual(self.service.jobs["long-job"].attempt, 1)

    async def test_redelivery_after_lost_ack_uses_saved_result(self):
        ack_deliveries = []
        original = Msg.ack_sync

        async def fail_first_ack(message, *args, **kwargs):
            ack_deliveries.append(message.metadata.num_delivered)
            self.assertTrue(
                (
                    self.service.root / self.service.jobs["redelivery"].result_file
                ).is_file()
            )
            if len(ack_deliveries) == 1:
                raise NatsError("Simulated disconnect before ack")
            return await original(message, *args, **kwargs)

        with patch.object(Msg, "ack_sync", fail_first_ack):
            await self.js.publish(
                self.settings.subject,
                json.dumps(
                    {"request_id": "redelivery", "url": self.url, "pages": [self.url]}
                ).encode(),
            )
            await self.wait_job("redelivery")
            await self.wait_acked(2)
        self.assertEqual(ack_deliveries, [1, 2])
        self.assertEqual(self.requested, [self.url])

    async def test_poison_message_is_recorded_without_raw_credentials_and_terminated(
        self,
    ):
        await self.js.publish(
            self.settings.subject,
            b'{"url":"https://example.test/","api_key":"accidental-secret"}',
        )
        await self.wait_acked(1)
        rejected = list((self.service.root / "rejected").glob("*.json"))
        self.assertEqual(len(rejected), 1)
        self.assertNotIn("accidental-secret", rejected[0].read_text())
        self.assertEqual(
            json.loads(rejected[0].read_text())["reason"], "invalid_request"
        )
        self.assertEqual(self.requested, [])

    async def test_rejection_write_failure_keeps_delivery_retryable(self):
        original = Msg.nak
        nacked = asyncio.Event()

        async def track_nak(message, *args, **kwargs):
            nacked.set()
            return await original(message, *args, **kwargs)

        with (
            patch(
                "company_research.service_nats.write_json",
                side_effect=OSError("disk unavailable"),
            ),
            patch.object(Msg, "nak", track_nak),
        ):
            await self.js.publish(self.settings.subject, b"not-json")
            await asyncio.wait_for(nacked.wait(), 5)
            info = await self.js.consumer_info(
                self.settings.stream, self.settings.durable
            )
            self.assertEqual(info.num_ack_pending, 1)
        self.assertFalse((self.service.root / "rejected").exists())

    async def test_request_without_id_and_restart_keep_stable_completed_job(self):
        await self.js.publish(
            self.settings.subject,
            json.dumps({"url": self.url, "pages": [self.url]}).encode(),
        )
        async with asyncio.timeout(5):
            while not self.service.jobs:
                await asyncio.sleep(0.02)
        request_id = next(iter(self.service.jobs))
        self.assertTrue(request_id.startswith("nats-"))
        await self.wait_job(request_id)
        await self.wait_acked(1)
        await self.input.close()
        await self.service.close()
        restarted = CrawlService(self.service.root, {}, concurrency=1, max_pending=2)
        await restarted.start()
        worker = JetStreamInput(restarted, self.settings, self.input.results)
        try:
            await worker.start()
            await self.js.publish(
                self.settings.subject,
                json.dumps(
                    {"request_id": request_id, "url": self.url, "pages": [self.url]}
                ).encode(),
            )
            await self.wait_acked(2)
            self.assertEqual(restarted.jobs[request_id].attempt, 1)
            self.assertEqual(self.requested, [self.url])
        finally:
            await worker.close()
            await restarted.close()


if __name__ == "__main__":
    unittest.main()
