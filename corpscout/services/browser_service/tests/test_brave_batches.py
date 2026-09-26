"""Durability, publication, cancellation and admission at the SQLite/HTTP boundaries."""

import asyncio
import json
import sqlite3
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4, uuid5

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from browser_service.brave_batch_control import BraveAdmissionError
from browser_service.brave_batch_results import (
    RESULT_COLUMNS,
    BraveBatchPublisher,
    BraveClickHouseSettings,
)
from browser_service.brave_batches import BraveBatchQueue, batch_router
from browser_service.brave_models import BraveBatchController, BraveBatchRequest


def payload(count=8):
    execution = uuid4()
    return BraveBatchRequest(
        batch_id=uuid4(),
        task_id=uuid4(),
        execution_id=execution,
        source_run_id=uuid4(),
        owner_request_id=uuid4(),
        query_type="official_website",
        processor_version="brave-draft-v1",
        options={
            "llm": {
                "profile_id": str(uuid4()),
                "profile_revision": 1,
                "provider": "fixture",
                "base_url": "http://llm.test/v1",
                "model": "test",
                "api_key_encrypted": "v1.encrypted.secret",
            }
        },
        items=[
            {
                "input_id": f"SE:{i}",
                "country_code": "SE",
                "company_id": str(i),
                "company_name": f"Company {i}",
                "result_id": uuid5(execution, f"SE:{i}"),
                "query": f"Find Company {i}",
            }
            for i in range(count)
        ],
    )


class BatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_storage_failure_cannot_leave_an_exited_worker_looking_running(self):
        batch = payload(4)
        await self.submit(batch)
        with (
            patch.object(
                self.queue.store,
                "save",
                side_effect=sqlite3.OperationalError("disk full"),
            ),
            patch.object(
                self.queue.store,
                "state",
                side_effect=sqlite3.OperationalError("disk full"),
            ),
        ):
            await asyncio.gather(self.queue.task, return_exceptions=True)
        state = (await self.http.get(f"/batches/{batch.batch_id}")).json()
        self.assertEqual(state["state"], "paused")
        self.assertIn("storage", state["reason"])
        self.assertEqual(self.active, 0)

    async def test_interrupted_request_gets_new_attempt_without_changing_result_identity(
        self,
    ):
        batch = payload(1)
        self.queue.store.submit(batch)
        item = self.queue.store.claim(str(batch.batch_id))
        self.saved[item["request_id"]] = {
            "request_id": item["request_id"],
            "status": "interrupted",
        }
        await self.queue.activate(
            str(batch.batch_id),
            BraveBatchController(
                controller_id=batch.source_run_id,
                owner_request_id=batch.owner_request_id,
            ),
        )
        await self.queue.task
        self.assertEqual(self.searched[0].request_id, item["request_id"] + "-retry-1")
        self.assertEqual(
            self.publications[0][0]["result_id"], str(batch.items[0].result_id)
        )
        self.control.finish.assert_any_await(batch, item["request_id"], "canceled")

    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.service = SimpleNamespace(
            root=Path(self.temporary.name), active={}, accepting=True
        )
        self.control = SimpleNamespace(admit=AsyncMock(), finish=AsyncMock())
        self.saved = {}
        self.searched = []
        self.active = self.peak = 0
        self.blocked = set()
        self.release = asyncio.Event()
        self.publications = []

        async def status(request_id):
            if request_id not in self.saved:
                raise HTTPException(404, "Unknown request")
            return self.saved[request_id]

        async def ask(request):
            self.searched.append(request)
            self.active += 1
            self.peak = max(self.active, self.peak)
            try:
                if request.query in self.blocked:
                    await self.release.wait()
                await asyncio.sleep(0)
                value = {
                    "request_id": request.request_id,
                    "query": request.query,
                    "route": request.route,
                    "status": "blocked"
                    if request.query == "Find Company 1"
                    else "success",
                    "answer": "https://example.se",
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "source_url": "https://search.brave.com/ask",
                    "error_type": "",
                    "error_stage": "",
                    "elapsed_ms": 100,
                    "challenge_runs": [],
                }
                self.saved[request.request_id] = value
                return value
            finally:
                self.active -= 1

        async def publish(batch, records):
            self.publications.append(records)

        self.queue = BraveBatchQueue(
            self.service,
            ask=ask,
            status=status,
            publisher=SimpleNamespace(publish=publish),
            control=self.control,
        )
        self.queue.start()
        self.app = FastAPI()
        self.app.include_router(batch_router(self.queue))
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(self.app), base_url="http://test"
        )

    async def asyncTearDown(self):
        await self.queue.close()
        await self.http.aclose()

    async def submit(self, batch):
        response = await self.http.post("/batches", json=batch.model_dump(mode="json"))
        self.assertEqual(response.status_code, 202, response.text)
        return response.json()

    async def until(self, predicate):
        async with asyncio.timeout(5):
            while not predicate():
                await asyncio.sleep(0.01)

    async def test_500_inputs_are_consumed_by_four_workers_then_published_together(
        self,
    ):
        batch = payload(500)
        self.blocked = {"Find Company 0"}
        await self.submit(batch)
        await self.until(
            lambda: self.queue.store.snapshot(str(batch.batch_id))["processed"] == 499
        )
        self.assertEqual(self.peak, 4)
        self.assertEqual(self.publications, [])
        self.assertEqual(len(self.searched), 500)
        self.release.set()
        await self.queue.task
        state = self.queue.store.snapshot(str(batch.batch_id))
        self.assertEqual(
            (state["state"], state["published"], state["succeeded"], state["failed"]),
            ("completed", 500, 499, 1),
        )
        self.assertEqual(len(self.publications), 1)
        self.assertEqual(len(self.publications[0]), 500)
        self.assertEqual(len({request.session_id for request in self.searched}), 4)
        self.assertNotIn("encrypted", json.dumps(state))
        await self.submit(batch)
        self.assertEqual(len(self.searched), 500)

    async def test_partial_batch_survives_restart_and_requires_explicit_resume(self):
        batch = payload()
        self.blocked = {"Find Company 0"}
        await self.submit(batch)
        await self.until(
            lambda: self.queue.store.snapshot(str(batch.batch_id))["processed"] == 7
        )
        await self.queue.stop("restart")
        self.queue.store.close()
        self.queue.start()
        state = self.queue.store.snapshot(str(batch.batch_id))
        self.assertEqual((state["state"], state["processed"]), ("paused", 7))
        self.release.set()
        controller = BraveBatchController(
            controller_id=uuid4(), owner_request_id=batch.owner_request_id
        )
        await self.queue.activate(str(batch.batch_id), controller)
        await self.queue.task
        self.assertEqual(len(self.searched), 9)
        self.assertEqual(self.queue.store.snapshot(str(batch.batch_id))["published"], 8)

    async def test_saved_browser_response_recovers_after_sqlite_checkpoint_is_lost(
        self,
    ):
        batch = payload(1)
        self.queue.store.submit(batch)
        item = self.queue.store.claim(str(batch.batch_id))
        self.saved[item["request_id"]] = {
            "request_id": item["request_id"],
            "query": batch.items[0].query,
            "route": "direct",
            "status": "success",
            "answer": "saved answer",
            "fetched_at": datetime.now(UTC).isoformat(),
            "source_url": "https://search.brave.com",
            "error_type": "",
            "error_stage": "",
            "elapsed_ms": 1,
        }
        await self.queue.activate(
            str(batch.batch_id),
            BraveBatchController(
                controller_id=batch.source_run_id,
                owner_request_id=batch.owner_request_id,
            ),
        )
        await self.queue.task
        self.assertEqual(self.searched, [])
        self.assertEqual(self.publications[0][0]["answer_text"], "saved answer")

    async def test_clickhouse_failure_retries_only_publication(self):
        batch = payload(4)
        attempts = 0
        published = asyncio.Event()

        async def publish(_batch, records):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                published.set()
                raise RuntimeError("fixture failed acknowledgement")
            self.publications.append(records)

        self.queue.publisher.publish = publish
        await self.submit(batch)
        await published.wait()
        self.assertEqual(
            self.queue.store.snapshot(str(batch.batch_id))["state"], "publishing"
        )
        self.assertEqual(self.queue.store.snapshot(str(batch.batch_id))["published"], 0)
        await self.queue.task
        self.assertEqual(
            (len(self.searched), attempts, len(self.publications)), (4, 2, 1)
        )

    async def test_cancel_retains_completed_results_and_fences_old_controller(self):
        batch = payload()
        self.blocked = {"Find Company 0"}
        await self.submit(batch)
        await self.until(
            lambda: self.queue.store.snapshot(str(batch.batch_id))["processed"] == 7
        )
        stale = {
            "controller_id": str(uuid4()),
            "owner_request_id": str(batch.owner_request_id),
        }
        self.assertEqual(
            (
                await self.http.post(f"/batches/{batch.batch_id}/cancel", json=stale)
            ).status_code,
            409,
        )
        owner = {
            "controller_id": str(batch.source_run_id),
            "owner_request_id": str(batch.owner_request_id),
        }
        response = await self.http.post(f"/batches/{batch.batch_id}/cancel", json=owner)
        self.assertEqual(response.json()["processed"], 7)
        self.assertEqual(response.json()["state"], "paused")
        self.assertEqual(self.active, 0)
        self.assertEqual(self.publications, [])

    async def test_disabled_llm_cancels_active_browsers_and_stops_queue(self):
        batch = payload(8)
        self.blocked = {item.query for item in batch.items}
        await self.submit(batch)
        await self.until(lambda: len(self.searched) == 4)
        self.control.admit.side_effect = BraveAdmissionError("LLM disabled")
        await asyncio.wait_for(self.queue.task, 5)
        self.assertEqual(len(self.searched), 4)
        self.assertEqual(self.active, 0)
        self.assertEqual(
            self.queue.store.snapshot(str(batch.batch_id))["reason"], "LLM disabled"
        )

    async def test_expired_controller_lease_stops_queue(self):
        batch = payload()
        self.blocked = {item.query for item in batch.items}
        await self.submit(batch)
        await self.until(lambda: len(self.searched) == 4)
        with self.queue.store.connection:
            self.queue.store.connection.execute("UPDATE batches SET lease_until=0")
        await asyncio.wait_for(self.queue.task, 5)
        self.assertEqual(self.active, 0)
        self.assertIn(
            "heartbeat expired",
            self.queue.store.snapshot(str(batch.batch_id))["reason"],
        )

    async def test_rejects_changed_batch_and_new_batch_before_completion(self):
        batch = payload()
        self.blocked = {item.query for item in batch.items}
        await self.submit(batch)
        changed = batch.model_copy(deep=True)
        changed.items[0].query = "changed question"
        self.assertEqual(
            (
                await self.http.post("/batches", json=changed.model_dump(mode="json"))
            ).status_code,
            409,
        )
        changed.batch_id = uuid4()
        self.assertEqual(
            (
                await self.http.post("/batches", json=changed.model_dump(mode="json"))
            ).status_code,
            409,
        )
        value = batch.model_dump(mode="json")
        value["items"] *= 100
        self.assertEqual(
            (await self.http.post("/batches", json=value)).status_code, 422
        )


class PublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_insert_and_lost_ack_reconcile_before_complete(self):
        batch = payload(4)
        ids = [str(item.result_id) for item in batch.items]
        saved = {ids[0]}
        calls = []
        records = [{"result_id": identifier} for identifier in ids]

        async def serve(request):
            body = request.content.decode()
            calls.append(body)
            if body.startswith("SELECT"):
                return httpx.Response(200, text="\n".join(saved) + "\n")
            if "FORMAT JSONEachRow" in body:
                rows = [json.loads(line) for line in body.splitlines()[1:]]
                self.assertEqual(len(rows), 3)
                saved.update(row["result_id"] for row in rows)
            return httpx.Response(200)

        real_client = httpx.AsyncClient
        with patch(
            "browser_service.brave_batch_results.httpx.AsyncClient",
            side_effect=lambda **kw: real_client(
                **kw, transport=httpx.MockTransport(serve)
            ),
        ):
            publisher = BraveBatchPublisher(
                BraveClickHouseSettings(
                    url="http://clickhouse.test", username="test", password="secret"
                )
            )
            await publisher.publish(batch, records)
            await publisher.publish(batch, records)
        self.assertEqual(sum("FORMAT JSONEachRow" in call for call in calls), 1)
        self.assertTrue(any("latest_success" in call for call in calls))

    def test_result_columns_match_dagster_publication_contract(self):
        source = (
            Path(__file__).resolve().parents[2]
            / "dagster_v3/src/dagster_v3/defs/company_domains/results.py"
        )
        import ast

        tree = ast.parse(source.read_text())
        node = next(
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == "RESULT_COLUMNS"
                for t in node.targets
            )
        )
        self.assertEqual(RESULT_COLUMNS, ast.literal_eval(node.value))

    def test_rejects_unstable_result_ids(self):
        value = payload().model_dump(mode="json")
        value["items"][0]["result_id"] = str(uuid4())
        with self.assertRaises(ValidationError):
            BraveBatchRequest.model_validate(value)
