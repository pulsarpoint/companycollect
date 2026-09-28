"""Durable service-owned Brave batches with browser workers and publication fencing."""

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable
from time import monotonic, time
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException

from browser_service.brave_batch_control import BraveAdmissionError, BraveBatchControl
from browser_service.brave_batch_results import BraveBatchPublisher, result_record
from browser_service.brave_batch_store import BraveBatchStore
from browser_service.brave_models import (
    BraveAskRequest,
    BraveBatchController,
    BraveBatchRequest,
)
from browser_service.runtime import BrowserService

LOGGER = logging.getLogger(__name__)
ROUTES = ("direct", "crawl_proxy1", "crawl_proxy2", "crawl_proxy3")


class BraveRequestError(RuntimeError):
    """A safe, contextual failure from the internal Brave request endpoint."""


def http_error_detail(error: HTTPException) -> str:
    detail = error.detail
    if isinstance(detail, dict):
        detail = detail.get("message", "Request rejected")
    if not isinstance(detail, str):
        detail = "Request rejected"
    detail = re.sub(r"(?i)\bBearer\s+\S+|\bsk-[A-Za-z0-9_-]+", "[redacted]", detail)
    detail = re.sub(r"(https?://)[^/\s@]+@", r"\1[redacted]@", detail)
    detail = re.sub(r"(https?://[^\s?#]+)[?#][^\s]*", r"\1?[redacted]", detail)
    detail = re.sub(
        r"""(?i)((?:api[_-]?key|password|token|secret)["']?\s*[=:]\s*["']?)[^"'\s,;}]+""",
        r"\1[redacted]",
        detail,
    )
    return " ".join(detail.split())[:500]


class BraveBatchQueue:
    def __init__(
        self,
        service: BrowserService,
        *,
        ask: Callable[[BraveAskRequest], Awaitable[dict]],
        status: Callable[[str], Awaitable[dict]],
        publisher: BraveBatchPublisher,
        control: BraveBatchControl,
    ):
        self.service, self.ask, self.status = service, ask, status
        self.publisher, self.control = publisher, control
        self.store: BraveBatchStore | None = None
        self.task: asyncio.Task | None = None
        self.batch_id: str | None = None
        self.lock = asyncio.Lock()
        self.pause_reason = ""
        self.draining = False
        self.capacity_waiters: dict[str, dict] = {}

    def start(self) -> None:
        self.store = BraveBatchStore(self.service.root / "brave-queue.sqlite3")

    async def close(self) -> None:
        await self.stop("Browser service is shutting down; resume the batch")
        if self.store is not None:
            self.store.close()
            self.store = None

    def require_store(self) -> BraveBatchStore:
        if self.store is None or not self.service.accepting:
            raise HTTPException(503, "Brave batch queue is not ready")
        return self.store

    def snapshot(self, batch_id: str) -> dict:
        result = self.require_store().snapshot(batch_id)
        waiters = (
            list(self.capacity_waiters.values()) if self.batch_id == batch_id else []
        )
        result["waiting_for_capacity"] = len(waiters)
        if result["state"] == "running" and waiters:
            result["reason"] = "Waiting for browser capacity: " + "; ".join(
                f"{item['route']} input={item['input_id']} request={item['request_id']} "
                f"waited={int(monotonic() - item['started'])}s HTTP 503: {item['detail']}"
                for item in waiters
            )
        if (
            self.batch_id == batch_id
            and self.task is not None
            and self.task.done()
            and result["state"] in {"running", "publishing"}
        ):
            # A storage failure may prevent persisting the paused state itself.
            error = None if self.task.cancelled() else self.task.exception()
            result.update(
                state="paused",
                reason=f"Batch worker exited before publication ({type(error).__name__}); check service storage and resume",
            )
        return result

    async def activate(
        self,
        batch_id: str,
        controller: BraveBatchController,
        submission: BraveBatchRequest | None = None,
    ) -> dict:
        async with self.lock:
            store = self.require_store()
            if submission is not None:
                await self.control.admit(submission)
                if (
                    self.task is not None
                    and not self.task.done()
                    and self.batch_id != batch_id
                ):
                    raise HTTPException(
                        409,
                        "Another Brave batch is active",
                        headers={"Retry-After": "2"},
                    )
                store.submit(submission)
            row = store.get(batch_id)
            payload = BraveBatchRequest.model_validate_json(row["payload"])
            if str(payload.owner_request_id) != str(controller.owner_request_id):
                old_owner = str(payload.owner_request_id)
                payload.owner_request_id = controller.owner_request_id
                await self.control.transfer(old_owner, payload)
            if row["state"] == "completed":
                return store.snapshot(batch_id)
            await self.control.admit(payload)
            if self.task is not None and not self.task.done():
                if self.batch_id != batch_id:
                    raise HTTPException(
                        409,
                        "Another Brave batch is active",
                        headers={"Retry-After": "2"},
                    )
                if row["controller_id"] != str(controller.controller_id):
                    if row["lease_until"] > time():
                        raise HTTPException(
                            409, "Another controller still owns this batch"
                        )
                    await self.stop("Controller lease expired")
                else:
                    store.heartbeat(batch_id, str(controller.controller_id))
                    return store.snapshot(batch_id)
            store.activate(
                batch_id,
                str(controller.controller_id),
                str(controller.owner_request_id),
            )
            payload.source_run_id = controller.controller_id
            self.batch_id, self.pause_reason = batch_id, ""
            self.draining = False
            self.task = asyncio.create_task(
                self.run(payload), name=f"brave-batch-{batch_id}"
            )
            return store.snapshot(batch_id)

    async def stop(self, reason: str) -> None:
        if self.task is not None and not self.task.done():
            # A failure already starts cleanup. Dagster's final /cancel must not
            # cancel that cleanup a second time or erase the original cause.
            if not self.draining and not self.task.cancelling():
                self.pause_reason = reason
                self.task.cancel()
            await asyncio.shield(asyncio.gather(self.task, return_exceptions=True))
            if (
                self.store is not None
                and self.batch_id is not None
                and self.store.get(self.batch_id)["state"]
                not in {"completed", "paused"}
            ):
                self.store.state(self.batch_id, "paused", reason)

    async def watch(self, batch: BraveBatchRequest, parent: asyncio.Task) -> None:
        try:
            while True:
                await asyncio.sleep(2)
                if (
                    self.require_store().get(str(batch.batch_id))["lease_until"]
                    <= time()
                ):
                    self.pause_reason = "Dagster heartbeat expired; resume the batch"
                    parent.cancel()
                    return
                await self.control.admit(batch)
        except asyncio.CancelledError:
            raise
        except BraveAdmissionError as error:
            self.pause_reason = str(error)
            parent.cancel()
        except Exception as error:
            self.pause_reason = f"LLM control check unavailable ({type(error).__name__}); resume when restored"
            parent.cancel()

    async def run(self, batch: BraveBatchRequest) -> None:
        store = self.require_store()
        batch_id = str(batch.batch_id)
        parent = asyncio.current_task()
        assert parent is not None
        watch = asyncio.create_task(self.watch(batch, parent))
        workers = [
            asyncio.create_task(self.worker(batch, route))
            for _ in range(batch.requests_per_route)
            for route in ROUTES
        ]
        try:
            await asyncio.gather(*workers)
            records = store.results(batch_id)
            store.state(batch_id, "publishing")
            failures = 0
            while True:
                try:
                    await self.publisher.publish(batch, records)
                    break
                except Exception as error:
                    failures += 1
                    response = getattr(error, "response", None)
                    detail = (
                        f"HTTP {response.status_code}"
                        if response is not None
                        else type(error).__name__
                    )
                    reason = f"ClickHouse publication failed ({detail}); retrying saved results"
                    store.state(batch_id, "publishing", reason)
                    LOGGER.warning("Brave batch %s: %s", batch_id, reason)
                    await asyncio.sleep(min(30, 2 ** min(failures, 5)))
            store.state(batch_id, "completed", published=len(records))
            LOGGER.info(
                "Brave batch %s: published %s/%s results",
                batch_id,
                len(records),
                len(batch.items),
            )
        except asyncio.CancelledError:
            store.state(
                batch_id,
                "paused",
                self.pause_reason or "Browser request cancelled; resume the batch",
            )
        except Exception as error:
            reason = (
                str(error)
                if isinstance(error, (BraveAdmissionError, BraveRequestError))
                else f"Batch processing failed ({type(error).__name__}); resume the batch"
            )
            store.state(batch_id, "paused", reason)
            LOGGER.error("Brave batch %s: %s", batch_id, reason)
        finally:
            self.draining = True
            watch.cancel()
            for worker in workers:
                if not worker.cancelling():
                    worker.cancel()
            await asyncio.gather(watch, *workers, return_exceptions=True)
            with store.connection:
                store.connection.execute(
                    "UPDATE items SET state='pending' WHERE batch_id=? AND state='running'",
                    (batch_id,),
                )

    async def worker(self, batch: BraveBatchRequest, route: str) -> None:
        store = self.require_store()
        batch_id, session_id = str(batch.batch_id), uuid4().hex
        item = None
        stage = "claim"
        try:
            while (item := store.claim(batch_id)) is not None:
                company = batch.items[item["position"]]
                deadline = monotonic() + 120
                while True:
                    await self.control.admit(batch, item["request_id"])
                    try:
                        stage = "status"
                        result = await self.status(item["request_id"])
                    except HTTPException as error:
                        if error.status_code != 404:
                            raise
                        result = None
                    if result is not None:
                        if (
                            result.get("error_type") == "Cancelled"
                            or result["status"] == "interrupted"
                        ):
                            await self.control.finish(
                                batch, item["request_id"], "canceled"
                            )
                            store.retry_request(batch_id, item)
                            continue
                        if result["status"] == "running":
                            if monotonic() > deadline:
                                raise RuntimeError(
                                    "Existing browser request did not finish"
                                )
                            await asyncio.sleep(2)
                            continue
                    else:
                        request = BraveAskRequest(
                            **batch.options.model_dump(),
                            request_id=item["request_id"],
                            session_id=session_id,
                            query=company.query,
                            route=route,
                        )
                        try:
                            stage = "submit"
                            # Individual cancellation addresses this child task, not the queue's worker.
                            result = await asyncio.create_task(self.ask(request))
                        except asyncio.CancelledError:
                            await self.control.finish(
                                batch, item["request_id"], "canceled"
                            )
                            raise
                        except HTTPException as error:
                            if error.status_code == 503:
                                # Shared browser capacity is backpressure, not a
                                # failed company search. The heartbeat/admission
                                # watcher still fences this wait and cancellation.
                                now = monotonic()
                                if session_id not in self.capacity_waiters:
                                    self.capacity_waiters[session_id] = {
                                        "route": route,
                                        "input_id": company.input_id,
                                        "request_id": item["request_id"],
                                        "started": now,
                                        "logged": now,
                                        "detail": http_error_detail(error),
                                    }
                                    LOGGER.warning(
                                        "Brave batch %s waiting for browser capacity: input=%s route=%s request=%s HTTP 503: %s",
                                        batch_id,
                                        company.input_id,
                                        route,
                                        item["request_id"],
                                        http_error_detail(error),
                                    )
                                elif (
                                    now - self.capacity_waiters[session_id]["logged"]
                                    >= 30
                                ):
                                    LOGGER.info(
                                        "Brave batch %s still waiting for browser capacity: input=%s route=%s waited=%.0fs",
                                        batch_id,
                                        company.input_id,
                                        route,
                                        now
                                        - self.capacity_waiters[session_id]["started"],
                                    )
                                    self.capacity_waiters[session_id]["logged"] = now
                                deadline = now + 120
                                await asyncio.sleep(2)
                                continue
                            if (
                                error.status_code == 409
                                and error.headers
                                and error.headers.get("Retry-After")
                            ):
                                if monotonic() <= deadline:
                                    await asyncio.sleep(2)
                                    continue
                            raise
                    waiting = self.capacity_waiters.pop(session_id, None)
                    if waiting is not None:
                        LOGGER.info(
                            "Brave batch %s capacity wait resolved: input=%s route=%s waited=%.0fs",
                            batch_id,
                            company.input_id,
                            route,
                            monotonic() - waiting["started"],
                        )
                    if result["request_id"] != item["request_id"]:
                        raise ValueError("Browser result belongs to another request")
                    store.save(
                        batch_id,
                        company.input_id,
                        result_record(batch, company, result),
                    )
                    await self.control.finish(batch, item["request_id"], "completed")
                    LOGGER.info(
                        "Brave batch %s saved input=%s route=%s status=%s",
                        batch_id,
                        company.input_id,
                        result["route"],
                        result["status"],
                    )
                    break
        except HTTPException as error:
            raise BraveRequestError(
                f"HTTP {error.status_code} during {stage}: {http_error_detail(error)}; "
                f"input={item['input_id'] if item else 'unknown'} route={route} "
                f"request={item['request_id'] if item else 'unknown'}; saved results retained for resume"
            ) from error
        finally:
            self.capacity_waiters.pop(session_id, None)
            session = self.service.active.get(session_id)
            if session is not None:
                await self.service.release(
                    session_id, execution_id=session.execution_id
                )


def batch_router(queue: BraveBatchQueue | None) -> APIRouter:
    router = APIRouter()

    def available() -> BraveBatchQueue:
        if queue is None:
            raise HTTPException(
                503,
                "Brave batches require service-side ClickHouse and LLM control connections",
            )
        queue.require_store()
        return queue

    @router.post("/batches", status_code=202)
    async def submit(payload: BraveBatchRequest) -> dict:
        current = available()
        return await current.activate(
            str(payload.batch_id),
            BraveBatchController(
                controller_id=payload.source_run_id,
                owner_request_id=payload.owner_request_id,
            ),
            submission=payload,
        )

    @router.get("/executions/{execution_id}/batch")
    async def unfinished(execution_id: UUID) -> dict:
        return {"batch": available().require_store().unfinished(str(execution_id))}

    @router.get("/batches/{batch_id}")
    async def status(batch_id: UUID) -> dict:
        return available().snapshot(str(batch_id))

    @router.get("/batches/{batch_id}/result-ids")
    async def result_ids(batch_id: UUID) -> dict:
        row = available().require_store().get(str(batch_id))
        return {
            "result_ids": [
                item["result_id"] for item in json.loads(row["payload"])["items"]
            ]
        }

    @router.post("/batches/{batch_id}/resume")
    async def resume(batch_id: UUID, payload: BraveBatchController) -> dict:
        return await available().activate(str(batch_id), payload)

    @router.post("/batches/{batch_id}/heartbeat")
    async def heartbeat(batch_id: UUID, payload: BraveBatchController) -> dict:
        store = available().require_store()
        row = store.get(str(batch_id))
        if json.loads(row["payload"])["owner_request_id"] != str(
            payload.owner_request_id
        ):
            raise HTTPException(409, "Batch belongs to another LLM execution")
        store.heartbeat(str(batch_id), str(payload.controller_id))
        return available().snapshot(str(batch_id))

    @router.post("/batches/{batch_id}/cancel")
    async def cancel(batch_id: UUID, payload: BraveBatchController) -> dict:
        current = available()
        async with current.lock:
            store = current.require_store()
            row = store.get(str(batch_id))
            if row["controller_id"] != str(payload.controller_id) or json.loads(
                row["payload"]
            )["owner_request_id"] != str(payload.owner_request_id):
                raise HTTPException(409, "Batch belongs to another controller")
            if current.batch_id == str(batch_id):
                await current.stop(
                    "Stopped by Dagster; saved results retained for resume"
                )
            return store.snapshot(str(batch_id))

    return router
