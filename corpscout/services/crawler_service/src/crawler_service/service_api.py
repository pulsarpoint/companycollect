"""REST submission and polling for locally persisted crawl jobs."""

import asyncio
import hmac
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from crawler_service.company_lookup import (
    CompanyLookupBatchRequest,
    CompanyLookupOptions,
    CompanyLookupRequest,
)
from crawler_service.debug_trace import read_trace
from crawler_service.identity_registration import register_requests
from crawler_service.llm_profile import LLMProfileError, VerifyLLMRequest, verify_llm
from crawler_service.service import (
    TERMINAL_STATES,
    AgentModel,
    AgentRunBudget,
    CrawlBatchRequest,
    CrawlJob,
    CrawlRequest,
    CrawlService,
    RequestConflict,
    ServiceUnavailable,
)


class RetryRequest(BaseModel):
    attempt: int = Field(ge=1)
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    interactive: bool = True
    challenge_agent_max_runs: AgentRunBudget | None = None
    challenge_agent_model: AgentModel | None = None


def create_app(
    service: CrawlService,
    *,
    api_token: str | None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            yield
        finally:
            await service.close()

    app = FastAPI(title="Crawler Service", version="1.0", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        _request: Request, error: RequestValidationError
    ) -> JSONResponse:
        # Invalid payloads can contain a plaintext key; never echo input in API errors.
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {"type": item["type"], "loc": item["loc"], "msg": item["msg"]}
                    for item in error.errors()
                ]
            },
        )

    def authenticate(authorization: Annotated[str | None, Header()] = None) -> None:
        if api_token is None:
            return
        scheme, _, supplied = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(
            supplied.encode(), api_token.encode()
        ):
            raise HTTPException(
                401, "Invalid bearer token", headers={"WWW-Authenticate": "Bearer"}
            )

    @app.get("/healthz")
    async def health() -> dict:
        if not service.healthy():
            raise HTTPException(503, "Crawl service is not ready")
        return {"status": "ok"}

    async def register_admission(requests: list[CrawlRequest], run_id: str) -> None:
        try:
            await asyncio.to_thread(register_requests, service.environment,
                [request.model_dump() for request in requests], run_id=run_id)
        except Exception as error:
            # Keep database URLs, credentials and SQL out of REST errors.
            raise HTTPException(503, "Central website registration failed; no new crawl was admitted",
                headers={"Retry-After": "5"}) from error

    @app.post(
        "/v1/crawls",
        response_model=CrawlJob,
        status_code=202,
        dependencies=[Depends(authenticate)],
    )
    async def submit(request: CrawlRequest, response: Response) -> CrawlJob:
        try:
            if request.request_id not in service.jobs:
                await register_admission([request], request.request_id)
            job = service.submit(request, source="rest")
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        except LLMProfileError as error:
            raise HTTPException(422, str(error)) from error
        except ServiceUnavailable as error:
            raise HTTPException(
                503, str(error), headers={"Retry-After": "5"}
            ) from error
        response.headers["Location"] = f"/v1/crawls/{job.request_id}"
        return job

    @app.post("/v1/company-lookups", response_model=CrawlJob, status_code=202,
              dependencies=[Depends(authenticate)])
    async def company_lookup(request: CompanyLookupRequest, response: Response) -> CrawlJob:
        crawl_request = CrawlRequest(
            **request.model_dump(exclude={"domain", "country", "skip_if_mapped", "max_pages"}),
            url=f"https://{request.domain}/", company_lookup=CompanyLookupOptions(country=request.country, skip_if_mapped=request.skip_if_mapped, max_pages=request.max_pages),
            debug=True, site_info=True,
        )
        return await submit(crawl_request, response)

    @app.post("/v1/crawl-batches", status_code=202, dependencies=[Depends(authenticate)])
    async def crawl_batch(request: CrawlBatchRequest):
        try:
            await register_admission([entry.request for entry in request.entries], request.batch_id)
            return service.submit_crawl_batch(request)
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        except ServiceUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except LLMProfileError as error:
            raise HTTPException(422, str(error)) from error

    @app.post("/v1/company-lookup-batches", status_code=202, dependencies=[Depends(authenticate)])
    async def lookup_batch(request: CompanyLookupBatchRequest):
        try:
            await register_admission([CrawlRequest(url=f"https://{domain}/", site_info=True)
                for domain in request.domains], request.batch_id)
            return service.submit_lookup_batch(request)
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        except ServiceUnavailable as error:
            raise HTTPException(503, str(error)) from error
        except LLMProfileError as error:
            raise HTTPException(422, str(error)) from error

    @app.get("/v1/crawl-batches/{batch_id}", dependencies=[Depends(authenticate)])
    @app.get("/v1/company-lookup-batches/{batch_id}", dependencies=[Depends(authenticate)])
    async def lookup_batch_status(batch_id: str):
        assert service.lookup_store is not None
        result = service.lookup_store.snapshot(batch_id)
        if result is None:
            raise HTTPException(404, "Unknown lookup batch")
        return result

    @app.delete("/v1/crawl-batches/{batch_id}", dependencies=[Depends(authenticate)])
    @app.delete("/v1/company-lookup-batches/{batch_id}", dependencies=[Depends(authenticate)])
    async def cancel_lookup_batch(batch_id: str):
        assert service.lookup_store is not None
        if service.lookup_store.snapshot(batch_id) is None:
            raise HTTPException(404, "Unknown lookup batch")
        for request_id in service.lookup_store.cancel(batch_id):
            if request_id in service.jobs and service.jobs[request_id].state not in TERMINAL_STATES:
                service.cancel(request_id)
        return service.lookup_store.snapshot(batch_id)

    def lookup_result_response(path: Path, request_id: str, attempt: int):
        document = json.loads(path.read_text())
        assert service.lookup_store is not None
        receipt = service.lookup_store.receipt(request_id, attempt)
        if not service.environment.get("CLICKHOUSE_RESULTS_URL") and receipt["state"] != "published":
            receipt["error"] = "ClickHouse result writer is not configured; delivery remains queued"
        document.update(publication=receipt, persisted_to_database=receipt["state"] == "published")
        return JSONResponse(document, headers={"Cache-Control": "no-store"})

    @app.post("/v1/crawls/validate", dependencies=[Depends(authenticate)])
    async def validate(request: CrawlRequest) -> dict:
        """Normalize a publisher's payload without enqueueing or opening a browser."""
        if request.llm is not None:
            try:
                request.llm.decrypt_api_key(service.environment)
            except LLMProfileError as error:
                raise HTTPException(422, str(error)) from error
        return request.model_dump(exclude_unset=True) | {
            "request_id": request.request_id
        }

    @app.post("/v1/llm/verify", dependencies=[Depends(authenticate)])
    async def verify_model(request: VerifyLLMRequest) -> dict:
        if api_token is None:
            raise HTTPException(
                503, "Configure crawler API authentication to verify LLM profiles"
            )
        return await verify_llm(
            request.llm, service.environment,
            preview_only=request.preview_only, include_exchange=request.include_exchange,
        )

    @app.get("/v1/crawls/status", dependencies=[Depends(authenticate)])
    async def statuses(
        state: str | None = None,
        domain: str | None = None,
        source: str | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict:
        assert service.history is not None
        return service.history.list_attempts(
            state=state, domain=domain, source=source, limit=limit, offset=offset
        ) | {
            "revision": service.history.revision(),
            "debug_available": True,
            "company_lookup_available": bool(service.environment.get("CLICKHOUSE_URL")),
            "human_enabled": service.human_enabled,
            "challenge_agent_enabled": service.challenge_agent_enabled,
            "challenge_agent_max_runs": service.challenge_agent_max_runs,
        }

    @app.get("/v1/crawls/events", dependencies=[Depends(authenticate)])
    async def events(
        request: Request,
        after: Annotated[int, Query(ge=0)] = 0,
    ) -> StreamingResponse:
        assert service.history is not None
        supplied = request.headers.get("last-event-id", "")
        if supplied.isdecimal():
            after = max(after, int(supplied))

        async def stream() -> AsyncIterator[str]:
            cursor = after
            while not await request.is_disconnected():
                assert service.history is not None
                batch = service.history.events(cursor)
                for event in batch:
                    cursor = event["id"]
                    yield f"id: {cursor}\nevent: crawl-status\ndata: {json.dumps(event['job'])}\n\n"
                if not batch:
                    yield ": heartbeat\n\n"
                    await asyncio.sleep(1)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-store",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/v1/crawls/{request_id}/status", dependencies=[Depends(authenticate)])
    async def crawl_status(request_id: str) -> CrawlJob:
        if request_id not in service.jobs:
            raise HTTPException(404, "Unknown crawl request")
        return service.jobs[request_id].model_copy(deep=True)

    @app.get("/v1/crawls/{request_id}/debug", dependencies=[Depends(authenticate)])
    async def debug_trace(
        request_id: str,
        http_request: Request,
        attempt: Annotated[int | None, Query(ge=0)] = None,
        after: Annotated[int, Query(ge=0)] = 0,
        event: Annotated[int | None, Query(ge=1)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
        download: bool = False,
        stream: bool = False,
        result: bool = False,
    ):
        if request_id not in service.jobs:
            raise HTTPException(404, "Unknown crawl request")
        job = service.jobs[request_id]
        number = job.attempt if attempt is None else attempt
        # Before the worker starts, attempt zero follows the upcoming attempt.
        if number == 0 and job.attempt > 0:
            number = job.attempt
        assert service.history is not None
        snapshot = job.model_dump() if number == job.attempt else service.history.get(request_id, number)
        if snapshot is None:
            raise HTTPException(404, "Unknown crawl attempt")
        directory = service.root / "jobs" / request_id / "attempts" / f"{number:04}"
        headers = {"Cache-Control": "no-store"}
        if result:
            path = directory / "result.json"
            if snapshot["state"] not in TERMINAL_STATES:
                raise HTTPException(409, "Attempt is still running")
            if not path.is_file():
                raise HTTPException(404, "No result saved for this attempt; inspect its error trace")
            if snapshot.get("purpose") == "company_lookup":
                return lookup_result_response(path, request_id, number)
            return FileResponse(path, media_type="application/json", headers=headers)
        if stream:
            supplied = http_request.headers.get("last-event-id", "")
            if supplied:
                previous_attempt, _, previous_cursor = supplied.partition(":")
                if not previous_attempt.isdecimal() or not previous_cursor.isdecimal():
                    raise HTTPException(400, "Invalid trace event cursor")
                if int(previous_attempt) == number:
                    after = max(after, int(previous_cursor))
            try:
                read_trace(directory, after, 1)
            except ValueError as error:
                raise HTTPException(400, str(error)) from error

            async def follow_file() -> AsyncIterator[str]:
                cursor, selected_attempt, folder = after, number, directory
                while not await http_request.is_disconnected():
                    current = service.jobs[request_id]
                    if selected_attempt == 0 and current.attempt > 0:
                        selected_attempt, cursor = current.attempt, 0
                        folder = service.root / "jobs" / request_id / "attempts" / f"{selected_attempt:04}"
                    status = current.model_dump() if selected_attempt == current.attempt else service.history.get(request_id, selected_attempt)
                    batch = read_trace(folder, cursor, limit)
                    cursor = batch["cursor"]
                    data = batch | {"job": status, "attempt": selected_attempt,
                                    "enabled": status.get("debug_enabled", False) or (folder / "debug").is_dir()}
                    done = status["state"] in TERMINAL_STATES and not batch["has_more"]
                    yield f"id: {selected_attempt}:{cursor}\nevent: {'crawl-debug-complete' if done else 'crawl-debug'}\ndata: {json.dumps(data)}\n\n"
                    if done:
                        break
                    await asyncio.sleep(0.05 if batch["has_more"] else 1)
            return StreamingResponse(follow_file(), media_type="text/event-stream", headers=headers | {"X-Accel-Buffering": "no"})
        if event is not None:
            path = directory / "debug" / f"{event:08}.json"
            if not path.is_file():
                raise HTTPException(404, "No details saved for this trace event")
            return FileResponse(path, media_type="application/json", headers=headers)
        if download:
            def export():
                cursor = 0
                while True:
                    batch = read_trace(directory, cursor, 100)
                    for item in batch["events"]:
                        if item["has_details"]:
                            item["details"] = json.loads((directory / "debug" / f"{item['id']:08}.json").read_text(encoding="utf-8"))
                        yield json.dumps(item, ensure_ascii=False) + "\n"
                    cursor = batch["cursor"]
                    if not batch["has_more"]:
                        break
            return StreamingResponse(export(), media_type="application/x-ndjson", headers=headers | {
                "Content-Disposition": f'attachment; filename="crawl-{request_id}-{number}-debug.jsonl"',
            })
        try:
            batch = read_trace(directory, after, limit)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        return JSONResponse(batch | {"job": snapshot, "attempt": number,
            "enabled": snapshot.get("debug_enabled", False) or (directory / "debug").is_dir()}, headers=headers)

    @app.post(
        "/v1/crawls/{request_id}/retry",
        status_code=202,
        dependencies=[Depends(authenticate)],
    )
    async def retry(request_id: str, payload: RetryRequest) -> CrawlJob:
        try:
            saved = service.root / "jobs" / request_id / "request.json"
            if request_id in service.jobs and saved.is_file():
                await register_admission([CrawlRequest.model_validate_json(saved.read_text(encoding="utf-8"))], payload.request_id or request_id)
            return service.retry(
                request_id,
                **payload.model_dump(exclude={"request_id"}),
                new_id=payload.request_id,
            )
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        except LLMProfileError as error:
            raise HTTPException(422, str(error)) from error
        except ServiceUnavailable as error:
            raise HTTPException(503, str(error)) from error

    @app.post(
        "/v1/crawls/{request_id}/verify-search",
        status_code=202,
        dependencies=[Depends(authenticate)],
    )
    async def verify_search(request_id: str) -> dict:
        try:
            service.activate_verification(request_id)
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        return {"state": "verification_requested"}

    @app.post(
        "/v1/crawls/{request_id}/resume",
        status_code=202,
        dependencies=[Depends(authenticate)],
    )
    async def resume(request_id: str) -> dict:
        try:
            service.resume(request_id)
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        return {"state": "resume_requested"}

    @app.post(
        "/v1/crawls/{request_id}/cancel",
        status_code=202,
        dependencies=[Depends(authenticate)],
    )
    async def cancel(request_id: str) -> dict:
        if service.lookup_store is not None and service.lookup_store.snapshot(request_id) is not None:
            return await cancel_lookup_batch(request_id)
        if request_id not in service.jobs:
            raise HTTPException(404, "Unknown crawl request")
        try:
            service.cancel(request_id)
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        return {"state": "cancel_requested"}

    @app.post(
        "/v1/crawls/{request_id}/browser-ticket", dependencies=[Depends(authenticate)]
    )
    async def browser_ticket(request_id: str) -> dict:
        session = service.human_sessions.get(request_id)
        job = service.jobs.get(request_id)
        if (
            session is None
            or not session.waiting
            or not session.browser_available
            or job is None
            or job.browser_lease_id is None
        ):
            raise HTTPException(409, "No interactive browser is waiting for this crawl")
        if not service.browser_url:
            raise HTTPException(503, "Browser service is not configured")
        try:
            async with httpx.AsyncClient(
                base_url=service.browser_url,
                timeout=10,
                headers={"Authorization": f"Bearer {service.browser_token}"}
                if service.browser_token
                else {},
            ) as http:
                response = await http.post(
                    f"/v1/browser/sessions/{job.browser_lease_id}/browser-ticket"
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPError as error:
            raise HTTPException(503, "Browser service is unavailable") from error

    @app.get(
        "/v1/crawls/{request_id}",
        response_model=CrawlJob,
        dependencies=[Depends(authenticate)],
    )
    async def status(request_id: str) -> CrawlJob:
        if service.lookup_store is not None:
            batch = service.lookup_store.snapshot(request_id)
            if batch is not None:
                # Existing LLM task monitor can observe/cancel this entire batch.
                return CrawlJob(request_id=request_id, purpose="company_lookup", source="rest",
                    state="completed" if batch["state"] == "published" else "cancelled" if batch["state"] == "cancelled" else "running",
                    submitted_at=batch["created_at"], finished_at=batch["published_at"],
                    reason=f"{batch['processed']}/{batch['total']} processed", error=batch["publication_error"])
        if request_id not in service.jobs:
            raise HTTPException(404, "Unknown crawl request")
        return service.jobs[request_id].model_copy(deep=True)

    @app.get("/v1/crawls/{request_id}/result", dependencies=[Depends(authenticate)])
    async def result(request_id: str) -> FileResponse:
        if request_id not in service.jobs:
            raise HTTPException(404, "Unknown crawl request")
        job = service.jobs[request_id]
        if job.state not in TERMINAL_STATES or job.result_file is None:
            raise HTTPException(
                409, "Crawl result is not ready", headers={"Retry-After": "5"}
            )
        path = service.root / job.result_file
        if not path.is_file():
            raise HTTPException(503, "Stored crawl result is unavailable")
        if job.purpose == "company_lookup":
            saved = path.parent / "result.json"
            return lookup_result_response(saved if saved.exists() else path, request_id, job.attempt)
        return FileResponse(path, media_type="application/json")

    return app
