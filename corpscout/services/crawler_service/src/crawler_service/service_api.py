"""REST submission and polling for locally persisted crawl jobs."""

import asyncio
import hmac
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
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
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from crawler_service.service import (
    TERMINAL_STATES,
    AgentModel,
    AgentRunBudget,
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

    @app.post(
        "/v1/crawls",
        response_model=CrawlJob,
        status_code=202,
        dependencies=[Depends(authenticate)],
    )
    async def submit(request: CrawlRequest, response: Response) -> CrawlJob:
        try:
            job = service.submit(request, source="rest")
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
        except ServiceUnavailable as error:
            raise HTTPException(
                503, str(error), headers={"Retry-After": "5"}
            ) from error
        response.headers["Location"] = f"/v1/crawls/{job.request_id}"
        return job

    @app.post("/v1/crawls/validate", dependencies=[Depends(authenticate)])
    async def validate(request: CrawlRequest) -> dict:
        """Normalize a publisher's payload without enqueueing or opening a browser."""
        return request.model_dump(exclude_unset=True) | {
            "request_id": request.request_id
        }

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

    @app.post(
        "/v1/crawls/{request_id}/retry",
        status_code=202,
        dependencies=[Depends(authenticate)],
    )
    async def retry(request_id: str, payload: RetryRequest) -> CrawlJob:
        try:
            return service.retry(
                request_id,
                **payload.model_dump(exclude={"request_id"}),
                new_id=payload.request_id,
            )
        except RequestConflict as error:
            raise HTTPException(409, str(error)) from error
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
        return FileResponse(path, media_type="application/json")

    return app
