"""REST submission and polling for locally persisted crawl jobs."""

import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi.responses import FileResponse

from company_research.service import (
    CrawlJob,
    CrawlRequest,
    CrawlService,
    RequestConflict,
    ServiceUnavailable,
)
from company_research.service_nats import JetStreamInput


def create_app(
    service: CrawlService,
    *,
    api_token: str | None,
    jetstream: JetStreamInput | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            if jetstream is not None:
                await jetstream.start()
            yield
        finally:
            if jetstream is not None:
                await jetstream.close()
            await service.close()

    app = FastAPI(title="Company Crawl Service", version="1.0", lifespan=lifespan)

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
        if not service.healthy() or (jetstream is not None and not jetstream.healthy()):
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
        if job.state not in {"completed", "failed"} or job.result_file is None:
            raise HTTPException(
                409, "Crawl result is not ready", headers={"Retry-After": "5"}
            )
        path = service.root / job.result_file
        if not path.is_file():
            raise HTTPException(503, "Stored crawl result is unavailable")
        return FileResponse(path, media_type="application/json")

    return app
