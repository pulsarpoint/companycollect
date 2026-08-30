import asyncio
import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status

from config import WebtechServiceSettings
from s3_store import RustfsStore
from scan_coordinator import (
    ScanBusyError,
    ScanCoordinator,
    ScanNotFoundError,
)
from scanner import scan_webtech_candidates
from service_models import ScanPollResponse, ScanRequest, ScanSnapshot


def create_app(
    settings: WebtechServiceSettings | None = None,
    store: RustfsStore | None = None,
    scan_function=scan_webtech_candidates,
) -> FastAPI:
    """Build the Webtech API without reading environment state at import time."""
    resolved_settings = settings or WebtechServiceSettings()
    resolved_store = store or RustfsStore(
        endpoint_url=resolved_settings.s3_endpoint,
        access_key=resolved_settings.s3_access_key.get_secret_value(),
        secret_key=resolved_settings.s3_secret_key.get_secret_value(),
        region_name=resolved_settings.s3_region,
        base_location=resolved_settings.base_location,
    )
    coordinator = ScanCoordinator(
        settings=resolved_settings,
        store=resolved_store,
        scan_function=scan_function,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await asyncio.to_thread(resolved_store.ensure_bucket)
        app.state.coordinator = coordinator
        try:
            yield
        finally:
            await coordinator.shutdown()

    app = FastAPI(
        title="Corpscout Webtech Scanner",
        version="1.0.0",
        lifespan=lifespan,
    )

    def require_api_token(
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        scheme, _, supplied_token = (authorization or "").partition(" ")
        expected_token = resolved_settings.api_token.get_secret_value()
        if scheme.lower() != "bearer" or not hmac.compare_digest(
            supplied_token,
            expected_token,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def request_coordinator(request: Request) -> ScanCoordinator:
        return request.app.state.coordinator

    @app.get("/healthz")
    async def healthz() -> dict[str, object]:
        active_scan_id = coordinator.active_scan_id
        return {
            "status": "ok",
            "active_scan": active_scan_id is not None,
            "active_scan_id": active_scan_id,
        }

    @app.post(
        "/v1/scans",
        response_model=ScanSnapshot,
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_api_token)],
    )
    async def submit_scan(
        scan_request: ScanRequest,
        scan_coordinator: Annotated[ScanCoordinator, Depends(request_coordinator)],
    ) -> ScanSnapshot:
        try:
            return await scan_coordinator.submit(scan_request)
        except ScanBusyError as error:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(error),
            ) from error
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(error),
            ) from error

    @app.get(
        "/v1/scans/{scan_id}",
        response_model=ScanPollResponse,
        dependencies=[Depends(require_api_token)],
    )
    async def poll_scan(
        scan_id: str,
        scan_coordinator: Annotated[ScanCoordinator, Depends(request_coordinator)],
        after_event: Annotated[int, Query(ge=0)] = 0,
        wait_seconds: Annotated[float, Query(ge=0, le=30)] = 30,
    ) -> ScanPollResponse:
        try:
            return await scan_coordinator.poll(
                scan_id,
                after_event=after_event,
                wait_seconds=wait_seconds,
            )
        except ScanNotFoundError as error:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Unknown scan ID; resubmit the idempotent scan request",
            ) from error

    @app.post(
        "/v1/scans/{scan_id}/cancel",
        response_model=ScanSnapshot,
        dependencies=[Depends(require_api_token)],
    )
    async def cancel_scan(
        scan_id: str,
        scan_coordinator: Annotated[ScanCoordinator, Depends(request_coordinator)],
    ) -> ScanSnapshot:
        try:
            return await scan_coordinator.cancel(scan_id)
        except ScanNotFoundError as error:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Unknown scan ID",
            ) from error

    return app
