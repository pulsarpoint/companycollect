"""HTTP browser requests bound to existing profiles by opaque session ID."""

import asyncio
import base64
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Path
from playwright.async_api import Error
from pydantic import Field, HttpUrl

from browser_service.browser_multiplexer import (
    BrowserMultiplexer,
    BrowserReservationError,
)
from browser_service.capture import StrictModel, public_response_headers


class ReserveBrowserRequest(StrictModel):
    requestId: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    domain: str = Field(min_length=1, max_length=253)
    id: str = Field(pattern=r"^[a-f0-9]{32}$")


class BrowserSessionReference(StrictModel):
    id: str = Field(pattern=r"^[a-f0-9]{32}$")


class BrowserExtractRequest(StrictModel):
    session: BrowserSessionReference
    browserId: str | None = Field(default=None, pattern=r"^browser-[1-9][0-9]*$")
    tab: str = Field(default="site", pattern=r"^[A-Za-z0-9_-]{1,64}$")
    url: HttpUrl | None = None
    browserHtml: bool = True
    screenshot: bool = False
    timeoutSeconds: float = Field(default=60, gt=0, le=300)
    checkRobotsTxt: bool = True


class BrowserTabRequest(StrictModel):
    action: Literal["open", "focus", "close", "recover"]
    url: HttpUrl | None = None
    reopenClosedTab: bool = False


SessionId = Annotated[str, Path(pattern=r"^[a-f0-9]{32}$")]
TabName = Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,64}$")]


def browser_router(multiplexer: BrowserMultiplexer) -> APIRouter:
    router = APIRouter()

    @router.post("/sessions", status_code=201)
    async def reserve(payload: ReserveBrowserRequest) -> dict:
        try:
            return multiplexer.reserve(
                identifier=payload.id,
                request_id=payload.requestId,
                domain=payload.domain,
            ).snapshot()
        except BrowserReservationError as error:
            raise HTTPException(error.status, str(error)) from error

    @router.get("/sessions/{identifier}")
    async def status(identifier: SessionId) -> dict:
        try:
            return multiplexer.get(identifier).snapshot()
        except BrowserReservationError as error:
            raise HTTPException(error.status, str(error)) from error

    @router.post("/sessions/{identifier}/heartbeat")
    async def heartbeat(identifier: SessionId) -> dict:
        try:
            return multiplexer.touch(identifier).snapshot()
        except BrowserReservationError as error:
            raise HTTPException(error.status, str(error)) from error

    @router.delete("/sessions/{identifier}")
    async def release(identifier: SessionId) -> dict:
        await multiplexer.release(identifier)
        return {"id": identifier, "state": "released"}

    @router.post("/sessions/{identifier}/tabs/{name}")
    async def tab_action(
        identifier: SessionId, name: TabName, payload: BrowserTabRequest
    ) -> dict:
        if payload.action == "recover" and payload.url is None:
            raise HTTPException(422, "Recovery requires the pending page URL")
        if payload.url is not None and (payload.url.username or payload.url.password):
            raise HTTPException(422, "Enter credentials in the browser, not the URL")
        try:
            reservation = multiplexer.ready(identifier)
            multiplexer.touch(identifier)
            async with asyncio.timeout(310), reservation.lock:
                multiplexer.ready(identifier)
                multiplexer.touch(identifier)
                if payload.action == "recover":
                    recovery = await multiplexer.recover_tab(
                        reservation,
                        name,
                        str(payload.url),
                        reopen_closed_tab=payload.reopenClosedTab,
                    )
                    return reservation.snapshot() | {
                        "recovered": recovery == "restored",
                        "tabAvailable": recovery != "tab_closed",
                    }
                if payload.action == "open":
                    await multiplexer.open_tab(reservation, name)
                elif payload.action == "focus":
                    await multiplexer.tab(reservation, name).page.bring_to_front()
                else:
                    tab = reservation.tabs.pop(name, None)
                    if tab is not None and not tab.page.is_closed():
                        await tab.page.close()
                return reservation.snapshot()
        except BrowserReservationError as error:
            raise HTTPException(error.status, str(error)) from error
        except TimeoutError as error:
            raise HTTPException(504, "Browser operation timed out") from error
        except Error as error:
            raise HTTPException(
                409, "Browser tab is unavailable; reopen it in the same session"
            ) from error

    @router.post("/extract")
    async def extract(payload: BrowserExtractRequest) -> dict:
        if payload.url is not None and (payload.url.username or payload.url.password):
            raise HTTPException(422, "Enter credentials in the browser, not the URL")
        try:
            async with asyncio.timeout(310):
                reservation = await multiplexer.for_request(
                    payload.session.id,
                    domain=payload.url.host if payload.url is not None else None,
                    browser_id=payload.browserId,
                )
                async with reservation.lock:
                    multiplexer.ready(payload.session.id)
                    multiplexer.touch(payload.session.id)
                    # First navigation may open a tab, but cannot start another browser.
                    if payload.tab not in reservation.tabs and payload.url is not None:
                        profile = reservation.profile
                        if profile is None or profile.state != "running":
                            raise BrowserReservationError(
                                409, "Assigned browser is unavailable"
                            )
                        await multiplexer.open_tab(reservation, payload.tab)
                    tab = multiplexer.tab(reservation, payload.tab)
                    async with asyncio.timeout(payload.timeoutSeconds + 10):
                        capture = (
                            await tab.navigate(
                                str(payload.url),
                                timeout_seconds=payload.timeoutSeconds,
                                check_robots_txt=payload.checkRobotsTxt,
                            )
                            if payload.url is not None
                            else await tab.capture()
                        )
                        result = {
                            "session": reservation.snapshot(),
                            "url": capture.url,
                            "statusCode": capture.status_code,
                            "headers": public_response_headers(capture.headers),
                            "error": capture.error,
                        }
                        if payload.browserHtml:
                            result["browserHtml"] = capture.html
                        if payload.screenshot:
                            result["screenshot"] = base64.b64encode(
                                await tab.page.screenshot(timeout=5000)
                            ).decode("ascii")
                        multiplexer.touch(reservation.id)
                        return result
        except BrowserReservationError as error:
            raise HTTPException(error.status, str(error)) from error
        except TimeoutError as error:
            raise HTTPException(504, "Browser request timed out") from error
        except Error as error:
            raise HTTPException(
                409, "Browser tab is unavailable; reopen it in the same session"
            ) from error

    return router
