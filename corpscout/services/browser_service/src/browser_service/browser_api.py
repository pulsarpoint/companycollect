"""HTTP browser requests bound to existing profiles by opaque session ID."""

import asyncio
import base64
import logging
from typing import Annotated, Literal
from uuid import uuid4

import httpx
from fastapi import APIRouter, Header, HTTPException, Path
from playwright.async_api import Error
from pydantic import Field, HttpUrl

from browser_service.capture import StrictModel, public_response_headers
from browser_service.challenge_agent import ChallengeAgent
from browser_service.runtime import BrowserService
from browser_service.session_store import BrowserSessionError


class ReserveBrowserRequest(StrictModel):
    requestId: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    domain: str = Field(min_length=1, max_length=253)
    id: str = Field(default_factory=lambda: uuid4().hex, pattern=r"^[a-f0-9]{32}$")
    route: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    headless: bool | None = Field(default=None, strict=True)


class BrowserSessionReference(StrictModel):
    id: str = Field(default_factory=lambda: uuid4().hex, pattern=r"^[a-f0-9]{32}$")
    executionId: str | None = Field(default=None, pattern=r"^[a-f0-9]{32}$")


class BrowserExtractRequest(StrictModel):
    session: BrowserSessionReference = Field(default_factory=BrowserSessionReference)
    headless: bool | None = Field(default=None, strict=True)
    route: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9_-]{1,64}$")
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


class ChallengeAgentRequest(StrictModel):
    confirm: bool = Field(strict=True)
    expectedUrl: HttpUrl
    expectedGeneration: str = Field(min_length=1, max_length=64)
    maxSteps: int = Field(default=12, ge=1, le=20, strict=True)
    timeoutSeconds: int = Field(default=120, ge=1, le=180, strict=True)
    model: Literal["deepseek-flash", "z-ai/glm-5.3-flash"] = "deepseek-flash"


SessionId = Annotated[str, Path(pattern=r"^[a-f0-9]{32}$")]
TabName = Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,64}$")]


def browser_router(
    service: BrowserService,
    *,
    deepseek_api_key: str | None,
    openrouter_api_key: str | None = None,
) -> APIRouter:
    router = APIRouter()

    @router.post("/sessions/{identifier}/tabs/{name}/challenge-agent")
    async def challenge_agent(
        identifier: SessionId, name: TabName, payload: ChallengeAgentRequest
    ) -> dict:
        if not payload.confirm:
            raise HTTPException(422, "Explicit approval is required for this agent run")
        is_deepseek = payload.model == "deepseek-flash"
        api_key = deepseek_api_key if is_deepseek else openrouter_api_key
        if not api_key:
            raise HTTPException(
                503,
                f"Configure {'DEEPSEEK' if is_deepseek else 'OPENROUTER_API_KEY'} on the browser service",
            )
        try:
            session = service.get(identifier)
            if session.lock.locked():
                raise HTTPException(
                    409, "Browser has an operation in progress; try again"
                )
            async with session.lock:
                service.get(identifier)
                page = service.tab(session, name).page
                if (
                    session.profile.generation != payload.expectedGeneration
                    or page.url != str(payload.expectedUrl)
                ):
                    raise HTTPException(409, "Browser or page changed; select it again")
                agent = ChallengeAgent(
                    service,
                    session,
                    name,
                    max_steps=payload.maxSteps,
                    timeout_seconds=payload.timeoutSeconds,
                    model=payload.model,
                )
                async with httpx.AsyncClient(
                    base_url="https://api.deepseek.com/"
                    if is_deepseek
                    else "https://openrouter.ai/api/v1/",
                    timeout=45,
                    headers={"Authorization": f"Bearer {api_key}"},
                ) as http:
                    return await agent.run(http)
        except BrowserSessionError as error:
            raise HTTPException(error.status, str(error)) from error
        except OSError as error:
            logging.getLogger(__name__).warning(
                "Cannot save challenge agent evidence (%s)", type(error).__name__
            )
            raise HTTPException(503, "Cannot save agent evidence") from error

    @router.post("/sessions", status_code=201)
    async def reserve(payload: ReserveBrowserRequest) -> dict:
        try:
            async with asyncio.timeout(310):
                await service.claim(
                    identifier=payload.id,
                    request_id=payload.requestId,
                    domain=payload.domain,
                    headless=payload.headless,
                    route=payload.route,
                )
            return service.snapshot(payload.id)
        except BrowserSessionError as error:
            raise HTTPException(
                error.status,
                {"message": str(error), "sessionId": payload.id},
                headers={"Retry-After": "1"} if error.status == 503 else None,
            ) from error

        except TimeoutError as error:
            raise HTTPException(
                504, {"message": "Browser claim timed out", "sessionId": payload.id}
            ) from error
        except Exception as error:
            logging.getLogger(__name__).warning(
                "Browser startup failed (%s)", type(error).__name__
            )
            raise HTTPException(
                500, {"message": "Browser could not start", "sessionId": payload.id}
            ) from error

    @router.get("/sessions/{identifier}")
    async def status(identifier: SessionId) -> dict:
        try:
            return service.snapshot(identifier)
        except BrowserSessionError as error:
            raise HTTPException(
                error.status,
                str(error),
                headers={"Retry-After": "1"} if error.status == 503 else None,
            ) from error

    @router.post("/sessions/{identifier}/heartbeat")
    async def heartbeat(
        identifier: SessionId, x_browser_execution_id: str | None = Header(default=None)
    ) -> dict:
        try:
            if x_browser_execution_id is None:
                raise BrowserSessionError(
                    409, "Send X-Browser-Execution-Id for this operation"
                )
            service.touch(identifier, execution_id=x_browser_execution_id)
            return service.snapshot(identifier)
        except BrowserSessionError as error:
            raise HTTPException(
                error.status,
                str(error),
                headers={"Retry-After": "1"} if error.status == 503 else None,
            ) from error

    @router.delete("/sessions/{identifier}")
    async def release(
        identifier: SessionId, x_browser_execution_id: str | None = Header(default=None)
    ) -> dict:
        try:
            if identifier in service.active and x_browser_execution_id is None:
                raise BrowserSessionError(
                    409, "Send X-Browser-Execution-Id before closing the browser"
                )
            await service.release(identifier, execution_id=x_browser_execution_id)
            return service.snapshot(identifier)
        except BrowserSessionError as error:
            raise HTTPException(error.status, str(error)) from error

    @router.post("/sessions/{identifier}/tabs/{name}")
    async def tab_action(
        identifier: SessionId,
        name: TabName,
        payload: BrowserTabRequest,
        x_browser_execution_id: str | None = Header(default=None),
    ) -> dict:
        if payload.action == "recover" and payload.url is None:
            raise HTTPException(422, "Recovery requires the pending page URL")
        if payload.url is not None and (payload.url.username or payload.url.password):
            raise HTTPException(422, "Enter credentials in the browser, not the URL")
        try:
            if x_browser_execution_id is None:
                raise BrowserSessionError(
                    409, "Send X-Browser-Execution-Id for this operation"
                )
            session = service.get(identifier, execution_id=x_browser_execution_id)
            service.touch(identifier)
            async with asyncio.timeout(310), session.lock:
                service.get(identifier, execution_id=session.execution_id)
                service.touch(identifier)
                if payload.action == "recover":
                    recovery = await service.recover_tab(
                        session,
                        name,
                        str(payload.url),
                        reopen_closed_tab=payload.reopenClosedTab,
                    )
                    return service.snapshot(session.id) | {
                        "recovered": recovery == "restored",
                        "tabAvailable": recovery != "tab_closed",
                    }
                if payload.action == "open":
                    await service.open_tab(session, name)
                elif payload.action == "focus":
                    await service.tab(session, name).page.bring_to_front()
                else:
                    tab = session.tabs.pop(name, None)
                    if tab is not None and not tab.page.is_closed():
                        await tab.page.close()
                return service.snapshot(session.id)
        except BrowserSessionError as error:
            raise HTTPException(
                error.status,
                str(error),
                headers={"Retry-After": "1"} if error.status == 503 else None,
            ) from error
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
                session = await service.for_request(
                    payload.session.id,
                    domain=payload.url.host if payload.url is not None else None,
                    headless=payload.headless,
                    route=payload.route,
                    execution_id=payload.session.executionId,
                )
                async with session.lock:
                    service.get(payload.session.id, execution_id=session.execution_id)
                    service.touch(payload.session.id)
                    # First navigation may open a tab, but cannot start another browser.
                    if payload.tab not in session.tabs and payload.url is not None:
                        profile = session.profile
                        if profile is None or profile.state != "running":
                            raise BrowserSessionError(
                                409, "Assigned browser is unavailable"
                            )
                        await service.open_tab(session, payload.tab)
                    tab = service.tab(session, payload.tab)
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
                            "session": service.snapshot(session.id),
                            "url": capture.url,
                            "statusCode": capture.status_code,
                            "headers": public_response_headers(capture.headers),
                            "error": capture.error,
                            "redirects": capture.redirects,
                            "navigationAttempts": capture.navigation_attempts,
                        }
                        if payload.browserHtml:
                            result["browserHtml"] = capture.html
                        if payload.screenshot:
                            result["screenshot"] = base64.b64encode(
                                await tab.page.screenshot(timeout=5000)
                            ).decode("ascii")
                        service.touch(session.id)
                        return result
        except BrowserSessionError as error:
            raise HTTPException(
                error.status,
                {"message": str(error), "sessionId": payload.session.id},
                headers={"Retry-After": "1"} if error.status == 503 else None,
            ) from error
        except TimeoutError as error:
            raise HTTPException(
                504,
                {
                    "message": "Browser request timed out",
                    "sessionId": payload.session.id,
                },
            ) from error
        except Error as error:
            raise HTTPException(
                409,
                {
                    "message": "Browser tab is unavailable; reopen it in the same session",
                    "sessionId": payload.session.id,
                },
            ) from error

        except Exception as error:
            logging.getLogger(__name__).warning(
                "Browser request failed (%s)", type(error).__name__
            )
            raise HTTPException(
                500,
                {"message": "Browser request failed", "sessionId": payload.session.id},
            ) from error

    return router
