"""Browser management, extraction and authenticated remote desktop access."""

import asyncio
import hmac
import secrets
import socket
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Annotated

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)
from pydantic import BaseModel, HttpUrl, StrictBool

from browser_service.browser_api import browser_router
from browser_service.browser_multiplexer import BrowserReservationError
from browser_service.browser_sessions import PersistentBrowserSession
from browser_service.runtime import BrowserService
from browser_service.virtual_desktop import ACTIVE_DESKTOPS


class OpenTabRequest(BaseModel):
    url: HttpUrl


class BrowserSettingsRequest(BaseModel):
    auto_restart: StrictBool


def create_app(service: BrowserService, *, api_token: str | None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            yield
        finally:
            await service.close()

    app = FastAPI(title="Browser Service", version="0.1.0", lifespan=lifespan)
    tickets: dict[str, tuple[str, str, float]] = {}

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

    app.include_router(
        browser_router(service.multiplexer),
        prefix="/v1/browser",
        dependencies=[Depends(authenticate)],
    )

    @app.get("/healthz")
    async def health() -> dict:
        if not service.multiplexer.accepting:
            raise HTTPException(503, "Browser service is not ready")
        return {"status": "ok"}

    def saved_browser(session_id: str) -> PersistentBrowserSession:
        session = service.pool.sessions.get(session_id)
        if session is None:
            raise HTTPException(404, "Unknown browser session")
        return session

    @app.get("/v1/server", dependencies=[Depends(authenticate)])
    async def server_status() -> dict:
        sessions = []
        for desktop in list(ACTIVE_DESKTOPS.values()):
            profile = service.pool.sessions.get(desktop.owner or "")
            if profile is None:
                continue
            snapshot = await profile.snapshot()
            sessions.append(
                {
                    "id": desktop.id,
                    "kind": "saved",
                    "name": profile.id,
                    "request_id": profile.request_id,
                    "url": snapshot["tabs"][-1]["url"] if snapshot["tabs"] else None,
                    "started_at": desktop.started_at,
                    "state": "recycling"
                    if profile.recycling
                    else "in_use"
                    if profile.request_id
                    else profile.state,
                }
            )
        return {
            "hostname": socket.gethostname(),
            "version": version("corpscout-browser-service"),
            "healthy": service.multiplexer.accepting,
            "sessions": sessions,
            "idle_timeout_seconds": service.multiplexer.idle_timeout,
            "leases": service.store.recent(),
        }

    @app.post(
        "/v1/desktops/{desktop_id}/browser-ticket", dependencies=[Depends(authenticate)]
    )
    async def desktop_ticket(desktop_id: str) -> dict:
        if desktop_id not in ACTIVE_DESKTOPS:
            raise HTTPException(404, "This browser desktop is no longer open")
        for key, (_, _, expires) in list(tickets.items()):
            if expires < time.monotonic():
                del tickets[key]
        token = secrets.token_urlsafe(32)
        tickets[token] = (f"desktop:{desktop_id}", desktop_id, time.monotonic() + 30)
        return {
            "websocket_path": f"/v1/desktops/{desktop_id}/browser?ticket={token}",
            "expires_in": 30,
        }

    @app.websocket("/v1/desktops/{desktop_id}/browser")
    async def desktop_socket(
        websocket: WebSocket, desktop_id: str, ticket: str = ""
    ) -> None:
        grant = tickets.pop(ticket, None)
        desktop = ACTIVE_DESKTOPS.get(desktop_id)
        if (
            grant is None
            or desktop is None
            or grant[0] != f"desktop:{desktop_id}"
            or grant[1] != desktop_id
            or grant[2] < time.monotonic()
        ):
            await websocket.close(code=1008)
            return
        await bridge_browser(
            websocket,
            desktop.vnc_port,
            lambda: ACTIVE_DESKTOPS.get(desktop_id) is desktop,
        )

    @app.get("/v1/browser-sessions", dependencies=[Depends(authenticate)])
    async def browser_sessions() -> dict:
        return {
            "sessions": [
                await session.snapshot() for session in service.pool.sessions.values()
            ]
        }

    @app.post(
        "/v1/browser-sessions/{session_id}/start", dependencies=[Depends(authenticate)]
    )
    async def start_browser(session_id: str) -> dict:
        session = saved_browser(session_id)
        try:
            await session.start(manual=True)
        except ValueError as error:
            raise HTTPException(409, str(error)) from error
        except Exception as error:
            raise HTTPException(503, "Browser could not start") from error
        return await session.snapshot()

    @app.post(
        "/v1/browser-sessions/{session_id}/settings",
        dependencies=[Depends(authenticate)],
    )
    async def browser_settings(
        session_id: str, payload: BrowserSettingsRequest
    ) -> dict:
        session = saved_browser(session_id)
        await session.set_auto_restart(payload.auto_restart)
        return await session.snapshot()

    @app.post(
        "/v1/browser-sessions/{session_id}/stop", dependencies=[Depends(authenticate)]
    )
    async def stop_browser(session_id: str) -> dict:
        session = saved_browser(session_id)
        try:
            await session.stop(manual=True)
        except ValueError as error:
            raise HTTPException(409, str(error)) from error
        return await session.snapshot()

    @app.post(
        "/v1/browser-sessions/{session_id}/tabs", dependencies=[Depends(authenticate)]
    )
    async def open_browser_tab(session_id: str, payload: OpenTabRequest) -> dict:
        session = saved_browser(session_id)
        if payload.url.username or payload.url.password:
            raise HTTPException(422, "Enter credentials in the browser, not the URL")
        try:
            identifier = await session.open_tab(str(payload.url))
        except ValueError as error:
            raise HTTPException(409, str(error)) from error
        return {"tab_id": identifier, "session": await session.snapshot()}

    @app.post(
        "/v1/browser-sessions/{session_id}/tabs/{tab_id}/focus",
        dependencies=[Depends(authenticate)],
    )
    async def focus_browser_tab(session_id: str, tab_id: str) -> dict:
        session = saved_browser(session_id)
        tab = session.tabs.get(tab_id)
        if tab is None or tab.page.is_closed():
            raise HTTPException(404, "Unknown browser tab")
        await tab.page.bring_to_front()
        return {"state": "focused"}

    @app.get(
        "/v1/browser-sessions/{session_id}/tabs/{tab_id}/inspect",
        dependencies=[Depends(authenticate)],
    )
    async def inspect_browser_tab(session_id: str, tab_id: str) -> dict:
        try:
            return await saved_browser(session_id).inspect_tab(tab_id)
        except ValueError as error:
            raise HTTPException(404, str(error)) from error

    @app.post(
        "/v1/browser-sessions/{session_id}/browser-ticket",
        dependencies=[Depends(authenticate)],
    )
    async def saved_browser_ticket(session_id: str) -> dict:
        session = saved_browser(session_id)
        if (
            session.state != "running"
            or session.desktop is None
            or session.generation is None
        ):
            raise HTTPException(409, "Browser session is not running")
        for key, (_, _, expires) in list(tickets.items()):
            if expires < time.monotonic():
                del tickets[key]
        token = secrets.token_urlsafe(32)
        tickets[token] = (
            f"saved:{session_id}",
            session.generation,
            time.monotonic() + 30,
        )
        return {
            "websocket_path": f"/v1/browser-sessions/{session_id}/browser?ticket={token}",
            "expires_in": 30,
        }

    @app.websocket("/v1/browser-sessions/{session_id}/browser")
    async def saved_browser_socket(
        websocket: WebSocket, session_id: str, ticket: str = ""
    ) -> None:
        grant = tickets.pop(ticket, None)
        session = service.pool.sessions.get(session_id)
        if (
            grant is None
            or session is None
            or session.desktop is None
            or grant[0] != f"saved:{session_id}"
            or grant[1] != session.generation
            or grant[2] < time.monotonic()
            or session.state != "running"
        ):
            await websocket.close(code=1008)
            return
        await bridge_browser(
            websocket,
            session.desktop.vnc_port,
            lambda: session.state == "running" and session.generation == grant[1],
        )

    @app.post(
        "/v1/browser/sessions/{identifier}/browser-ticket",
        dependencies=[Depends(authenticate)],
    )
    async def lease_ticket(identifier: str) -> dict:
        try:
            reservation = service.multiplexer.ready(identifier)
        except BrowserReservationError as error:
            raise HTTPException(error.status, str(error)) from error
        profile = reservation.profile
        if profile is None or profile.desktop is None or profile.generation is None:
            raise HTTPException(409, "Assigned browser is unavailable")
        for key, (_, _, expires) in list(tickets.items()):
            if expires < time.monotonic():
                del tickets[key]
        token = secrets.token_urlsafe(32)
        tickets[token] = (identifier, profile.generation, time.monotonic() + 30)
        return {
            "websocket_path": f"/v1/browser/sessions/{identifier}/browser?ticket={token}",
            "expires_in": 30,
        }

    @app.websocket("/v1/browser/sessions/{identifier}/browser")
    async def lease_socket(
        websocket: WebSocket, identifier: str, ticket: str = ""
    ) -> None:
        grant = tickets.pop(ticket, None)
        reservation = service.multiplexer.reservations.get(identifier)
        profile = reservation.profile if reservation else None
        if (
            grant is None
            or reservation is None
            or reservation.state != "ready"
            or profile is None
            or profile.desktop is None
            or grant[0] != identifier
            or grant[1] != profile.generation
            or grant[2] < time.monotonic()
        ):
            await websocket.close(code=1008)
            return
        await bridge_browser(
            websocket,
            profile.desktop.vnc_port,
            lambda: reservation.state == "ready" and profile.generation == grant[1],
        )

    async def bridge_browser(websocket, port, active) -> None:
        await websocket.accept(
            subprotocol="binary"
            if "binary" in websocket.headers.get("sec-websocket-protocol", "")
            else None
        )
        writer = None
        tasks = []
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)

            async def receive() -> None:
                assert writer is not None
                while active():
                    data = await websocket.receive_bytes()
                    if not active() or len(data) > 1024 * 1024:
                        return
                    writer.write(data)
                    await writer.drain()

            async def send() -> None:
                while active():
                    data = await reader.read(65536)
                    if not data:
                        return
                    await websocket.send_bytes(data)

            async def monitor() -> None:
                while active():
                    await asyncio.sleep(0.2)

            tasks = [
                asyncio.create_task(receive()),
                asyncio.create_task(send()),
                asyncio.create_task(monitor()),
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        except (WebSocketDisconnect, ConnectionError, OSError):
            pass
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            if websocket.client_state.name != "DISCONNECTED":
                await websocket.close()

    return app
