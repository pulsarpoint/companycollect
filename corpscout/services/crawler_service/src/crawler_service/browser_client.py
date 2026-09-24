"""Crawler-side HTTP client; no browser launch, profile, or Playwright access."""

import asyncio
import base64
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from crawler_service.browser import BrowserUnavailable, PageCapture, interpret_capture


class BrowserLeaseClient:
    def __init__(
        self, http: httpx.AsyncClient, *, on_ready: Callable[[dict], None] | None = None
    ):
        self.http = http
        self.on_ready = on_ready
        self.id: str | None = None
        self.execution_id: str | None = None
        self.profile_id: str | None = None
        self.generation: str | None = None
        self.heartbeat: asyncio.Task | None = None

    async def request(
        self, method: str, path: str, payload: dict | None = None
    ) -> dict:
        response = await self.http.request(
            method,
            "/v1/browser" + path,
            json=payload,
            headers={"X-Browser-Execution-Id": self.execution_id}
            if self.execution_id is not None
            else {},
        )
        if response.status_code == 410:
            raise RuntimeError(
                "Browser session expired or was interrupted; retry the crawl"
            )
        if response.status_code == 409:
            raise BrowserUnavailable(
                "Assigned browser is unavailable; retry in the same session"
            )
        response.raise_for_status()
        document = response.json()
        session = document.get("session", document)
        if session.get("state") == "ready":
            self.execution_id = session.get("executionId")
            self.profile_id, self.generation = (
                session["profileId"],
                session["generation"],
            )
            if self.on_ready is not None:
                self.on_ready(session)
        return document

    @asynccontextmanager
    async def lease(
        self,
        *,
        identifier: str,
        request_id: str,
        domain: str,
        headless: bool | None = None,
    ) -> AsyncIterator["BrowserLeaseClient"]:
        self.id = identifier
        self.execution_id = None
        reserved = False
        try:
            while True:
                try:
                    await self.request(
                        "POST",
                        "/sessions",
                        {"id": identifier, "requestId": request_id, "domain": domain}
                        | ({"headless": headless} if headless is not None else {}),
                    )
                    break
                except httpx.HTTPStatusError as error:
                    if error.response.status_code != 503:
                        raise
                    await asyncio.sleep(1)
            reserved = True
            async with asyncio.TaskGroup() as tasks:
                self.heartbeat = tasks.create_task(self.keep_alive())
                try:
                    yield self
                finally:
                    self.heartbeat.cancel()
        except BaseExceptionGroup as error:
            if len(error.exceptions) == 1:
                raise error.exceptions[0] from None
            raise
        finally:
            if reserved:
                cleanup = asyncio.create_task(self.release())
                cancelled = False
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        cancelled = True
                await cleanup
                self.id = None
                if cancelled:
                    raise asyncio.CancelledError
            else:
                self.id = None

    async def release(self) -> None:
        if self.heartbeat is not None:
            self.heartbeat.cancel()
            await asyncio.gather(self.heartbeat, return_exceptions=True)
        try:
            async with asyncio.timeout(15):
                await self.request("DELETE", f"/sessions/{self.id}")
        except (httpx.HTTPError, TimeoutError, RuntimeError):
            # The service expires the lease even if the crawler cannot reach it.
            logging.getLogger(__name__).warning(
                "Could not release browser session; idle timeout will reclaim it"
            )

    async def keep_alive(self) -> None:
        while True:
            await asyncio.sleep(20)
            await self.request("POST", f"/sessions/{self.id}/heartbeat")

    async def open_tab(self, name: str) -> "BrowserPageClient":
        if self.id is None:
            raise RuntimeError("Reserve a browser session before opening a tab")
        await self.request(
            "POST", f"/sessions/{self.id}/tabs/{name}", {"action": "open"}
        )
        return BrowserPageClient(self, name)


class BrowserPageClient:
    def __init__(self, lease: BrowserLeaseClient, name: str):
        self.lease, self.name = lease, name
        self.closed = False
        self.available = True
        self.generation = lease.generation

    async def extract(self, **options) -> dict:
        return await self.lease.request(
            "POST",
            "/extract",
            {
                "session": {
                    "id": self.lease.id,
                    "executionId": self.lease.execution_id,
                },
                "tab": self.name,
            }
            | options,
        )

    def capture_result(self, document: dict) -> PageCapture:
        capture = interpret_capture(
            url=document["url"],
            html=document["browserHtml"],
            status_code=document["statusCode"],
            headers=document["headers"],
            error=document["error"],
        )
        capture.redirects = document.get("redirects", [])
        capture.navigation_attempts = document.get("navigationAttempts", [])
        return capture

    async def navigate(
        self, url: str, *, timeout_seconds: float, check_robots_txt: bool
    ) -> PageCapture:
        return self.capture_result(
            await self.extract(
                url=url, timeoutSeconds=timeout_seconds, checkRobotsTxt=check_robots_txt
            )
        )

    async def capture(self) -> PageCapture:
        return self.capture_result(await self.extract())

    async def run_challenge_agent(
        self, url: str, *, model: str = "deepseek-flash"
    ) -> dict:
        observed = await self.extract(browserHtml=False)
        if (urlsplit(observed["url"]).hostname or "").removeprefix("www.") != (
            urlsplit(url).hostname or ""
        ).removeprefix("www."):
            raise BrowserUnavailable("Challenge tab moved to another website")
        return await self.lease.request(
            "POST",
            f"/sessions/{self.lease.id}/tabs/{self.name}/challenge-agent",
            {
                "confirm": True,
                "model": model,
                "expectedUrl": observed["url"],
                "expectedGeneration": observed["session"]["generation"],
            },
        )

    async def screenshot(self, path: Path) -> None:
        document = await self.extract(browserHtml=False, screenshot=True)
        path.write_bytes(base64.b64decode(document["screenshot"]))

    async def focus(self) -> None:
        await self.lease.request(
            "POST", f"/sessions/{self.lease.id}/tabs/{self.name}", {"action": "focus"}
        )

    async def recover(self, url: str, *, reopen_closed_tab: bool = False) -> bool:
        document = await self.lease.request(
            "POST",
            f"/sessions/{self.lease.id}/tabs/{self.name}",
            {
                "action": "recover",
                "url": url,
                "reopenClosedTab": reopen_closed_tab,
            },
        )
        self.generation = self.lease.generation
        self.available = document["tabAvailable"]
        return document["recovered"]

    async def close(self) -> None:
        if not self.closed:
            await self.lease.request(
                "POST",
                f"/sessions/{self.lease.id}/tabs/{self.name}",
                {"action": "close"},
            )
            self.closed = True
