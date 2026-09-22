"""Brave Ask execution in a leased browser, including bounded CAPTCHA assistance."""

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Literal
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
from fastapi import APIRouter, HTTPException
from playwright.async_api import Error
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from pydantic import Field, field_validator

from browser_service.browser import BrowserSession
from browser_service.capture import StrictModel
from browser_service.challenge_agent import ChallengeAgent
from browser_service.runtime import ActiveBrowserSession, BrowserService
from browser_service.session_store import BrowserSessionError

LOGGER = logging.getLogger(__name__)
BRAVE_ORIGIN = "https://search.brave.com"
COPY_CAPTURE_SCRIPT = """(() => {
    window.__companyBraveCopiedText = null;
    Object.defineProperty(navigator.clipboard, 'writeText', {
        configurable: true,
        value: async (text) => { window.__companyBraveCopiedText = String(text); }
    });
})();"""


class BraveAskRequest(StrictModel):
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    session_id: str | None = Field(default=None, pattern=r"^[a-f0-9]{32}$")
    query: str = Field(min_length=1, max_length=10000)
    route: Literal["direct", "crawl_proxy1", "crawl_proxy2", "crawl_proxy3"] = "direct"
    headless: bool | None = Field(default=None, strict=True)
    page_timeout_seconds: float = Field(default=60, gt=0, le=300)
    answer_timeout_seconds: float = Field(default=180, gt=0, le=600)
    timeout_seconds: float = Field(default=900, gt=0, le=1800)
    challenge_agent_max_runs: int = Field(default=3, ge=0, le=1000, strict=True)
    challenge_agent_model: Literal["deepseek-flash", "z-ai/glm-5.3-flash"] = (
        "deepseek-flash"
    )

    @field_validator("query")
    @classmethod
    def nonempty_query(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Query must contain text")
        return value


class BraveStepError(Exception):
    def __init__(self, stage: str, error_type: str):
        self.stage, self.error_type = stage, error_type
        super().__init__(f"Brave {stage} failed ({error_type})")


def write_result(path: Path, result: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, indent=2), encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


class BraveAsk:
    def __init__(
        self,
        service: BrowserService,
        session: ActiveBrowserSession,
        request: BraveAskRequest,
        directory: Path,
        api_key: str | None,
    ):
        self.service, self.session, self.request = service, session, request
        self.directory, self.api_key = directory, api_key
        self.runs: list[dict] = []
        self.stage = "page_setup"
        self.tab: BrowserSession | None = None

    def progress(self, stage: str) -> None:
        self.stage = stage
        self.session.operation = {
            "kind": "brave",
            "request_id": self.request.request_id,
            "stage": stage,
            "agent_runs": len(self.runs),
        }
        self.service.touch(self.session.id)

    async def access_problem(self) -> str | None:
        assert self.tab is not None
        page = self.tab.page
        parsed = urlsplit(page.url)
        if parsed.hostname != "search.brave.com":
            return "blocked"
        if parsed.path.rstrip("/") in {"/captcha", "/challenge", "/sorry"}:
            return "captcha"
        # Ask can show this verification dialog over an answer error, with its
        # footer controls still visible underneath.
        if await page.get_by_role(
            "button", name="I'm not a robot", exact=True
        ).is_visible():
            return "captcha"
        # Brave's proof-of-work challenge stays on /ask and returns HTTP 429.
        # It has no hCaptcha widget or "verify you are human" text.
        if await page.get_by_role(
            "button", name="Switch to traditional CAPTCHA", exact=True
        ).is_visible():
            return "captcha"
        if (
            await page.get_by_role(
                "heading", name="Verifying you're not a bot", exact=True
            ).is_visible()
            and await page.get_by_role("button", name="Verify", exact=True).is_visible()
        ):
            return "captcha"
        # Inspect visible challenge controls, not words inside generated answers.
        markers = page.locator(
            '.h-captcha, .g-recaptcha, #captcha, form[action*="captcha"], iframe[src*="hcaptcha.com"], iframe[src*="recaptcha"], iframe[src*="challenges.cloudflare.com"]'
        )
        for marker in await markers.all():
            if await marker.is_visible():
                return "captcha"
        if not await page.locator('button[aria-label="Try again"]').is_visible():
            # A query about CAPTCHA can itself appear in the title of an Ask page.
            title = (await page.title()).casefold().strip()
            if title.startswith(
                ("just a moment", "attention required", "security check")
            ):
                return "captcha"
            # Brave's challenge page can show plain text without a widget yet.
            for text in (
                "Verify you are human",
                "Confirm you are human",
                "Prove you are human",
                "Are you a robot?",
            ):
                for marker in await page.get_by_text(text, exact=True).all():
                    if await marker.is_visible():
                        return "captcha"
        else:
            # Some challenges replace their body without a new document response.
            return None
        return "blocked" if self.tab.document_status in {401, 403, 429} else None

    async def solve_challenge(self) -> None:
        assert self.tab is not None
        problem = await self.access_problem()
        if problem == "blocked":
            # A rejected document may render its verification controls after
            # DOMContentLoaded. Give that UI time to appear before declaring failure.
            deadline = monotonic() + min(5, self.request.page_timeout_seconds)
            while problem == "blocked" and monotonic() < deadline:
                await asyncio.sleep(min(0.25, max(0, deadline - monotonic())))
                problem = await self.access_problem()
        while problem == "captcha":
            self.progress("captcha")
            if len(self.runs) >= self.request.challenge_agent_max_runs:
                raise BraveStepError("captcha", "AgentBudgetExhausted")
            if not self.api_key:
                raise BraveStepError("captcha", "AgentNotConfigured")
            run_number = len(self.runs) + 1
            (self.directory / f"captcha-{run_number}.html").write_text(
                await self.tab.page.content(), encoding="utf-8"
            )
            await self.tab.page.screenshot(
                path=str(self.directory / f"captcha-{run_number}.png"), timeout=5000
            )
            self.progress("captcha_agent")
            agent = ChallengeAgent(
                self.service,
                self.session,
                "brave",
                max_steps=12,
                timeout_seconds=120,
                model=self.request.challenge_agent_model,
            )
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/"
                if self.request.challenge_agent_model == "deepseek-flash"
                else "https://openrouter.ai/api/v1/",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=60,
            ) as http:
                try:
                    result = await agent.run(http)
                finally:
                    # Preserve attempts interrupted by the overall deadline as well.
                    self.runs.append(agent.result)
                    write_result(
                        self.directory / "challenge-runs.json", {"runs": self.runs}
                    )
            # Model completion is only advisory; inspect the actual page again.
            problem = await self.access_problem()
            if problem == "captcha" and result["state"] in {
                "error",
                "interrupted",
                "cancelled",
            }:
                raise BraveStepError("captcha", "AgentFailed")
        if problem is not None:
            raise BraveStepError(
                "access",
                {401: "Unauthorized", 403: "Forbidden", 429: "RateLimited"}.get(
                    self.tab.document_status, "Blocked"
                ),
            )

    async def save_failure_evidence(self) -> dict:
        if self.tab is None:
            return {}
        evidence = {
            "http_status": self.tab.document_status,
            "retry_after": self.tab.document_headers.get("retry-after"),
        }
        try:
            async with asyncio.timeout(6):
                evidence["title"] = await self.tab.page.title()
                html = self.directory / "failure.html"
                html.write_text(await self.tab.page.content(), encoding="utf-8")
                html.chmod(0o600)
                evidence["html"] = html.name
                screenshot = self.directory / "failure.png"
                await self.tab.page.screenshot(path=str(screenshot), timeout=3000)
                screenshot.chmod(0o600)
                evidence["screenshot"] = screenshot.name
        except (Error, OSError, TimeoutError) as error:
            # Evidence collection must not replace the original request failure.
            evidence["capture_error"] = type(error).__name__
        return evidence

    def requested_query_is_open(self) -> bool:
        assert self.tab is not None
        parsed = urlsplit(self.tab.page.url)
        return (
            parsed.hostname == "search.brave.com"
            and parsed.path.rstrip("/") == "/ask"
            and parse_qs(parsed.query).get("q") == [self.request.query]
        )

    async def navigate(self) -> None:
        assert self.tab is not None
        self.progress("page_load")
        try:
            await self.tab.page.goto(
                f"{BRAVE_ORIGIN}/ask?{urlencode({'q': self.request.query})}",
                wait_until="domcontentloaded",
                timeout=self.request.page_timeout_seconds * 1000,
            )
        except PlaywrightTimeoutError:
            if await self.access_problem() != "captcha":
                raise

    async def copy_answer(self) -> str:
        self.progress("page_setup")
        self.tab = await self.service.open_tab(self.session, "brave")
        page = self.tab.page
        page.set_default_timeout(self.request.page_timeout_seconds * 1000)
        await page.add_init_script(COPY_CAPTURE_SCRIPT)
        await self.navigate()
        remaining = self.request.answer_timeout_seconds
        while True:
            await self.solve_challenge()
            # Verification can land on the home page even during answer generation.
            if not self.requested_query_is_open():
                await self.navigate()
                await self.solve_challenge()
                if not self.requested_query_is_open():
                    raise BraveStepError("page_load", "UnexpectedPage")
            self.progress("answer_generation")
            started = monotonic()
            try:
                # The inline error also has a text button named Try again. Only
                # the answer footer's labeled icon marks generation as finished.
                await page.locator('button[aria-label="Try again"]').wait_for(
                    state="visible", timeout=min(remaining, 1) * 1000
                )
            except PlaywrightTimeoutError:
                remaining -= monotonic() - started
                await self.solve_challenge()
                if remaining <= 0:
                    raise BraveStepError("answer_generation", "TimeoutError") from None
                continue
            if (
                await self.access_problem() == "captcha"
                or not self.requested_query_is_open()
            ):
                continue
            self.progress("copy")
            try:
                # The question has an icon-only Copy; use the answer's labeled control.
                await page.evaluate("window.__companyBraveCopiedText = null")
                await (
                    page.get_by_role("button", name="Copy", exact=True)
                    .filter(has_text="Copy")
                    .click()
                )
                await page.wait_for_function(
                    "() => typeof window.__companyBraveCopiedText === 'string' && window.__companyBraveCopiedText.trim().length > 0"
                )
            except PlaywrightTimeoutError:
                if await self.access_problem() == "captcha":
                    continue
                raise
            if not self.requested_query_is_open():
                raise BraveStepError("copy", "UnexpectedPage")
            answer = await page.evaluate("() => window.__companyBraveCopiedText")
            if not isinstance(answer, str) or not answer.strip():
                raise BraveStepError("copy", "EmptyAnswer")
            return answer

    async def run(self) -> dict:
        started = monotonic()
        result = {
            "request_id": self.request.request_id,
            "query": self.request.query,
            "route": self.request.route,
            "session_id": self.session.id,
            "profile_id": self.session.profile.id,
            "status": "error",
            "answer": "",
            "source_url": BRAVE_ORIGIN,
            "error_type": "",
            "error_stage": "",
            "challenge_runs": self.runs,
        }
        try:
            async with asyncio.timeout(self.request.timeout_seconds):
                result["answer"] = await self.copy_answer()
                result["status"] = "success"
        except BraveStepError as error:
            result.update(
                status="blocked" if error.stage in {"captcha", "access"} else "error",
                error_type=error.error_type,
                error_stage=error.stage,
            )
        except Error as error:
            # Classify known browser failures without exposing exception text,
            # which can contain URLs, credentials, or page content.
            message = str(error)
            category = "BrowserError"
            if "strict mode violation" in message:
                category = "AmbiguousElement"
            elif "Execution context was destroyed" in message:
                category = "NavigationInterrupted"
            elif "Target page, context or browser has been closed" in message:
                category = "BrowserClosed"
            elif isinstance(error, PlaywrightTimeoutError):
                category = "TimeoutError"
            result.update(error_type=category, error_stage=self.stage)
        except (TimeoutError, BrowserSessionError) as error:
            result.update(error_type=type(error).__name__, error_stage=self.stage)
        except asyncio.CancelledError:
            result.update(error_type="Cancelled", error_stage=self.stage)
            raise
        except Exception as error:
            # Browser exceptions may embed credentials; log only the category.
            LOGGER.error(
                "Brave request %s failed (%s)",
                self.request.request_id,
                type(error).__name__,
            )
            result.update(error_type=type(error).__name__, error_stage=self.stage)
        finally:
            result["session_id"] = self.session.id
            result["execution_id"] = self.session.execution_id
            result["http_status"] = (
                self.tab.document_status if self.tab is not None else None
            )
            if result["status"] != "success":
                result["failure_evidence"] = await self.save_failure_evidence()
            result["source_url"] = (
                self.tab.page.url if self.tab is not None else BRAVE_ORIGIN
            )
            result["fetched_at"] = datetime.now(UTC).isoformat()
            result["elapsed_ms"] = round((monotonic() - started) * 1000)
            write_result(self.directory / "result.json", result)
            self.session.operation = None
        return result


def brave_router(
    service: BrowserService,
    *,
    deepseek_api_key: str | None,
    openrouter_api_key: str | None,
) -> APIRouter:
    router = APIRouter()
    active: dict[str, ActiveBrowserSession | None] = {}

    @router.post("/ask")
    async def ask(payload: BraveAskRequest) -> dict:
        directory = service.root / "brave-requests" / payload.request_id
        request_file, result_file = (
            directory / "request.json",
            directory / "result.json",
        )
        if payload.request_id in active:
            raise HTTPException(
                409, "This Brave request is still running", headers={"Retry-After": "1"}
            )
        if request_file.exists():
            if (
                BraveAskRequest.model_validate_json(
                    request_file.read_text(encoding="utf-8")
                ).model_dump()
                != payload.model_dump()
            ):
                raise HTTPException(
                    409,
                    "Request ID belongs to a different Brave query or configuration",
                )
            if result_file.exists():
                return json.loads(result_file.read_text(encoding="utf-8"))
            raise HTTPException(
                410, "Brave request was interrupted; use a new request ID"
            )
        active[payload.request_id] = None
        session = None
        identifier = payload.session_id or uuid4().hex
        try:
            session = await service.claim(
                identifier=identifier,
                request_id=payload.request_id,
                domain="search.brave.com",
                headless=payload.headless,
                route=payload.route,
            )
            active[payload.request_id] = session
            directory.mkdir(parents=True, mode=0o700)
            write_result(request_file, payload.model_dump())
            key = (
                deepseek_api_key
                if payload.challenge_agent_model == "deepseek-flash"
                else openrouter_api_key
            )
            async with session.lock:
                return await BraveAsk(service, session, payload, directory, key).run()
        except BrowserSessionError as error:
            raise HTTPException(
                error.status,
                {"message": str(error), "sessionId": identifier},
                headers={"Retry-After": "1"} if error.status == 503 else None,
            ) from error
        finally:
            try:
                if session is not None:
                    await service.release(session.id, execution_id=session.execution_id)
            finally:
                active.pop(payload.request_id, None)

    @router.get("/requests/{request_id}")
    async def status(request_id: str) -> dict:
        # Use the same identifier validation as submissions before touching disk.
        if (
            not request_id
            or any(
                c
                not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for c in request_id
            )
            or len(request_id) > 128
        ):
            raise HTTPException(422, "Invalid request ID")
        directory = service.root / "brave-requests" / request_id
        if (directory / "result.json").exists():
            return json.loads((directory / "result.json").read_text(encoding="utf-8"))
        if request_id in active:
            session = active[request_id]
            return {
                "request_id": request_id,
                "status": "running",
                "operation": session.operation if session else None,
                "session_id": session.id if session else None,
                "execution_id": session.execution_id if session else None,
            }
        if (directory / "request.json").exists():
            return {"request_id": request_id, "status": "interrupted"}
        raise HTTPException(404, "Unknown Brave request")

    return router
