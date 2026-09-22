"""Keep blocked documents alive until explicit human confirmation or a deadline."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Literal
from urllib.parse import urlsplit
from uuid import uuid4

import httpx

from company_research.browser import BrowserUnavailable, PageCapture
from company_research.browser_client import BrowserPageClient
from company_research.storage import utc_now, write_json


class HumanAssistanceExpired(RuntimeError):
    pass


def access_problem(
    status: int | None, html: str, error: str = ""
) -> Literal["blocked", "captcha"] | None:
    content = html.casefold()
    if (
        any(
            marker in content
            for marker in (
                "<title>just a moment",
                "cf-chl-widget",
                "cf-chl-opt",
                "_cf_chl_opt",
                "<title>attention required",
                "verify you are human",
            )
        )
        or "cloudflare js challenge" in error.casefold()
    ):
        return "captcha"
    if status in {401, 403, 429} or "blocked by anti-bot" in error.casefold():
        return "blocked"
    return None


class HumanSession:
    def __init__(
        self,
        *,
        headed: bool,
        interactive: bool,
        timeout: float,
        notify: Callable[[str, dict], None],
        request_id: str | None = None,
        challenge_agent_max_runs: int = 0,
        challenge_agent_model: str = "deepseek-flash",
    ):
        self.headed = headed
        self.interactive = interactive
        self.timeout = timeout
        self.notify = notify
        self.request_id = request_id
        self.session_id = uuid4().hex
        self.browser_available = False
        self.resume_requested = asyncio.Event()
        self.verification_requested = asyncio.Event()
        self.verification_available = False
        self.verifying_search = False
        self.waiting = False
        self.paused_once = False
        self.failure: dict | None = None
        self.challenge_agent_max_runs = challenge_agent_max_runs
        self.challenge_agent_model = challenge_agent_model
        self.challenge_agent_budget_exhausted = False
        self.challenge_agent_urls: set[str] = set()
        self.challenge_agent_results: list[dict] = []
        self.challenge_agent_result: dict | None = None

    async def attempt_challenge(
        self, crawler: BrowserPageClient, url: str, evidence: Path
    ) -> PageCapture | None:
        """Use one bounded run per URL, within the crawl's total agent budget."""
        self.challenge_agent_urls.add(url)
        self.waiting = True
        started = monotonic()
        self.notify(
            "captcha",
            {
                "current_url": url,
                "reason": "CAPTCHA agent running; checking access automatically when it finishes.",
                "blocked_reason": "captcha",
                "assistance_deadline": None,
                "browser_available": self.browser_available,
                "browser_session_id": self.session_id,
                "challenge_agent_running": True,
                "challenge_agent_result": None,
            },
        )
        captured = None
        outcome = None
        try:
            async with asyncio.timeout(190):
                outcome = await crawler.run_challenge_agent(
                    url, model=self.challenge_agent_model
                )
                captured = await crawler.capture()
                await crawler.screenshot(evidence / "agent-after.png")
        except (httpx.HTTPError, TimeoutError, RuntimeError, ValueError) as error:
            outcome = (
                outcome
                or {
                    "runId": None,
                    "state": "error",
                    "reason": "Automatic CAPTCHA assistance failed",
                    "steps": [],
                    "usage": {"prompt_tokens": 0, "completion_tokens": 0},
                    "elapsedSeconds": round(monotonic() - started, 3),
                }
            ) | {"verificationError": type(error).__name__}
            captured = None
        except asyncio.CancelledError:
            self.notify("captcha", {"challenge_agent_running": False})
            raise
        finally:
            self.waiting = False
        access_verified = (
            captured is not None
            and captured.successful
            and bool(captured.cleaned_html)
            and (urlsplit(captured.url).hostname or "").removeprefix("www.")
            == (urlsplit(url).hostname or "").removeprefix("www.")
            and access_problem(
                captured.status_code, captured.html, captured.error or ""
            )
            is None
        )
        self.challenge_agent_result = outcome | {
            "trigger": "automatic",
            "accessVerified": access_verified,
            "pageUrl": url,
        }
        self.challenge_agent_results.append(self.challenge_agent_result)
        write_json(evidence / "agent-result.json", self.challenge_agent_result)
        if captured is not None:
            (evidence / "agent-after.html").write_text(captured.html, encoding="utf-8")
        self.notify(
            "running" if access_verified else "captcha",
            {
                "challenge_agent_running": False,
                "challenge_agent_result": self.challenge_agent_result,
                "challenge_agent_results": self.challenge_agent_results.copy(),
                "blocked_reason": None if access_verified else "captcha",
                "reason": "CAPTCHA agent completed; access verified by crawler."
                if access_verified
                else "CAPTCHA agent could not verify access; human assistance is needed.",
                "browser_available": False
                if access_verified
                else self.browser_available,
            },
        )
        if access_verified:
            self.failure = None
            write_json(
                evidence / "resolved.json",
                {
                    "url": captured.url,
                    "resolved_at": utc_now(),
                    "by": "challenge_agent",
                },
            )
            return captured
        return None

    async def wait_for_resume(
        self, crawler: BrowserPageClient, url: str, state: str
    ) -> None:
        if not isinstance(crawler, BrowserPageClient):
            await self.resume_requested.wait()
            return
        unavailable = False
        tab_closed = False
        while True:
            try:
                recovered = await crawler.recover(
                    url,
                    reopen_closed_tab=tab_closed and self.resume_requested.is_set(),
                )
            except BrowserUnavailable:
                self.resume_requested.clear()
                if not unavailable:
                    self.notify(
                        state,
                        {
                            "browser_available": False,
                            "reason": "Browser unavailable. The crawl remains paused until recovery or its existing deadline.",
                        },
                    )
                unavailable = True
                await asyncio.sleep(1)
                continue
            if not crawler.available:
                if not tab_closed:
                    self.resume_requested.clear()
                    self.notify(
                        state,
                        {
                            "browser_available": self.browser_available,
                            "reason": "Verification tab closed. The crawl is paused. Select Resume crawl to reopen the pending page for verification.",
                        },
                    )
                tab_closed = True
            if crawler.available and (recovered or unavailable):
                # A confirmation sent to the closed browser cannot resume a new one.
                self.resume_requested.clear()
                self.notify(
                    state,
                    {
                        "browser_available": self.browser_available,
                        "browser_session_id": self.session_id,
                        "reason": "Browser restored. Complete verification on the reopened page, then resume the crawl.",
                    },
                )
                unavailable = False
                tab_closed = False
            if self.resume_requested.is_set() and crawler.available:
                return
            try:
                async with asyncio.timeout(1):
                    await self.resume_requested.wait()
            except TimeoutError:
                pass

    async def check_result(
        self,
        crawler: BrowserPageClient,
        result: PageCapture,
        url: str,
        root: Path,
        page_id: str,
    ) -> PageCapture:
        # Robots denial returns before opening/navigating a page. It is not a challenge.
        if "robots" in (result.error or "").casefold():
            return result
        html = result.html
        problem = access_problem(result.status_code, html, result.error or "")
        if problem is None and not (self.interactive and not self.paused_once):
            return result
        self.paused_once = True
        self.failure = {
            "reason": problem or "manual_review",
            "url": url,
            "status_code": result.status_code,
            "detected_at": utc_now(),
        }
        evidence = root / "human-assistance" / page_id
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "blocked.html").write_text(html, encoding="utf-8")
        write_json(evidence / "failure.json", self.failure)
        await crawler.screenshot(evidence / "blocked.png")
        agent_reason = None
        if (
            problem == "captcha"
            and self.challenge_agent_max_runs > 0
            and not self.interactive
        ):
            if url in self.challenge_agent_urls:
                agent_reason = (
                    "CAPTCHA returned on a page already attempted by the agent."
                )
            elif len(self.challenge_agent_urls) >= self.challenge_agent_max_runs:
                self.challenge_agent_budget_exhausted = True
                self.failure["agent_budget_exhausted"] = True
                agent_reason = f"Automatic CAPTCHA agent budget exhausted ({self.challenge_agent_max_runs} runs)."
            else:
                captured = await self.attempt_challenge(crawler, url, evidence)
                if captured is not None:
                    return captured
                agent_reason = "CAPTCHA agent could not verify access on this page."
            self.failure["agent_reason"] = agent_reason
            write_json(evidence / "failure.json", self.failure)
        deadline = asyncio.get_running_loop().time() + self.timeout
        expires_at = (datetime.now(UTC) + timedelta(seconds=self.timeout)).isoformat()
        reason = (
            "Complete verification or authentication, then resume the crawl."
            if self.interactive
            else "Automatic crawl blocked. After this attempt fails, retry interactively to open a browser."
        )
        if agent_reason is not None:
            reason = f"{agent_reason} After this attempt fails, retry interactively."
        try:
            while True:
                self.resume_requested.clear()
                self.waiting = True
                self.notify(
                    problem or "awaiting_human",
                    {
                        "current_url": url,
                        "reason": reason,
                        "blocked_reason": problem,
                        "challenge_agent_budget_exhausted": self.challenge_agent_budget_exhausted,
                        "assistance_deadline": expires_at,
                        "browser_available": self.browser_available,
                        "browser_session_id": self.session_id,
                    },
                )
                try:
                    async with asyncio.timeout_at(deadline):
                        await self.wait_for_resume(
                            crawler, url, problem or "awaiting_human"
                        )
                except TimeoutError as error:
                    raise HumanAssistanceExpired(
                        f"{problem or 'manual_review'}: human assistance timed out after {self.timeout:g} seconds"
                    ) from error
                self.waiting = False
                try:
                    captured = await crawler.capture()
                except BrowserUnavailable:
                    reason = "Browser closed during confirmation. Restore the pending page before resuming."
                    continue
                current_url = captured.url
                if (urlsplit(current_url).hostname or "").removeprefix("www.") != (
                    urlsplit(url).hostname or ""
                ).removeprefix("www."):
                    reason = "Return to the requested website before resuming."
                    continue
                problem = access_problem(captured.status_code, captured.html)
                if (
                    problem is not None
                    or captured.status_code is None
                    or captured.status_code >= 400
                ):
                    reason = "The page is still blocked. Complete verification before resuming."
                    continue
                self.notify(
                    "running",
                    {
                        "reason": "Resumed by operator",
                        "browser_available": False,
                        "assistance_deadline": None,
                    },
                )
                if access_problem(
                    captured.status_code,
                    captured.html,
                    captured.error or "",
                ):
                    problem = "captcha"
                    continue
                write_json(
                    evidence / "resolved.json",
                    {"url": current_url, "resolved_at": utc_now()},
                )
                self.failure = None
                return captured
        finally:
            self.waiting = False
