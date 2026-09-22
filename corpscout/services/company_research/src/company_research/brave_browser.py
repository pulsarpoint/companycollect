"""One serialized Brave session, stopped until an operator clears a challenge."""

from __future__ import annotations

import asyncio
import json
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup

from company_research.browser import BrowserUnavailable, PageCapture
from company_research.browser_client import BrowserLeaseClient, BrowserPageClient
from company_research.discovery import normalize_url
from company_research.human_control import (
    HumanAssistanceExpired,
    HumanSession,
    access_problem,
)
from company_research.models import ResearchConfig
from company_research.storage import utc_now, write_json


class BraveSearchBlocked(RuntimeError):
    pass


def brave_access_problem(result: PageCapture) -> str | None:
    # Robots denial is a crawl policy, not a verification the user can bypass.
    if "robots" in (result.error or "").casefold():
        return None
    if result.status_code == 200 and brave_results(result.html, 1):
        return None
    soup = BeautifulSoup(result.html, "html.parser")
    # Queries about CAPTCHAs can legitimately contain challenge wording in results.
    for node in soup.select(".snippet, script, style, input, textarea"):
        node.decompose()
    problem = access_problem(result.status_code, str(soup), result.error or "")
    if problem is not None:
        return problem
    text = soup.get_text(" ", strip=True).casefold()
    path = urlsplit(result.url).path.rstrip("/")
    if (
        soup.select_one(".h-captcha, .g-recaptcha, #captcha, form[action*='captcha']")
        or path in {"/captcha", "/challenge", "/sorry"}
        or any(
            marker in text
            for marker in (
                "confirm you are human",
                "confirm you're human",
                "verify that you are human",
                "prove you are human",
                "are you a robot",
                "unusual traffic",
                "automated queries",
                "complete the security check",
                "solve the captcha",
                "complete the captcha",
            )
        )
    ):
        return "captcha"
    return None


def brave_results(html: str, limit: int) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen = set()
    for item in soup.select('.snippet[data-type="web"]'):
        title = item.select_one(".search-snippet-title")
        anchor = title.find_parent("a", href=True) if title is not None else None
        if title is None or anchor is None:
            continue
        try:
            url = normalize_url(str(anchor["href"]))
        except ValueError:
            continue
        host = urlsplit(url).hostname or ""
        if (
            host == "search.brave.com"
            or host.endswith(".search.brave.com")
            or url in seen
        ):
            continue
        seen.add(url)
        description = item.select_one(".generic-snippet")
        results.append(
            {
                "url": url,
                "title": title.get_text(" ", strip=True)[:500],
                "snippet": description.get_text(" ", strip=True)[:1600]
                if description is not None
                else "",
                "rank": len(results) + 1,
            }
        )
        if len(results) >= limit:
            break
    return results


class BraveSearch:
    """Shared by REST/manual jobs in the service that owns this directory.

    The profile is private service state, outside all per-job result archives.
    A durable latch survives cancellation/restart; only successful manual
    verification removes it. The lock covers navigation *and* human assistance.
    """

    def __init__(
        self,
        root: Path,
        *,
        lock: asyncio.Lock | None = None,
        browser_client: BrowserLeaseClient | None = None,
    ):
        self.root = root
        self.profile = root / "profile"
        self.block_file = root / "blocked.json"
        self.lock = lock if lock is not None else asyncio.Lock()
        self.browser_client = browser_client
        self.stack = AsyncExitStack()
        self.browser: BrowserPageClient | None = None

    async def close(self) -> None:
        try:
            if isinstance(self.browser, BrowserPageClient):
                await self.browser.close()
            await self.stack.aclose()
        finally:
            self.browser = None

    async def open(self, human: HumanSession | None = None) -> None:
        if self.browser_client is None:
            raise ValueError(
                "An external browser-service session is required for search"
            )
        await self.close()
        self.browser = await self.browser_client.open_tab("search")
        if human is not None:
            await self.browser.focus()

    async def navigate(self, url: str, config: ResearchConfig) -> PageCapture:
        assert self.browser is not None
        try:
            async with asyncio.timeout(config.page_timeout_seconds + 10):
                return await self.browser.navigate(
                    url,
                    timeout_seconds=config.page_timeout_seconds,
                    check_robots_txt=config.check_robots_txt,
                )
        except Exception:
            # A challenge can keep DOMContentLoaded pending until goto times out.
            if (
                isinstance(self.browser, BrowserPageClient)
                or not self.browser.page.is_closed()
            ):
                captured = await self.browser.capture()
                if brave_access_problem(captured) is not None:
                    return captured
            raise

    async def fetch(
        self,
        url: str,
        config: ResearchConfig,
        human: HumanSession | None,
        root: Path,
        search_id: str,
    ) -> PageCapture:
        if self.lock.locked() and human is not None:
            human.notify(
                "blocked",
                {
                    "current_url": url,
                    "reason": "Waiting for the shared Brave search session. No search request has been sent.",
                    "blocked_reason": "brave_search_waiting",
                    "verification_available": False,
                    "browser_available": False,
                },
            )
        async with self.lock:
            if self.block_file.exists():
                blocked = json.loads(self.block_file.read_text(encoding="utf-8"))
                captured = await self.verify(blocked, config, human, root, search_id)
                if blocked["url"] == url:
                    return captured
            if self.browser is None or (
                isinstance(self.browser, BrowserPageClient)
                and (
                    self.browser.closed
                    or self.browser.generation != self.browser.lease.generation
                )
            ):
                await self.open()
            result = await self.navigate(url, config)
            problem = brave_access_problem(result)
            if problem is not None:
                blocked = {
                    "url": url,
                    "reason": problem,
                    "status_code": result.status_code,
                    "detected_at": utc_now(),
                }
                # Persist before screenshot/notification, which may themselves fail.
                write_json(self.block_file, blocked)
                evidence = root / "human-assistance" / search_id
                write_json(evidence / "failure.json", blocked)
                (evidence / "blocked.html").write_text(result.html, encoding="utf-8")
                result = await self.verify(blocked, config, human, root, search_id)
            if human is not None:
                human.notify(
                    "running",
                    {
                        "current_url": url,
                        "reason": "Brave search completed",
                        "blocked_reason": None,
                    },
                )
            return result

    async def verify(
        self,
        blocked: dict,
        config: ResearchConfig,
        human: HumanSession | None,
        root: Path,
        search_id: str,
    ) -> PageCapture:
        if human is None:
            raise BraveSearchBlocked(
                "Brave search is paused; enable human assistance to verify the pending search"
            )
        previous_available = human.browser_available
        human.browser_available = False
        human.failure = blocked
        human.verification_requested.clear()
        human.resume_requested.clear()
        human.verification_available = True
        human.waiting = True
        human.notify(
            blocked["reason"],
            {
                "current_url": blocked["url"],
                "reason": "Brave searches are paused. Start verification to open the pending search in a visible browser.",
                "blocked_reason": "brave_" + blocked["reason"],
                "verification_available": True,
                "assistance_deadline": None,
                "browser_available": False,
            },
        )
        try:
            # No automatic retry/deadline before the operator activates the browser.
            await human.verification_requested.wait()
            human.verification_available = False
            human.waiting = False
            human.notify(
                "awaiting_human",
                {
                    "reason": "Opening the pending Brave search for verification.",
                    "verification_available": False,
                },
            )
            if self.browser_client is not None:
                self.browser = await self.browser_client.open_tab("search")
                await self.browser.focus()
            else:
                await self.open(human)
            assert self.browser is not None
            await self.navigate(blocked["url"], config)
            human.browser_available = True
            human.verifying_search = True
            deadline = asyncio.get_running_loop().time() + 900
            expires_at = (datetime.now(UTC) + timedelta(seconds=900)).isoformat()
            reason = "Complete Brave verification, then select Resume crawl. The pending query must be visible in the results."
            while True:
                human.resume_requested.clear()
                human.waiting = True
                human.notify(
                    "awaiting_human",
                    {
                        "current_url": blocked["url"],
                        "reason": reason,
                        "assistance_deadline": expires_at,
                        "browser_available": True,
                        "browser_session_id": human.session_id,
                    },
                )
                try:
                    async with asyncio.timeout_at(deadline):
                        await human.wait_for_resume(
                            self.browser, blocked["url"], "awaiting_human"
                        )
                except TimeoutError as error:
                    raise HumanAssistanceExpired(
                        "Brave verification timed out; further searches remain paused"
                    ) from error
                try:
                    captured = await self.browser.capture()
                except BrowserUnavailable:
                    reason = "Browser closed during confirmation. Restore the pending page before resuming."
                    continue
                expected = urlsplit(blocked["url"])
                actual = urlsplit(captured.url)
                if (
                    not captured.successful
                    or captured.status_code != 200
                    or brave_access_problem(captured) is not None
                    or actual.hostname != expected.hostname
                    or actual.path.rstrip("/") != expected.path.rstrip("/")
                    or parse_qs(actual.query).get("q")
                    != parse_qs(expected.query).get("q")
                    or not brave_results(captured.html, 1)
                ):
                    reason = "Verification is incomplete. Return to the results for the pending Brave query before resuming."
                    continue
                write_json(
                    root / "human-assistance" / search_id / "resolved.json",
                    {"url": captured.url, "resolved_at": utc_now()},
                )
                self.block_file.unlink()
                human.failure = None
                human.notify(
                    "running",
                    {
                        "reason": "Brave search verified by operator; continuing the pending query.",
                        "blocked_reason": None,
                        "assistance_deadline": None,
                        "browser_available": False,
                        "verification_available": False,
                    },
                )
                return captured
        except (HumanAssistanceExpired, asyncio.CancelledError):
            raise
        except Exception as error:
            raise BraveSearchBlocked(
                "Brave verification could not complete; further searches remain paused"
            ) from error
        finally:
            human.waiting = False
            human.verification_available = False
            human.verifying_search = False
            try:
                if self.block_file.exists():
                    await self.close()
            finally:
                human.browser_available = previous_available
