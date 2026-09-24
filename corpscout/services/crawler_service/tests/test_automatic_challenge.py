"""Automatic assistance through the browser HTTP boundary and independent access checks."""

import asyncio
import base64
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx

from crawler_service.browser import BrowserUnavailable, interpret_capture
from crawler_service.browser_client import BrowserLeaseClient, BrowserPageClient
from crawler_service.crawl import crawl_company
from crawler_service.human_control import HumanAssistanceExpired, HumanSession
from crawler_service.service import CrawlService
from crawler_service.service_api import create_app

URL = "https://company.test/"
BLOCKED = "<title>Just a moment...</title>Verify you are human"
CLEAR = "<title>Company</title><h1>We make sensors</h1>"


class AutomaticChallengeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.events = []
        self.calls = []
        self.agent_started = asyncio.Event()
        self.agent_hold = None
        self.agent_delay = 0
        self.agent_status = 200
        self.after_html, self.after_status = CLEAR, 200
        self.after_url, self.after_error = None, None
        self.observed_url = URL
        self.solved = False
        self.http = httpx.AsyncClient(
            transport=httpx.MockTransport(self.handle), base_url="http://browser"
        )
        self.addAsyncCleanup(self.http.aclose)
        lease = BrowserLeaseClient(self.http)
        lease.id, lease.generation = "a" * 32, "generation-one"
        self.page = BrowserPageClient(lease, "site")

    async def handle(self, request):
        payload = json.loads(request.content) if request.content else {}
        self.calls.append((request.url.path, payload))
        session = {
            "id": "a" * 32,
            "state": "ready",
            "profileId": "browser-1",
            "generation": "generation-one",
        }
        if request.url.path.endswith("/challenge-agent"):
            self.agent_started.set()
            if self.agent_hold is not None:
                await self.agent_hold.wait()
            await asyncio.sleep(self.agent_delay)
            self.solved = True
            return httpx.Response(
                self.agent_status,
                json={
                    "runId": "run-one",
                    "state": "appears_clear",
                    "reason": "Page clear",
                    "steps": [],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5},
                    "elapsedSeconds": 0.1,
                },
            )
        if request.url.path.endswith("/extract"):
            if payload.get("url"):
                self.observed_url = payload["url"]
                self.solved = False
            return httpx.Response(
                200,
                json={
                    "session": session,
                    "url": (self.after_url or self.observed_url)
                    if self.solved
                    else self.observed_url,
                    "statusCode": self.after_status if self.solved else 403,
                    "browserHtml": self.after_html if self.solved else BLOCKED,
                    "headers": {},
                    "error": self.after_error if self.solved else None,
                    "screenshot": base64.b64encode(b"fixture").decode(),
                },
            )
        return httpx.Response(
            200,
            json={
                **session,
                "tabAvailable": True,
                "recovered": False,
            },
        )

    def session(self, **options):
        return HumanSession(
            headed=True,
            interactive=options.get("interactive", False),
            timeout=0.02,
            challenge_agent_max_runs=options.get("max_runs", 3)
            if options.get("enabled", True)
            else 0,
            notify=lambda state, values: self.events.append((state, values)),
        )

    async def check(
        self, human, *, html=BLOCKED, status=403, error=None, page_id="p0001", url=URL
    ):
        self.observed_url = url
        capture = interpret_capture(
            url=url, html=html, status_code=status, headers={}, error=error
        )
        return await human.check_result(self.page, capture, url, self.root, page_id)

    def agent_calls(self):
        return [
            payload for path, payload in self.calls if path.endswith("/challenge-agent")
        ]

    async def test_success_continues_without_resume_and_does_not_spend_human_deadline(
        self,
    ):
        self.agent_delay = 0.05  # Longer than the later human assistance window.
        human = self.session()
        capture = await self.check(human)
        self.assertTrue(capture.successful)
        self.assertIsNone(human.failure)
        self.assertFalse(human.waiting)
        self.assertFalse(human.resume_requested.is_set())
        self.assertEqual(
            self.agent_calls(),
            [
                {
                    "confirm": True,
                    "model": "deepseek-flash",
                    "expectedUrl": URL,
                    "expectedGeneration": "generation-one",
                }
            ],
        )
        report = json.loads(
            (self.root / "human-assistance/p0001/agent-result.json").read_text()
        )
        self.assertTrue(report["accessVerified"])
        self.assertEqual(report["trigger"], "automatic")
        self.assertEqual(self.events[0][1]["challenge_agent_running"], True)
        self.assertEqual(self.events[-1][0], "running")
        self.assertFalse(self.events[-1][1]["challenge_agent_running"])
        self.assertIsNone(self.events[-1][1]["blocked_reason"])

    async def test_model_success_does_not_override_actual_challenge(self):
        self.after_html, self.after_status = BLOCKED, 403
        human = self.session()
        with self.assertRaises(HumanAssistanceExpired):
            await self.check(human)
        self.assertFalse(human.challenge_agent_result["accessVerified"])
        self.assertEqual(human.challenge_agent_result["state"], "appears_clear")
        self.assertEqual(len(self.agent_calls()), 1)

    async def test_other_domain_and_capture_error_do_not_resume(self):
        for after_url, error in [
            ("https://elsewhere.test/", None),
            (URL, "fetch error"),
        ]:
            with self.subTest(after_url=after_url, error=error):
                self.after_url, self.after_error = after_url, error
                human = self.session()
                with self.assertRaises(HumanAssistanceExpired):
                    await self.check(human)
                self.assertFalse(human.challenge_agent_result["accessVerified"])

    async def test_unavailable_agent_falls_back_to_saved_failure(self):
        self.agent_status = 503
        human = self.session()
        with self.assertRaises(HumanAssistanceExpired):
            await self.check(human)
        self.assertEqual(human.challenge_agent_result["state"], "error")
        self.assertFalse(human.challenge_agent_result["accessVerified"])
        self.assertTrue((self.root / "human-assistance/p0001/blocked.html").exists())
        self.assertEqual(len(self.agent_calls()), 1)

    async def test_same_page_cannot_repeat_the_agent(self):
        human = self.session()
        await self.check(human)
        with self.assertRaises(HumanAssistanceExpired):
            await self.check(human, page_id="p0002")
        self.assertEqual(len(self.agent_calls()), 1)
        self.assertIn("already attempted", human.failure["agent_reason"])

    async def test_later_pages_can_use_agent_until_total_budget_is_exhausted(self):
        human = self.session(max_runs=2)
        await self.check(human)
        await self.check(human, page_id="p0002", url=URL + "second")
        self.assertEqual(len(self.agent_calls()), 2)
        self.assertEqual(
            [result["pageUrl"] for result in human.challenge_agent_results],
            [URL, URL + "second"],
        )
        self.assertTrue(
            all(result["accessVerified"] for result in human.challenge_agent_results)
        )
        with self.assertRaises(HumanAssistanceExpired):
            await self.check(human, page_id="p0003", url=URL + "third")
        self.assertEqual(len(self.agent_calls()), 2)
        self.assertIn("budget exhausted (2 runs)", human.failure["agent_reason"])

    async def test_later_challenge_failure_preserves_partial_pages_and_run_history(
        self,
    ):
        human = self.session(max_runs=2)
        root = self.root / "partial"
        manifest = await crawl_company(
            URL,
            output_dir=root,
            pages=[URL, URL + "second", URL + "third"],
            human=human,
            browser_client=self.page.lease,
        )
        saved = json.loads((root / "result.json").read_text())
        self.assertEqual(manifest["status"], "partial")
        self.assertEqual(manifest["stop_reason"], "human_assistance_timeout")
        self.assertEqual(len(saved["documents"]), 2)
        self.assertEqual(len(saved["crawl"]["challenge_agent_results"]), 2)
        self.assertEqual(manifest["human_assistance"]["url"], URL + "third")
        self.assertIn("budget exhausted", manifest["human_assistance"]["agent_reason"])

    async def test_disabled_and_interactive_attempts_keep_manual_flow(self):
        for options in ({"enabled": False}, {"interactive": True}):
            with self.subTest(options=options):
                with self.assertRaises(HumanAssistanceExpired):
                    await self.check(self.session(**options))
        self.assertEqual(self.agent_calls(), [])

    async def test_plain_access_denial_robots_and_normal_pages_do_not_activate_agent(
        self,
    ):
        with self.assertRaises(HumanAssistanceExpired):
            await self.check(self.session(), html="Access denied")
        await self.check(self.session(), error="Denied by robots.txt")
        await self.check(self.session(), html=CLEAR, status=200)
        self.assertEqual(self.agent_calls(), [])

    async def test_cancellation_clears_live_flag_without_human_wait(self):
        self.agent_hold = asyncio.Event()
        task = asyncio.create_task(self.check(self.session()))
        await asyncio.wait_for(self.agent_started.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.events[-1][1]["challenge_agent_running"])

    async def test_live_browser_remains_available_while_agent_is_running(self):
        self.agent_hold = asyncio.Event()
        human = self.session()
        human.browser_available = True
        task = asyncio.create_task(self.check(human))
        try:
            await asyncio.wait_for(self.agent_started.wait(), 1)
            self.assertTrue(human.waiting)
            self.assertTrue(self.events[0][1]["browser_available"])
        finally:
            self.agent_hold.set()
            await task
        self.assertFalse(human.waiting)

    async def test_changed_tab_is_rejected_before_agent_call(self):
        self.observed_url = "https://elsewhere.test/"
        with self.assertRaises(BrowserUnavailable):
            await self.page.run_challenge_agent(URL)
        self.assertEqual(self.agent_calls(), [])

    def test_operator_configuration_requires_failure_tracking(self):
        with self.assertRaisesRegex(ValueError, "CRAWL_HUMAN_ENABLED"):
            CrawlService(
                self.root,
                {"CRAWL_CHALLENGE_AGENT_ENABLED": "true"},
                concurrency=1,
                max_pending=1,
            )
        service = CrawlService(
            self.root,
            {
                "CRAWL_HUMAN_ENABLED": "true",
                "CRAWL_CHALLENGE_AGENT_ENABLED": "true",
            },
            concurrency=1,
            max_pending=1,
        )
        self.assertTrue(service.challenge_agent_enabled)
        self.assertEqual(service.challenge_agent_max_runs, 3)
        for invalid in ("0", "2", "1001"):
            with self.assertRaisesRegex(ValueError, "between 3 and 1000"):
                CrawlService(
                    self.root,
                    {"CRAWL_CHALLENGE_AGENT_MAX_RUNS": invalid},
                    concurrency=1,
                    max_pending=1,
                )

    async def test_portable_result_retains_agent_outcome_on_success_and_failure(self):
        for clear in (True, False):
            with self.subTest(clear=clear):
                self.solved = False
                self.after_html, self.after_status = (
                    (CLEAR, 200) if clear else (BLOCKED, 403)
                )
                root = self.root / str(clear)
                human = self.session()
                manifest = await crawl_company(
                    URL,
                    output_dir=root,
                    pages=[URL],
                    human=human,
                    browser_client=self.page.lease,
                )
                saved = json.loads((root / "result.json").read_text())
                self.assertEqual(
                    saved["crawl"]["challenge_agent"], human.challenge_agent_result
                )
                self.assertEqual(manifest["challenge_agent"]["accessVerified"], clear)
                self.assertEqual(manifest["status"], "finished" if clear else "failed")

    async def test_request_budgets_retry_escalation_and_model_persist_through_restart(
        self,
    ):
        environment = {
            "BROWSER_API_URL": "http://browser",
            "CRAWL_HUMAN_ENABLED": "true",
            "CRAWL_CHALLENGE_AGENT_ENABLED": "true",
        }
        original = httpx.AsyncHTTPTransport.handle_async_request

        async def browser_http(transport, request):
            if request.url.host == "browser":
                return await self.handle(request)
            return await original(transport, request)

        service = CrawlService(
            self.root / "service", environment, concurrency=1, max_pending=10
        )
        app = create_app(service, api_token=None)
        with patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", browser_http
        ):
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="http://service"
                ) as client:
                    response = await client.post(
                        "/v1/crawls",
                        json={
                            "request_id": "budget-three",
                            "url": URL,
                            "pages": [URL + str(i) for i in range(7)],
                            "challenge_agent_max_runs": 3,
                            "challenge_agent_model": "z-ai/glm-5.3-flash",
                        },
                    )
                    self.assertEqual(response.status_code, 202, response.text)
                    job = await service.wait("budget-three")
                    self.assertEqual(job.collected_pages, 3)
                    self.assertTrue(job.challenge_agent_budget_exhausted)
                    self.assertTrue(
                        all(
                            c["model"] == "z-ai/glm-5.3-flash"
                            for c in self.agent_calls()
                        )
                    )
                    retry = {
                        "attempt": 1,
                        "request_id": "budget-six",
                        "interactive": False,
                    }
                    response = await client.post(
                        "/v1/crawls/budget-three/retry", json=retry
                    )
                    self.assertEqual(response.status_code, 202, response.text)
                    self.assertEqual(response.json()["challenge_agent_max_runs"], 6)
                    self.assertEqual(
                        (
                            await client.post(
                                "/v1/crawls/budget-three/retry", json=retry
                            )
                        ).status_code,
                        202,
                    )
                    changed = await client.post(
                        "/v1/crawls/budget-three/retry",
                        json=retry | {"challenge_agent_max_runs": 9},
                    )
                    self.assertEqual(changed.status_code, 409)
                    job = await service.wait("budget-six")
                    self.assertEqual(job.collected_pages, 6)
                    self.assertTrue(job.challenge_agent_budget_exhausted)
            # Retry uses SQLite and saved request/result files after a service restart.
            service = CrawlService(
                self.root / "service", environment, concurrency=1, max_pending=10
            )
            app = create_app(service, api_token=None)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="http://service"
                ) as client:
                    retry = {
                        "attempt": 1,
                        "request_id": "budget-twelve",
                        "interactive": False,
                    }
                    response = await client.post(
                        "/v1/crawls/budget-six/retry", json=retry
                    )
                    self.assertEqual(response.status_code, 202, response.text)
                    self.assertEqual(response.json()["challenge_agent_max_runs"], 12)
                    job = await service.wait("budget-twelve")
                    self.assertEqual(job.state, "completed")
                    self.assertEqual(job.collected_pages, 7)
                    self.assertFalse(job.challenge_agent_budget_exhausted)
                    result = (
                        await client.get("/v1/crawls/budget-twelve/result")
                    ).json()
                    self.assertEqual(result["crawl"]["challenge_agent_max_runs"], 12)
                    self.assertEqual(len(result["crawl"]["challenge_agent_results"]), 7)
                    override = {
                        "attempt": 1,
                        "request_id": "budget-override",
                        "interactive": False,
                        "challenge_agent_max_runs": 8,
                        "challenge_agent_model": "deepseek-flash",
                    }
                    response = await client.post(
                        "/v1/crawls/budget-six/retry", json=override
                    )
                    self.assertEqual(response.status_code, 202, response.text)
                    self.assertEqual(response.json()["challenge_agent_max_runs"], 8)
                    self.assertEqual(
                        response.json()["challenge_agent_model"], "deepseek-flash"
                    )
                    self.assertEqual(
                        (await service.wait("budget-override")).state, "completed"
                    )
                    for invalid in [2, 1001, True, 3.5, "6"]:
                        response = await client.post(
                            "/v1/crawls/budget-six/retry",
                            json=override | {"challenge_agent_max_runs": invalid},
                        )
                        self.assertEqual(response.status_code, 422, response.text)
