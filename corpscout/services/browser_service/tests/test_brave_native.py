"""Real browser/CDP tests with intercepted Brave pages and a fixture model endpoint."""

import asyncio
import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import uuid4

import httpx

from browser_service.api import create_app
from browser_service.browser_sessions import PersistentBrowserSession
from browser_service.runtime import BrowserRuntimeSettings, BrowserService

COPY_PAGE = r"""<!doctype html><html><head><title>Brave Ask</title></head><body>
<button aria-label="Copy" id="question-copy"></button>
<button id="copy" aria-label="Copy"> Copy</button>
<button id="retry" aria-label="Try again" hidden></button>
<script>
const query = new URL(location.href).searchParams.get('q');
let answer = 'Incomplete streamed answer';
document.querySelector('#question-copy').onclick = () => navigator.clipboard.writeText(query);
document.querySelector('#copy').onclick = () => navigator.clipboard.writeText(answer);
if (query !== 'never_finished') setTimeout(() => {
    answer = query === 'empty' ? '' : 'Answer: ' + query + '\nhttps://example.se/';
    document.querySelector('#retry').hidden = false;
}, query === 'slow' ? 900 : 50);
</script></body></html>"""

CHALLENGE_PAGE = """<!doctype html><html><head><title>Security check</title></head>
<body><h1>Verify you are human</h1><button id="captcha"
style="position:absolute;left:100px;top:100px;width:240px;height:60px"
onclick="sessionStorage.setItem('verified','yes');location.href='/'">Verify you are human</button></body></html>"""

POW_CHALLENGE_PAGE = """<!doctype html><html><head><title>Brave Search</title></head>
<body><h1>Verifying you're not a bot</h1><p>Quick check before you continue searching.</p>
<button style="position:absolute;left:100px;top:100px;width:240px;height:60px"
onclick="sessionStorage.setItem('verified','yes');location.href='/'">Verify</button>
<button>Switch to traditional CAPTCHA</button>
<a href="/help/pow-captcha">Why am I seeing this?</a></body></html>"""


@unittest.skipUnless(
    os.environ.get("BRAVE_NATIVE_TEST") == "1", "opt-in native Brave fixture"
)
class BraveNativeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.service = BrowserService(
            Path(self.temporary.name),
            settings=BrowserRuntimeSettings(
                max_browsers=1, idle_timeout_seconds=120, session_retention_days=7
            ),
            proxy_routes={"crawl_proxy1": "http://fixture:secret@127.0.0.1:9"},
        )
        self.mode = "normal"
        self.navigations = []
        self.agent_calls = []
        original_start = PersistentBrowserSession.start

        async def start(profile, **kwargs):
            await original_start(profile, **kwargs)
            await profile.context.route("**/*", self.serve)

        self.start_patch = patch.object(PersistentBrowserSession, "start", start)
        self.start_patch.start()
        self.addCleanup(self.start_patch.stop)
        await self.service.start()
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(
                create_app(
                    self.service,
                    api_token="fixture-token",
                    deepseek_api_key="fixture-key",
                )
            ),
            base_url="http://test",
            headers={"Authorization": "Bearer fixture-token"},
        )
        original_client = httpx.AsyncClient

        def agent_client(**kwargs):
            return original_client(
                **kwargs,
                **(
                    {"transport": httpx.MockTransport(self.model)}
                    if "base_url" in kwargs
                    else {}
                ),
            )

        self.agent_patch = patch(
            "browser_service.brave.httpx.AsyncClient", agent_client
        )
        self.agent_patch.start()
        self.addCleanup(self.agent_patch.stop)

    async def asyncTearDown(self):
        # Browser launch probes also use httpx; remove the fixture transport first.
        self.agent_patch.stop()
        await self.http.aclose()
        await self.service.close()

    async def serve(self, route):
        url = route.request.url
        self.navigations.append(url)
        body = (
            COPY_PAGE
            if "/ask?" in url
            else "<html><title>Brave Search</title><h1>Search</h1></html>"
        )
        if self.mode == "duplicate_retry" and "/ask?" in url:
            body += '<button data-sveltekit-reload="true">Try again</button>'
        if self.mode == "modal_challenge" and "/ask?" in url:
            dialog = """<div role="dialog"><h1>Prove you are human</h1>
<button style="position:absolute;left:100px;top:100px;width:240px;height:60px"
onclick="sessionStorage.setItem('verified','yes');location.href='/'">I'm not a robot</button></div>
<button data-sveltekit-reload="true">Try again</button>"""
            body += """<script>
if (sessionStorage.getItem('verified') !== 'yes') {
 setTimeout(() => {
  document.getElementById('retry').hidden = false;
  document.body.insertAdjacentHTML('beforeend', DIALOG);
 }, 100);
}
</script>""".replace("DIALOG", json.dumps(dialog))
        if self.mode in {"pow_challenge", "pow_delayed"} and "/ask?" in url:
            body = """<html><head><title>Brave Search</title></head><body><script>
if(sessionStorage.getItem('verified') === 'yes') {
 document.open();document.write(ANSWER);document.close();
} else {
 setTimeout(() => {document.open();document.write(CHALLENGE);document.close();}, DELAY);
}
</script></body></html>""".replace(
                "ANSWER", json.dumps(COPY_PAGE).replace("</", "<\\/")
            )
            body = body.replace("CHALLENGE", json.dumps(POW_CHALLENGE_PAGE)).replace(
                "DELAY", "200" if self.mode == "pow_delayed" else "0"
            )
            await route.fulfill(status=429, content_type="text/html", body=body)
            return
        if self.mode == "rate_limited":
            await route.fulfill(
                status=429,
                headers={"Retry-After": "120"},
                content_type="text/html",
                body="<html><title>Brave Search</title><h1>Too many requests</h1><p>Please try later.</p></html>",
            )
            return
        if (
            self.mode in {"challenge", "false_clear", "late_challenge", "agent_timeout"}
            and "/ask?" in url
        ):
            # Keep all traffic in the fixture; verification state belongs to this page session.
            script = (
                "<script>if(sessionStorage.getItem('verified') !== 'yes') { document.open();document.write("
                + json.dumps(CHALLENGE_PAGE)
                + ");document.close(); }</script>"
            )
            if self.mode == "late_challenge":
                body = (
                    "<html><title>Brave Ask</title><h1>Generating</h1>"
                    + script.replace(
                        "if(sessionStorage", "setTimeout(() => { if(sessionStorage"
                    ).replace("}</script>", "} }, 100);</script>")
                    + "</html>"
                )
                # After verification, serve a completed answer on the resumed query.
                body += (
                    '<script>if(sessionStorage.getItem("verified") === "yes") {document.open();document.write('
                    + json.dumps(COPY_PAGE).replace("</", "<\\/")
                    + ");document.close();}</script>"
                )
            else:
                body += script
        await route.fulfill(content_type="text/html", body=body)

    async def model(self, request):
        self.agent_calls.append(json.loads(request.content))
        if self.mode == "agent_timeout":
            await asyncio.sleep(10)
        action = {
            "action": "finish",
            "outcome": "appears_clear",
            "reason": "Fixture verification completed",
        }
        if self.mode != "false_clear" and len(self.agent_calls) % 2 == 1:
            action = {
                "action": "click",
                "x": 200,
                "y": 130,
                "reason": "Click fixture verification button",
            }
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": json.dumps(action)},
                    }
                ]
            },
        )

    async def ask(self, query, **options):
        # Avoid changing HTTP probe behavior when a released profile is recycled.
        self.agent_patch.stop()
        # Patch only for the leased operation, not service browser creation/recycling.
        from browser_service.brave import BraveAsk

        original = BraveAsk.solve_challenge

        async def solve(operation):
            with self.agent_patch:
                return await original(operation)

        with patch.object(BraveAsk, "solve_challenge", solve):
            return await self.http.post(
                "/v1/brave/ask",
                json={
                    "request_id": uuid4().hex,
                    "query": query,
                    "page_timeout_seconds": 2,
                    "answer_timeout_seconds": 3,
                    **options,
                },
            )

    async def test_copy_special_query_idempotency_and_empty_answer(self):
        query = "+1 Kommunikationsbyrå AB & Co? #1"
        response = await self.ask(query)
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        self.assertEqual(
            result["answer"], f"Answer: {query}\nhttps://example.se/", result
        )
        payload = json.loads(
            (
                self.service.root
                / "brave-requests"
                / result["request_id"]
                / "request.json"
            ).read_text()
        )
        before = len(self.navigations)
        repeated = await self.http.post("/v1/brave/ask", json=payload)
        self.assertEqual(repeated.json(), result)
        self.assertEqual(len(self.navigations), before)
        conflict = await self.http.post(
            "/v1/brave/ask", json=payload | {"query": "Different"}
        )
        self.assertEqual(conflict.status_code, 409)
        empty = (await self.ask("empty", page_timeout_seconds=0.3)).json()
        self.assertEqual(empty["answer"], "")
        self.assertEqual(empty["error_stage"], "copy")
        self.assertFalse(self.service.active)

    async def test_inline_retry_does_not_finish_answer_before_footer_appears(self):
        self.mode = "duplicate_retry"
        result = (await self.ask("slow")).json()
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(result["answer"], "Answer: slow\nhttps://example.se/")
        self.assertGreaterEqual(result["elapsed_ms"], 900)
        self.assertEqual(self.agent_calls, [])

    async def test_verification_dialog_over_duplicate_retry_buttons_runs_agent(self):
        self.mode = "modal_challenge"
        result = (await self.ask("slow")).json()
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(result["answer"], "Answer: slow\nhttps://example.se/")
        self.assertEqual(len(result["challenge_runs"]), 1, result)
        self.assertEqual(len(self.agent_calls), 2)
        self.assertGreaterEqual(sum("/ask?" in url for url in self.navigations), 2)

    async def test_slow_answer_uses_answer_budget_and_query_about_captcha_is_not_a_challenge(
        self,
    ):
        short = (
            await self.ask("slow", page_timeout_seconds=0.5, answer_timeout_seconds=0.2)
        ).json()
        self.assertEqual(short["error_stage"], "answer_generation")
        slow = (
            await self.ask("slow", page_timeout_seconds=0.5, answer_timeout_seconds=2)
        ).json()
        self.assertEqual(slow["status"], "success", slow)
        about = (await self.ask("What is CAPTCHA?")).json()
        self.assertEqual(about["status"], "success", about)
        self.assertEqual(self.agent_calls, [])

    async def test_captcha_automatically_runs_real_agent_and_resumes_pending_query(
        self,
    ):
        self.mode = "challenge"
        result = (await self.ask("Find Novelic")).json()
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(result["answer"], "Answer: Find Novelic\nhttps://example.se/")
        self.assertEqual(len(result["challenge_runs"]), 1)
        self.assertEqual(len(self.agent_calls), 2)
        self.assertEqual(len([u for u in self.navigations if "/ask?" in u]), 2)
        self.assertFalse(self.service.active)

    async def test_late_captcha_resumes_same_query_after_verification(self):
        self.mode = "late_challenge"
        result = (await self.ask("Find Melexis")).json()
        self.assertEqual(result["status"], "success", result)
        self.assertEqual(result["answer"], "Answer: Find Melexis\nhttps://example.se/")
        self.assertEqual(len(result["challenge_runs"]), 1)

    async def test_agent_claiming_success_does_not_override_page_verification(self):
        self.mode = "false_clear"
        result = (await self.ask("Find blocked company")).json()
        self.assertEqual(result["status"], "blocked", result)
        self.assertEqual(result["error_type"], "AgentBudgetExhausted")
        self.assertEqual(len(result["challenge_runs"]), 3)
        self.assertEqual(result["answer"], "")
        disabled = (await self.ask("Disabled", challenge_agent_max_runs=0)).json()
        self.assertEqual(disabled["error_type"], "AgentBudgetExhausted")
        self.assertEqual(disabled["challenge_runs"], [])

    async def test_auth_validation_capacity_and_cancellation_release(self):
        denied = await self.http.post(
            "/v1/brave/ask",
            headers={"Authorization": "Bearer wrong"},
            json={"request_id": "one", "query": "Example"},
        )
        self.assertEqual(denied.status_code, 401)
        for options in [
            {"query": "  "},
            {"request_id": "../escape"},
            {"challenge_agent_max_runs": -1},
            {"route": "http://secret@proxy"},
        ]:
            response = await self.http.post(
                "/v1/brave/ask",
                json={"request_id": "one", "query": "Example", **options},
            )
            self.assertEqual(response.status_code, 422)
        pending = asyncio.create_task(self.ask("never_finished", request_id="pending"))
        async with asyncio.timeout(5):
            while not any(s.operation for s in self.service.active.values()):
                await asyncio.sleep(0.01)
        busy = await self.http.post(
            "/v1/brave/ask", json={"request_id": "busy", "query": "Example"}
        )
        self.assertEqual(busy.status_code, 503)
        self.assertFalse((self.service.root / "brave-requests" / "busy").exists())
        status = (await self.http.get("/v1/brave/requests/pending")).json()
        self.assertEqual(status["status"], "running")
        duplicate = await self.http.post(
            "/v1/brave/ask", json={"request_id": "pending", "query": "never_finished"}
        )
        self.assertEqual(duplicate.status_code, 409)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertFalse(self.service.active)
        result = (await self.http.get("/v1/brave/requests/pending")).json()
        self.assertEqual(result["error_type"], "Cancelled")

    async def test_proxy_profiles_preserve_verification_without_crossing_routes(self):
        self.agent_patch.stop()
        identifier = uuid4().hex
        session = await self.service.claim(
            identifier=identifier, request_id="seed", domain="search.brave.com"
        )
        await session.profile.context.add_cookies(
            [
                {
                    "name": "verified",
                    "value": "direct-only",
                    "url": "https://search.brave.com",
                }
            ]
        )
        await self.service.release(identifier)
        direct = (await self.ask("Direct", session_id=identifier)).json()
        self.assertEqual(direct["status"], "success", direct)
        self.assertEqual(direct["session_id"], identifier)
        self.assertFalse(self.service.active)
        proxied = (await self.ask("Proxied", route="crawl_proxy1")).json()
        self.assertEqual(proxied["status"], "success", proxied)
        self.assertNotEqual(proxied["session_id"], identifier)
        session = await self.service.claim(
            identifier=identifier, request_id="check", domain="search.brave.com"
        )
        self.assertTrue(
            any(
                c["name"] == "verified" for c in await session.profile.context.cookies()
            )
        )
        await self.service.release(identifier)
        self.assertNotIn("secret", (await self.http.get("/v1/server")).text)

    async def test_overall_timeout_retains_interrupted_agent_evidence(self):
        self.mode = "agent_timeout"
        result = (await self.ask("Find timed-out company", timeout_seconds=2)).json()
        self.assertEqual(result["error_type"], "TimeoutError", result)
        self.assertEqual(result["error_stage"], "captcha_agent")
        self.assertEqual(len(result["challenge_runs"]), 1)
        self.assertEqual(result["challenge_runs"][0]["state"], "cancelled")
        self.assertFalse(self.service.active)

    async def test_brave_429_proof_of_work_challenge_invokes_agent_before_rejecting(
        self,
    ):
        for mode in ("pow_challenge", "pow_delayed"):
            with self.subTest(mode=mode):
                self.mode = mode
                self.agent_calls.clear()
                result = (await self.ask("Find Masmästaren Näktergalen AB")).json()
                self.assertEqual(result["status"], "success", result)
                self.assertEqual(len(result["challenge_runs"]), 1)
                self.assertEqual(len(self.agent_calls), 2)
                self.assertIn("Masmästaren Näktergalen AB", result["answer"])

    async def test_plain_rate_limit_saves_evidence_without_starting_agent(self):
        self.mode = "rate_limited"
        result = (await self.ask("Rate limited", page_timeout_seconds=0.3)).json()
        self.assertEqual(result["error_type"], "RateLimited", result)
        self.assertEqual(result["http_status"], 429)
        self.assertEqual(result["challenge_runs"], [])
        self.assertEqual(result["failure_evidence"]["retry_after"], "120")
        directory = self.service.root / "brave-requests" / result["request_id"]
        self.assertIn("Too many requests", (directory / "failure.html").read_text())
        self.assertTrue((directory / "failure.png").read_bytes().startswith(b"\x89PNG"))
