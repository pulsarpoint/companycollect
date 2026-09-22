"""Opt-in Xvfb/CDP integration against our own cross-origin checkbox fixture."""

import asyncio
import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import httpx

from browser_service.challenge_agent import ChallengeAgent
from browser_service.runtime import BrowserRuntimeSettings, BrowserService


class ChallengeFixture(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        if self.path == "/challenge":
            html = """<html><body style="margin:0;font:20px sans-serif;background:#eee">
<button style="position:absolute;left:30px;top:40px;width:250px;height:60px"
onclick="parent.postMessage('fixture-verified','*')">Verify you are human</button>
</body></html>"""
        else:
            html = f"""<html><head><title>Just a moment - local test fixture</title></head>
<body style="margin:0;font:24px sans-serif"><h1>Local test verification</h1>
<iframe title="Verification" style="position:absolute;left:70px;top:80px;width:400px;height:200px;border:0"
src="http://127.0.0.1:{self.server.server_port}/challenge"></iframe>
<script>addEventListener('message',event=>{{if(event.data==='fixture-verified'){{
document.title='Company fixture';document.body.innerHTML='<h1>Verification complete</h1><p>Company information is now available.</p>';}}}});</script>
</body></html>"""
        self.wfile.write(html.encode())


@unittest.skipUnless(
    os.environ.get("COMPANY_RESEARCH_XVFB_TEST") == "1", "opt-in Linux/Xvfb test"
)
class NativeChallengeAgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_cross_origin_click_uses_existing_browser_and_preserves_lease(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), ChallengeFixture)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with TemporaryDirectory() as directory:
                service = BrowserService(
                    Path(directory),
                    settings=BrowserRuntimeSettings(
                        max_browsers=1,
                        idle_timeout_seconds=120,
                        session_retention_days=7,
                    ),
                )
                await service.start()
                try:
                    session = await service.claim(
                        identifier=uuid4().hex,
                        request_id="agent-fixture",
                        domain="localhost",
                    )
                    tab = await service.open_tab(session, "site")
                    await tab.page.goto(
                        f"http://localhost:{server.server_port}/",
                        wait_until="networkidle",
                    )
                    generation = session.profile.generation
                    calls = []

                    def model(request):
                        payload = json.loads(request.content)
                        calls.append(payload)
                        action = (
                            {
                                "action": "click",
                                "x": 200,
                                "y": 150,
                                "reason": "Click the visible verification button",
                            }
                            if len(calls) == 1
                            else {
                                "action": "finish",
                                "outcome": "appears_clear",
                                "reason": "Verification is complete",
                            }
                        )
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

                    agent = ChallengeAgent(
                        service,
                        session,
                        "site",
                        max_steps=5,
                        timeout_seconds=60,
                        model="deepseek-flash",
                    )
                    live_model = (
                        os.environ.get("CHALLENGE_AGENT_LIVE_MODEL_TEST") == "1"
                    )
                    async with (
                        session.lock,
                        httpx.AsyncClient(
                            base_url="https://api.deepseek.com",
                            timeout=45,
                            headers={
                                "Authorization": "Bearer " + os.environ["DEEPSEEK"]
                            }
                            if live_model
                            else {},
                            transport=None
                            if live_model
                            else httpx.MockTransport(model),
                        ) as http,
                    ):
                        result = await agent.run(http)
                    self.assertEqual(result["state"], "appears_clear", result)
                    self.assertIn(
                        "Company information is now available",
                        await tab.page.inner_text("body"),
                    )
                    self.assertEqual(service.snapshot(session.id)["state"], "ready")
                    self.assertEqual(session.profile.generation, generation)
                    self.assertEqual(len(service.active), 1)
                    self.assertTrue(
                        (agent.directory / "01.png").read_bytes().startswith(b"\x89PNG")
                    )
                    if live_model:
                        print(
                            "LIVE_AGENT_RESULT "
                            + json.dumps(
                                {
                                    key: result[key]
                                    for key in (
                                        "state",
                                        "steps",
                                        "usage",
                                        "elapsedSeconds",
                                    )
                                }
                            )
                        )
                    else:
                        self.assertEqual(len(calls), 2)
                finally:
                    await service.close()
        finally:
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            thread.join()
