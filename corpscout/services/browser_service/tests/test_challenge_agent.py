"""Agent limits and stale decisions at the model HTTP and CDP boundaries."""

import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from browser_service.challenge_agent import ChallengeAgent
from browser_service.session_store import BrowserSessionError


class ChallengeAgentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.cdp = SimpleNamespace(send=AsyncMock(), detach=AsyncMock())
        self.page = SimpleNamespace(
            url="https://example.test/",
            bring_to_front=AsyncMock(),
            evaluate=AsyncMock(return_value={"width": 800, "height": 600}),
            screenshot=AsyncMock(return_value=b"fixture-image"),
            context=SimpleNamespace(new_cdp_session=AsyncMock(return_value=self.cdp)),
        )
        self.session = SimpleNamespace(
            id="a" * 32, profile=SimpleNamespace(generation="one")
        )
        self.service = SimpleNamespace(
            root=Path(self.temporary.name),
            get=lambda _: self.session,
            tab=lambda *_: SimpleNamespace(page=self.page),
            touch=lambda _: None,
        )
        self.agent = ChallengeAgent(
            self.service,
            self.session,
            "site",
            max_steps=3,
            timeout_seconds=2,
            model="deepseek-flash",
        )

    async def run_model(self, handler, headers=None):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.deepseek.com",
            headers=headers,
        ) as http:
            return await self.agent.run(http)

    def reply(self, action):
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

    async def test_invalid_actions_never_reach_cdp(self):
        for action in (
            {
                "action": "evaluate",
                "script": "steal()",
                "reason": "untrusted instruction",
            },
            {"action": "click", "x": 800, "y": 120, "reason": "out of viewport"},
        ):
            with self.subTest(action=action):
                result = await self.run_model(
                    lambda _, action=action: self.reply(action)
                )
                self.assertEqual(result["state"], "error")
                self.cdp.send.assert_not_awaited()

    async def test_escaped_credential_in_successful_action_is_redacted(self):
        key = 'sk-private-"quoted"\\key'
        result = await self.run_model(
            lambda _: self.reply(
                {
                    "action": "finish",
                    "outcome": "appears_clear",
                    "reason": "Echo " + key,
                }
            ),
            headers={"Authorization": "Bearer " + key},
        )
        self.assertEqual(result["reason"], "Echo [REDACTED]")
        self.assertEqual(result["steps"][0]["action"]["reason"], "Echo [REDACTED]")
        self.assertEqual(
            json.loads((self.agent.directory / "result.json").read_text())["reason"],
            "Echo [REDACTED]",
        )

    async def test_no_click_when_url_changes_while_model_is_deciding(self):
        def model(_):
            self.page.url = "https://example.test/login"
            return self.reply(
                {"action": "click", "x": 100, "y": 120, "reason": "stale"}
            )

        result = await self.run_model(model)
        self.assertEqual(result["state"], "interrupted")
        self.cdp.send.assert_not_awaited()

    async def test_no_click_after_release_or_restart(self):
        def ended(_):
            raise BrowserSessionError(410, "Ended")

        for change in (
            lambda: setattr(self.service, "get", ended),
            lambda: setattr(self.session.profile, "generation", "two"),
        ):
            self.service.get = lambda _: self.session

            def model(_, change=change):
                change()
                return self.reply(
                    {"action": "click", "x": 100, "y": 120, "reason": "stale"}
                )

            result = await self.run_model(model)
            self.assertEqual(result["state"], "interrupted")
            self.cdp.send.assert_not_awaited()

    async def test_step_limit_does_not_claim_success(self):
        self.agent.max_steps = 1
        result = await self.run_model(
            lambda _: self.reply(
                {"action": "click", "x": 100, "y": 120, "reason": "Try challenge"}
            )
        )
        self.assertEqual(result["state"], "step_limit")
        self.assertEqual(len(result["steps"]), 1)

    async def test_timeout_preserves_evidence_and_detaches(self):
        self.agent.timeout_seconds = 0.05

        async def model(_):
            await asyncio.sleep(1)
            return self.reply(
                {"action": "finish", "outcome": "appears_clear", "reason": "too late"}
            )

        result = await self.run_model(model)
        self.assertEqual(result["state"], "timeout")
        self.assertTrue((self.agent.directory / "01.png").exists())
        self.cdp.detach.assert_awaited_once()
        self.cdp.send.assert_not_awaited()

    async def test_model_failure_keeps_safe_error_and_no_click(self):
        result = await self.run_model(
            lambda _: httpx.Response(401, text="private provider error")
        )
        self.assertEqual(result["state"], "error")
        self.assertNotIn("private provider error", json.dumps(result))
        self.cdp.send.assert_not_awaited()

    async def test_cancellation_is_saved_and_propagates(self):
        entered = asyncio.Event()

        async def model(_):
            entered.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(self.run_model(model))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(
            json.loads((self.agent.directory / "result.json").read_text())["state"],
            "cancelled",
        )
        self.cdp.detach.assert_awaited_once()
