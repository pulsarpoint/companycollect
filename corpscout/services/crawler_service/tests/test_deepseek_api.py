"""Exercise the direct API at the HTTP boundary without using a live key."""

import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import catalog_fixture
from test_package import response

from crawler_service.llm import ModelClient, ModelUnavailable
from crawler_service.models import ResearchConfig


class DeepSeekAPITests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_tool_round_preserves_reasoning_and_local_catalog(self):
        requests = []
        schema = {"type": "object", "properties": {"technology": {"type": "string"}}}

        def handle(request):
            self.assertEqual(
                str(request.url), "https://api.deepseek.com/chat/completions"
            )
            self.assertEqual(request.headers["authorization"], "Bearer fake-secret")
            body = json.loads(request.content)
            requests.append(body)
            self.assertNotIn("provider", body)
            self.assertNotIn("reasoning", body)
            self.assertNotIn("temperature", body)
            self.assertEqual(body["model"], "deepseek-flash")
            self.assertEqual(body["thinking"], {"type": "enabled"})
            self.assertEqual(body["reasoning_effort"], "low")
            self.assertEqual(body["max_tokens"], 65536)
            self.assertEqual(body["response_format"], {"type": "json_object"})
            self.assertIn(
                json.dumps(schema, sort_keys=True), body["messages"][0]["content"]
            )
            if len(requests) == 1:
                return httpx.Response(
                    200,
                    json={
                        "model": "deepseek-flash",
                        "choices": [
                            {
                                "finish_reason": "tool_calls",
                                "message": {
                                    "role": "assistant",
                                    "content": None,
                                    "reasoning_content": "Need the catalog result.",
                                    "tool_calls": [
                                        {
                                            "id": "lookup",
                                            "type": "function",
                                            "function": {
                                                "name": "search_technologies",
                                                "arguments": '{"queries":["Python"]}',
                                            },
                                        }
                                    ],
                                },
                            }
                        ],
                    },
                )
            self.assertEqual(
                body["messages"][2]["reasoning_content"], "Need the catalog result."
            )
            self.assertEqual(body["messages"][3]["role"], "tool")
            self.assertEqual(body["messages"][3]["tool_call_id"], "lookup")
            self.assertIn("Python", body["messages"][3]["content"])
            payload = response({"technology": "Python"})
            payload.update(
                model="deepseek-flash",
                usage={"prompt_tokens": 100, "completion_tokens": 20},
            )
            return httpx.Response(200, json=payload)

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(
                    client,
                    "fake-secret",
                    ResearchConfig(model="deepseek-flash"),
                    root,
                    api="deepseek",
                )
                reply = await llm.ask(
                    "Find Python and return JSON.",
                    schema,
                    task="catalog",
                    catalog=catalog_fixture(),
                )
            self.assertEqual(reply.document, {"technology": "Python"})
            self.assertEqual(len(reply.searches), 1)
            self.assertEqual(llm.calls[-1]["response_model"], "deepseek-flash")
            self.assertEqual(llm.usage()["unknown_cost_calls"], 2)
            for path in (root / "calls").glob("*.json"):
                self.assertNotIn("fake-secret", path.read_text(encoding="utf-8"))

    async def test_both_endpoints_receive_identical_json_mode_prompts(self):
        requests = []

        def handle(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=response({"value": 1}))

        with TemporaryDirectory() as directory:
            for api in ("openrouter", "deepseek"):
                async with httpx.AsyncClient(
                    base_url="https://test.invalid/",
                    transport=httpx.MockTransport(handle),
                ) as client:
                    llm = ModelClient(
                        client,
                        "fake",
                        ResearchConfig(reasoning_effort="none"),
                        Path(directory) / api,
                        api=api,
                        json_mode="json_object",
                    )
                    await llm.ask("Return JSON.", {"type": "object"}, task="comparison")
        self.assertEqual(requests[0]["messages"], requests[1]["messages"])
        self.assertEqual(requests[0]["response_format"], requests[1]["response_format"])
        self.assertEqual(requests[1]["thinking"], {"type": "disabled"})

    async def test_direct_auth_errors_are_redacted_and_stop_requests(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        401, json={"error": {"message": "Bad fake-secret"}}
                    )
                ),
            ) as client:
                llm = ModelClient(
                    client, "fake-secret", ResearchConfig(), root, api="deepseek"
                )
                with self.assertRaisesRegex(ModelUnavailable, "DeepSeek HTTP 401"):
                    await llm.ask("Return JSON.", {}, task="auth")
                with self.assertRaises(ModelUnavailable):
                    await llm.ask("Return JSON.", {}, task="auth")
            self.assertEqual(len(llm.calls), 1)
            self.assertNotIn(
                "fake-secret", (root / "calls/00001.json").read_text(encoding="utf-8")
            )

    async def test_direct_deadline_and_truncation_remain_failures(self):
        async def handle(request):
            await asyncio.sleep(0.02)
            return httpx.Response(200, json=response({}))

        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(
                    client,
                    "fake",
                    ResearchConfig(model_timeout_seconds=0.001),
                    Path(directory),
                    api="deepseek",
                )
                reply = await llm.ask("Return JSON.", {}, task="deadline")
                assert reply.error is not None
                self.assertIn("DeepSeek exceeded", reply.error)
            payload = response({"complete": False})
            payload["choices"][0]["finish_reason"] = "length"
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, json=payload)
                ),
            ) as client:
                llm = ModelClient(
                    client,
                    "fake",
                    ResearchConfig(),
                    Path(directory) / "truncated",
                    api="deepseek",
                )
                reply = await llm.ask("Return JSON.", {}, task="truncated")
                self.assertIsNone(reply.document)
                self.assertEqual(reply.error, "Incomplete model response (length)")
