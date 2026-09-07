"""Verify the real local HTTP boundary and evidence/usage capture."""

import json
import tempfile
import unittest
from pathlib import Path

import httpx

from company_full_analysis_lab.compare import compare
from company_full_analysis_lab.run import load_records, summarize
from company_full_analysis_lab.transport import (
    ResponsesRecorder,
    append_json,
    public_value,
)


class RecorderTests(unittest.IsolatedAsyncioTestCase):
    def test_smoke_probe_cannot_be_compared_with_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "run"
            reference = root / "reference"
            run.mkdir()
            reference.mkdir()
            (run / "run.json").write_text(
                json.dumps({"status": "completed", "prompt_sha256": "smoke"})
            )
            (reference / "metadata.json").write_text(
                json.dumps({"prompt_sha256": "real-user-prompt"})
            )
            with self.assertRaisesRegex(ValueError, "Prompt mismatch"):
                compare(run, reference)

    async def test_sse_forwarding_preserves_tools_and_usage_without_logging_reasoning(
        self,
    ) -> None:
        events = [
            {
                "type": "response.output_item.done",
                "item": {"type": "reasoning", "content": ["PRIVATE_THOUGHT"]},
            },
            {
                "type": "response.output_item.done",
                "item": {
                    "type": "openrouter:web_search",
                    "id": "search-1",
                    "action": {"query": "example company"},
                },
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "gen-1",
                    "status": "completed",
                    "usage": {"cost": 0.025, "input_tokens": 25},
                    "output": [
                        {"type": "reasoning", "content": ["PRIVATE_THOUGHT"]},
                        {
                            "type": "message",
                            "content": [
                                {"type": "output_text", "text": "Public report"}
                            ],
                        },
                    ],
                },
            },
        ]
        wire = (
            "".join("data: " + json.dumps(event) + "\n\n" for event in events)
            + "data: [DONE]\n\n"
        )

        def upstream(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url, "https://openrouter.ai/api/v1/responses")
            self.assertEqual(
                json.loads(request.content)["input"], "Exact original prompt"
            )
            return httpx.Response(
                200, text=wire, headers={"Content-Type": "text/event-stream"}
            )

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            recorder = ResponsesRecorder("TEST_SECRET", output)
            await recorder.client.aclose()
            recorder.client = httpx.AsyncClient(
                base_url="https://openrouter.ai/api/v1/",
                transport=httpx.MockTransport(upstream),
            )
            try:
                url = await recorder.start()
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        url + "/responses",
                        json={
                            "model": "test",
                            "input": "Exact original prompt",
                            "tools": [{"type": "web_search"}],
                            "stream": True,
                        },
                    )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.text, wire)
                saved = (output / "provider-events.jsonl").read_text()
                self.assertNotIn("PRIVATE_THOUGHT", saved)
                self.assertNotIn("TEST_SECRET", saved)
                self.assertIn("example company", saved)
                self.assertEqual(summarize(output)["known_cost_usd"], 0.025)
                self.assertIsNone(
                    load_records(output / "requests.jsonl")[0]["max_output_tokens"]
                )
            finally:
                await recorder.close()

    def test_unknown_cost_is_not_reported_as_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            append_json(
                output / "provider-events.jsonl",
                {"type": "response.completed", "usage": {"input_tokens": 5}},
            )
            result = summarize(output)
            self.assertIsNone(result["known_cost_usd"])
            self.assertEqual(result["responses_without_reported_cost"], 1)

    def test_nested_turn_reasoning_is_removed_without_losing_public_evidence(
        self,
    ) -> None:
        value = {
            "turn": {
                "items": [
                    {"type": "reasoning", "content": ["private"]},
                    {"type": "agentMessage", "text": "public"},
                ]
            },
            "reasoning_details": [{"text": "private"}],
        }
        self.assertEqual(
            public_value(value),
            {"turn": {"items": [{"type": "agentMessage", "text": "public"}]}},
        )


if __name__ == "__main__":
    unittest.main()
