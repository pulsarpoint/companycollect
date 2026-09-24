"""Exercise selector replay against an HTTP boundary without live model charges."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from compare_selector import compare

from crawler_service.models import ResearchConfig
from crawler_service.prompts import selection_prompt
from crawler_service.storage import write_json


class SelectorReplayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.baseline = self.root / "baseline"
        self.config = ResearchConfig(
            model="test/model", provider="test-provider", reasoning_effort="high"
        )
        self.assessment = {
            "candidate_id": "c1",
            "requested_content": {"potential": "high", "role": "direct"},
            "target_relevance": "target",
            "follow_scope": "single_page",
            "reason": "A named opening at the target company",
        }
        self.document = {"assessments": [self.assessment]}
        write_json(
            self.baseline / "crawl-manifest.json", {"config": self.config.model_dump()}
        )
        self.prompt = selection_prompt(
            "https://example.test/",
            [
                {
                    "candidate_id": "c1",
                    "url": "https://example.test/jobs/engineer",
                    "external": False,
                }
            ],
            instructions="Get job descriptions",
        )
        write_json(
            self.baseline / "calls/00001.json",
            {
                "task": "link_assessment",
                "request": {
                    "messages": [
                        {
                            "role": "user",
                            "content": self.prompt
                            + "\n\nCorrect the previous response.",
                        }
                    ]
                },
                "response": self.response(self.document),
                "usage": {"prompt_tokens": 10, "completion_tokens": 20},
                "elapsed_seconds": 1,
            },
        )

    def response(self, document):
        return {
            "choices": [
                {"finish_reason": "stop", "message": {"content": json.dumps(document)}}
            ],
            "provider": "Test Provider",
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "cost": 0.001},
        }

    async def test_openrouter_uses_identical_prompts_and_records_provider_and_cost(
        self,
    ):
        requests = []

        async def send(client, request, **kwargs):
            requests.append(request)
            return httpx.Response(
                200, json=self.response(self.document), request=request
            )

        with patch.object(httpx.AsyncClient, "send", send):
            result = await compare(
                self.baseline,
                self.root / "output",
                "test-secret",
                api="openrouter",
                config=self.config,
            )
        self.assertEqual(
            str(requests[0].url), "https://openrouter.ai/api/v1/chat/completions"
        )
        body = json.loads(requests[0].content)
        self.assertEqual(body["messages"][1]["content"], self.prompt)
        self.assertEqual(body["model"], "test/model")
        self.assertEqual(body["provider"]["only"], ["test-provider"])
        self.assertEqual(body["reasoning"]["effort"], "high")
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertEqual(result["eligible_agreement"], 1)
        self.assertEqual(result["response_providers"], ["Test Provider"])
        self.assertEqual(result["compact_usage"]["known_cost_usd"], 0.001)
        for path in (self.root / "output").rglob("*.json"):
            self.assertNotIn("test-secret", path.read_text())

    async def test_invalid_batch_keeps_candidates_in_denominator(self):
        async def send(client, request, **kwargs):
            return httpx.Response(
                200, json=self.response({"assessments": []}), request=request
            )

        with patch.object(httpx.AsyncClient, "send", send):
            result = await compare(self.baseline, self.root / "output", "test-secret")
        self.assertEqual(result["batch_count"], 1)
        self.assertEqual(result["valid_batches"], 0)
        self.assertEqual(result["candidate_occurrences"], 1)
        self.assertEqual(result["validated_candidate_occurrences"], 0)
        self.assertEqual(result["eligible_agreement"], 0)

    async def test_permanent_http_failure_is_saved_in_comparison(self):
        async def send(client, request, **kwargs):
            return httpx.Response(
                400,
                json={"error": {"message": "Unsupported parameter"}},
                request=request,
            )

        with patch.object(httpx.AsyncClient, "send", send):
            result = await compare(self.baseline, self.root / "output", "test-secret")
        saved = json.loads((self.root / "output/comparison.json").read_text())
        self.assertEqual(saved, result)
        self.assertEqual(result["valid_batches"], 0)
        self.assertEqual(result["attempted_batches"], 1)
        self.assertEqual(result["compact_usage"]["unknown_cost_calls"], 1)


if __name__ == "__main__":
    unittest.main()
