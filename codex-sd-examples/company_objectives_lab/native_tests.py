"""Exercise native Crawl4AI and its real LiteLLM HTTP boundary against a local server."""

import json
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

import crawl4ai.utils
from crawl4ai import LLMConfig
from crawl4ai.adaptive_crawler import EmbeddingStrategy
from crawl4ai.extraction_strategy import LLMExtractionStrategy

from company_objectives_lab.models import RECORD_TYPES, Extraction
from company_objectives_lab.native import (
    TASK,
    record_completions,
    repair_query_envelope,
)
from company_objectives_lab.native_report import inspect_blocks


@contextmanager
def completion_server(content: str):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(
                json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            )
            body = json.dumps(
                {
                    "id": "test-response",
                    "object": "chat.completion",
                    "created": 1,
                    "model": "native-test",
                    "provider": "local-test",
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {"role": "assistant", "content": content},
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 20,
                        "completion_tokens": 10,
                        "total_tokens": 30,
                    },
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class NativeBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_keeps_a_record_whose_quotation_our_gate_rejects(self):
        document = {key: [] for key in RECORD_TYPES}
        document["people"] = [
            {
                "name": "Ada Example",
                "role": "CEO",
                "company": "Example",
                "profile_url": None,
                "as_of": None,
                "evidence": "CEO Ada Example",
            }
        ]
        html = "<main><h1>Example</h1><p>CEO File title: Ada Example</p></main>"
        content = "<blocks>" + json.dumps([document]) + "</blocks>"
        with (
            completion_server(content) as (base_url, requests),
            TemporaryDirectory() as directory,
        ):
            root = Path(directory)
            strategy = LLMExtractionStrategy(
                llm_config=LLMConfig(
                    provider="openai/native-test",
                    api_token="test-secret",
                    base_url=base_url,
                    backoff_max_attempts=1,
                ),
                schema=Extraction.model_json_schema(),
                instruction="Extract supported people.",
                input_format="html",
                apply_chunking=False,
            )
            token = TASK.set("sample")
            try:
                with record_completions(
                    root,
                    "test-secret",
                    {
                        "max_tokens": 1024,
                        "timeout": 10,
                        "num_retries": 0,
                        "extra_body": {"provider": {"only": ["test-route"]}},
                    },
                ):
                    blocks = await strategy.arun("https://example.test/", [html])
            finally:
                TASK.reset(token)
            self.assertEqual(blocks[0]["people"], document["people"])
            self.assertFalse(blocks[0]["error"])
            audit = inspect_blocks(blocks, "https://example.test/", html)
            self.assertEqual(len(audit["records"]), 1)
            self.assertEqual(audit["after_common_gate"], [])
            self.assertEqual(audit["issues"][0]["type"], "evidence_absent")
            self.assertEqual(len(requests), 1)
            self.assertEqual(requests[0]["provider"], {"only": ["test-route"]})
            saved_text = (root / "calls/sample/001.json").read_text(encoding="utf-8")
            self.assertNotIn("test-secret", saved_text)
            saved = json.loads(saved_text)
            self.assertEqual(
                saved["response"]["choices"][0]["message"]["content"], content
            )
            self.assertEqual(saved["request"]["messages"], requests[0]["messages"])

    async def test_native_query_array_failure_and_explicit_envelope_adapter(self):
        content = json.dumps(["company contacts", "company services", "company people"])
        with (
            completion_server(content) as (base_url, requests),
            TemporaryDirectory() as directory,
        ):
            root = Path(directory)
            config = LLMConfig(
                provider="openai/native-test",
                api_token="test-secret",
                base_url=base_url,
                backoff_max_attempts=1,
            )
            strategy = EmbeddingStrategy(query_llm_config=config)
            token = TASK.set("sample")
            try:
                with record_completions(
                    root, "test-secret", {"timeout": 10, "num_retries": 0}
                ):
                    with self.assertRaisesRegex(TypeError, "list indices"):
                        await strategy.map_query_semantic_space("company information")
                    with repair_query_envelope(root):
                        response = crawl4ai.utils.perform_completion_with_backoff(
                            provider=config.provider,
                            api_token=config.api_token,
                            base_url=base_url,
                            json_response=True,
                            max_attempts=1,
                            prompt_with_variables="Generate queries as a JSON array of strings.",
                        )
            finally:
                TASK.reset(token)
            self.assertEqual(
                json.loads(response.choices[0].message.content),
                {"queries": json.loads(content)},
            )
            self.assertEqual(len(requests), 2)
            saved = json.loads(
                (root / "calls/sample/002.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                saved["response"]["choices"][0]["message"]["content"], content
            )
            self.assertTrue((root / "query-envelope-repairs/sample.json").exists())


if __name__ == "__main__":
    unittest.main()
