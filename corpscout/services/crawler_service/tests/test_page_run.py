"""Exercise the JSON command at the model HTTP boundary without external services."""

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from test_catalog_search import catalog_fixture
from test_mentions import HTML, decision, mention

from crawler_service import page_run
from crawler_service.models import OBJECTIVES
from crawler_service.storage import content_hash, write_json


class PageRunTests(unittest.TestCase):
    def test_json_stdout_and_optional_local_diagnostics(self):
        for keep_diagnostics, api in (
            (False, "deepseek"),
            (True, "deepseek"),
            (True, "openrouter"),
        ):
            with self.subTest(keep_diagnostics=keep_diagnostics, api=api):
                with TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    catalog = root / "catalog.json"
                    write_json(catalog, catalog_fixture().snapshot.model_dump())
                    targets = ["https://example.test/"]
                    if keep_diagnostics:
                        targets.append("https://another.test/")
                    argv = [
                        "crawler-service-pages",
                        "--catalog",
                        str(catalog),
                        "--offline-catalog",
                    ]
                    if api == "openrouter":
                        argv.extend(
                            [
                                "--api",
                                "openrouter",
                                "--model",
                                "z-ai/glm-5.3-flash",
                                "--provider",
                                "baseten/fp8",
                            ]
                        )
                    for index, target in enumerate(targets):
                        source = root / f"page-{index}"
                        source.mkdir()
                        (source / "page.html").write_text(HTML, encoding="utf-8")
                        write_json(
                            source / "input.json",
                            {
                                "page": {
                                    "page_id": str(index),
                                    "source_url": target,
                                    "html_file": "page.html",
                                    "html_sha256": content_hash(HTML),
                                    "fetched_at": "2026-09-17T00:00:00Z",
                                },
                                "target_url": target,
                                "links": [],
                                "headings": [],
                            },
                        )
                        argv.extend(["--page", str(source)])
                    output = root / "diagnostics"
                    if keep_diagnostics:
                        argv.extend(["--output", str(output)])
                    requests = []

                    def handle(
                        request: httpx.Request,
                        requests: list[httpx.Request] = requests,
                    ) -> httpx.Response:
                        requests.append(request)
                        body = json.loads(request.content)
                        prompt = body["messages"][1]["content"]
                        if "SOURCE SNAPSHOT:" in prompt:
                            snapshot = json.loads(prompt.split("SOURCE SNAPSHOT:\n")[1])
                            document = {
                                "data": {
                                    key: []
                                    for key in OBJECTIVES
                                    if key != "technology_signals"
                                },
                                "technology_mentions": [
                                    mention(snapshot["source_sections"])
                                ],
                                "links": [],
                            }
                        else:
                            batch = json.loads(
                                prompt.split("MENTIONS AND ORIGINAL SECTIONS:\n")[1]
                            )
                            document = {"decisions": [decision(batch["mentions"][0])]}
                        # Simulate a dependency that prints to stdout itself.
                        print("library progress")
                        return httpx.Response(
                            200,
                            json={
                                "choices": [
                                    {
                                        "finish_reason": "stop",
                                        "message": {"content": json.dumps(document)},
                                    }
                                ],
                                "usage": {
                                    "prompt_tokens": 10,
                                    "completion_tokens": 20,
                                },
                            },
                        )

                    client = httpx.AsyncClient(
                        base_url="https://api.deepseek.com/"
                        if api == "deepseek"
                        else "https://openrouter.ai/api/v1/",
                        transport=httpx.MockTransport(handle),
                    )
                    stdout, stderr = io.StringIO(), io.StringIO()
                    with (
                        patch("sys.argv", argv),
                        patch.dict(
                            "os.environ",
                            {
                                "DEEPSEEK": "never-save-test-key",
                                "OPENROUTER_API_KEY": "router-test-key",
                            },
                        ),
                        patch("tempfile.tempdir", str(root)),
                        patch(
                            "crawler_service.analysis.httpx.AsyncClient",
                            return_value=client,
                        ),
                        redirect_stdout(stdout),
                        redirect_stderr(stderr),
                    ):
                        page_run.main()
                    payload = json.loads(stdout.getvalue())
                    self.assertIsInstance(payload, dict)
                    results = payload["results"] if keep_diagnostics else [payload]
                    self.assertEqual([r["target_url"] for r in results], targets)
                    for result in results:
                        self.assertEqual(
                            result["schema_version"], "company-research-result/1.0"
                        )
                        self.assertEqual(
                            result["pages"][0]["captures"]["native_cleaned_html"][
                                "content"
                            ],
                            HTML,
                        )
                        self.assertEqual(result["model_usage"]["calls"], 2)
                        self.assertEqual(result["model_api"], api)
                        self.assertEqual(
                            result["technology_classification"]["decisions"][0][
                                "classification"
                            ]["relationships"][0]["scope"],
                            "team",
                        )
                    self.assertNotIn("never-save-test-key", stdout.getvalue())
                    self.assertNotIn("router-test-key", stdout.getvalue())
                    self.assertIn("library progress", stderr.getvalue())
                    self.assertIn("Collected", stderr.getvalue())
                    self.assertEqual(
                        {request.url.host for request in requests},
                        {"api.deepseek.com" if api == "deepseek" else "openrouter.ai"},
                    )
                    for request in requests:
                        body = json.loads(request.content)
                        self.assertEqual(
                            body["response_format"], {"type": "json_object"}
                        )
                        self.assertIn(
                            "Required JSON Schema", body["messages"][0]["content"]
                        )
                        self.assertEqual(body["max_tokens"], 65536)
                        if api == "openrouter":
                            self.assertEqual(body["model"], "z-ai/glm-5.3-flash")
                            self.assertEqual(body["provider"]["only"], ["baseten/fp8"])
                            self.assertFalse(body["provider"]["allow_fallbacks"])
                            self.assertEqual(body["reasoning"]["effort"], "high")
                            self.assertEqual(
                                request.headers["Authorization"],
                                "Bearer router-test-key",
                            )
                        else:
                            self.assertEqual(body["model"], "deepseek-flash")
                            self.assertEqual(body["reasoning_effort"], "high")
                    self.assertEqual(list(root.glob("crawler-service-*")), [])
                    self.assertEqual(output.exists(), keep_diagnostics)
                    if keep_diagnostics:
                        saved = list(output.glob("company-*/result.json"))
                        self.assertEqual(len(saved), 2)
                        manifest = json.loads(
                            (output / "manifest.json").read_text(encoding="utf-8")
                        )
                        self.assertEqual(manifest["status"], "finished")
                        self.assertTrue(
                            all("upload" not in item for item in manifest["results"])
                        )
