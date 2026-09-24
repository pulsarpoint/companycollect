"""Exercise collection at browser/model boundaries and replay its local artifacts."""

import argparse
import json
import shutil
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from test_catalog_search import catalog_fixture
from test_mentions import HTML, decision, mention
from test_package import response
from test_site_gate import classification

from crawler_service import page_run
from crawler_service.browser import PageCapture
from crawler_service.captures import load_crawl
from crawler_service.crawl import DEFAULT_SELECTION_INSTRUCTIONS, crawl_company
from crawler_service.models import OBJECTIVES, ResearchConfig
from crawler_service.page_agent import PageInput
from crawler_service.storage import content_hash, write_json


@asynccontextmanager
async def browser_responses(responses: dict, requested: list[str]):
    async def navigate(url, *, timeout_seconds, check_robots_txt):
        requested.append(url)
        html, links, status, redirect = responses[url]
        return PageCapture(
            url=redirect or url,
            html=html,
            cleaned_html=html,
            status_code=status,
            headers={},
            metadata={},
            links=links,
            error="denied" if status == 403 else None,
            redirects=[{"url": url, "location": redirect, "status_code": 301}]
            if redirect else [],
            navigation_attempts=[{"url": url, "error": None}],
        )

    yield SimpleNamespace(navigate=navigate)


class CrawlTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_pages_need_no_model_and_manifest_survives_move_and_failures(
        self,
    ):
        requested = []
        responses = {
            "https://example.test/jobs": (
                HTML,
                [{"href": "https://example.test/unrequested"}],
                200,
                "https://example.test/careers",
            ),
            "https://example.test/failure": ("denied", [], 403, None),
        }
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "crawl"
            with (
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda responses=responses, requested=requested: browser_responses(
                        responses, requested
                    ),
                ),
                patch.object(
                    httpx.AsyncClient,
                    "send",
                    side_effect=AssertionError("No network outside browser boundary"),
                ),
                patch.dict("os.environ", {}, clear=True),
            ):
                manifest = await crawl_company(
                    "https://example.test/",
                    output_dir=root,
                    pages=[
                        "/jobs#opening",
                        "/jobs?utm_source=mail",
                        "/careers",
                        "/failure",
                    ],
                )
            self.assertEqual(requested, list(responses))
            self.assertEqual(manifest["status"], "partial")
            self.assertEqual(manifest["usage"]["calls"], 0)
            self.assertEqual(manifest["site_gate"]["reason"], "supplied_pages")
            self.assertFalse((root / "sitemaps.json").exists())
            self.assertFalse((root / "calls").exists())
            moved = Path(temporary) / "moved"
            shutil.move(root, moved)
            loaded, paths = load_crawl(moved)
            self.assertEqual(loaded, manifest)
            self.assertEqual(len(paths), 1)
            self.assertEqual(PageInput.load(paths[0]).html, HTML)
            self.assertNotIn("extraction_status", loaded["pages"][0])
            (paths[0] / "page.html").write_text("modified", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                load_crawl(moved)

    async def test_first_page_gate_precedes_sitemap_and_link_ranking_without_extraction(
        self,
    ):
        for skip in (False, True):
            with self.subTest(skip=skip), TemporaryDirectory() as temporary:
                events = []
                requested = []
                home = (
                    "<h1>Example Media</h1><p>Latest world news</p>"
                    if skip
                    else "<h1>Example</h1><p>Industrial robot design</p>"
                )
                home += '<a href="/contact">Contact</a>'
                responses = {
                    "https://example.test/": (
                        home,
                        [{"href": "https://example.test/contact", "text": "Contact"}],
                        200,
                        None,
                    ),
                    "https://example.test/contact": (
                        "<h1>Contact Example</h1>",
                        [],
                        200,
                        None,
                    ),
                }

                async def model_boundary(
                    client, request, *, events=events, skip=skip, **kwargs
                ):
                    if request.url.host == "api.deepseek.com":
                        body = json.loads(request.content)
                        prompt = next(
                            m["content"]
                            for m in body["messages"]
                            if m["role"] == "user"
                        )
                        payload = json.loads(prompt.split("INPUT DATA:\n")[1])
                        if payload.get("task") == "site_classification":
                            events.append("gate")
                            result = (
                                classification()
                                if skip
                                else classification(
                                    crawl_decision="continue_crawling",
                                    site_description="Example offers industrial robot design.",
                                    site_types=["company"],
                                    research_profiles=["service_provider"],
                                    purpose="Industrial robot design",
                                    operator_name="Example",
                                    business_activities=["Industrial robot design"],
                                    evidence=["Example", "Industrial robot design"],
                                )
                            )
                        else:
                            self.assertIn("candidates", payload)
                            self.assertEqual(
                                payload["selection_instructions"],
                                DEFAULT_SELECTION_INSTRUCTIONS,
                            )
                            self.assertNotIn("OBJECTIVES:\n", prompt)
                            self.assertNotIn('"technology_signals"', prompt)
                            events.append("rank")
                            result = {
                                "assessments": [
                                    {
                                        "candidate_id": c["candidate_id"],
                                        "target_relevance": "target",
                                        "follow_scope": "single_page",
                                        "requested_content": {
                                            "potential": "high",
                                            "role": "direct",
                                        },
                                        "reason": "Company contact information",
                                    }
                                    for c in payload["candidates"]
                                ]
                            }
                        return httpx.Response(200, json=response(result))
                    events.append(request.url.path)
                    return httpx.Response(404, text="missing")

                with (
                    patch(
                        "crawler_service.crawl.open_browser",
                        lambda responses=responses, requested=requested: (
                            browser_responses(responses, requested)
                        ),
                    ),
                    patch.object(httpx.AsyncClient, "send", model_boundary),
                    patch(
                        "crawler_service.discovery.CrawlQueue.pick",
                        side_effect=AssertionError(
                            "Legacy research priorities disabled"
                        ),
                    ),
                ):
                    manifest = await crawl_company(
                        "https://example.test/",
                        output_dir=Path(temporary),
                        api_key="test-key",
                        config=ResearchConfig(
                            model="deepseek-flash",
                            provider=None,
                            max_pages=2,
                            max_corrections=0,
                        ),
                    )
                self.assertEqual(events[0], "gate")
                if skip:
                    self.assertEqual(events, ["gate"])
                    self.assertEqual(manifest["status"], "skip_crawling")
                    self.assertEqual(len(requested), 1)
                    with self.assertRaisesRegex(ValueError, "skip_crawling"):
                        load_crawl(Path(temporary))
                else:
                    self.assertEqual(
                        events, ["gate", "/robots.txt", "/sitemap.xml", "rank"]
                    )
                    self.assertEqual(requested, list(responses))
                    self.assertEqual(manifest["stop_reason"], "page_budget")
                    self.assertEqual(manifest["usage"]["calls"], 2)
                    self.assertEqual(manifest["processing"]["job_analysis"], "deferred")
                    self.assertEqual(
                        manifest["processing"]["technology_analysis"], "deferred"
                    )
                    self.assertIsNone(manifest["selection_instructions"])

    async def test_invalid_inputs_do_not_start_browser_or_create_output(self):
        with TemporaryDirectory() as temporary:
            for pages in ([], ["/report.pdf"], ["/a", "/b"]):
                root = Path(temporary) / "run"
                with self.subTest(pages=pages), self.assertRaises(ValueError):
                    await crawl_company(
                        "https://example.test/",
                        output_dir=root,
                        pages=pages,
                        config=ResearchConfig(max_pages=1),
                    )
                self.assertFalse(root.exists())
            (Path(temporary) / "keep.txt").write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not empty"):
                await crawl_company(
                    "https://example.test/", output_dir=Path(temporary), pages=["/a"]
                )

    async def test_failed_site_classification_blocks_followups_and_analysis(self):
        requested = []
        responses = {
            "https://example.test/": (
                HTML,
                [{"href": "https://example.test/jobs"}],
                200,
                None,
            ),
        }

        async def invalid_model(client, request, **kwargs):
            self.assertEqual(request.url.host, "api.deepseek.com")
            return httpx.Response(200, json=response({"invalid": True}))

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda: browser_responses(responses, requested),
                ),
                patch.object(httpx.AsyncClient, "send", invalid_model),
            ):
                manifest = await crawl_company(
                    "https://example.test/",
                    output_dir=root,
                    api_key="test-key",
                    config=ResearchConfig(
                        model="deepseek-flash", provider=None, max_corrections=0
                    ),
                )
            self.assertEqual(manifest["status"], "needs_review")
            self.assertEqual(manifest["site_gate"]["decision"], "needs_review")
            self.assertEqual(requested, ["https://example.test/"])
            self.assertEqual(manifest["usage"]["calls"], 1)
            self.assertEqual(
                (root / "pages/p0001/page.html").read_text(encoding="utf-8"), HTML
            )
            with self.assertRaisesRegex(ValueError, "needs_review"):
                load_crawl(root)

    async def test_crawl_to_llm_replay_never_recrawls_and_preserves_partial_capture(
        self,
    ):
        for save_artifacts in (True, False):
            requested = []
            responses = {
                "https://example.test/jobs": (HTML, [], 200, None),
                "https://example.test/failure": ("denied", [], 403, None),
            }
            with TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch(
                    "crawler_service.crawl.open_browser",
                    lambda responses=responses, requested=requested: browser_responses(
                        responses, requested
                    ),
                ):
                    await crawl_company(
                        "https://example.test/",
                        output_dir=root / "crawl",
                        pages=list(responses),
                        save_artifacts=save_artifacts,
                    )
                files_before = {
                    str(path): content_hash(path.read_text(encoding="utf-8"))
                    for path in (root / "crawl").rglob("*")
                    if path.is_file()
                }
                catalog = root / "catalog.json"
                write_json(catalog, catalog_fixture().snapshot.model_dump())

                def handle(request):
                    self.assertEqual(request.url.host, "api.deepseek.com")
                    prompt = json.loads(request.content)["messages"][1]["content"]
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
                    return httpx.Response(200, json=response(document))

                client = httpx.AsyncClient(
                    transport=httpx.MockTransport(handle),
                    base_url="https://api.deepseek.com/",
                )
                with (
                    patch.dict("os.environ", {"DEEPSEEK": "test-key"}),
                    patch(
                        "crawler_service.analysis.httpx.AsyncClient",
                        return_value=client,
                    ),
                    patch(
                        "crawler_service.crawl.open_browser",
                        side_effect=AssertionError("Analysis must not fetch"),
                    ),
                ):
                    result = await page_run.run(
                        argparse.Namespace(
                            crawl=root / "crawl"
                            if save_artifacts
                            else root / "crawl/result.json",
                            page=None,
                            output=root / "analysis",
                            api="deepseek",
                            model="deepseek-flash",
                            provider=None,
                            reasoning_effort="high",
                            timeout=30,
                            env_file=[],
                            catalog=catalog,
                            offline_catalog=True,
                        )
                    )
                self.assertEqual(result["crawl_status"], "partial")
                self.assertEqual(result["processing_status"], "partial")
                self.assertEqual(
                    result["pages"][0]["captures"]["native_cleaned_html"]["content"],
                    HTML,
                )
                self.assertEqual(len(result["crawl"]["pages"]), 2)
                self.assertEqual(result["model_usage"]["calls"], 2)
                capture = json.loads((root / "crawl/result.json").read_text())
                observed = capture["documents"][0]["input"]["observations"]
                self.assertEqual(result["pages"][0]["observations"], observed)
                self.assertEqual(result["page_observations"], [observed])
                self.assertEqual(
                    files_before,
                    {
                        str(path): content_hash(path.read_text(encoding="utf-8"))
                        for path in (root / "crawl").rglob("*")
                        if path.is_file()
                    },
                )
