"""Collection entry points retain source evidence without invoking interpretation."""

import io
import json
import runpy
import unittest
from contextlib import redirect_stderr, redirect_stdout
from importlib.metadata import distribution
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from browser_http_fixture import install_browser_api, no_model_requests
from browser_http_fixture import original_send as browser_fixture_send
from test_crawl import browser_responses
from test_package import response
from test_selection_instructions import requested_assessment
from test_site_info import COMPANY, HTML, SITE

from company_research import cli
from company_research.crawl import crawl_company
from company_research.discovery import CrawlQueue, crawlable_url
from company_research.models import ResearchConfig
from company_research.service import CrawlRequest


class CollectionEntryPointTests(unittest.TestCase):
    def setUp(self):
        install_browser_api(self)

    def test_document_urls_with_trailing_slash_are_references_not_crawl_candidates(
        self,
    ):
        queue = CrawlQueue(SITE, ResearchConfig())
        for path in (
            "report.pdf/",
            "report.PDF/?download=1",
            "report.xlsx///",
            "report.docx",
        ):
            url = SITE + path
            with self.subTest(url=url):
                self.assertFalse(crawlable_url(url))
                queue.add(url, source=SITE, label="Annual report")
                self.assertNotIn(url, queue.candidates)
                self.assertIn(url, queue.document_candidates)
        self.assertTrue(crawlable_url(SITE + "reports/"))

    def test_full_cli_runs_discovery_and_bare_crawl_still_works(self):
        async def boundary(client, request, **kwargs):
            if request.url.host == "browser-fixture":
                return await browser_fixture_send(client, request, **kwargs)
            if request.url.host == "api.deepseek.com":
                prompt = json.loads(request.content)["messages"][1]["content"]
                return httpx.Response(
                    200,
                    json=response(
                        {"queries": []} if "max_new_queries" in prompt else COMPANY
                    ),
                )
            return httpx.Response(404)

        for flags, expected_mode in [
            (["--crawl", "full"], "full"),
            (["--site-info", "--crawl"], "discovery"),
        ]:
            with self.subTest(flags=flags), TemporaryDirectory() as directory:
                output = Path(directory) / "result"
                requested = []
                stdout = io.StringIO()
                with (
                    patch(
                        "sys.argv",
                        ["company-research", SITE, *flags, "--output-dir", str(output)],
                    ),
                    patch.dict("os.environ", {"DEEPSEEK": "test-key"}),
                    patch(
                        "company_research.crawl.open_browser",
                        lambda *_args, requested=requested, **_: browser_responses(
                            {SITE: (HTML, [], 200, None)}, requested
                        ),
                    ),
                    patch.object(httpx.AsyncClient, "send", boundary),
                    redirect_stdout(stdout),
                    redirect_stderr(io.StringIO()),
                ):
                    cli.main()
                manifest = json.loads(stdout.getvalue())
                self.assertEqual(manifest["mode"], expected_mode)
                self.assertIs(manifest["crawl_requested"], True)
                self.assertEqual(manifest["status"], "finished")
                self.assertGreaterEqual(manifest["elapsed_seconds"], 0)
                self.assertEqual(
                    manifest["config"]["max_pages"],
                    100 if expected_mode == "full" else 20,
                )
                self.assertEqual(
                    manifest["config"]["max_external_pages"],
                    30 if expected_mode == "full" else 3,
                )

    def test_full_service_requests_roundtrip_and_reject_conflicting_scopes(self):
        request = CrawlRequest(
            url=SITE,
            crawl="full",
            site_info=True,
            config=ResearchConfig(max_pages=40, max_external_pages=20),
        )
        replay = CrawlRequest.model_validate_json(request.model_dump_json())
        self.assertEqual(replay.crawl, "full")
        self.assertEqual(
            replay.config.model_dump(exclude_unset=True),
            {"max_pages": 40, "max_external_pages": 20},
        )
        for extra in ({"pages": [SITE]}, {"instructions": "jobs"}):
            with (
                self.subTest(extra=extra),
                self.assertRaisesRegex(ValueError, "cannot be combined"),
            ):
                CrawlRequest(url=SITE, crawl="full", **extra)
        with self.assertRaises(ValueError):
            CrawlRequest(url=SITE, crawl="everything")

    def test_installed_commands_module_and_old_entry_point_only_collect(self):
        commands = {
            entry.name: entry.load()
            for entry in distribution("company-research").entry_points
            if entry.name in {"company-research", "company-research-crawl"}
        }
        self.assertEqual(len(commands), 2)
        commands["legacy-installed-script"] = cli.main
        commands["python-module"] = lambda: runpy.run_module(
            "company_research", run_name="__main__"
        )
        for command, main in commands.items():
            with self.subTest(command=command), TemporaryDirectory() as directory:
                output = Path(directory) / "result"
                requested = []
                stdout = io.StringIO()
                with (
                    patch(
                        "sys.argv",
                        [command, SITE, "--pages", SITE, "--output-dir", str(output)],
                    ),
                    patch(
                        "company_research.crawl.open_browser",
                        lambda *_args, requested=requested, **_: browser_responses(
                            {SITE: (HTML, [], 200, None)}, requested
                        ),
                    ),
                    patch.object(
                        httpx.AsyncClient,
                        "send",
                        new=no_model_requests,
                    ),
                    patch(
                        "company_research.cli.load_catalog",
                        side_effect=AssertionError("No technology catalog"),
                    ),
                    patch(
                        "company_research.cli.research_company",
                        side_effect=AssertionError("No combined research"),
                    ),
                    redirect_stdout(stdout),
                    redirect_stderr(io.StringIO()),
                ):
                    main()
                manifest = json.loads(stdout.getvalue())
                self.assertEqual(manifest["usage"]["calls"], 0)
                self.assertEqual(manifest["status"], "finished")
                self.assertEqual(requested, [SITE])
                result = json.loads((output / "result.json").read_text())
                self.assertEqual(result["documents"][0]["html"], HTML)
                self.assertNotIn("records", result)


class DefaultCollectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        install_browser_api(self)

    async def test_default_discovery_preserves_jobs_and_financial_links_without_analysis(
        self,
    ):
        job = {
            "@type": "JobPosting",
            "title": "Engineer",
            "description": "Use Python and C++.",
            "baseSalary": None,
        }
        links = [
            {"href": SITE + path, "text": title}
            for path, title in [
                ("contact", "Contact"),
                ("careers", "Careers"),
                ("about", "About us"),
                ("investors", "Investor relations"),
                ("engineering-blog", "Our technology stack"),
            ]
        ]
        pages = {
            SITE: (HTML, links, 200, None),
            SITE + "contact": (
                '<a href="mailto:info@example.test">Email</a>',
                [],
                200,
                None,
            ),
            SITE + "careers": (
                '<script type="application/ld+json">'
                + json.dumps(job)
                + "</script><p>Use Python and C++.</p>",
                [],
                200,
                None,
            ),
            SITE + "about": (
                "<p>Example develops industrial sensors.</p>",
                [],
                200,
                None,
            ),
            SITE + "investors": (
                '<a href="/annual-report.pdf">Annual report</a>',
                [{"href": SITE + "annual-report.pdf", "text": "Annual report"}],
                200,
                None,
            ),
        }
        requested, tasks = [], []

        async def boundary(client, request, **kwargs):
            if request.url.host == "browser-fixture":
                return await browser_fixture_send(client, request, **kwargs)
            if request.url.host == "api.deepseek.com":
                body = json.loads(request.content)
                prompt = next(
                    m["content"] for m in body["messages"] if m["role"] == "user"
                )
                payload = json.loads(prompt.split("INPUT DATA:\n")[1])
                if payload.get("task") == "site_classification":
                    tasks.append("gate")
                    reply = COMPANY
                else:
                    self.assertIn("candidates", payload)
                    self.assertNotIn('"technology_signals"', prompt)
                    tasks.append("selection")
                    reply = {
                        "assessments": [
                            requested_assessment(
                                candidate,
                                "none"
                                if candidate["url"].endswith("engineering-blog")
                                else "direct",
                            )
                            for candidate in payload["candidates"]
                        ]
                    }
                return httpx.Response(200, json=response(reply))
            self.assertIn(request.url.path, {"/robots.txt", "/sitemap.xml"})
            return httpx.Response(404)

        with TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch(
                    "company_research.crawl.open_browser",
                    lambda *_args, **_: browser_responses(pages, requested),
                ),
                patch.object(httpx.AsyncClient, "send", boundary),
            ):
                manifest = await crawl_company(
                    SITE,
                    output_dir=root,
                    api_key="test-key",
                    crawl="full",
                    config=ResearchConfig(max_pages=10, web_search=False),
                )
            self.assertEqual(manifest["status"], "finished")
            self.assertEqual(manifest["config"]["max_pages"], 10)
            self.assertEqual(manifest["mode"], "full")
            self.assertEqual(set(requested), set(pages))
            self.assertEqual(tasks, ["gate", "selection"])
            self.assertEqual(manifest["stop_reason"], "no_matching_candidates")
            bundle = json.loads((root / "result.json").read_text())
            observations = {
                doc["url"]: doc["input"]["observations"] for doc in bundle["documents"]
            }
            self.assertEqual(
                observations[SITE + "careers"]["structured_data"]["jsonld_entities"][0][
                    "data"
                ],
                job,
            )
            self.assertEqual(
                observations[SITE + "contact"]["contacts"][0]["value"],
                "info@example.test",
            )
            self.assertIn(
                "industrial sensors", observations[SITE + "about"]["text"]["main"]
            )
            self.assertEqual(
                observations[SITE + "investors"]["financial_links"][0]["url"],
                SITE + "annual-report.pdf",
            )
            self.assertTrue(
                all(obs["trackers"] is None for obs in observations.values())
            )
            self.assertNotIn("records", bundle)
