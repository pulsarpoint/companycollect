"""Brief descriptions share the admission evidence and do not require discovery."""

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from browser_http_fixture import install_browser_api
from browser_http_fixture import original_send as browser_fixture_send
from pydantic import ValidationError
from test_crawl import browser_responses
from test_package import assessment, response
from test_selection_instructions import requested_assessment
from test_site_gate import classification

from crawler_service.captures import load_crawl
from crawler_service.crawl import crawl_company, main
from crawler_service.models import ResearchConfig, SiteClassification

SITE = "https://example.test/"
HTML = "<h1>Example</h1><p>Industrial robot design and testing services for manufacturers.</p>"
COMPANY = classification(
    crawl_decision="continue_crawling",
    site_description="Example designs industrial robots. It provides testing services for manufacturers.",
    site_types=["company"],
    research_profiles=["service_provider"],
    purpose="Robot design and testing",
    operator_name="Example",
    business_activities=["Industrial robot design", "Testing services"],
    evidence=[
        "Example",
        "Industrial robot design and testing services for manufacturers.",
    ],
)


class SiteInfoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        install_browser_api(self)

    async def run_case(
        self, *, document=None, html=HTML, status=200, redirect=None, model_status=200, **options
    ):
        requested, tasks, web_requests = [], [], []
        pages = {
            SITE: (
                html,
                [{"href": SITE + "jobs", "text": "Current jobs"}],
                status,
                redirect,
            ),
            SITE + "jobs": ("<h1>Example Engineer vacancy</h1>", [], 200, None),
        }

        async def boundary(client, request, **kwargs):
            if request.url.host == "browser-fixture":
                return await browser_fixture_send(client, request, **kwargs)
            if request.url.host != "api.deepseek.com":
                web_requests.append(str(request.url))
                return httpx.Response(404)
            if model_status != 200:
                return httpx.Response(model_status, json={"error": {"message": "Model unavailable"}})
            prompt = json.loads(request.content)["messages"][1]["content"]
            data = json.loads(prompt.split("INPUT DATA:\n")[1])
            if data.get("task") == "site_classification":
                tasks.append("site_classification")
                self.assertNotIn("selection_instructions", data)
                result = COMPANY if document is None else document
            else:
                tasks.append("link_assessment")
                result = {
                    "assessments": [
                        requested_assessment(candidate, "direct")
                        if "selection_instructions" in data
                        else assessment(
                            candidate["candidate_id"], jobs="high"
                        ).model_dump()
                        for candidate in data["candidates"]
                    ]
                }
            return httpx.Response(200, json=response(result))

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda *_args, **_: browser_responses(pages, requested),
                ),
                patch.object(httpx.AsyncClient, "send", boundary),
            ):
                manifest = await crawl_company(
                    SITE,
                    output_dir=root,
                    api_key="test-key",
                    config=ResearchConfig(
                        model="deepseek-flash",
                        provider=None,
                        max_pages=4,
                        max_corrections=0,
                        page_attempts=1,
                        web_search=False,
                    ),
                    **options,
                )
            self.assertEqual(
                json.loads((root / "crawl-manifest.json").read_text()), manifest
            )
            if manifest["site_info"] is not None:
                self.assertEqual(
                    json.loads((root / "site-info.json").read_text()),
                    manifest["site_info"],
                )
            else:
                self.assertFalse((root / "site-info.json").exists())
            if manifest["status"] == "finished":
                loaded, captures = load_crawl(root)
                self.assertEqual(loaded, manifest)
                self.assertEqual(len(captures), len(requested))
            return manifest, requested, tasks, web_requests

    async def test_info_alone_reads_only_input_and_uses_admission_call(self):
        result, requested, tasks, web = await self.run_case(site_info=True)
        self.assertEqual(requested, [SITE])
        self.assertEqual(tasks, ["site_classification"])
        self.assertEqual(web, [])
        self.assertEqual(result["mode"], "site_info")
        self.assertFalse(result["crawl_requested"])
        self.assertEqual(result["status"], "finished")
        self.assertEqual(result["stop_reason"], "site_info_complete")
        self.assertEqual(result["sitemap"], {"status": "not_requested"})
        self.assertEqual(
            result["site_info"]["site_description"], COMPANY["site_description"]
        )
        self.assertEqual(
            result["site_info"]["business_activities"], COMPANY["business_activities"]
        )
        self.assertEqual(result["site_info"]["scope"], "first_page_only")

    async def test_news_site_basic_info_finishes_but_deeper_crawl_is_skipped(self):
        company, *_ = await self.run_case(site_info=True)
        for flag in (False, True):
            with self.subTest(site_info=flag):
                result, requested, tasks, web = await self.run_case(
                    site_info=flag,
                    document=classification(),
                    html="<h1>Latest world news</h1><p>Example Media</p>",
                )
                self.assertEqual(
                    result["status"], "finished" if flag else "skip_crawling"
                )
                self.assertEqual(
                    result["stop_reason"],
                    "site_info_complete" if flag else "not_company_website",
                )
                self.assertEqual(result["site_gate"]["decision"], "skip_crawling")
                self.assertEqual(set(result["site_info"]), set(company["site_info"]))
                self.assertEqual(result["site_info"]["site_types"], ["news_media"])
                self.assertEqual(
                    result["site_info"]["site_description"],
                    classification()["site_description"],
                )
                self.assertEqual(requested, [SITE])
                self.assertEqual(tasks, ["site_classification"])
                self.assertEqual(web, [])

    async def test_uncertain_deeper_crawl_eligibility_does_not_block_basic_info(self):
        result, requested, tasks, web = await self.run_case(
            site_info=True,
            document=classification(crawl_decision="needs_review", site_types=["mixed"]),
            html="<h1>Latest world news</h1><p>Example Media</p>",
        )
        self.assertEqual(result["status"], "finished")
        self.assertEqual(result["stop_reason"], "site_info_complete")
        self.assertEqual(result["site_info"]["evidence_status"], "source_matched")
        self.assertEqual(result["site_info"]["crawl_decision"], "needs_review")
        self.assertEqual(requested, [SITE])
        self.assertEqual(tasks, ["site_classification"])
        self.assertEqual(web, [])

    async def test_basic_info_model_failure_is_not_a_successful_description(self):
        result, requested, _, web = await self.run_case(site_info=True, model_status=404)
        self.assertEqual(result["status"], "needs_review")
        self.assertEqual(result["stop_reason"], "model_unavailable")
        self.assertTrue(result["errors"])
        self.assertEqual(result["usage"]["successful_responses"], 0)
        self.assertEqual(requested, [SITE])
        self.assertEqual(web, [])

    async def test_info_can_accompany_lists_instructions_and_general_discovery(self):
        for options in (
            {"pages": ["/jobs"]},
            {"pages": ["/jobs"], "instructions": "Get jobs"},
            {"instructions": "Get jobs"},
            {"crawl": True},
            {"crawl": "full"},
        ):
            with self.subTest(options=options):
                result, requested, tasks, web = await self.run_case(
                    site_info=True, **options
                )
                self.assertEqual(result["status"], "finished")
                self.assertTrue(result["crawl_requested"])
                self.assertEqual(requested, [SITE, SITE + "jobs"])
                self.assertEqual(tasks[0], "site_classification")
                self.assertEqual(tasks.count("site_classification"), 1)
                self.assertEqual(
                    result["site_info"]["site_description"], COMPANY["site_description"]
                )
                if "pages" in options:
                    self.assertEqual(web, [])
                else:
                    self.assertEqual(web, [SITE + "robots.txt", SITE + "sitemap.xml"])

    async def test_redirected_info_uses_final_source_url(self):
        final_url = "https://different-company.test/company"
        result, requested, tasks, web = await self.run_case(
            site_info=True, redirect=final_url
        )
        self.assertEqual(result["site_url"], final_url)
        self.assertEqual(result["input_url"], SITE)
        self.assertEqual(result["pages"][0]["requested_url"], SITE)
        self.assertEqual(result["pages"][0]["source_url"], final_url)
        self.assertEqual(result["pages"][0]["redirects"], [
            {"url": SITE, "location": final_url, "status_code": 301}
        ])
        self.assertEqual(result["pages"][0]["navigation_attempts"], [
            {"url": SITE, "error": None}
        ])
        self.assertEqual(result["site_info"]["source_url"], final_url)
        self.assertEqual(requested, [SITE])
        self.assertEqual(tasks, ["site_classification"])
        self.assertEqual(web, [])

    async def test_failed_or_unmatched_first_page_returns_unknown_without_followups(
        self,
    ):
        cases = (
            {"status": 403, "html": "Denied"},
            {"document": COMPANY | {"evidence": ["Invented company evidence"]}},
        )
        for options in cases:
            with self.subTest(options=options):
                result, requested, _, web = await self.run_case(
                    site_info=True, **options
                )
                self.assertEqual(result["status"], "needs_review")
                self.assertEqual(result["site_info"]["crawl_decision"], "needs_review")
                self.assertEqual(result["site_info"]["evidence_status"], "needs_review")
                self.assertIsNone(result["site_info"]["operator_name"])
                self.assertEqual(result["site_info"]["business_activities"], [])
                self.assertEqual(result["site_info"]["evidence"], [])
                self.assertEqual(requested, [SITE])
                self.assertEqual(web, [])

    async def test_page_budget_includes_explicitly_requested_info_page(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "unused"
            with self.assertRaisesRegex(ValueError, "max_pages"):
                await crawl_company(
                    SITE,
                    output_dir=root,
                    site_info=True,
                    pages=["/jobs"],
                    config=ResearchConfig(max_pages=1),
                    api_key="test-key",
                )
            self.assertFalse(root.exists())
            with self.assertRaisesRegex(ValueError, "crawl=False"):
                await crawl_company(
                    SITE, output_dir=root, crawl=False, api_key="test-key"
                )
            self.assertFalse(root.exists())


class SiteInfoCliTests(unittest.TestCase):
    def setUp(self):
        install_browser_api(self)

    def test_flag_alone_emits_one_json_and_does_not_discover(self):
        requested = []
        pages = {SITE: (HTML, [{"href": SITE + "jobs"}], 200, None)}

        async def boundary(client, request, **kwargs):
            if request.url.host == "browser-fixture":
                return await browser_fixture_send(client, request, **kwargs)
            self.assertEqual(request.url.host, "api.deepseek.com")
            prompt = json.loads(request.content)["messages"][1]["content"]
            self.assertIn('"task": "site_classification"', prompt)
            return httpx.Response(200, json=response(COMPANY))

        with TemporaryDirectory() as temporary:
            stdout = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "crawler-service-crawl",
                        SITE,
                        "--site-info",
                        "--output-dir",
                        temporary,
                    ],
                ),
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda *_args, **_: browser_responses(pages, requested),
                ),
                patch.object(httpx.AsyncClient, "send", boundary),
                patch.dict("os.environ", {"DEEPSEEK": "test-key"}),
                redirect_stdout(stdout),
                redirect_stderr(io.StringIO()),
            ):
                main()
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["status"], "finished")
            self.assertEqual(result["usage"]["calls"], 1)
            self.assertEqual(requested, [SITE])
            self.assertEqual(result["site_info"]["operator_name"], "Example")

    def test_blank_descriptions_are_rejected(self):
        for text in ("", "  \n  "):
            with self.subTest(text=text), self.assertRaises(ValidationError):
                SiteClassification.model_validate(COMPANY | {"site_description": text})
