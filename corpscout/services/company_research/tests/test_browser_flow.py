"""Opt-in real browser test; only the paid model HTTP boundary is replaced."""

import json
import os
import threading
import unittest
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from test_catalog_search import catalog_fixture

from company_research import ResearchConfig
from company_research.fetch import open_browser
from company_research.models import OBJECTIVES, RECORD_TYPES
from company_research.research import research_company
from company_research.storage import content_hash


@unittest.skipUnless(
    os.environ.get("COMPANY_RESEARCH_BROWSER_TEST") == "1", "opt-in browser test"
)
class BrowserFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_url_to_json_with_browser_sitemap_selection_failure_and_secondary_facts(
        self,
    ):
        await self.run_browser_flow()

    async def test_direct_deepseek_endpoint_for_full_crawler(self):
        await self.run_browser_flow(api="deepseek")

    async def test_news_site_stops_after_homepage_without_sitemap_or_other_pages(self):
        await self.run_browser_flow(api="deepseek", skip_site=True)

    async def test_robots_denial_preserves_reason_without_model_or_page_request(self):
        await self.run_browser_flow(api="deepseek", deny_robots=True)

    async def test_closed_browser_retries_same_page_with_fresh_context(self):
        await self.run_browser_flow(restart_budget=1)

    async def test_closed_browser_exhaustion_stops_without_spending_remaining_pages(
        self,
    ):
        await self.run_browser_flow(restart_budget=0)

    async def run_browser_flow(
        self,
        restart_budget: int | None = None,
        api="openrouter",
        skip_site=False,
        deny_robots=False,
    ):
        requested_paths = []
        browsers = []

        @asynccontextmanager
        async def tracked_browser():
            async with open_browser() as crawler:
                browsers.append(crawler)
                yield crawler

        class Website(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args) -> None:
                pass

            def do_GET(self):
                requested_paths.append(self.path)
                if self.path == "/robots.txt":
                    body, status = (
                        "User-agent: *\nDisallow: /"
                        if deny_robots
                        else "User-agent: *\nAllow: /",
                        200,
                    )
                elif self.path == "/sitemap.xml":
                    body, status = (
                        "<urlset><url><loc>/contact</loc></url><url><loc>/jobs</loc></url>"
                        "<url><loc>/failed</loc></url></urlset>",
                        200,
                    )
                elif self.path == "/" and skip_site:
                    body, status = (
                        "<html><body><h1>Latest world news</h1><p>Read our breaking news, "
                        "opinion articles and daily coverage of politics, sport and culture. "
                        "This news site is operated by DemoWorks Media. Subscribe for unlimited "
                        "access to our reporting, analysis and exclusive interviews.</p>"
                        '<p><a href="/contact">Contact the publisher</a> '
                        '<a href="/jobs">Jobs at our company</a> '
                        '<a href="/advertise">Advertise with us</a></p></body></html>',
                        200,
                    )
                elif self.path == "/":
                    body, status = (
                        '<html><body><header><nav aria-label="Our businesses"><a href="https://nova.example/?utm_source=header#labs"><img alt="Nova Labs" src="/logo.png"></a></nav></header><main><h1>DemoWorks</h1><p>We build equipment and provide engineering services '
                        "for customers worldwide. Our website introduces our company and the people who work here.</p>"
                        '<p><a href="/contact">Contact our offices and staff</a> '
                        '<a href="/jobs">View our current job openings</a> '
                        '<a href="/failed">Find our office locations</a></p><div id="dynamic"></div>'
                        '<script>document.getElementById("dynamic").innerHTML="<p>Rendered with JavaScript: '
                        'DemoWorks welcomes customers to explore our products and talk to our team about their needs.</p>";</script>'
                        "<p>DemoWorks financial statements for year ended 31 March 2025: "
                        '<a href="/reports/accounts.pdf">Financial statements</a>.</p>'
                        '<p>Our implementation partner <a href="https://nova.example/?utm_source=header#labs">Nova Labs</a> helps customers deploy equipment.</p>'
                        '</main><footer><a href="https://www.linkedin.com/company/demoworks/?trk=footer">LinkedIn</a></footer></body></html>',
                        200,
                    )
                elif self.path == "/contact":
                    body, status = (
                        "<html><body><main><h1>Contact DemoWorks</h1><p>For business enquiries contact DemoWorks at "
                        '<a href="mailto:hello@demoworks.test">hello@demoworks.test</a>. Our team will help you find '
                        "the correct service and answer your questions about our company.</p><p>No open positions in "
                        "our Berlin office. Please check the main jobs page for opportunities in other locations.</p></main></body></html>",
                        200,
                    )
                elif self.path == "/jobs":
                    body, status = (
                        "<html><body><main><h1>Jobs at DemoWorks</h1><p>Our current opening: "
                        '<a href="/jobs/engineer">Engineer</a>, London. Join our team to design and deliver '
                        "engineering solutions to customers around the world.</p><p>Recruitment enquiries: "
                        '<a href="mailto:recruiting@demoworks.test">recruiting@demoworks.test</a>. Please use '
                        "this address to contact our recruitment department with questions about your application.</p></main></body></html>",
                        200,
                    )
                elif self.path == "/jobs/engineer":
                    body, status = (
                        "<html><body><main><h1>DemoWorks Engineer</h1><p>Our engineering team develops tools in Python "
                        "to support customer projects and improve the equipment we design and deliver around the world.</p></main></body></html>",
                        200,
                    )
                else:
                    body, status = "Unavailable page", 404
                self.send_response(status)
                self.send_header(
                    "Content-Type",
                    "application/xml"
                    if self.path == "/sitemap.xml"
                    else "text/html; charset=utf-8",
                )
                self.end_headers()
                self.wfile.write(body.encode())

        server = ThreadingHTTPServer(("127.0.0.1", 0), Website)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        site_url = f"http://127.0.0.1:{server.server_port}/"
        original_send = httpx.AsyncClient.send
        extraction_inputs = []

        async def model_boundary(client, request, **kwargs):
            model_host = "api.deepseek.com" if api == "deepseek" else "openrouter.ai"
            if request.url.host != model_host:
                return await original_send(client, request, **kwargs)
            payload = json.loads(request.content)
            if api == "deepseek":
                self.assertEqual(
                    str(request.url), "https://api.deepseek.com/chat/completions"
                )
                self.assertEqual(payload["model"], "deepseek-flash")
                self.assertEqual(payload["response_format"], {"type": "json_object"})
                self.assertNotIn("provider", payload)
            data = json.loads(
                next(
                    message["content"]
                    for message in payload["messages"]
                    if message["role"] == "user"
                ).split("INPUT DATA:\n", 1)[1]
            )
            if data.get("task") == "external_link_context":
                document = {
                    "assessments": [
                        {
                            "link_id": link["link_id"],
                            "relationship": "unknown",
                            "basis": "unknown",
                            "related_entity_name": None,
                            "description": "No verified relationship in this browser boundary test.",
                            "evidence": [],
                        }
                        for link in data["links"]
                    ]
                }
            elif data.get("task") == "summary_review":
                document = {
                    "reviews": [
                        {
                            "statement_id": value["statement_id"],
                            "supported": True,
                            "reason": "Supported test statement",
                        }
                        for value in data["statements"]
                    ]
                }
            elif data.get("task") == "claim_review":
                document = {
                    "reviews": [
                        {
                            "record_id": claim["record_id"],
                            "supported": True,
                            "reason": "Explicit team usage in the quoted role description",
                            "source_subject": claim["data"].get("company"),
                            "source_object": None,
                            "specific_technology": True,
                            "source_signal": claim["data"].get("signal"),
                            "source_scope": claim["data"].get("scope"),
                            "source_subject_kind": "company"
                            if "technology" in claim["data"]
                            and claim["data"].get("company")
                            else None,
                            "identity_basis": None,
                            "source_value": claim["data"].get(
                                "value", claim["data"].get("document_url")
                            ),
                            "source_claim_type": claim["data"].get("document_type"),
                        }
                        for claim in data["claims"]
                    ]
                }
            elif data.get("task") == "site_classification":
                self.assertNotIn("/sitemap.xml", requested_paths)
                if restart_budget is not None:
                    # Kill the real Playwright context while the crawler is doing model work.
                    await browsers[
                        0
                    ].crawler_strategy.browser_manager.default_context.close()
                document = {
                    "crawl_decision": "continue_crawling",
                    "site_description": "DemoWorks provides engineering services.",
                    "site_types": ["company"],
                    "research_profiles": ["service_provider"],
                    "purpose": "Company website",
                    "operator_name": "DemoWorks",
                    "business_activities": [],
                    "evidence": ["DemoWorks", "Rendered with JavaScript"],
                }
                if skip_site:
                    document.update(
                        crawl_decision="skip_crawling",
                        site_description="The site publishes news, opinion and interviews about politics, sport and culture, with subscription access and advertising.",
                        site_types=["news_media"],
                        research_profiles=["media_community"],
                        purpose="News portal",
                        operator_name="DemoWorks Media",
                        evidence=[
                            "Latest world news",
                            "This news site is operated by DemoWorks Media.",
                        ],
                    )
            elif data.get("task") == "company_summary":
                document = {
                    "site_description": {
                        "text": "Company website",
                        "record_ids": [data["records"][0]["record_id"]],
                    },
                    "company_name": None,
                    "company_description": None,
                    "products_services": [],
                    "industries": [],
                    "company_relationships": [],
                    "certifications_compliance": [],
                }
            elif "candidates" in data:
                assessments = []
                for candidate in data["candidates"]:
                    target = {
                        "/contact": "company_contacts",
                        "/jobs": "jobs",
                        "/failed": "locations",
                        "/jobs/engineer": "technology_signals",
                    }.get(httpx.URL(candidate["url"]).path)
                    assessments.append(
                        {
                            "candidate_id": candidate["candidate_id"],
                            "reason": "Published link label and path indicate page purpose",
                            "objectives": {
                                o: {
                                    "potential": "high" if o == target else "low",
                                    "role": "direct" if o == target else "none",
                                }
                                for o in OBJECTIVES
                            },
                        }
                    )
                document = {"assessments": assessments}
            else:
                extraction_inputs.append(data)
                document = {o: [] for o in RECORD_TYPES}
                path = httpx.URL(data["source_url"]).path
                if path == "/":
                    document["document_links"] = [
                        {
                            "document_url": "/reports/accounts.pdf",
                            "label": "Financial statements",
                            "document_type": "financial_statement",
                            "company": "DemoWorks",
                            "reporting_period": "year ended 31 March 2025",
                            "evidence": [
                                "DemoWorks financial statements for year ended 31 March 2025:",
                                "Financial statements",
                            ],
                        }
                    ]
                    document["company_profile"] = [
                        {
                            "company": "DemoWorks",
                            "field": "trading_name",
                            "value": "DemoWorks",
                            "as_of": None,
                            "evidence": ["DemoWorks"],
                        }
                    ]
                if path in {"/contact", "/jobs"}:
                    value = (
                        "hello@demoworks.test"
                        if path == "/contact"
                        else "recruiting@demoworks.test"
                    )
                    document["company_contacts"] = [
                        {
                            "owner": "DemoWorks",
                            "owner_kind": "company",
                            "type": "email",
                            "value": value,
                            "purpose": None,
                            "evidence": ["DemoWorks", value],
                        }
                    ]
                if path == "/contact":
                    document["explicit_negatives"] = [
                        {
                            "objective": "jobs",
                            "scope": "Berlin office",
                            "evidence": ["No open positions in our Berlin office."],
                        }
                    ]
                if path == "/jobs":
                    document["jobs"] = [
                        {
                            "employer": "DemoWorks",
                            "title": "Engineer",
                            "location": "London",
                            "department": None,
                            "employment_type": None,
                            "workplace_type": None,
                            "job_url": "/jobs/engineer",
                            "evidence": ["DemoWorks", "Engineer", "London"],
                        }
                    ]
                if path == "/jobs/engineer":
                    document["technology_signals"] = [
                        {
                            "company": "DemoWorks",
                            "technology": "Python",
                            "category": "programming_language",
                            "signal": "stated_use",
                            "scope": "team",
                            "job_employer": "DemoWorks",
                            "job_title": "Engineer",
                            "job_url": data["source_url"],
                            "alternative_group": None,
                            "context": "The engineering team develops tools in Python.",
                            "as_of": None,
                            "evidence": [
                                "DemoWorks",
                                "Engineer",
                                "Our engineering team develops tools in Python",
                            ],
                        }
                    ]
            return httpx.Response(
                200,
                request=request,
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
                        "cost": 0.001,
                    },
                },
            )

        try:
            with (
                TemporaryDirectory() as directory,
                patch.object(httpx.AsyncClient, "send", model_boundary),
                patch("company_research.research.open_browser", tracked_browser),
            ):
                root = Path(directory)
                result = await research_company(
                    site_url,
                    technology_catalog=catalog_fixture(),
                    api_key="fake-key",
                    api=api,
                    output_dir=root,
                    config=ResearchConfig(
                        model="deepseek-flash"
                        if api == "deepseek"
                        else ResearchConfig().model,
                        max_pages=5,
                        max_external_pages=0,
                        max_model_calls=20,
                        page_timeout_seconds=15.0,
                        max_browser_restarts=restart_budget
                        if restart_budget is not None
                        else 2,
                    ),
                )
                if deny_robots:
                    self.assertEqual(result.status, "needs_review")
                    self.assertEqual(result.stop_reason, "initial_page_unavailable")
                    self.assertEqual(result.usage["calls"], 0)
                    self.assertEqual(len(result.pages), 1)
                    self.assertEqual(result.pages[0].attempts, 1)
                    self.assertEqual(result.pages[0].status_code, 403)
                    self.assertIn(
                        "Access denied by robots.txt", result.pages[0].errors[0]
                    )
                    self.assertNotIn("/", requested_paths)
                    self.assertNotIn("/sitemap.xml", requested_paths)
                    self.assertEqual(extraction_inputs, [])
                    self.assertFalse((root / "sitemaps.json").exists())
                    self.assertEqual(
                        json.loads((root / "fetches/p0001-1.json").read_text())[
                            "error"
                        ],
                        "Access denied by robots.txt",
                    )
                    return
                if skip_site:
                    self.assertEqual(
                        result.status, "skip_crawling", result.model_dump()
                    )
                    self.assertEqual(result.stop_reason, "not_company_website")
                    self.assertEqual(len(result.pages), 1)
                    self.assertEqual(result.usage["calls"], 1)
                    self.assertEqual(extraction_inputs, [])
                    self.assertNotIn("/sitemap.xml", requested_paths)
                    self.assertNotIn("/contact", requested_paths)
                    self.assertNotIn("/jobs", requested_paths)
                    self.assertNotIn("/advertise", requested_paths)
                    self.assertFalse((root / "sitemaps.json").exists())
                    self.assertEqual(
                        json.loads((root / "result.json").read_text())["status"],
                        "skip_crawling",
                    )
                    return
                self.assertEqual(result.status, "partial", result.model_dump())
                self.assertEqual(result.discovery["model_api"], api)
                external = [
                    link
                    for link in result.external_links
                    if link.source_page_id == "p0001"
                ]
                nova = [
                    link for link in external if link.destination_host == "nova.example"
                ]
                self.assertEqual(len(nova), 2)
                self.assertEqual(nova[0].page_region, "header")
                self.assertEqual(nova[0].image_alt, ["Nova Labs"])
                self.assertEqual(nova[0].section_heading, "Our businesses")
                self.assertEqual(nova[0].extraction_method, "rendered_html")
                self.assertEqual(
                    nova[0].url, "https://nova.example/?utm_source=header#labs"
                )
                self.assertIn(
                    "Our implementation partner", nova[1].surrounding_text or ""
                )
                self.assertEqual(
                    nova[0].html_sha256,
                    content_hash(
                        (root / nova[0].html_file).read_text(encoding="utf-8")
                    ),
                )
                self.assertTrue(
                    any(
                        link.destination_host == "www.linkedin.com" for link in external
                    )
                )
                saved = json.loads((root / "result.json").read_text(encoding="utf-8"))
                self.assertEqual(saved["schema_version"], "1.11")
                self.assertEqual(
                    len(saved["external_links"]), len(result.external_links)
                )
                self.assertEqual(result.pages[0].external_link_count, len(external))
                self.assertTrue(
                    all(
                        value.external_link_count is not None
                        for value in result.pages
                        if value.fetch_status == "fetched"
                    )
                )
                self.assertEqual(result.records.company_relationships, [])
                if restart_budget == 0:
                    self.assertEqual(result.stop_reason, "browser_unavailable")
                    self.assertEqual(len(result.pages), 2)
                    self.assertEqual(len(browsers), 1)
                    self.assertEqual(result.pages[1].fetch_status, "failed")
                    self.assertEqual(result.pages[1].extraction_status, "not_assessed")
                    return
                if restart_budget == 1:
                    self.assertEqual(len(browsers), 2)
                    self.assertEqual(result.pages[1].attempts, 2)
                    self.assertEqual(result.pages[1].fetch_status, "fetched")
                    self.assertEqual(len(result.discovery["browser_recoveries"]), 1)
                    self.assertFalse(
                        json.loads((root / "fetches/p0002-1.json").read_text())[
                            "success"
                        ]
                    )
                    self.assertTrue(
                        json.loads((root / "fetches/p0002-2.json").read_text())[
                            "success"
                        ]
                    )
                self.assertEqual(result.stop_reason, "page_budget")
                self.assertEqual(len(result.pages), 5)
                assert result.site_profile is not None
                self.assertEqual(result.site_profile.data["site_types"], ["company"])
                self.assertIsNotNone(result.company_overview)
                self.assertEqual(result.discovery["sitemap"]["url_count"], 3)
                self.assertEqual(
                    sum(p.fetch_status == "failed" for p in result.pages), 1
                )
                self.assertEqual(result.objectives["jobs"].status, "found")
                self.assertEqual(result.objectives["document_links"].status, "found")
                self.assertFalse(
                    result.records.document_links[0].data["content_examined"]
                )
                self.assertNotIn("/reports/accounts.pdf", requested_paths)
                self.assertEqual(len(result.discovery["document_candidates"]), 1)
                self.assertEqual(result.objectives["jobs"].explicit_negative_count, 1)
                self.assertEqual(result.objectives["people"].status, "not_found")
                self.assertTrue(
                    any(value.external_link_count == 0 for value in result.pages)
                )
                self.assertEqual(result.objectives["company_contacts"].record_count, 2)
                self.assertEqual(result.pages[-1].selected_for, "job_detail_followup")
                self.assertEqual(
                    result.objectives["technology_signals"].status, "found"
                )
                self.assertEqual(result.technology_summary[0].distinct_job_url_count, 1)
                self.assertEqual(result.technology_summary[0].technology, "Python")
                self.assertEqual(
                    result.records.explicit_negatives[0].data["scope"], "Berlin office"
                )
                self.assertEqual(
                    result.records.jobs[0].data["job_url"], site_url + "jobs/engineer"
                )
                self.assertIn(
                    "Rendered with JavaScript", extraction_inputs[0]["cleaned_html"]
                )
                self.assertNotIn("<script", extraction_inputs[0]["cleaned_html"])
                for objective in RECORD_TYPES:
                    for finding in getattr(result.records, objective):
                        self.assertEqual(finding.evidence_status, "source_matched")
                        for source in finding.sources:
                            html = (root / "html" / f"{source.page_id}.html").read_text(
                                encoding="utf-8"
                            )
                            self.assertEqual(content_hash(html), source.html_sha256)
                            self.assertGreater(source.chunk_end, source.chunk_start)
                self.assertEqual(
                    json.loads((root / "result.json").read_text())["status"], "partial"
                )
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
