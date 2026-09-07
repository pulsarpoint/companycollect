"""Opt-in real browser test; only the paid model HTTP boundary is replaced."""

import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from test_catalog_search import catalog_fixture

from company_research import ResearchConfig, research_company
from company_research.models import OBJECTIVES, RECORD_TYPES
from company_research.storage import content_hash


@unittest.skipUnless(
    os.environ.get("COMPANY_RESEARCH_BROWSER_TEST") == "1", "opt-in browser test"
)
class BrowserFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_url_to_json_with_browser_sitemap_selection_failure_and_secondary_facts(
        self,
    ):
        requested_paths = []

        class Website(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args) -> None:
                pass

            def do_GET(self):
                requested_paths.append(self.path)
                if self.path == "/robots.txt":
                    body, status = "User-agent: *\nAllow: /", 200
                elif self.path == "/sitemap.xml":
                    body, status = (
                        "<urlset><url><loc>/contact</loc></url><url><loc>/jobs</loc></url>"
                        "<url><loc>/failed</loc></url></urlset>",
                        200,
                    )
                elif self.path == "/":
                    body, status = (
                        "<html><body><main><h1>DemoWorks</h1><p>We build equipment and provide engineering services "
                        "for customers worldwide. Our website introduces our company and the people who work here.</p>"
                        '<p><a href="/contact">Contact our offices and staff</a> '
                        '<a href="/jobs">View our current job openings</a> '
                        '<a href="/failed">Find our office locations</a></p><div id="dynamic"></div>'
                        '<script>document.getElementById("dynamic").innerHTML="<p>Rendered with JavaScript: '
                        'DemoWorks welcomes customers to explore our products and talk to our team about their needs.</p>";</script>'
                        "<p>DemoWorks financial statements for year ended 31 March 2025: "
                        '<a href="/reports/accounts.pdf">Financial statements</a>.</p>'
                        "</main></body></html>",
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
            if request.url.host != "openrouter.ai":
                return await original_send(client, request, **kwargs)
            payload = json.loads(request.content)
            data = json.loads(
                next(
                    message["content"]
                    for message in payload["messages"]
                    if message["role"] == "user"
                ).split("INPUT DATA:\n", 1)[1]
            )
            if data.get("task") == "claim_review":
                document = {
                    "reviews": [
                        {
                            "record_id": claim["record_id"],
                            "supported": True,
                            "reason": "Explicit team usage in the quoted role description",
                            "source_subject": None,
                            "source_object": None,
                            "specific_technology": True,
                            "source_signal": claim["data"]["signal"],
                        }
                        for claim in data["claims"]
                    ]
                }
            elif data.get("task") == "site_classification":
                document = {
                    "site_types": ["company"],
                    "research_profiles": ["service_provider"],
                    "purpose": "Company website",
                    "operator_name": None,
                    "business_activities": [],
                    "evidence": ["Rendered with JavaScript"],
                }
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
            ):
                root = Path(directory)
                result = await research_company(
                    site_url,
                    technology_catalog=catalog_fixture(),
                    api_key="fake-key",
                    output_dir=root,
                    config=ResearchConfig(
                        max_pages=5, max_model_calls=20, page_timeout_seconds=15.0
                    ),
                )
                self.assertEqual(result.status, "partial", result.model_dump())
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
                self.assertEqual(result.objectives["company_contacts"].record_count, 2)
                self.assertEqual(result.pages[-1].selected_for, "technology_signals")
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
