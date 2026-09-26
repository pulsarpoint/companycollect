"""First-page admission must precede all discovery and company extraction."""

import json
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import httpx
from pydantic import ValidationError
from test_catalog_search import catalog_fixture
from test_package import response

from crawler_service.models import SiteClassification
from crawler_service.research import ResearchConfig, research_company
from crawler_service.storage import content_hash


def classification(**changes):
    return {
        "crawl_decision": "skip_crawling",
        "site_description": "The site publishes current news and opinion articles.",
        "site_types": ["news_media"],
        "research_profiles": ["media_community"],
        "purpose": "News and opinion",
        "operator_name": "Example Media",
        "business_activities": [],
        "evidence": ["Latest world news", "Example Media"],
    } | changes


class SiteGateTests(unittest.IsolatedAsyncioTestCase):
    async def run_gate(self, replies, *, fetched=True, calls=2, html=None):
        html = (
            html
            if html is not None
            else (
                "<h1>Latest world news</h1><p>Example Media</p>"
                '<a href="/about">About our company</a><a href="/jobs">Jobs</a>'
            )
        )
        events, requests = [], []
        final_url = "https://www.example.test/"

        @asynccontextmanager
        async def browser():
            yield object()

        async def fetch(crawler, page, config, root):
            events.append("fetch")
            page.attempts = 1
            page.source_url = final_url
            page.fetch_status = "fetched" if fetched else "failed"
            page.status_code = 200 if fetched else 403
            page.html_file = "html/p0001.html"
            page.html_sha256 = content_hash(html)
            (root / "html").mkdir()
            (root / page.html_file).write_text(html, encoding="utf-8")
            return html, [{"href": final_url + "about", "text": "About our company"}]

        async def model_http(client, request, **kwargs):
            self.assertEqual(request.url.host, "api.deepseek.com")
            data = json.loads(request.content)
            prompt = next(m["content"] for m in data["messages"] if m["role"] == "user")
            self.assertIn('"task": "site_classification"', prompt)
            self.assertNotIn("sitemap_hints", prompt)
            self.assertIn(html.replace('"', '\\"'), prompt)
            self.assertNotIn("tools", data)
            requests.append(data)
            events.append("classify")
            return httpx.Response(
                200, json=response(replies[min(len(requests) - 1, len(replies) - 1)])
            )

        async def sitemap(client, url, config):
            self.assertEqual(url, final_url)
            events.append("sitemap")
            return {
                "urls": [final_url + "jobs"],
                "url_sources": {final_url + "jobs": final_url + "sitemap.xml"},
            }

        async def extract(result, page, llm, root, catalog):
            events.append("extract")
            page.extraction_status = "complete"

        with (
            TemporaryDirectory() as directory,
            patch("crawler_service.research.open_browser", browser),
            patch(
                "crawler_service.research.fetch_page", side_effect=fetch
            ) as fetch_mock,
            patch.object(httpx.AsyncClient, "send", model_http),
            patch(
                "crawler_service.research.sitemap_urls", side_effect=sitemap
            ) as sitemap_mock,
            patch(
                "crawler_service.research.extract_saved_page", side_effect=extract
            ) as extraction,
            patch(
                "crawler_service.research.assess_links", new_callable=AsyncMock
            ) as ranking,
            patch(
                "crawler_service.research.summarize_company", return_value=None
            ) as summary,
            patch(
                "crawler_service.research.assess_external_links", return_value=None
            ) as external,
        ):
            root = Path(directory)
            result = await research_company(
                "https://example.test/",
                api="deepseek",
                api_key="test-key",
                output_dir=root,
                config=ResearchConfig(
                    model="deepseek-flash",
                    provider=None,
                    max_pages=1,
                    max_model_calls=calls,
                ),
                technology_catalog=catalog_fixture(),
            )
            saved = json.loads((root / "result.json").read_text())
            self.assertEqual(saved, result.model_dump())
            self.assertEqual(result.usage["calls"], len(requests))
            self.assertEqual(len(result.pages), 1)
            assert result.site_description is not None
            self.assertLessEqual(len(result.site_description.split()), 200)
            fetch_mock.assert_awaited_once()
            ranking.assert_not_awaited()
            if result.status in {"skip_crawling", "needs_review"}:
                for operation in (sitemap_mock, extraction, summary, external):
                    operation.assert_not_awaited()
                self.assertEqual(result.discovery["sitemap"]["status"], "not_requested")
                self.assertFalse((root / "sitemaps.json").exists())
                self.assertEqual(result.pages[0].extraction_status, "not_assessed")
                self.assertTrue(
                    all(o.status == "not_assessed" for o in result.objectives.values())
                )
                self.assertTrue(
                    all(
                        records == []
                        for records in result.records.model_dump().values()
                    )
                )
                queue = json.loads((root / "queue.json").read_text())
                self.assertEqual(len(queue["candidates"]), 1)
            return result, events, len(requests)

    async def test_content_site_stops_even_with_a_named_company_and_about_jobs_links(
        self,
    ):
        result, events, calls = await self.run_gate([classification()], calls=1)
        self.assertEqual(result.status, "skip_crawling")
        self.assertEqual(result.stop_reason, "not_company_website")
        self.assertEqual(events, ["fetch", "classify"])
        self.assertEqual(calls, 1)
        self.assertEqual(result.site_url, "https://www.example.test/")
        self.assertEqual(result.discovery["site_gate"]["source_url"], result.site_url)

    async def test_uncertain_page_stops_without_asserting_it_is_a_noncompany(self):
        result, _, _ = await self.run_gate(
            [classification(crawl_decision="needs_review", site_types=["unknown"])]
        )
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(result.stop_reason, "site_eligibility_uncertain")

    async def test_unmatched_evidence_cannot_admit_or_skip_a_site(self):
        for decision, site_types in (
            ("continue_crawling", ["company"]),
            ("skip_crawling", ["news_media"]),
        ):
            with self.subTest(decision=decision):
                result, _, calls = await self.run_gate(
                    [
                        classification(
                            crawl_decision=decision,
                            site_types=site_types,
                            evidence=["Invented quotation"],
                        )
                    ]
                )
                self.assertEqual(result.status, "needs_review")
                self.assertEqual(
                    result.discovery["site_gate"]["evidence_status"], "needs_review"
                )
                self.assertEqual(calls, 2)

    async def test_failed_initial_fetch_does_not_call_model_or_discover_links(self):
        result, events, calls = await self.run_gate([], fetched=False)
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(result.stop_reason, "initial_page_unavailable")
        self.assertEqual(events, ["fetch"])
        self.assertEqual(calls, 0)

    async def test_schema_conflict_never_allows_a_content_site_to_continue(self):
        with self.assertLogs("crawler_service.research", level="ERROR"):
            result, _, calls = await self.run_gate(
                [
                    classification(
                        crawl_decision="continue_crawling",
                        site_types=["company", "news_media"],
                    )
                ]
            )
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(calls, 2)

    async def test_long_description_is_corrected_before_returning_skip(self):
        result, _, calls = await self.run_gate(
            [classification(site_description="word " * 201), classification()]
        )
        self.assertEqual(result.status, "skip_crawling")
        self.assertEqual(calls, 2)
        self.assertEqual(result.site_description, classification()["site_description"])

    async def test_invalid_model_output_stops_without_followup_work(self):
        with self.assertLogs("crawler_service.research", level="ERROR"):
            result, _, calls = await self.run_gate([{"not_a_classification": True}])
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(calls, 2)

    async def test_admitted_company_is_classified_before_sitemap_and_extraction(self):
        result, events, _ = await self.run_gate(
            [
                classification(
                    crawl_decision="continue_crawling",
                    site_types=["company"],
                    site_description="Example Media provides publishing services.",
                    evidence=["Example Media", "publishing services"],
                )
            ],
            html="<h1>Example Media</h1><p>We provide publishing services.</p>",
        )
        self.assertEqual(events, ["fetch", "classify", "sitemap", "extract"])
        self.assertEqual(result.status, "partial")
        self.assertEqual(result.discovery["site_gate"]["decision"], "continue_crawling")
        self.assertEqual(result.site_profile.data["operator_name"], "Example Media")


class SiteClassificationTests(unittest.TestCase):
    def test_description_word_limit(self):
        SiteClassification.model_validate(
            classification(site_description="word " * 200)
        )
        with self.assertRaises(ValidationError):
            SiteClassification.model_validate(
                classification(site_description="word " * 201)
            )

    def test_excluded_primary_types_cannot_continue_despite_company_owner(self):
        for site_type in (
            "online_store",
            "content_site",
            "news_media",
            "entertainment",
            "forum_community",
            "marketplace",
            "search_engine",
            "advertising_portal",
            "directory",
            "parked_domain",
            "personal",
            "unknown",
            "mixed",
        ):
            with self.subTest(site_type=site_type), self.assertRaises(ValidationError):
                SiteClassification.model_validate(
                    classification(
                        crawl_decision="continue_crawling",
                        site_types=["company", site_type],
                    )
                )

    def test_continuing_requires_identified_company(self):
        for changes in (
            {"site_types": ["nonprofit"]},
            {"site_types": ["company"], "operator_name": None},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                SiteClassification.model_validate(
                    classification(crawl_decision="continue_crawling", **changes)
                )
