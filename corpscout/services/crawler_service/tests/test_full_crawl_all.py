"""Non-company first pages stop by default; explicit opt-in preserves their classification."""
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import httpx
from test_crawl import browser_responses
from test_package import response
from test_site_gate import classification

from crawler_service.crawl import crawl_company
from crawler_service.models import ResearchConfig
from crawler_service.service import CrawlRequest

SITE = "https://2525.se/"
HTML = "<h1>2525 AB</h1><p>Online store for truck accessories. Klarna payments.</p>"


class FullCrawlAllTests(unittest.IsolatedAsyncioTestCase):
    async def run_gate(self, site_type, *, override=False, basic=False, uncertain=False):
        requests = []
        document = classification(
            crawl_decision="needs_review" if uncertain else "continue_crawling" if site_type == "company" else "skip_crawling",
            site_types=[site_type], research_profiles=["general"], operator_name="2525 AB",
            site_description="2525 AB operates an online store for truck accessories.",
            purpose="Online store for truck accessories", evidence=["2525 AB", "Klarna payments"],
        )
        async def model_http(client, request, **kwargs):
            self.assertEqual(request.url.host, "api.deepseek.com")
            body = json.loads(request.content)
            self.assertIn("online_store", json.dumps(body))
            return httpx.Response(200, json=response(document))
        sitemap = AsyncMock(return_value={"urls": [], "url_sources": {}, "status": "not_found"})
        with (
            TemporaryDirectory() as directory,
            patch("crawler_service.crawl.open_browser", lambda: browser_responses({SITE: (HTML, [], 200, None)}, requests)),
            patch("crawler_service.crawl.sitemap_urls", sitemap),
            patch.object(httpx.AsyncClient, "send", model_http),
        ):
            result = await crawl_company(SITE, output_dir=Path(directory), api_key="test-key",
                crawl=False if basic else "full", site_info=basic, full_crawl_all=override,
                config=ResearchConfig(max_pages=2, max_model_calls=3, max_corrections=0, web_search=False))
        self.assertEqual(requests, [SITE])
        return result, sitemap

    async def test_shops_news_forums_and_content_stop_before_discovery(self):
        for site_type in ("online_store", "news_media", "forum_community", "content_site"):
            with self.subTest(site_type=site_type):
                result, sitemap = await self.run_gate(site_type)
                self.assertEqual(result["status"], "skip_crawling")
                self.assertIn(site_type, result["site_gate"]["reason"])
                self.assertFalse(result["full_crawl_all"])
                self.assertFalse(result["site_gate"]["overridden"])
                self.assertEqual(result["site_info"]["site_types"], [site_type])
                sitemap.assert_not_awaited()

    async def test_opt_in_allows_discovery_without_relabeling_the_site(self):
        for site_type in ("online_store", "news_media", "forum_community", "content_site"):
            with self.subTest(site_type=site_type):
                result, sitemap = await self.run_gate(site_type, override=True)
                self.assertEqual(result["status"], "finished")
                self.assertEqual(result["site_gate"]["decision"], "skip_crawling")
                self.assertTrue(result["site_gate"]["overridden"])
                self.assertEqual(result["site_gate"]["override_reason"], "full_crawl_all")
                sitemap.assert_awaited_once()

    async def test_company_sites_continue_by_default(self):
        result, sitemap = await self.run_gate("company")
        self.assertEqual(result["status"], "finished")
        sitemap.assert_awaited_once()

    async def test_basic_info_still_collects_a_shop_description(self):
        result, sitemap = await self.run_gate("online_store", basic=True)
        self.assertEqual(result["status"], "finished")
        self.assertEqual(result["stop_reason"], "site_info_complete")
        self.assertEqual(result["site_info"]["crawl_decision"], "skip_crawling")
        sitemap.assert_not_awaited()

    async def test_override_does_not_bypass_uncertain_classification(self):
        result, sitemap = await self.run_gate("unknown", override=True, uncertain=True)
        self.assertEqual(result["status"], "needs_review")
        self.assertFalse(result["site_gate"]["overridden"])
        sitemap.assert_not_awaited()

    def test_service_flag_is_default_off_strict_boolean_and_roundtrips(self):
        self.assertFalse(CrawlRequest(url=SITE, crawl="full").full_crawl_all)
        request = CrawlRequest(url=SITE, crawl="full", full_crawl_all=True)
        self.assertTrue(CrawlRequest.model_validate_json(request.model_dump_json()).full_crawl_all)
        for invalid in ("false", "true", 1, 0, None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                CrawlRequest(url=SITE, crawl="full", full_crawl_all=invalid)
        with self.assertRaises(ValueError):
            CrawlRequest(url=SITE, site_info=True, full_crawl_all=True)
