from __future__ import annotations

from fastapi.testclient import TestClient

from corpscout_crawl_service.api import create_app
from corpscout_crawl_service.crawl4ai_service import Crawl4AiResponse
from corpscout_crawl_service.service import CrawlService

from tests.fakes import FakeCrawl4AiService


def test_search_fetch_endpoint_returns_search_page_artifact() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://bortigard.no/"],
                duration_ms=10,
            )
        }
    )
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=fake)))

    response = client.post(
        "/v1/search/fetch",
        json={
            "search_term": "BORTIGARD AS NO website",
            "search_engine": "duckduckgo",
            "timeout_seconds": 60,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["markdown_hash"] == "search-hash"
    assert body["links"] == ["https://bortigard.no/"]


def test_search_analyze_endpoint_returns_candidate_artifact() -> None:
    client = TestClient(
        create_app(
            crawl_service=CrawlService(
                crawl4ai_service=FakeCrawl4AiService({}),
                search_analyzer=StaticSearchAnalyzer(),
                site_analyzer=StaticSiteAnalyzer(),
            )
        )
    )

    response = client.post(
        "/v1/search/analyze",
        json={
            "company_name": "BORTIGARD AS",
            "organization_number": "810202572",
            "country": "NO",
            "search_engine": "duckduckgo",
            "search_term": "BORTIGARD AS NO website",
            "links": ["https://bortigard.no/"],
            "markdown": "# Search results",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["candidates"][0]["normalized_domain"] == "bortigard.no"
    assert body["candidates"][0]["score"] == 88


def test_page_crawl_endpoint_returns_crawl_artifact() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://bortigard.no/": Crawl4AiResponse(
                url="https://bortigard.no/",
                final_url="https://bortigard.no/",
                status="succeeded",
                markdown="# BORTIGARD AS",
                markdown_hash="site-hash",
                links=[],
                duration_ms=9,
            )
        }
    )
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=fake)))

    response = client.post("/v1/pages/crawl", json={"url": "https://bortigard.no/", "timeout_seconds": 60})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["markdown_hash"] == "site-hash"


def test_page_analyze_endpoint_returns_site_analysis_artifact() -> None:
    client = TestClient(
        create_app(
            crawl_service=CrawlService(
                crawl4ai_service=FakeCrawl4AiService({}),
                search_analyzer=StaticSearchAnalyzer(),
                site_analyzer=StaticSiteAnalyzer(),
            )
        )
    )

    response = client.post(
        "/v1/pages/analyze-company",
        json={
            "company_name": "BORTIGARD AS",
            "organization_number": "810202572",
            "country": "NO",
            "url": "https://bortigard.no/",
            "final_url": "https://bortigard.no/",
            "normalized_domain": "bortigard.no",
            "markdown": "# BORTIGARD AS",
            "candidate_score": 88,
            "candidate_reason": "Search result matches.",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["analysis"]["decision"] == "accepted"
    assert body["analysis"]["site_type"] == "company_website"
    assert body["analysis"]["owned_domain"] is True


class StaticSearchAnalyzer:
    async def analyze_search(self, payload):
        return {
            "candidates": [
                {
                    "url": "https://bortigard.no/",
                    "domain": "bortigard.no",
                    "score": 88,
                    "reason": "Search result matches.",
                }
            ]
        }


class StaticSiteAnalyzer:
    async def analyze_site(self, payload):
        return {
            "decision": "accepted",
            "score": 91,
            "site_type": "company_website",
            "relationship": "primary_web_presence",
            "owned_domain": True,
            "reason": "The page names BORTIGARD AS.",
            "evidence": ["BORTIGARD AS"],
        }
