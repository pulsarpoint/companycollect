import unittest
from typing import Any, ClassVar, Self
from unittest.mock import patch

from ex3.candidates import PageCandidate
from ex3.llm import StructuredTurnOutcome
from ex3.llm_selection import select_pages_with_llm
from ex3.models import PageSelectionDecision, PageSelectionResponse
from ex3.prompty import create_page_selection_prompt
from ex3.seeding import (
    HeadMetadata,
    fetch_head_metadata,
    seed_sitemap_urls,
    select_pages,
)

BASE_URL = "https://www.example.se/en/"


class _FakeSeeder:
    instances: ClassVar[list["_FakeSeeder"]] = []
    entries: ClassVar[list[dict[str, Any]]] = []
    head_entries: ClassVar[list[dict[str, Any]]] = []
    error: ClassVar[Exception | None] = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.domains: list[str] = []
        self.configs: list[Any] = []
        self.head_urls: list[str] = []
        self.closed = False
        _FakeSeeder.instances.append(self)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closed = True

    async def urls(self, domain: str, config: Any) -> list[dict[str, Any]]:
        if _FakeSeeder.error is not None:
            raise _FakeSeeder.error
        self.domains.append(domain)
        self.configs.append(config)
        return list(_FakeSeeder.entries)

    async def extract_head_for_urls(
        self,
        urls: list[str],
        config: Any = None,
        concurrency: int = 10,
        timeout: int = 5,
    ) -> list[dict[str, Any]]:
        self.head_urls.extend(urls)
        return [entry for entry in _FakeSeeder.head_entries if entry["url"] in urls]


class SitemapSeedingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        _FakeSeeder.instances = []
        _FakeSeeder.entries = []
        _FakeSeeder.head_entries = []
        _FakeSeeder.error = None

    async def test_returns_unique_sitemap_urls_for_the_base_host(self) -> None:
        _FakeSeeder.entries = [
            {"url": "https://www.example.se/en/", "status": "unknown"},
            {"url": "https://www.example.se/en/about-us", "status": "unknown"},
            {"url": "https://www.example.se/en/", "status": "unknown"},
        ]

        with patch("ex3.seeding.AsyncUrlSeeder", _FakeSeeder):
            outcome = await seed_sitemap_urls(
                BASE_URL,
                source="sitemap",
                max_urls=100,
                accept_language="en-US,en;q=0.9",
                proxy=None,
            )

        self.assertIsNone(outcome.error)
        self.assertEqual(
            outcome.urls,
            ["https://www.example.se/en/", "https://www.example.se/en/about-us"],
        )
        seeder = _FakeSeeder.instances[0]
        self.assertEqual(seeder.domains, ["www.example.se"])
        self.assertEqual(seeder.configs[0].source, "sitemap")
        self.assertEqual(seeder.configs[0].max_urls, 100)
        self.assertFalse(seeder.configs[0].extract_head)
        self.assertTrue(seeder.closed)

    async def test_reports_seeding_errors_instead_of_raising(self) -> None:
        _FakeSeeder.error = RuntimeError("sitemap fetch failed")

        with patch("ex3.seeding.AsyncUrlSeeder", _FakeSeeder):
            outcome = await seed_sitemap_urls(
                BASE_URL,
                source="sitemap",
                max_urls=100,
                accept_language="en",
                proxy=None,
            )

        self.assertEqual(outcome.urls, [])
        self.assertIn("sitemap fetch failed", outcome.error or "")

    async def test_fetches_language_title_and_description_from_heads(self) -> None:
        _FakeSeeder.head_entries = [
            {
                "url": "https://www.example.se/alingsas",
                "status": "valid",
                "head_data": {
                    "lang": "sv",
                    "title": "Handelsbanken Alingsås",
                    "meta": {"description": "Kontoret i Alingsås"},
                },
            },
            {
                "url": "https://www.example.se/en/about-us",
                "status": "valid",
                "head_data": {"lang": "", "title": None, "meta": {}},
            },
            {
                "url": "https://www.example.se/en/broken",
                "status": "not_valid",
                "head_data": {},
            },
        ]

        with patch("ex3.seeding.AsyncUrlSeeder", _FakeSeeder):
            metadata = await fetch_head_metadata(
                [
                    "https://www.example.se/alingsas",
                    "https://www.example.se/en/about-us",
                    "https://www.example.se/en/broken",
                ],
                accept_language="en",
                proxy=None,
                concurrency=4,
            )

        self.assertEqual(
            metadata["https://www.example.se/alingsas"],
            HeadMetadata(
                language="sv",
                title="Handelsbanken Alingsås",
                description="Kontoret i Alingsås",
            ),
        )
        self.assertEqual(
            metadata["https://www.example.se/en/about-us"],
            HeadMetadata(language=None, title=None, description=None),
        )
        self.assertNotIn("https://www.example.se/en/broken", metadata)


class PageSelectionPlanTest(unittest.IsolatedAsyncioTestCase):
    async def test_checks_heads_for_a_shortlist_and_ranks_non_english_pages_last(
        self,
    ) -> None:
        inventory = [
            "https://www.example.se/en/about-us/legal-documents/privacy-notice",
            "https://www.example.se/alingsas",
            "https://www.example.se/en/careers",
            "https://www.example.se/en/about-us",
            "https://www.example.se/sv/om-oss",
            "https://www.example.se/en/",
            "https://www.example.se/en/reports/annual.pdf",
            "https://www.example.se/en/personal/contact-and-support",
        ]
        checked: list[list[str]] = []

        async def fetch_heads(urls: list[str]) -> dict[str, HeadMetadata]:
            checked.append(list(urls))
            return {
                "https://www.example.se/alingsas": HeadMetadata(
                    language="sv",
                    title="Alingsås",
                    description=None,
                ),
                "https://www.example.se/en/about-us": HeadMetadata(
                    language="en",
                    title="About the company",
                    description=None,
                ),
            }

        result = await select_pages(
            inventory,
            base_url=BASE_URL,
            limit=3,
            fetch_heads=fetch_heads,
        )

        self.assertEqual(
            [scored.url for scored in result.selected],
            [
                "https://www.example.se/en/",
                "https://www.example.se/en/about-us",
                "https://www.example.se/en/personal/contact-and-support",
            ],
        )
        self.assertEqual(len(checked), 1)
        self.assertIn("https://www.example.se/alingsas", checked[0])
        eligible_by_url = {scored.url: scored for scored in result.eligible}
        self.assertIn(
            "document language 'sv'",
            eligible_by_url["https://www.example.se/alingsas"].reasons,
        )
        self.assertIn(
            "other locale 'sv'",
            eligible_by_url["https://www.example.se/sv/om-oss"].reasons,
        )
        self.assertEqual(
            {scored.url: scored.exclusion for scored in result.excluded},
            {"https://www.example.se/en/reports/annual.pdf": "file extension '.pdf'"},
        )
        self.assertEqual(result.inventory_urls, 8)
        self.assertEqual(result.base_page_links, 0)
        self.assertEqual(result.head_checked_urls, len(checked[0]))
        self.assertEqual(result.selected[1].language, "en")
        self.assertEqual(result.selected[1].title, "About the company")

    async def test_always_considers_the_base_url_and_base_page_links(self) -> None:
        async def fetch_heads(urls: list[str]) -> dict[str, HeadMetadata]:
            return {}

        result = await select_pages(
            ["https://www.example.se/en/personal/savings"],
            base_url=BASE_URL,
            limit=2,
            fetch_heads=fetch_heads,
            base_page_links=[
                "https://www.example.se/en/about-us",
                "https://www.example.se/sv/om-oss",
            ],
        )

        self.assertEqual(
            [scored.url for scored in result.selected],
            ["https://www.example.se/en/", "https://www.example.se/en/about-us"],
        )
        self.assertIn("linked from base page", result.selected[1].reasons)
        self.assertEqual(result.inventory_urls, 4)
        self.assertEqual(result.base_page_links, 2)

    async def test_ignores_excluded_urls_such_as_already_crawled_pages(self) -> None:
        async def fetch_heads(urls: list[str]) -> dict[str, HeadMetadata]:
            return {}

        result = await select_pages(
            [
                "https://www.example.se/en/about-us",
                "https://www.example.se/en/contact",
            ],
            base_url=BASE_URL,
            limit=2,
            fetch_heads=fetch_heads,
            exclude_urls={
                "https://www.example.se/en",
                "https://www.example.se/en/about-us/",
            },
        )

        self.assertEqual(
            [scored.url for scored in result.selected],
            ["https://www.example.se/en/contact"],
        )
        self.assertEqual(result.inventory_urls, 1)

    async def test_prefers_the_site_language_over_other_languages(self) -> None:
        async def fetch_heads(urls: list[str]) -> dict[str, HeadMetadata]:
            return {
                "https://www.example.se/sv/om-oss": HeadMetadata(
                    language="sv",
                    title="Om oss",
                    description=None,
                )
            }

        result = await select_pages(
            ["https://www.example.se/fi/yritys", "https://www.example.se/sv/om-oss"],
            base_url="https://www.example.se/sv/",
            limit=5,
            fetch_heads=fetch_heads,
            preferred_languages=frozenset({"en", "sv"}),
        )

        self.assertEqual(
            [scored.url for scored in result.selected],
            [
                "https://www.example.se/sv/",
                "https://www.example.se/sv/om-oss",
                "https://www.example.se/fi/yritys",
            ],
        )
        self.assertNotIn("other locale 'sv'", result.selected[1].reasons)
        self.assertEqual(result.selected[1].language, "sv")
        self.assertIn("other locale 'fi'", result.selected[2].reasons)

    async def test_selects_only_the_base_url_for_an_empty_inventory(self) -> None:
        async def fetch_heads(urls: list[str]) -> dict[str, HeadMetadata]:
            return {}

        result = await select_pages(
            [],
            base_url=BASE_URL,
            limit=5,
            fetch_heads=fetch_heads,
        )

        self.assertEqual([scored.url for scored in result.selected], [BASE_URL])
        self.assertEqual(result.head_checked_urls, 1)


def _candidate(
    url: str, *, score: float = 10.0, title: str | None = None
) -> PageCandidate:
    return PageCandidate(url=url, score=score, title=title, source="inventory")


class LlmPageSelectionTest(unittest.IsolatedAsyncioTestCase):
    async def test_keeps_only_known_unique_picks_within_the_limit(self) -> None:
        candidates = [
            _candidate("https://www.example.se/en/", score=68.0, title="Home"),
            _candidate(
                "https://www.example.se/en/about-us", score=44.0, title="About us"
            ),
            _candidate("https://www.example.se/en/careers", score=34.0),
        ]
        response = PageSelectionResponse(
            pages=[
                PageSelectionDecision(
                    url="https://www.example.se/en/about-us",
                    reason="company profile",
                    expected_fields=["description", "founded_year"],
                ),
                PageSelectionDecision(
                    url="https://www.example.se/en/about-us/",
                    reason="duplicate",
                    expected_fields=[],
                ),
                PageSelectionDecision(
                    url="https://www.example.se/en/invented",
                    reason="made up",
                    expected_fields=[],
                ),
                PageSelectionDecision(
                    url="https://www.example.se/en/careers",
                    reason="jobs",
                    expected_fields=["jobs"],
                ),
                PageSelectionDecision(
                    url="https://www.example.se/en/",
                    reason="homepage",
                    expected_fields=["company_name"],
                ),
            ]
        )

        async def fake_turn(**kwargs):
            self.assertIn("about-us", kwargs["prompt"])
            return StructuredTurnOutcome(value=response, token_usage=None, error=None)

        with patch("ex3.llm_selection.run_structured_turn", new=fake_turn):
            picks, status = await select_pages_with_llm(
                candidates, base_url=BASE_URL, limit=2, timeout_seconds=30
            )

        self.assertEqual(
            [pick.url for pick in picks],
            ["https://www.example.se/en/about-us", "https://www.example.se/en/careers"],
        )
        self.assertEqual(picks[0].title, "About us")
        self.assertEqual(
            picks[0].reasons,
            ["llm", "company profile", "fields: description, founded_year"],
        )
        self.assertTrue(status.succeeded)
        self.assertEqual(len(status.warnings), 3)
        self.assertTrue(any("unknown" in warning for warning in status.warnings))
        self.assertTrue(any("duplicate" in warning for warning in status.warnings))
        self.assertTrue(any("limit" in warning for warning in status.warnings))

    async def test_reports_failure_and_returns_no_picks(self) -> None:
        async def fake_turn(**kwargs):
            return StructuredTurnOutcome(
                value=None, token_usage=None, error="timed out"
            )

        with patch("ex3.llm_selection.run_structured_turn", new=fake_turn):
            picks, status = await select_pages_with_llm(
                [_candidate("https://www.example.se/en/")],
                base_url=BASE_URL,
                limit=5,
                timeout_seconds=1,
            )

        self.assertEqual(picks, [])
        self.assertFalse(status.succeeded)
        self.assertEqual(status.error, "timed out")

    def test_prompt_lists_requirements_language_policy_and_candidates(self) -> None:
        prompt = create_page_selection_prompt(
            BASE_URL,
            candidates=[
                _candidate("https://www.example.se/en/about-us", title="About us")
            ],
            limit=20,
        )

        self.assertIn("- identifiers:", prompt)
        self.assertIn("at most 20", prompt)
        self.assertIn("only source", prompt)
        self.assertIn("https://www.example.se/en/about-us", prompt)
        self.assertIn("Never invent", prompt)


if __name__ == "__main__":
    unittest.main()
