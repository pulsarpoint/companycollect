import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from crawl4ai import AsyncWebCrawler
from crawl4ai.models import CrawlResult
from openai_codex import AsyncCodex, AsyncTurnHandle

from ex1.models import Contact, Evidence, UsefulInformation
from ex3.crawler import (
    BatchOutcome,
    CrawlSettings,
    MarkdownBatch,
    _collect_bfs_results,
    _crawl_seeded_pages,
    _internal_links,
    _preferred_languages,
    _run_turn_with_timeout,
    _select_and_crawl,
    _validate_batch_output,
    create_batches,
    persist_markdown_pages,
)
from ex3.language import (
    detect_document_language,
    inspect_language_page,
    is_english_language,
)
from ex3.models import (
    BatchExtraction,
    MarkdownPage,
    PageExtraction,
    batch_extraction_output_schema,
)
from ex3.prompty import create_prompt
from ex3.seeding import HeadMetadata, SeedingOutcome


class EnglishDiscoveryTest(unittest.TestCase):
    def test_prefers_alternate_hreflang_over_visible_language_link(self) -> None:
        language, candidates = inspect_language_page(
            "https://example.rs/",
            """
            <html lang="sr">
              <head>
                <link rel="alternate" hreflang="en-US" href="https://example.com/">
              </head>
              <body><a href="/en/">English</a></body>
            </html>
            """,
        )

        self.assertEqual(language, "sr")
        self.assertEqual(candidates[0].url, "https://example.com/")
        self.assertEqual(candidates[0].detection_method, "alternate_hreflang")
        self.assertEqual(candidates[1].url, "https://example.rs/en/")

    def test_detects_the_declared_document_language(self) -> None:
        self.assertEqual(
            detect_document_language('<html lang="sv-SE"><body></body></html>'),
            "sv-SE",
        )
        self.assertIsNone(detect_document_language("<html><body></body></html>"))
        self.assertIsNone(detect_document_language(""))

    def test_recognizes_english_language_variants(self) -> None:
        self.assertTrue(is_english_language("en"))
        self.assertTrue(is_english_language("en_GB"))
        self.assertFalse(is_english_language("de"))
        self.assertFalse(is_english_language(None))


class MarkdownPersistenceTest(unittest.TestCase):
    def test_persists_successful_pages_and_records_crawl_failures(self) -> None:
        successful = CrawlResult(
            url="https://example.com/",
            html="<html lang='en'></html>",
            success=True,
            markdown={
                "raw_markdown": "# Example\nUseful content",
                "markdown_with_citations": "",
                "references_markdown": "",
            },
            metadata={"depth": 0},
        )
        failed = CrawlResult(
            url="https://example.com/missing",
            html="",
            success=False,
            error_message="HTTP 404",
            metadata={"depth": 1},
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            markdown_pages, failures = persist_markdown_pages(
                [successful, failed],
                markdown_dir=Path(temporary_directory),
                max_markdown_chars=10_000,
            )
            stored_content = Path(markdown_pages[0].markdown_path).read_text(
                encoding="utf-8"
            )

        self.assertEqual(len(markdown_pages), 1)
        self.assertEqual(stored_content, "# Example\nUseful content")
        self.assertEqual(markdown_pages[0].depth, 0)
        self.assertEqual(len(failures), 1)
        self.assertIn("HTTP 404", failures[0].error)

    def test_records_language_selection_and_score_for_each_page(self) -> None:
        seeded = CrawlResult(
            url="https://example.com/en/about-us",
            html='<html lang="en"><body>About</body></html>',
            success=True,
            markdown={
                "raw_markdown": "# About",
                "markdown_with_citations": "",
                "references_markdown": "",
            },
            metadata={"depth": 0, "selection": "selected", "score": 71.5},
        )
        discovered = CrawlResult(
            url="https://example.com/alingsas",
            html='<html lang="sv"><body>Kontor</body></html>',
            success=True,
            markdown={
                "raw_markdown": "# Kontor",
                "markdown_with_citations": "",
                "references_markdown": "",
            },
            metadata={"depth": 1, "score": 12.0},
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            markdown_pages, _ = persist_markdown_pages(
                [seeded, discovered],
                markdown_dir=Path(temporary_directory),
                max_markdown_chars=10_000,
            )

        self.assertEqual(markdown_pages[0].language, "en")
        self.assertEqual(markdown_pages[0].selection, "selected")
        self.assertEqual(markdown_pages[0].score, 71.5)
        self.assertEqual(markdown_pages[1].language, "sv")
        self.assertEqual(markdown_pages[1].selection, "discovery")
        self.assertEqual(markdown_pages[1].score, 12.0)


class BfsCollectionTest(unittest.IsolatedAsyncioTestCase):
    async def test_counts_successful_pages_and_closes_the_stream(self) -> None:
        stream_closed = False
        crawl_cancelled = False

        async def result_stream():
            nonlocal stream_closed
            try:
                yield _crawl_result("https://example.com/failed", success=False)
                yield _crawl_result("https://example.com/1", success=True)
                yield _crawl_result("https://example.com/2", success=True)
                yield _crawl_result("https://example.com/3", success=True)
                yield _crawl_result("https://example.com/4", success=True)
            finally:
                stream_closed = True

        def cancel() -> None:
            nonlocal crawl_cancelled
            crawl_cancelled = True

        results = await _collect_bfs_results(
            result_stream(),
            max_successful_pages=3,
            cancel=cancel,
        )

        self.assertEqual(len(results), 4)
        self.assertEqual(sum(result.success for result in results), 3)
        self.assertTrue(crawl_cancelled)
        self.assertTrue(stream_closed)

    async def test_does_not_count_pages_that_were_already_crawled(self) -> None:
        async def result_stream():
            yield _crawl_result("https://example.com/", success=True)
            yield _crawl_result("https://example.com/2", success=True)
            yield _crawl_result("https://example.com/3", success=True)
            yield _crawl_result("https://example.com/4", success=True)

        results = await _collect_bfs_results(
            result_stream(),
            max_successful_pages=2,
            cancel=lambda: None,
            already_crawled={"https://example.com"},
        )

        self.assertEqual(
            [result.url for result in results],
            ["https://example.com/", "https://example.com/2", "https://example.com/3"],
        )


class PreferredLanguagesTest(unittest.TestCase):
    def test_always_prefers_english_plus_the_selected_site_language(self) -> None:
        self.assertEqual(_preferred_languages(None), frozenset({"en"}))
        self.assertEqual(_preferred_languages("en-GB"), frozenset({"en"}))
        self.assertEqual(_preferred_languages("sv-SE"), frozenset({"en", "sv"}))
        self.assertEqual(_preferred_languages(" NB "), frozenset({"en", "nb"}))


class BasePageLinkHarvestTest(unittest.TestCase):
    def test_returns_normalized_internal_links_only(self) -> None:
        result = CrawlResult(
            url="https://www.example.se/en/",
            html="",
            success=True,
            links={
                "internal": [
                    {"href": "/en/about-us"},
                    {"href": "https://www.example.se/en/about-us#team"},
                    {"href": "mailto:info@example.se"},
                    {"href": ""},
                ],
                "external": [{"href": "https://www.linkedin.com/company/x"}],
            },
        )

        links = _internal_links(result, base_url="https://www.example.se/en/")

        self.assertEqual(links, ["https://www.example.se/en/about-us"])

    def test_returns_nothing_for_a_missing_result(self) -> None:
        self.assertEqual(_internal_links(None, base_url="https://www.example.se/"), [])


class TwoWaveSelectionTest(unittest.IsolatedAsyncioTestCase):
    async def test_second_wave_ranks_links_harvested_from_first_wave_pages(
        self,
    ) -> None:
        base_url = "https://www.example.se/en/"
        crawler = _FakeCrawler(
            [
                _crawl_result(
                    base_url,
                    success=True,
                    links=[{"href": "/en/careers"}],
                ),
                _crawl_result(
                    "https://www.example.se/en/about-us",
                    success=True,
                    links=[{"href": "/en/contact"}, {"href": "/en/about-us"}],
                ),
                _crawl_result("https://www.example.se/en/contact", success=True),
                _crawl_result("https://www.example.se/en/careers", success=True),
            ]
        )
        base_result = _crawl_result(
            base_url,
            success=True,
            links=[{"href": "/en/about-us"}],
        )

        async def fake_seed(*args, **kwargs) -> SeedingOutcome:
            return SeedingOutcome(urls=["https://www.example.se/en/personal/savings"])

        async def fake_heads(urls, **kwargs) -> dict[str, HeadMetadata]:
            return {}

        with (
            patch("ex3.crawler.seed_sitemap_urls", new=fake_seed),
            patch("ex3.crawler.fetch_head_metadata", new=fake_heads),
        ):
            seeding, results = await _select_and_crawl(
                cast(AsyncWebCrawler, crawler),
                settings=_crawl_settings(max_pages=3, seed_share=0.6),
                base_url=base_url,
                base_result=base_result,
                markdown_dir=Path(tempfile.mkdtemp()),
            )

        self.assertEqual(
            crawler.requested_urls,
            [
                base_url,
                "https://www.example.se/en/about-us",
                "https://www.example.se/en/contact",
            ],
        )
        self.assertEqual(len(results), 3)
        self.assertEqual(seeding.selection_waves, 2)
        self.assertEqual(seeding.harvested_links, 2)
        self.assertEqual(
            [scored.url for scored in seeding.selected],
            [
                base_url,
                "https://www.example.se/en/about-us",
                "https://www.example.se/en/contact",
            ],
        )


class SeededCrawlTest(unittest.IsolatedAsyncioTestCase):
    async def test_tags_seeded_results_with_selection_metadata(self) -> None:
        crawler = _FakeCrawler(
            [
                _crawl_result("https://example.com/en/about-us", success=True),
                _crawl_result("https://example.com/en/careers", success=False),
            ]
        )

        results = await _crawl_seeded_pages(
            cast(AsyncWebCrawler, crawler),
            scored_urls={
                "https://example.com/en/about-us": 71.5,
                "https://example.com/en/careers": 32.0,
            },
            settings=_crawl_settings(),
        )

        self.assertEqual(
            crawler.requested_urls,
            ["https://example.com/en/about-us", "https://example.com/en/careers"],
        )
        self.assertEqual(len(results), 2)
        about = next(result for result in results if result.url.endswith("about-us"))
        metadata = about.metadata
        if metadata is None:
            self.fail("Seeded result is missing metadata")
        self.assertEqual(metadata["selection"], "selected")
        self.assertEqual(metadata["score"], 71.5)
        self.assertEqual(metadata["depth"], 0)

    async def test_returns_no_results_without_seeded_urls(self) -> None:
        crawler = _FakeCrawler([])

        results = await _crawl_seeded_pages(
            cast(AsyncWebCrawler, crawler),
            scored_urls={},
            settings=_crawl_settings(),
        )

        self.assertEqual(results, [])
        self.assertEqual(crawler.requested_urls, [])


class MarkdownBatchTest(unittest.TestCase):
    def test_batches_by_page_and_character_limits(self) -> None:
        pages = [
            MarkdownPage(
                source_url=f"https://example.com/{index}",
                depth=1,
                markdown_path=f"/tmp/{index}.md",
                markdown_chars=size,
            )
            for index, size in enumerate([60, 60, 30, 30], start=1)
        ]

        batches = create_batches(
            pages,
            max_page_chars=1_000,
            max_batch_pages=2,
            max_batch_chars=100,
        )

        self.assertEqual([len(batch.pages) for batch in batches], [1, 2, 1])
        self.assertEqual([batch.markdown_chars for batch in batches], [60, 90, 30])

    def test_applies_analysis_page_limit_before_batching(self) -> None:
        pages = [
            MarkdownPage(
                source_url=f"https://example.com/{index}",
                depth=1,
                markdown_path=f"/tmp/{index}.md",
                markdown_chars=1_000,
            )
            for index in range(3)
        ]

        batches = create_batches(
            pages,
            max_page_chars=30,
            max_batch_pages=5,
            max_batch_chars=60,
        )

        self.assertEqual([len(batch.pages) for batch in batches], [2, 1])
        self.assertEqual([batch.markdown_chars for batch in batches], [60, 30])

    def test_validates_page_ids_and_replaces_evidence_urls(self) -> None:
        stored_page = MarkdownPage(
            source_url="https://example.com/contact",
            depth=1,
            markdown_path="/tmp/contact.md",
            markdown_chars=100,
        )
        batch = MarkdownBatch(number=1, pages=(stored_page,), markdown_chars=100)
        extraction = PageExtraction(
            source_url=stored_page.source_url,
            useful_information=UsefulInformation(
                contacts=[
                    Contact(
                        type="email",
                        value="hello@example.com",
                        evidence=Evidence(
                            text="hello@example.com",
                            source_url="https://wrong.example/",
                        ),
                    )
                ]
            ),
        )

        pages, warnings, failures = _validate_batch_output(
            batch,
            outcome=BatchOutcome(
                extraction=BatchExtraction(pages=[extraction]),
                token_usage=None,
                error=None,
            ),
        )

        self.assertEqual(warnings, [])
        self.assertEqual(failures, [])
        metadata = pages[0].extraction_metadata
        if metadata is None:
            self.fail("Validated page is missing extraction metadata")
        self.assertTrue(metadata.succeeded)
        self.assertEqual(
            pages[0].useful_information.contacts[0].evidence.source_url,
            stored_page.source_url,
        )


class TurnTimeoutCleanupTest(unittest.IsolatedAsyncioTestCase):
    async def test_force_closes_and_drains_a_blocked_turn(self) -> None:
        released = asyncio.Event()
        codex = _BlockedCodex(released)
        turn = _BlockedTurn(released)

        with (
            patch("ex3.crawler.TURN_INTERRUPT_TIMEOUT_SECONDS", 0),
            patch("ex3.crawler.TURN_COMPLETION_TIMEOUT_SECONDS", 0),
        ):
            result, timed_out = await _run_turn_with_timeout(
                cast(AsyncCodex, codex),
                cast(AsyncTurnHandle, turn),
                timeout_seconds=0,
                operation_name="analysis batch 2",
            )

        self.assertIsNone(result)
        self.assertTrue(timed_out)
        self.assertTrue(codex.closed)
        self.assertTrue(turn.run_finished)
        self.assertTrue(turn.interrupt_finished)
        self.assertFalse(turn.run_cancelled)


class BatchPromptSchemaTest(unittest.TestCase):
    def test_prompt_contains_stable_page_ids_but_no_navigation_task(self) -> None:
        page = MarkdownPage(
            source_url="https://example.com/about",
            depth=1,
            markdown_path="/tmp/about.md",
            markdown_chars=20,
        )

        prompt = create_prompt(1, pages=[(page, "# About Example")])

        self.assertIn(page.source_url, prompt)
        self.assertIn("any language", prompt)
        self.assertIn("in English", prompt)
        self.assertIn("every supplied page has been persisted", prompt)
        self.assertIn("exactly one pages entry", prompt)
        self.assertIn("Do not browse", prompt)
        self.assertNotIn("next_links", prompt)

    def test_schema_excludes_runtime_metadata_and_navigation(self) -> None:
        schema = batch_extraction_output_schema()
        serialized = str(schema)

        self.assertNotIn("extraction_metadata", serialized)
        self.assertNotIn("next_links", serialized)
        self._assert_strict_objects(schema)

    def _assert_strict_objects(self, schema: object) -> None:
        if isinstance(schema, list):
            for item in schema:
                self._assert_strict_objects(item)
            return
        if not isinstance(schema, dict):
            return

        self.assertNotIn("default", schema)
        properties = schema.get("properties")
        if isinstance(properties, dict):
            self.assertEqual(set(schema["required"]), set(properties))
        for value in schema.values():
            self._assert_strict_objects(value)


def _crawl_result(
    url: str,
    *,
    success: bool,
    links: list[dict[str, str]] | None = None,
) -> CrawlResult:
    return CrawlResult(
        url=url,
        html="",
        success=success,
        error_message=None if success else "failed",
        links={"internal": links or [], "external": []},
    )


class _FakeCrawler:
    def __init__(self, results: list[CrawlResult]) -> None:
        self._results = results
        self.requested_urls: list[str] = []

    async def arun_many(self, urls: list[str], config: object):
        self.requested_urls.extend(urls)
        requested = set(urls)

        async def stream():
            for result in self._results:
                if result.url in requested:
                    yield result

        return stream()


def _crawl_settings(*, max_pages: int = 5, seed_share: float = 0.6) -> CrawlSettings:
    return CrawlSettings(
        start_url="https://example.com/",
        markdown_dir=Path("/tmp/unused"),
        max_pages=max_pages,
        max_depth=2,
        max_markdown_chars=10_000,
        crawl_concurrency=2,
        cdp_port=9_245,
        headless=True,
        include_external=False,
        check_robots_txt=True,
        proxy=None,
        seed=True,
        seed_source="sitemap",
        seed_max_urls=1_000,
        seed_share=seed_share,
        discovery_strategy="best_first",
        accept_language="en-US,en;q=0.9",
    )


class _BlockedCodex:
    def __init__(self, released: asyncio.Event) -> None:
        self._released = released
        self.closed = False

    async def close(self) -> None:
        self.closed = True
        self._released.set()


class _BlockedTurn:
    def __init__(self, released: asyncio.Event) -> None:
        self._released = released
        self.run_finished = False
        self.interrupt_finished = False
        self.run_cancelled = False

    async def run(self) -> None:
        try:
            await self._released.wait()
        except asyncio.CancelledError:
            self.run_cancelled = True
            raise
        finally:
            self.run_finished = True

    async def interrupt(self) -> None:
        await self._released.wait()
        self.interrupt_finished = True


if __name__ == "__main__":
    unittest.main()
