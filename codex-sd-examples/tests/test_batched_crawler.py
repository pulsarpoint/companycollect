import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from crawl4ai.models import CrawlResult
from openai_codex import AsyncCodex, AsyncTurnHandle

from ex1.models import Contact, Evidence, UsefulInformation
from ex3.crawler import (
    BatchOutcome,
    MarkdownBatch,
    _collect_bfs_results,
    _run_turn_with_timeout,
    _validate_batch_output,
    create_batches,
    persist_markdown_pages,
)
from ex3.language import inspect_language_page, is_english_language
from ex3.models import (
    BatchExtraction,
    MarkdownPage,
    PageExtraction,
    batch_extraction_output_schema,
)
from ex3.prompty import create_prompt


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
                batch_number=2,
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


def _crawl_result(url: str, *, success: bool) -> CrawlResult:
    return CrawlResult(
        url=url,
        html="",
        success=success,
        error_message=None if success else "failed",
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
