import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner

from ex3.crawler import AnalysisSettings, load_manifest, run_analysis
from ex3.main import cli
from ex3.models import (
    BatchAnalysis,
    CrawlManifest,
    CrawlStats,
    LanguageDiscovery,
    MarkdownPage,
    PageExtraction,
    PageExtractionMetadata,
)


class ManifestLoadingTest(unittest.TestCase):
    def test_resolves_relative_markdown_paths_from_manifest_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            markdown_path = directory / "page.md"
            markdown_path.write_text("# Example", encoding="utf-8")
            manifest_path = directory / "crawl-manifest.json"
            manifest_path.write_text(
                _manifest(markdown_path="page.md").model_dump_json(indent=2),
                encoding="utf-8",
            )

            manifest = load_manifest(manifest_path)

        self.assertEqual(
            manifest.markdown_pages[0].markdown_path,
            str(markdown_path.resolve()),
        )

    def test_rejects_manifest_when_markdown_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            manifest_path = Path(temporary_directory) / "crawl-manifest.json"
            manifest_path.write_text(
                _manifest(markdown_path="missing.md").model_dump_json(indent=2),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(FileNotFoundError, "missing.md"):
                load_manifest(manifest_path)


class SeparateAnalysisPhaseTest(unittest.IsolatedAsyncioTestCase):
    async def test_analyzes_manifest_without_starting_a_browser(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            markdown_path = directory / "page.md"
            markdown_path.write_text("# Example", encoding="utf-8")
            manifest_path = directory / "crawl-manifest.json"
            manifest_path.write_text(
                _manifest(markdown_path="page.md").model_dump_json(indent=2),
                encoding="utf-8",
            )
            page = PageExtraction(
                source_url="https://example.com/",
                extraction_metadata=PageExtractionMetadata(
                    depth=0,
                    markdown_path=str(markdown_path),
                    batch_number=1,
                    succeeded=True,
                ),
            )
            batch = BatchAnalysis(
                batch_number=1,
                page_urls=[page.source_url],
                markdown_chars=9,
                succeeded=True,
            )

            with (
                patch("ex3.crawler.launch_async") as launch_browser,
                patch(
                    "ex3.crawler._analyze_batches",
                    new=AsyncMock(return_value=([page], [batch], [])),
                ) as analyze_batches,
            ):
                report = await run_analysis(
                    AnalysisSettings(
                        manifest_path=manifest_path,
                        max_page_chars=30_000,
                        max_batch_pages=5,
                        max_batch_chars=60_000,
                        analysis_timeout_seconds=300,
                    )
                )

        launch_browser.assert_not_called()
        analyze_batches.assert_awaited_once()
        self.assertEqual(report.batch_stats.submitted_pages, 1)
        self.assertEqual(report.batch_stats.successful_batches, 1)


class PhaseCliTest(unittest.TestCase):
    def test_crawl_and_analysis_expose_separate_options(self) -> None:
        runner = CliRunner()

        crawl_help = runner.invoke(cli, ["crawl", "--help"])
        analysis_help = runner.invoke(cli, ["analyze", "--help"])

        self.assertEqual(crawl_help.exit_code, 0)
        self.assertEqual(analysis_help.exit_code, 0)
        self.assertIn("--max-pages", crawl_help.output)
        self.assertNotIn("--max-page-chars", crawl_help.output)
        self.assertNotIn("--max-batch-pages", crawl_help.output)
        self.assertIn("--max-page-chars", analysis_help.output)
        self.assertIn("--max-batch-pages", analysis_help.output)
        self.assertNotIn("--max-pages", analysis_help.output)


def _manifest(*, markdown_path: str) -> CrawlManifest:
    return CrawlManifest(
        requested_start_url="https://example.com/",
        selected_base_url="https://example.com/",
        stopped_reason="max_pages_reached",
        language_discovery=LanguageDiscovery(
            requested_url="https://example.com/",
            probe_url="https://example.com/",
            probe_succeeded=True,
            probe_language="en",
            selected_base_url="https://example.com/",
            selected_language="en",
            selection_method="already_english",
        ),
        crawl_stats=CrawlStats(
            configured_max_pages=1,
            configured_max_depth=0,
            include_external=False,
            pages_returned=1,
            successful_pages=1,
            failed_pages=0,
            stored_markdown_pages=1,
            stored_markdown_chars=9,
            max_depth_reached=0,
        ),
        failed_pages=[],
        markdown_pages=[
            MarkdownPage(
                source_url="https://example.com/",
                depth=0,
                markdown_path=markdown_path,
                markdown_chars=9,
            )
        ],
    )


if __name__ == "__main__":
    unittest.main()
