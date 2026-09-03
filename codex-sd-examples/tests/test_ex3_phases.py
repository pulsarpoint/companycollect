import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner

from ex3.crawler import (
    AnalysisSettings,
    _external_domain_candidates,
    _validate_related_domain_selection,
    discover_urls,
    load_manifest,
    run_analysis,
)
from ex3.main import cli
from ex3.models import (
    BatchAnalysis,
    CrawlManifest,
    CrawlStats,
    DiscoveredUrl,
    LanguageDiscovery,
    LlmCallStatus,
    MarkdownPage,
    PageExtraction,
    PageExtractionMetadata,
    PassSummary,
    RelatedDomain,
    RelatedDomainAnalysis,
    RelatedDomainDecision,
    RelatedDomainSelection,
    ScoredUrl,
    UrlSeeding,
)
from ex3.prompty import create_related_domains_prompt


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

    def test_loads_manifests_written_before_page_selection_existed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            (directory / "page.md").write_text("# Example", encoding="utf-8")
            legacy = json.loads(_manifest(markdown_path="page.md").model_dump_json())
            legacy.pop("url_seeding")
            legacy.pop("discovery_strategy")
            for page in legacy["markdown_pages"]:
                for field in ("language", "selection", "score"):
                    page.pop(field)
            manifest_path = directory / "crawl-manifest.json"
            manifest_path.write_text(json.dumps(legacy), encoding="utf-8")

            manifest = load_manifest(manifest_path)

        self.assertIsNone(manifest.url_seeding)
        self.assertEqual(manifest.discovery_strategy, "breadth_first")
        self.assertIsNone(manifest.markdown_pages[0].language)
        self.assertEqual(manifest.markdown_pages[0].selection, "discovery")

    def test_rejects_manifest_when_markdown_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            manifest_path = Path(temporary_directory) / "crawl-manifest.json"
            manifest_path.write_text(
                _manifest(markdown_path="missing.md").model_dump_json(indent=2),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(FileNotFoundError, "missing.md"):
                load_manifest(manifest_path)

    def test_new_selection_fields_have_defaults_and_round_trip(self) -> None:
        page = MarkdownPage(
            source_url="https://example.com/jobs",
            depth=0,
            markdown_path="jobs.md",
            markdown_chars=3,
            selection="suggested",
            selection_reason="jobs gap",
            pass_number=2,
        )
        seeding = UrlSeeding(
            enabled=True,
            source="sitemap",
            succeeded=True,
            selector="llm",
            selection_method="llm",
            candidate_count=120,
            llm=LlmCallStatus(attempted=True, succeeded=True),
        )

        restored = MarkdownPage.model_validate_json(page.model_dump_json())
        legacy = MarkdownPage.model_validate(
            {
                "source_url": "https://example.com/",
                "depth": 0,
                "markdown_path": "a.md",
                "markdown_chars": 1,
            }
        )

        self.assertEqual(restored, page)
        self.assertEqual(legacy.pass_number, 1)
        self.assertIsNone(legacy.selection_reason)
        self.assertEqual(seeding.selection_method, "llm")
        self.assertEqual(
            PassSummary(
                pass_number=1, pages=1, new_pages=1, batches=1
            ).token_totals.total_tokens,
            0,
        )


class SeparateAnalysisPhaseTest(unittest.IsolatedAsyncioTestCase):
    async def test_analyzes_manifest_without_starting_a_browser(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            markdown_path = directory / "page.md"
            markdown_path.write_text(
                "# Example\n[Global](https://example.org)",
                encoding="utf-8",
            )
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
            related_domain = RelatedDomain(
                domain="example.org",
                url="https://example.org/",
                relationship="official_global_site",
                reason="The candidate is labelled as the global site.",
                labels=["Global"],
                source_urls=[page.source_url],
                occurrences=1,
            )
            domain_analysis = RelatedDomainAnalysis(
                attempted=True,
                candidate_domains=1,
                succeeded=True,
            )

            with (
                patch("ex3.crawler.launch_async") as launch_browser,
                patch(
                    "ex3.crawler._analyze_related_domains",
                    new=AsyncMock(return_value=([related_domain], domain_analysis)),
                ) as analyze_domains,
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
                        skip_non_english=True,
                    )
                )

        launch_browser.assert_not_called()
        analyze_domains.assert_awaited_once()
        analyze_batches.assert_awaited_once()
        self.assertEqual(report.batch_stats.submitted_pages, 1)
        self.assertEqual(report.batch_stats.successful_batches, 1)
        self.assertEqual(len(report.discovered_urls), 1)
        self.assertEqual(report.related_domains, [related_domain])
        self.assertEqual(report.analysis_stats.attempted_analyses, 2)

    async def test_skips_non_english_pages_but_still_discovers_their_links(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            (directory / "english.md").write_text("# Example", encoding="utf-8")
            (directory / "swedish.md").write_text(
                "# Kontor\n[Global](https://example.org)",
                encoding="utf-8",
            )
            manifest_path = directory / "crawl-manifest.json"
            manifest_path.write_text(
                _manifest(
                    markdown_path="english.md",
                    extra_pages=[
                        MarkdownPage(
                            source_url="https://example.com/alingsas",
                            depth=1,
                            markdown_path="swedish.md",
                            markdown_chars=9,
                            language="sv",
                        )
                    ],
                ).model_dump_json(indent=2),
                encoding="utf-8",
            )
            captured_batches = []

            async def fake_analyze_batches(batches, **kwargs):
                captured_batches.extend(batches)
                return [], [], []

            with (
                patch(
                    "ex3.crawler._analyze_related_domains",
                    new=AsyncMock(
                        return_value=(
                            [],
                            RelatedDomainAnalysis(
                                attempted=False,
                                candidate_domains=0,
                                succeeded=True,
                            ),
                        )
                    ),
                ),
                patch("ex3.crawler._analyze_batches", new=fake_analyze_batches),
            ):
                report = await run_analysis(
                    AnalysisSettings(
                        manifest_path=manifest_path,
                        max_page_chars=30_000,
                        max_batch_pages=5,
                        max_batch_chars=60_000,
                        analysis_timeout_seconds=300,
                        skip_non_english=True,
                    )
                )

        submitted_urls = [
            page.source_url for batch in captured_batches for page in batch.pages
        ]
        self.assertEqual(submitted_urls, ["https://example.com/"])
        self.assertEqual(
            [page.source_url for page in report.skipped_pages],
            ["https://example.com/alingsas"],
        )
        self.assertEqual(report.batch_stats.submitted_pages, 1)
        self.assertEqual(report.batch_stats.skipped_non_english_pages, 1)
        self.assertEqual(
            {discovered.url for discovered in report.discovered_urls},
            {"https://example.org/"},
        )


class UrlDiscoveryTest(unittest.TestCase):
    def test_collects_all_urls_before_related_domain_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            first_path = directory / "first.md"
            first_path.write_text(
                """
                [Global startpage](https://www.handelsbanken.com)
                [Netherlands](https://www.handelsbanken.nl)
                [Norway](https://www.handelsbanken.no)
                [United Kingdom](https://www.handelsbanken.co.uk)
                [Current site](https://www.handelsbanken.se/en/)
                [Contact](/en/contact)
                [Privacy provider](https://www.citibank.com/privacy)
                [LinkedIn](https://www.linkedin.com/company/handelsbanken/)
                """,
                encoding="utf-8",
            )
            second_path = directory / "second.md"
            second_path.write_text(
                """
                [Global site](https://www.handelsbanken.com/sv/)
                [Global startpage](https://www.handelsbanken.com)
                [United Kingdom](https://www.handelsbanken.co.uk/contact)
                """,
                encoding="utf-8",
            )
            pages = [
                MarkdownPage(
                    source_url="https://www.handelsbanken.se/en/",
                    depth=0,
                    markdown_path=str(first_path),
                    markdown_chars=first_path.stat().st_size,
                ),
                MarkdownPage(
                    source_url="https://www.handelsbanken.se/en/about-us",
                    depth=1,
                    markdown_path=str(second_path),
                    markdown_chars=second_path.stat().st_size,
                ),
            ]

            discovered_urls = discover_urls(
                pages,
                searched_url="https://www.handelsbanken.se/en/",
            )

        self.assertEqual(
            {discovered.url for discovered in discovered_urls},
            {
                "https://www.handelsbanken.com/",
                "https://www.handelsbanken.com/sv/",
                "https://www.handelsbanken.nl/",
                "https://www.handelsbanken.no/",
                "https://www.handelsbanken.co.uk/",
                "https://www.handelsbanken.co.uk/contact",
                "https://www.handelsbanken.se/en/",
                "https://www.handelsbanken.se/en/contact",
                "https://www.citibank.com/privacy",
                "https://www.linkedin.com/company/handelsbanken/",
            },
        )
        discovered_by_url = {
            discovered.url: discovered for discovered in discovered_urls
        }
        global_homepage = discovered_by_url["https://www.handelsbanken.com/"]
        self.assertEqual(global_homepage.occurrences, 2)
        self.assertEqual(len(global_homepage.source_urls), 2)
        self.assertEqual(
            discovered_by_url["https://www.citibank.com/privacy"].link_type,
            "external",
        )
        self.assertEqual(
            discovered_by_url["https://www.handelsbanken.se/en/contact"].link_type,
            "internal",
        )

    def test_ignores_image_links(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "page.md"
            path.write_text(
                "![Logo](https://www.example.se/logo.png)\n[About](https://www.example.se/en/about)",
                encoding="utf-8",
            )
            discovered = discover_urls(
                [
                    MarkdownPage(
                        source_url="https://www.example.se/en/",
                        depth=0,
                        markdown_path=str(path),
                        markdown_chars=10,
                    )
                ],
                searched_url="https://www.example.se/en/",
            )

        self.assertEqual(
            [item.url for item in discovered],
            ["https://www.example.se/en/about"],
        )

    def test_accepts_only_llm_domains_from_discovered_candidates(self) -> None:
        discovered_urls = [
            _discovered_url(
                "https://www.handelsbanken.com/about",
                domain="handelsbanken.com",
                label="Global site",
            ),
            _discovered_url(
                "https://www.handelsbanken.nl/",
                domain="handelsbanken.nl",
                label="Netherlands",
            ),
            _discovered_url(
                "https://www.linkedin.com/company/handelsbanken/",
                domain="linkedin.com",
                label="LinkedIn",
            ),
        ]
        candidates = _external_domain_candidates(discovered_urls)
        selection = RelatedDomainSelection(
            related_domains=[
                RelatedDomainDecision(
                    domain="handelsbanken.com",
                    relationship="official_global_site",
                    reason="The link is labelled as the global site.",
                ),
                RelatedDomainDecision(
                    domain="handelsbanken.nl",
                    relationship="official_country_site",
                    reason="The link is labelled Netherlands.",
                ),
                RelatedDomainDecision(
                    domain="invented.example",
                    relationship="other_official_site",
                    reason="Invented by the model.",
                ),
                RelatedDomainDecision(
                    domain="handelsbanken.com",
                    relationship="official_global_site",
                    reason="Duplicate.",
                ),
            ]
        )

        related_domains, warnings = _validate_related_domain_selection(
            selection,
            candidates=candidates,
            discovered_urls=discovered_urls,
        )

        self.assertEqual(
            [related.domain for related in related_domains],
            ["handelsbanken.com", "handelsbanken.nl"],
        )
        self.assertEqual(len(warnings), 2)
        self.assertIn("unknown", warnings[0])
        self.assertIn("duplicate", warnings[1])

        prompt = create_related_domains_prompt(
            "https://www.handelsbanken.se/en/",
            candidates=candidates,
        )
        self.assertIn("Never invent a domain", prompt)
        self.assertIn("linkedin.com", prompt)


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
        self.assertIn("--no-seed", crawl_help.output)
        self.assertIn("--seed-source", crawl_help.output)
        self.assertIn("--seed-share", crawl_help.output)
        self.assertIn("--discovery", crawl_help.output)
        self.assertIn("--accept-language", crawl_help.output)
        self.assertIn("--selector", crawl_help.output)
        self.assertIn("--llm-candidates", crawl_help.output)
        self.assertIn("default: breadth_first", crawl_help.output)
        self.assertIn("--keep-non-english", analysis_help.output)
        self.assertIn("default: keep-non-english", analysis_help.output)
        self.assertEqual(runner.invoke(cli, ["suggest", "--help"]).exit_code, 0)


def _manifest(
    *,
    markdown_path: str,
    extra_pages: list[MarkdownPage] | None = None,
) -> CrawlManifest:
    return CrawlManifest(
        requested_start_url="https://example.com/",
        selected_base_url="https://example.com/",
        stopped_reason="max_pages_reached",
        discovery_strategy="best_first",
        url_seeding=UrlSeeding(
            enabled=True,
            source="sitemap",
            succeeded=True,
            inventory_urls=1,
            eligible_urls=1,
            excluded_urls=0,
            head_checked_urls=1,
            selected=[
                ScoredUrl(
                    url="https://example.com/",
                    score=40.0,
                    reasons=["homepage"],
                    language="en",
                )
            ],
        ),
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
                language="en",
                selection="selected",
                score=40.0,
            ),
            *(extra_pages or []),
        ],
    )


def _discovered_url(url: str, *, domain: str, label: str) -> DiscoveredUrl:
    return DiscoveredUrl(
        url=url,
        domain=domain,
        link_type="external",
        labels=[label],
        source_urls=["https://www.handelsbanken.se/en/"],
        occurrences=1,
    )


if __name__ == "__main__":
    unittest.main()
