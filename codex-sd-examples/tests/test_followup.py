import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ex1.models import CompanyInformation, UsefulInformation
from ex3.followup import SuggestSettings, run_suggest
from ex3.llm import StructuredTurnOutcome
from ex3.models import (
    AggregateAnalysisStats,
    BatchStats,
    CrawlManifest,
    CrawlStats,
    DiscoveredUrl,
    LanguageDiscovery,
    MarkdownPage,
    PageSelectionDecision,
    PageSelectionResponse,
    RelatedDomainAnalysis,
    ResearchReport,
)

BASE_URL = "https://www.example.se/en/"


class SuggestPassTest(unittest.IsolatedAsyncioTestCase):
    async def test_suggests_validated_unprocessed_urls_for_the_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            manifest_path, report_path = _write_fixtures(directory)
            prompts: list[str] = []

            async def fake_turn(**kwargs):
                prompts.append(kwargs["prompt"])
                return StructuredTurnOutcome(
                    value=PageSelectionResponse(
                        pages=[
                            PageSelectionDecision(
                                url="https://www.example.se/en/careers",
                                reason="jobs gap",
                                expected_fields=["jobs"],
                            ),
                            PageSelectionDecision(
                                url=BASE_URL,
                                reason="already processed",
                                expected_fields=[],
                            ),
                            PageSelectionDecision(
                                url="https://www.example.se/en/nowhere",
                                reason="invented",
                                expected_fields=[],
                            ),
                        ]
                    ),
                    token_usage=None,
                    error=None,
                )

            with patch("ex3.followup.run_structured_turn", new=fake_turn):
                suggestions = await run_suggest(
                    SuggestSettings(
                        manifest_path=manifest_path,
                        report_path=report_path,
                        max_suggestions=5,
                    )
                )

        self.assertEqual(suggestions.pass_number, 2)
        self.assertEqual(
            [item.url for item in suggestions.suggestions],
            ["https://www.example.se/en/careers"],
        )
        self.assertEqual(suggestions.suggestions[0].expected_fields, ["jobs"])
        self.assertIn("jobs", {gap.field for gap in suggestions.gaps})
        self.assertEqual(suggestions.candidate_count, 1)
        self.assertTrue(suggestions.llm.succeeded)
        self.assertEqual(len(suggestions.llm.warnings), 2)
        self.assertIn("- jobs:", prompts[0])
        self.assertIn(BASE_URL, prompts[0])

    async def test_skips_the_llm_when_nothing_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            manifest_path, report_path = _write_fixtures(directory, complete=True)

            async def fake_turn(**kwargs):
                self.fail("LLM must not be called without gaps")

            with patch("ex3.followup.run_structured_turn", new=fake_turn):
                suggestions = await run_suggest(
                    SuggestSettings(
                        manifest_path=manifest_path, report_path=report_path
                    )
                )

        self.assertFalse(suggestions.llm.attempted)
        self.assertEqual(suggestions.suggestions, [])


def _write_fixtures(directory: Path, *, complete: bool = False) -> tuple[Path, Path]:
    (directory / "home.md").write_text("# Home", encoding="utf-8")
    manifest = CrawlManifest(
        requested_start_url=BASE_URL,
        selected_base_url=BASE_URL,
        stopped_reason="crawl_completed",
        language_discovery=LanguageDiscovery(
            requested_url=BASE_URL,
            probe_url=BASE_URL,
            probe_succeeded=True,
            probe_language="en",
            selected_base_url=BASE_URL,
            selected_language="en",
            selection_method="already_english",
        ),
        crawl_stats=CrawlStats(
            configured_max_pages=1,
            configured_max_depth=1,
            include_external=False,
            pages_returned=1,
            successful_pages=1,
            failed_pages=0,
            stored_markdown_pages=1,
            stored_markdown_chars=6,
            max_depth_reached=0,
        ),
        failed_pages=[],
        markdown_pages=[
            MarkdownPage(
                source_url=BASE_URL,
                depth=0,
                markdown_path="home.md",
                markdown_chars=6,
                language="en",
            )
        ],
    )
    manifest_path = directory / "crawl-manifest.json"
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")

    information = UsefulInformation()
    if complete:
        information = _complete_information()
    report = ResearchReport(
        requested_start_url=BASE_URL,
        selected_base_url=BASE_URL,
        stopped_reason="crawl_completed",
        manifest_path=str(manifest_path),
        language_discovery=manifest.language_discovery,
        crawl_stats=manifest.crawl_stats,
        batch_stats=BatchStats(
            configured_max_page_chars=1000,
            configured_max_batch_pages=1,
            configured_max_batch_chars=1000,
            total_batches=1,
            successful_batches=1,
            failed_batches=0,
            submitted_pages=1,
            successfully_extracted_pages=1,
            failed_extraction_pages=0,
        ),
        failed_pages=[],
        markdown_pages=manifest.markdown_pages,
        analysis_stats=AggregateAnalysisStats(),
        batches=[],
        discovered_urls=[
            DiscoveredUrl(
                url="https://www.example.se/en/careers",
                domain="example.se",
                link_type="internal",
                labels=["Careers"],
                source_urls=[BASE_URL],
                occurrences=2,
            ),
            DiscoveredUrl(
                url=BASE_URL,
                domain="example.se",
                link_type="internal",
                labels=["Home"],
                source_urls=[BASE_URL],
                occurrences=1,
            ),
        ],
        related_domain_analysis=RelatedDomainAnalysis(
            attempted=False, candidate_domains=0, succeeded=True
        ),
        related_domains=[],
        useful_information=information,
        pages=[],
    )
    report_path = directory / "report-pass-1.json"
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return manifest_path, report_path


def _complete_information() -> UsefulInformation:
    from ex1.models import Contact, Evidence, Job, OtherFact, Product

    evidence = Evidence(text="x", source_url=BASE_URL)
    return UsefulInformation(
        contacts=[
            Contact(type="phone", value="1", evidence=evidence),
            Contact(type="email", value="a@b.se", evidence=evidence),
            Contact(type="address", value="Street 1", evidence=evidence),
            Contact(
                type="social", value="https://linkedin.com/company/x", evidence=evidence
            ),
        ],
        company=CompanyInformation(
            name="Example",
            legal_name="Example AB",
            description="A" * 100,
            industries=["Banking"],
            identifiers=["502007-7862"],
        ),
        products=[Product(name="Loans", evidence=evidence)],
        jobs=[Job(title="Engineer", evidence=evidence)],
        other_facts=[
            OtherFact(name="Founded", value="Established in 1871", evidence=evidence),
            OtherFact(name="Employees", value="12,000 employees", evidence=evidence),
            OtherFact(name="CEO", value="Jane Doe", evidence=evidence),
            OtherFact(name="Subsidiaries", value="Stadshypotek AB", evidence=evidence),
        ],
    )


if __name__ == "__main__":
    unittest.main()
