import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ex3.crawler import CrawlSettings
from ex3.models import LlmCallStatus, PassSuggestions, SuggestedPage
from ex3.pipeline import ResearchSettings, run_research


class ResearchPipelineTest(unittest.IsolatedAsyncioTestCase):
    async def test_runs_crawl_analyze_and_a_configurable_second_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            workdir = Path(temporary_directory)
            calls: list[str] = []

            async def fake_crawl(settings):
                calls.append("crawl")
                (workdir / "crawl-manifest.json").write_text("{}", encoding="utf-8")

            async def fake_analysis(settings):
                calls.append(f"analyze:{settings.previous_report_path is not None}")
                return _FakeReport()

            async def fake_suggest(settings):
                calls.append("suggest")
                return PassSuggestions(
                    manifest_path=str(settings.manifest_path),
                    report_path=str(settings.report_path),
                    pass_number=2,
                    llm=LlmCallStatus(attempted=True, succeeded=True),
                    suggestions=[
                        SuggestedPage(
                            url="https://example.com/jobs",
                            reason="jobs",
                            expected_fields=["jobs"],
                        )
                    ],
                )

            async def fake_extend(settings):
                calls.append("extend")

            with (
                patch("ex3.pipeline.run_crawl", new=fake_crawl),
                patch("ex3.pipeline.run_analysis", new=fake_analysis),
                patch("ex3.pipeline.run_suggest", new=fake_suggest),
                patch("ex3.pipeline.run_extend", new=fake_extend),
                patch(
                    "ex3.pipeline.save_report",
                    new=lambda report, path: path.write_text("{}", encoding="utf-8"),
                ),
                patch(
                    "ex3.pipeline.save_model",
                    new=lambda model, path: path.write_text("{}", encoding="utf-8"),
                ),
            ):
                await run_research(_settings(workdir, max_passes=2))
                two_pass_calls = list(calls)
                calls.clear()
                await run_research(_settings(workdir, max_passes=1))

            self.assertEqual(
                two_pass_calls,
                ["crawl", "analyze:False", "suggest", "extend", "analyze:True"],
            )
            self.assertEqual(calls, ["crawl", "analyze:False"])
            self.assertTrue((workdir / "report-pass-1.json").exists())
            self.assertTrue((workdir / "suggestions-pass-2.json").exists())
            self.assertTrue((workdir / "report-pass-2.json").exists())
            self.assertTrue((workdir / "report.json").exists())


class _FakeReport:
    pass


def _settings(workdir: Path, *, max_passes: int) -> ResearchSettings:
    crawl = CrawlSettings(
        start_url="https://example.com/",
        markdown_dir=workdir,
        max_pages=5,
        max_depth=1,
        max_markdown_chars=10_000,
        crawl_concurrency=1,
        cdp_port=9_245,
        headless=True,
        include_external=False,
        check_robots_txt=True,
        proxy=None,
    )
    return ResearchSettings(
        start_url="https://example.com/",
        workdir=workdir,
        max_pages=5,
        pass_pages=3,
        max_passes=max_passes,
        crawl=crawl,
    )


if __name__ == "__main__":
    unittest.main()
