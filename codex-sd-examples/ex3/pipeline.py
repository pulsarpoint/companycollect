"""End-to-end research run: crawl, analyze, then bounded LLM-guided passes."""

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from ex3.crawler import (
    AnalysisSettings,
    CrawlSettings,
    run_analysis,
    run_crawl,
    save_model,
    save_report,
)
from ex3.extend import ExtendSettings, run_extend
from ex3.followup import SuggestSettings, run_suggest
from ex3.models import ResearchReport

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ResearchSettings:
    start_url: str
    workdir: Path
    crawl: CrawlSettings
    max_pages: int = 20
    max_depth: int = 2
    pass_pages: int = 10
    max_passes: int = 2
    analysis_timeout_seconds: int = 300
    max_page_chars: int = 30_000
    max_batch_pages: int = 5
    max_batch_chars: int = 60_000


async def run_research(settings: ResearchSettings) -> ResearchReport:
    """Run pass one and up to ``max_passes - 1`` suggestion-driven extra passes."""
    workdir = settings.workdir.resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    manifest_path = workdir / "crawl-manifest.json"

    await run_crawl(settings.crawl)
    report_path = workdir / "report-pass-1.json"
    report = await run_analysis(
        _analysis_settings(settings, manifest_path, previous=None, suggestions=None)
    )
    save_report(report, report_path)

    for pass_number in range(2, settings.max_passes + 1):
        try:
            suggestions = await run_suggest(
                SuggestSettings(
                    manifest_path=manifest_path,
                    report_path=report_path,
                    max_suggestions=settings.pass_pages,
                    timeout_seconds=settings.analysis_timeout_seconds,
                )
            )
            suggestions_path = workdir / f"suggestions-pass-{pass_number}.json"
            save_model(suggestions, suggestions_path)
            if not suggestions.suggestions:
                LOGGER.info("Pass %d: no suggestions; stopping", pass_number)
                break
            await run_extend(
                ExtendSettings(
                    manifest_path=manifest_path,
                    suggestions_path=suggestions_path,
                    max_markdown_chars=settings.crawl.max_markdown_chars,
                    crawl_concurrency=settings.crawl.crawl_concurrency,
                    cdp_port=settings.crawl.cdp_port,
                    headless=settings.crawl.headless,
                    check_robots_txt=settings.crawl.check_robots_txt,
                    proxy=settings.crawl.proxy,
                    accept_language=settings.crawl.accept_language,
                )
            )
            next_report_path = workdir / f"report-pass-{pass_number}.json"
            report = await run_analysis(
                _analysis_settings(
                    settings,
                    manifest_path,
                    previous=report_path,
                    suggestions=suggestions_path,
                )
            )
            save_report(report, next_report_path)
            report_path = next_report_path
        except Exception:
            LOGGER.exception(
                "Pass %d failed; keeping the last completed report", pass_number
            )
            break

    shutil.copyfile(report_path, workdir / "report.json")
    return report


def _analysis_settings(
    settings: ResearchSettings,
    manifest_path: Path,
    *,
    previous: Path | None,
    suggestions: Path | None,
) -> AnalysisSettings:
    return AnalysisSettings(
        manifest_path=manifest_path,
        max_page_chars=settings.max_page_chars,
        max_batch_pages=settings.max_batch_pages,
        max_batch_chars=settings.max_batch_chars,
        analysis_timeout_seconds=settings.analysis_timeout_seconds,
        previous_report_path=previous,
        suggestions_path=suggestions,
    )
