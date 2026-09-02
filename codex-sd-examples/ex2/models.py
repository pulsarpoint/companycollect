from typing import Literal

from openai_codex.models import JsonObject
from pydantic import Field
from pydantic.json_schema import SkipJsonSchema

from ex1.models import (
    AggregateAnalysisStats,
    FailedPage,
    PageAnalysisMetadata,
    StrictModel,
    UsefulInformation,
    aggregate_analysis_metadata,
    consolidate_useful_information,
    set_information_source_url,
    structured_output_schema,
)


class PageExtraction(StrictModel):
    """Structured facts extracted from one page returned by Crawl4AI."""

    source_url: str
    page_summary: str | None = None
    useful_information: UsefulInformation = Field(default_factory=UsefulInformation)
    analysis_metadata: SkipJsonSchema[PageAnalysisMetadata] = Field(
        default_factory=PageAnalysisMetadata
    )


class BFSCrawlStats(StrictModel):
    configured_max_pages: int = Field(ge=1)
    configured_max_depth: int = Field(ge=0)
    include_external: bool
    pages_returned: int = Field(ge=0)
    successful_pages: int = Field(ge=0)
    failed_pages: int = Field(ge=0)
    max_depth_reached: int = Field(ge=0)


class ResearchReport(StrictModel):
    start_url: str
    crawl_strategy: Literal["breadth_first"] = "breadth_first"
    stopped_reason: Literal["crawl_completed", "max_pages_reached"]
    crawled_urls: list[str]
    crawl_stats: BFSCrawlStats
    failed_pages: list[FailedPage]
    analysis_stats: AggregateAnalysisStats
    useful_information: UsefulInformation
    pages: list[PageExtraction]


def page_extraction_output_schema() -> JsonObject:
    """Return the extraction-only OpenAI Structured Outputs schema."""
    return structured_output_schema(PageExtraction)


def build_report(
    *,
    start_url: str,
    max_pages: int,
    max_depth: int,
    include_external: bool,
    crawled_urls: list[str],
    crawl_depths: list[int],
    successful_crawls: int,
    failed_pages: list[FailedPage],
    pages: list[PageExtraction],
) -> ResearchReport:
    """Consolidate Crawl4AI crawl results and page-level LLM extractions."""
    return ResearchReport(
        start_url=start_url,
        stopped_reason=(
            "max_pages_reached" if len(crawled_urls) >= max_pages else "crawl_completed"
        ),
        crawled_urls=crawled_urls,
        crawl_stats=BFSCrawlStats(
            configured_max_pages=max_pages,
            configured_max_depth=max_depth,
            include_external=include_external,
            pages_returned=len(crawled_urls),
            successful_pages=successful_crawls,
            failed_pages=len(crawled_urls) - successful_crawls,
            max_depth_reached=max(crawl_depths, default=0),
        ),
        failed_pages=failed_pages,
        analysis_stats=aggregate_analysis_metadata(
            [page.analysis_metadata for page in pages]
        ),
        useful_information=consolidate_useful_information(
            [page.useful_information for page in pages]
        ),
        pages=pages,
    )


def set_source_url(extraction: PageExtraction, source_url: str) -> None:
    """Replace model-provided provenance with the URL actually crawled."""
    extraction.source_url = source_url
    set_information_source_url(extraction.useful_information, source_url)
