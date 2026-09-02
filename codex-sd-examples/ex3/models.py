from typing import Literal

from openai_codex.models import JsonObject
from pydantic import Field
from pydantic.json_schema import SkipJsonSchema

from ex1.models import (
    AggregateAnalysisStats,
    AnalysisTokenUsage,
    FailedPage,
    PageAnalysisMetadata,
    StrictModel,
    UsefulInformation,
    aggregate_analysis_metadata,
    consolidate_useful_information,
    set_information_source_url,
    structured_output_schema,
)

type LanguageSelectionMethod = Literal[
    "already_english",
    "alternate_hreflang",
    "anchor_hreflang",
    "language_link",
    "english_url",
    "fallback_requested_url",
]
type LanguageCandidateMethod = Literal[
    "alternate_hreflang",
    "anchor_hreflang",
    "language_link",
    "english_url",
]


class LanguageCandidate(StrictModel):
    url: str
    detection_method: LanguageCandidateMethod
    score: int = Field(ge=0)
    verification_status: Literal[
        "not_checked", "confirmed", "reachable", "rejected", "failed"
    ] = "not_checked"
    detected_language: str | None = None
    verification_error: str | None = None


class LanguageDiscovery(StrictModel):
    requested_url: str
    probe_url: str
    probe_succeeded: bool
    probe_language: str | None = None
    selected_base_url: str
    selected_language: str | None = None
    selection_method: LanguageSelectionMethod
    candidates: list[LanguageCandidate] = Field(default_factory=list)
    error: str | None = None


class MarkdownPage(StrictModel):
    source_url: str
    depth: int = Field(ge=0)
    markdown_path: str
    markdown_chars: int = Field(ge=0)


class PageExtractionMetadata(StrictModel):
    depth: int = Field(ge=0)
    markdown_path: str
    batch_number: int = Field(ge=1)
    succeeded: bool
    error: str | None = None


class PageExtraction(StrictModel):
    source_url: str
    page_summary: str | None = None
    useful_information: UsefulInformation = Field(default_factory=UsefulInformation)
    extraction_metadata: SkipJsonSchema[PageExtractionMetadata | None] = None


class BatchExtraction(StrictModel):
    pages: list[PageExtraction] = Field(default_factory=list)


class BatchAnalysis(StrictModel):
    batch_number: int = Field(ge=1)
    page_urls: list[str]
    markdown_chars: int = Field(ge=0)
    succeeded: bool
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)
    token_usage: AnalysisTokenUsage | None = None


class DiscoveredUrl(StrictModel):
    url: str
    domain: str
    link_type: Literal["internal", "external"]
    labels: list[str]
    source_urls: list[str]
    occurrences: int = Field(ge=1)


class DiscoveredDomainCandidate(StrictModel):
    domain: str
    homepage_url: str
    discovered_urls: list[str]
    labels: list[str]
    occurrences: int = Field(ge=1)
    source_page_count: int = Field(ge=1)


class RelatedDomainDecision(StrictModel):
    domain: str
    relationship: Literal[
        "official_global_site",
        "official_country_site",
        "parent_company",
        "subsidiary_or_brand",
        "other_official_site",
    ]
    reason: str


class RelatedDomainSelection(StrictModel):
    related_domains: list[RelatedDomainDecision] = Field(default_factory=list)


class RelatedDomainAnalysis(StrictModel):
    attempted: bool
    candidate_domains: int = Field(ge=0)
    succeeded: bool
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)
    token_usage: AnalysisTokenUsage | None = None


class CrawlStats(StrictModel):
    configured_max_pages: int = Field(ge=1)
    configured_max_depth: int = Field(ge=0)
    include_external: bool
    pages_returned: int = Field(ge=0)
    successful_pages: int = Field(ge=0)
    failed_pages: int = Field(ge=0)
    stored_markdown_pages: int = Field(ge=0)
    stored_markdown_chars: int = Field(ge=0)
    max_depth_reached: int = Field(ge=0)


class BatchStats(StrictModel):
    configured_max_page_chars: int = Field(ge=1)
    configured_max_batch_pages: int = Field(ge=1)
    configured_max_batch_chars: int = Field(ge=1)
    total_batches: int = Field(ge=0)
    successful_batches: int = Field(ge=0)
    failed_batches: int = Field(ge=0)
    submitted_pages: int = Field(ge=0)
    successfully_extracted_pages: int = Field(ge=0)
    failed_extraction_pages: int = Field(ge=0)


class RelatedDomain(StrictModel):
    domain: str
    url: str
    relationship: Literal[
        "official_global_site",
        "official_country_site",
        "parent_company",
        "subsidiary_or_brand",
        "other_official_site",
    ]
    reason: str
    labels: list[str]
    source_urls: list[str]
    occurrences: int = Field(ge=1)


class CrawlManifest(StrictModel):
    requested_start_url: str
    selected_base_url: str
    stopped_reason: Literal["crawl_completed", "max_pages_reached"]
    language_discovery: LanguageDiscovery
    crawl_stats: CrawlStats
    failed_pages: list[FailedPage]
    markdown_pages: list[MarkdownPage]


class ResearchReport(StrictModel):
    requested_start_url: str
    selected_base_url: str
    crawl_strategy: Literal["breadth_first"] = "breadth_first"
    stopped_reason: Literal["crawl_completed", "max_pages_reached"]
    manifest_path: str
    language_discovery: LanguageDiscovery
    crawl_stats: CrawlStats
    batch_stats: BatchStats
    failed_pages: list[FailedPage]
    markdown_pages: list[MarkdownPage]
    analysis_stats: AggregateAnalysisStats
    batches: list[BatchAnalysis]
    discovered_urls: list[DiscoveredUrl]
    related_domain_analysis: RelatedDomainAnalysis
    related_domains: list[RelatedDomain]
    useful_information: UsefulInformation
    pages: list[PageExtraction]


def batch_extraction_output_schema() -> JsonObject:
    """Return the multi-page extraction Structured Outputs schema."""
    return structured_output_schema(BatchExtraction)


def related_domain_output_schema() -> JsonObject:
    """Return the related-domain Structured Outputs schema."""
    return structured_output_schema(RelatedDomainSelection)


def set_source_url(extraction: PageExtraction, source_url: str) -> None:
    """Replace model-provided provenance with a validated input page URL."""
    extraction.source_url = source_url
    set_information_source_url(extraction.useful_information, source_url)


def build_analysis_stats(
    batches: list[BatchAnalysis],
    related_domain_analysis: RelatedDomainAnalysis,
) -> AggregateAnalysisStats:
    """Aggregate usage once for every page batch and domain-classification call."""
    metadata = [
        PageAnalysisMetadata(
            succeeded=batch.succeeded,
            error=batch.error,
            token_usage=batch.token_usage,
        )
        for batch in batches
    ]
    if related_domain_analysis.attempted:
        metadata.append(
            PageAnalysisMetadata(
                succeeded=related_domain_analysis.succeeded,
                error=related_domain_analysis.error,
                token_usage=related_domain_analysis.token_usage,
            )
        )
    return aggregate_analysis_metadata(metadata)


def consolidate_extractions(pages: list[PageExtraction]) -> UsefulInformation:
    """Merge extracted facts from every batch while retaining evidence."""
    return consolidate_useful_information([page.useful_information for page in pages])
