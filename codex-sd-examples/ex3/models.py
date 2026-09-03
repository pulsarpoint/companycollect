from collections.abc import Sequence
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
    TokenUsageBreakdown,
    UsefulInformation,
    aggregate_analysis_metadata,
    consolidate_useful_information,
    set_information_source_url,
    structured_output_schema,
)
from ex3.requirements import Gap

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


class ScoredUrl(StrictModel):
    """Deterministic pre-crawl assessment of one candidate page URL."""

    url: str
    score: float
    reasons: list[str] = Field(default_factory=list)
    exclusion: str | None = None
    language: str | None = None
    title: str | None = None


type PageSelection = Literal["selected", "discovery", "suggested"]
type DiscoveryStrategy = Literal["best_first", "breadth_first"]
type SeedingSource = Literal["sitemap", "sitemap+cc", "cc"]
type Selector = Literal["llm", "deterministic"]
type SelectionMethod = Literal["llm", "deterministic", "none"]


class LlmCallStatus(StrictModel):
    """Outcome bookkeeping for one LLM call."""

    attempted: bool
    succeeded: bool
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)
    token_usage: AnalysisTokenUsage | None = None


class PageSelectionDecision(StrictModel):
    """One LLM-selected candidate page, as returned by the model."""

    url: str
    reason: str
    expected_fields: list[str] = Field(default_factory=list)


class PageSelectionResponse(StrictModel):
    """LLM output for a page-selection Structured Outputs turn."""

    pages: list[PageSelectionDecision] = Field(default_factory=list)


class SuggestedPage(StrictModel):
    """One LLM-suggested page for a follow-up pass."""

    url: str
    reason: str
    expected_fields: list[str] = Field(default_factory=list)


class UrlSeeding(StrictModel):
    """Outcome of the pre-crawl URL inventory and deterministic page selection."""

    enabled: bool
    source: SeedingSource
    succeeded: bool
    error: str | None = None
    inventory_urls: int = Field(default=0, ge=0)
    eligible_urls: int = Field(default=0, ge=0)
    excluded_urls: int = Field(default=0, ge=0)
    head_checked_urls: int = Field(default=0, ge=0)
    base_page_links: int = Field(default=0, ge=0)
    harvested_links: int = Field(default=0, ge=0)
    selection_waves: int = Field(default=0, ge=0)
    selected: list[ScoredUrl] = Field(default_factory=list)
    inventory_path: str | None = None
    selector: Selector = "deterministic"
    selection_method: SelectionMethod = "none"
    candidate_count: int = Field(default=0, ge=0)
    llm: LlmCallStatus | None = None


class MarkdownPage(StrictModel):
    source_url: str
    depth: int = Field(ge=0)
    markdown_path: str
    markdown_chars: int = Field(ge=0)
    language: str | None = None
    selection: PageSelection = "discovery"
    score: float | None = None
    pass_number: int = Field(default=1, ge=1)
    selection_reason: str | None = None


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
    selected_pages: int = Field(default=0, ge=0)
    discovered_pages: int = Field(default=0, ge=0)
    suggested_pages: int = Field(default=0, ge=0)


class BatchStats(StrictModel):
    configured_max_page_chars: int = Field(ge=1)
    configured_max_batch_pages: int = Field(ge=1)
    configured_max_batch_chars: int = Field(ge=1)
    total_batches: int = Field(ge=0)
    successful_batches: int = Field(ge=0)
    failed_batches: int = Field(ge=0)
    submitted_pages: int = Field(ge=0)
    skipped_non_english_pages: int = Field(default=0, ge=0)
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


class PassSuggestions(StrictModel):
    """Persisted record of one LLM-guided follow-up pass's page suggestions."""

    manifest_path: str
    report_path: str
    pass_number: int = Field(ge=2)
    gaps: list[Gap] = Field(default_factory=list)
    candidate_count: int = Field(default=0, ge=0)
    llm: LlmCallStatus
    suggestions: list[SuggestedPage] = Field(default_factory=list)


class MergeAnalysis(LlmCallStatus):
    """Outcome of merging a follow-up pass's extraction into the prior report."""

    dropped_items: int = Field(default=0, ge=0)


class PassSummary(StrictModel):
    """Per-pass bookkeeping recorded on the final research report."""

    pass_number: int = Field(ge=1)
    pages: int = Field(ge=0)
    new_pages: int = Field(ge=0)
    batches: int = Field(ge=0)
    token_totals: TokenUsageBreakdown = Field(default_factory=TokenUsageBreakdown)


class CrawlManifest(StrictModel):
    requested_start_url: str
    selected_base_url: str
    stopped_reason: Literal["crawl_completed", "max_pages_reached"]
    discovery_strategy: DiscoveryStrategy = "breadth_first"
    url_seeding: UrlSeeding | None = None
    language_discovery: LanguageDiscovery
    crawl_stats: CrawlStats
    failed_pages: list[FailedPage]
    markdown_pages: list[MarkdownPage]


class ResearchReport(StrictModel):
    requested_start_url: str
    selected_base_url: str
    crawl_strategy: str = "breadth_first"
    stopped_reason: Literal["crawl_completed", "max_pages_reached"]
    manifest_path: str
    language_discovery: LanguageDiscovery
    url_seeding: UrlSeeding | None = None
    crawl_stats: CrawlStats
    batch_stats: BatchStats
    failed_pages: list[FailedPage]
    markdown_pages: list[MarkdownPage]
    skipped_pages: list[MarkdownPage] = Field(default_factory=list)
    analysis_stats: AggregateAnalysisStats
    batches: list[BatchAnalysis]
    discovered_urls: list[DiscoveredUrl]
    related_domain_analysis: RelatedDomainAnalysis
    related_domains: list[RelatedDomain]
    useful_information: UsefulInformation
    pages: list[PageExtraction]
    passes: list[PassSummary] = Field(default_factory=list)
    run_token_totals: TokenUsageBreakdown = Field(default_factory=TokenUsageBreakdown)
    gaps: list[Gap] = Field(default_factory=list)
    merge_analysis: MergeAnalysis | None = None
    previous_report_path: str | None = None
    suggestions_path: str | None = None


def batch_extraction_output_schema() -> JsonObject:
    """Return the multi-page extraction Structured Outputs schema."""
    return structured_output_schema(BatchExtraction)


def set_source_url(extraction: PageExtraction, source_url: str) -> None:
    """Replace model-provided provenance with a validated input page URL."""
    extraction.source_url = source_url
    set_information_source_url(extraction.useful_information, source_url)


def build_analysis_stats(
    batches: list[BatchAnalysis],
    related_domain_analysis: RelatedDomainAnalysis,
    extra_calls: Sequence[LlmCallStatus] = (),
) -> AggregateAnalysisStats:
    """Aggregate usage once for every page batch, domain-classification and extra call."""
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
    for call in extra_calls:
        if call.attempted:
            metadata.append(
                PageAnalysisMetadata(
                    succeeded=call.succeeded,
                    error=call.error,
                    token_usage=call.token_usage,
                )
            )
    return aggregate_analysis_metadata(metadata)


def sum_token_totals(passes: Sequence[PassSummary]) -> TokenUsageBreakdown:
    """Add every pass's token totals field by field into one run total."""
    total = TokenUsageBreakdown()
    for summary in passes:
        usage = summary.token_totals
        total.input_tokens += usage.input_tokens
        total.cached_input_tokens += usage.cached_input_tokens
        total.cache_write_input_tokens += usage.cache_write_input_tokens
        total.output_tokens += usage.output_tokens
        total.reasoning_output_tokens += usage.reasoning_output_tokens
        total.total_tokens += usage.total_tokens
    return total


def consolidate_extractions(pages: list[PageExtraction]) -> UsefulInformation:
    """Merge extracted facts from every batch while retaining evidence."""
    return consolidate_useful_information([page.useful_information for page in pages])
