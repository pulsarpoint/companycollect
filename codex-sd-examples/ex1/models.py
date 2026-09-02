from typing import Literal

from openai_codex.models import JsonObject
from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema


class StrictModel(BaseModel):
    """Base model that rejects fields outside the declared JSON contract."""

    model_config = ConfigDict(extra="forbid")


class Evidence(StrictModel):
    text: str = Field(description="Short text supporting the extracted fact.")
    source_url: str = Field(description="URL of the page containing the evidence.")


class Contact(StrictModel):
    type: Literal["email", "phone", "address", "social", "other"]
    value: str
    label: str | None = None
    evidence: Evidence


class CompanyInformation(StrictModel):
    name: str | None = None
    legal_name: str | None = None
    description: str | None = None
    industries: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    identifiers: list[str] = Field(default_factory=list)
    social_profiles: list[str] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)


class Product(StrictModel):
    name: str
    category: str | None = None
    description: str | None = None
    price: str | None = None
    currency: str | None = None
    url: str | None = None
    evidence: Evidence


class Job(StrictModel):
    title: str
    location: str | None = None
    employment_type: str | None = None
    description: str | None = None
    application_url: str | None = None
    evidence: Evidence


class OtherFact(StrictModel):
    name: str
    value: str
    evidence: Evidence


class UsefulInformation(StrictModel):
    contacts: list[Contact] = Field(default_factory=list)
    company: CompanyInformation = Field(default_factory=CompanyInformation)
    products: list[Product] = Field(default_factory=list)
    jobs: list[Job] = Field(default_factory=list)
    other_facts: list[OtherFact] = Field(default_factory=list)


class LinkRecommendation(StrictModel):
    url: str
    link_type: Literal["internal", "external"]
    category: Literal["contact", "company", "product", "job", "other"]
    priority: int = Field(ge=1, le=5)
    reason: str
    expected_information: list[str] = Field(default_factory=list)


class TokenUsageBreakdown(StrictModel):
    input_tokens: int = Field(default=0, ge=0)
    cached_input_tokens: int = Field(default=0, ge=0)
    cache_write_input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    reasoning_output_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)


class AnalysisTokenUsage(StrictModel):
    last: TokenUsageBreakdown
    thread_total: TokenUsageBreakdown
    model_context_window: int | None = Field(default=None, ge=0)
    duration_ms: int | None = Field(default=None, ge=0)


class PageAnalysisMetadata(StrictModel):
    depth: int = Field(default=0, ge=0)
    succeeded: bool = True
    error: str | None = None
    token_usage: AnalysisTokenUsage | None = None


class PageAnalysis(StrictModel):
    source_url: str
    page_summary: str | None = None
    useful_information: UsefulInformation = Field(default_factory=UsefulInformation)
    next_links: list[LinkRecommendation] = Field(default_factory=list, max_length=5)
    crawl_complete: bool = False
    analysis_metadata: SkipJsonSchema[PageAnalysisMetadata] = Field(
        default_factory=PageAnalysisMetadata
    )


class CandidateLink(StrictModel):
    url: str
    text: str = ""
    title: str = ""
    link_type: Literal["internal", "external"]
    score: int


class PendingUrl(StrictModel):
    url: str
    depth: int = Field(ge=0)
    priority: int
    sequence: int = Field(ge=0)


class FailedPage(StrictModel):
    url: str
    depth: int = Field(ge=0)
    error: str


class CrawlState(StrictModel):
    start_url: str
    visited_urls: list[str] = Field(default_factory=list)
    known_urls: list[str] = Field(default_factory=list)
    pending_urls: list[PendingUrl] = Field(default_factory=list)
    pages: list[PageAnalysis] = Field(default_factory=list)
    failed_pages: list[FailedPage] = Field(default_factory=list)
    next_sequence: int = Field(default=0, ge=0)


class AggregateAnalysisStats(StrictModel):
    attempted_analyses: int = Field(default=0, ge=0)
    successful_analyses: int = Field(default=0, ge=0)
    failed_analyses: int = Field(default=0, ge=0)
    analyses_with_usage: int = Field(default=0, ge=0)
    analyses_without_usage: int = Field(default=0, ge=0)
    token_totals: TokenUsageBreakdown = Field(default_factory=TokenUsageBreakdown)


class ResearchReport(StrictModel):
    start_url: str
    crawl_complete: bool
    stopped_reason: str
    visited_urls: list[str]
    remaining_urls: list[str]
    failed_pages: list[FailedPage]
    analysis_stats: AggregateAnalysisStats
    useful_information: UsefulInformation
    pages: list[PageAnalysis]


def page_analysis_output_schema() -> JsonObject:
    """Return the Pydantic schema in OpenAI Structured Outputs form."""
    return structured_output_schema(PageAnalysis)


def structured_output_schema(model: type[BaseModel]) -> JsonObject:
    """Convert a Pydantic model schema to OpenAI Structured Outputs form."""
    schema = model.model_json_schema()
    _require_all_object_properties(schema)
    return schema


def consolidate_information(pages: list[PageAnalysis]) -> UsefulInformation:
    """Merge page-level facts while retaining their source evidence."""
    return consolidate_useful_information([page.useful_information for page in pages])


def consolidate_useful_information(
    information_sets: list[UsefulInformation],
) -> UsefulInformation:
    """Merge extracted facts while retaining their source evidence."""
    consolidated = UsefulInformation()

    for page_information in information_sets:
        _merge_company(consolidated.company, page_information.company)
        _append_unique_contacts(consolidated.contacts, page_information.contacts)
        _append_unique_products(consolidated.products, page_information.products)
        _append_unique_jobs(consolidated.jobs, page_information.jobs)
        _append_unique_facts(consolidated.other_facts, page_information.other_facts)

    return consolidated


def aggregate_analysis_stats(pages: list[PageAnalysis]) -> AggregateAnalysisStats:
    """Aggregate per-page Codex usage without double-counting thread totals."""
    return aggregate_analysis_metadata([page.analysis_metadata for page in pages])


def aggregate_analysis_metadata(
    metadata_items: list[PageAnalysisMetadata],
) -> AggregateAnalysisStats:
    """Aggregate Codex usage without double-counting thread totals."""
    stats = AggregateAnalysisStats(attempted_analyses=len(metadata_items))

    for metadata in metadata_items:
        if metadata.succeeded:
            stats.successful_analyses += 1
        else:
            stats.failed_analyses += 1

        usage = metadata.token_usage
        if usage is None:
            stats.analyses_without_usage += 1
            continue

        stats.analyses_with_usage += 1
        _add_token_usage(stats.token_totals, usage.last)

    return stats


def set_source_url(analysis: PageAnalysis, source_url: str) -> None:
    """Replace model-provided provenance with the URL actually crawled."""
    analysis.source_url = source_url
    set_information_source_url(analysis.useful_information, source_url)


def set_information_source_url(
    information: UsefulInformation,
    source_url: str,
) -> None:
    """Replace model-provided evidence URLs with the URL actually crawled."""

    for contact in information.contacts:
        contact.evidence.source_url = source_url
    for evidence in information.company.evidence:
        evidence.source_url = source_url
    for product in information.products:
        product.evidence.source_url = source_url
    for job in information.jobs:
        job.evidence.source_url = source_url
    for fact in information.other_facts:
        fact.evidence.source_url = source_url


def _merge_company(target: CompanyInformation, source: CompanyInformation) -> None:
    if target.name is None and source.name is not None:
        target.name = source.name
    if target.legal_name is None and source.legal_name is not None:
        target.legal_name = source.legal_name
    if source.description is not None and (
        target.description is None or len(source.description) > len(target.description)
    ):
        target.description = source.description

    target.industries = _unique_strings([*target.industries, *source.industries])
    target.locations = _unique_strings([*target.locations, *source.locations])
    target.identifiers = _unique_strings([*target.identifiers, *source.identifiers])
    target.social_profiles = _unique_strings(
        [*target.social_profiles, *source.social_profiles]
    )

    known_evidence = {
        (item.source_url.casefold(), item.text.casefold()) for item in target.evidence
    }
    for evidence in source.evidence:
        key = (evidence.source_url.casefold(), evidence.text.casefold())
        if key not in known_evidence:
            target.evidence.append(evidence)
            known_evidence.add(key)


def _append_unique_contacts(target: list[Contact], source: list[Contact]) -> None:
    known = {(item.type, item.value.casefold()) for item in target}
    for item in source:
        key = (item.type, item.value.casefold())
        if key not in known:
            target.append(item)
            known.add(key)


def _append_unique_products(target: list[Product], source: list[Product]) -> None:
    known = {(item.name.casefold(), (item.url or "").casefold()) for item in target}
    for item in source:
        key = (item.name.casefold(), (item.url or "").casefold())
        if key not in known:
            target.append(item)
            known.add(key)


def _append_unique_jobs(target: list[Job], source: list[Job]) -> None:
    known = {
        (
            item.title.casefold(),
            (item.location or "").casefold(),
            (item.application_url or "").casefold(),
        )
        for item in target
    }
    for item in source:
        key = (
            item.title.casefold(),
            (item.location or "").casefold(),
            (item.application_url or "").casefold(),
        )
        if key not in known:
            target.append(item)
            known.add(key)


def _append_unique_facts(target: list[OtherFact], source: list[OtherFact]) -> None:
    known = {(item.name.casefold(), item.value.casefold()) for item in target}
    for item in source:
        key = (item.name.casefold(), item.value.casefold())
        if key not in known:
            target.append(item)
            known.add(key)


def _unique_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    known: set[str] = set()
    for value in values:
        key = value.casefold()
        if key not in known:
            result.append(value)
            known.add(key)
    return result


def _add_token_usage(
    target: TokenUsageBreakdown,
    source: TokenUsageBreakdown,
) -> None:
    target.input_tokens += source.input_tokens
    target.cached_input_tokens += source.cached_input_tokens
    target.cache_write_input_tokens += source.cache_write_input_tokens
    target.output_tokens += source.output_tokens
    target.reasoning_output_tokens += source.reasoning_output_tokens
    target.total_tokens += source.total_tokens


def _require_all_object_properties(schema: object) -> None:
    if isinstance(schema, list):
        for item in schema:
            _require_all_object_properties(item)
        return
    if not isinstance(schema, dict):
        return

    schema.pop("default", None)
    properties = schema.get("properties")
    if isinstance(properties, dict):
        schema["required"] = list(properties)

    for value in schema.values():
        _require_all_object_properties(value)
