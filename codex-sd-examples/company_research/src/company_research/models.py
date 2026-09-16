"""Company research objectives and strict model-response schemas."""

import unicodedata
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

type Objective = Literal[
    "company_profile",
    "company_contacts",
    "locations",
    "products_services",
    "people",
    "company_relationships",
    "jobs",
    "technology_signals",
    "certifications_compliance",
    "document_links",
]

OBJECTIVES: dict[Objective, str] = {
    "company_profile": "Company trading/legal identity, typed identifiers, activities, industries, founding date, dated employee count, official domains and social profiles.",
    "company_contacts": "Published company, department and person business contacts: exact email/phone/form/social values, owner and purpose.",
    "locations": "Headquarters, registered offices and branches: exact postal address, company association, location type and country when stated.",
    "products_services": "Products and services the company actually offers. Capture each service's name and a source-grounded description of the work, deliverables and specialization, with URLs and prices when stated. Radar system development, outsourced hardware design and custom embedded-device development belong here, not in technology signals. Do not infer a commercial service from a skill or job requirement alone.",
    "people": "Named people with stated professional roles and affiliations, profile URLs, and explicitly attributable business contacts.",
    "company_relationships": "Explicit corporate or commercial relationships, including parent/subsidiary, owned brand, partner, customer, supplier and distributor; preserve direction, date, and entity identity.",
    "jobs": "Identifiable current advertised openings with title, location, employer, department, work types and application URL. Careers landing pages can provide navigation to a board.",
    "technology_signals": "Specific named applications, tools, platforms, libraries, languages or hardware products associated with a company, team or role, especially in job descriptions. Exclude data formats (XML, JSON), generic disciplines, methods, architectures and component categories (AI, radar, FPGA, embedded systems). Company capabilities belong in described services when explicitly offered. Distinguish usage, requirements, plans and mentions; preserve attribution, alternatives, dates and evidence.",
    "certifications_compliance": "Explicit certifications and compliance claims held by an identified company, service, product or facility. Capture standard, scope, issuer, document references and stated dates. Prioritize quality, certifications, trust/security and about pages. Separate held credentials from consulting services, customer credentials and planned certification. A website claim is not independent verification.",
    "document_links": "Links to company annual reports, financial statements, ownership disclosures and certificates. Preserve discovery context, named company and stated period. Discover documents without reading or verifying their contents.",
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_min_length=1)


class Potential(StrictModel):
    potential: Literal["high", "medium", "low", "unknown"]
    role: Literal["direct", "navigation", "none"]


class ObjectivePotentials(StrictModel):
    company_profile: Potential
    company_contacts: Potential
    locations: Potential
    products_services: Potential
    people: Potential
    company_relationships: Potential
    jobs: Potential
    technology_signals: Potential
    certifications_compliance: Potential
    document_links: Potential


class CandidateAssessment(StrictModel):
    candidate_id: str
    page_kind: Literal[
        "service_detail",
        "product_detail",
        "job_detail",
        "job_list",
        "news",
        "company_info",
        "navigation",
        "unknown",
    ] = "unknown"
    target_relevance: Literal[
        "target", "target_evidence", "related_company", "unrelated", "unknown"
    ] = "unknown"
    follow_scope: Literal["single_page", "target_navigation"] = "single_page"
    objectives: ObjectivePotentials
    reason: str = Field(min_length=1)


class Selection(StrictModel):
    assessments: list[CandidateAssessment]


class ExternalLinkAssessment(StrictModel):
    link_id: str
    relationship: Literal[
        "partner",
        "customer",
        "supplier",
        "parent_company",
        "subsidiary",
        "group_company",
        "brand",
        "other_business",
        "recruitment",
        "social_profile",
        "documentation",
        "technology_provider",
        "media_reference",
        "reference",
        "unknown",
    ]
    basis: Literal["explicit_text", "contextual_hint", "unknown"]
    related_entity_name: str | None
    description: str
    evidence: list[str] = Field(max_length=5)
    independently_verified: Literal[False] = False


class ExternalLinkAssessments(StrictModel):
    assessments: list[ExternalLinkAssessment]


class ExternalLink(StrictModel):
    """Observed hyperlink occurrence; an assessment never establishes ownership."""

    link_id: str
    url: str
    raw_href: str
    destination_host: str
    destination_domain: str
    source_page_id: str
    source_url: str
    source_domain: str
    fetched_at: str
    html_file: str
    html_sha256: str
    extraction_method: Literal["rendered_html", "cleaned_html", "crawl4ai_links"]
    anchor_index: int | None
    anchor_text: str | None
    title: str | None
    aria_label: str | None
    image_alt: list[str]
    rel: list[str]
    page_region: Literal[
        "header", "footer", "navigation", "main", "aside", "body", "unknown"
    ]
    dom_path: list[str]
    section_heading: str | None
    surrounding_text: str | None
    context_truncated: bool
    assessment_status: Literal["not_assessed", "assessed", "needs_review", "failed"] = (
        "not_assessed"
    )
    assessment: ExternalLinkAssessment | None = None
    assessment_error: str | None = None


class CompanyFact(StrictModel):
    company: str
    field: Literal[
        "trading_name",
        "legal_name",
        "registration_number",
        "vat_number",
        "lei",
        "other_identifier",
        "description",
        "industry",
        "founded",
        "employee_count",
        "official_domain",
        "social_profile",
    ]
    value: str
    as_of: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class Contact(StrictModel):
    owner: str | None
    owner_kind: Literal["company", "department", "person", "location", "unknown"]
    type: Literal["email", "phone", "form", "social"]
    value: str
    purpose: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class Location(StrictModel):
    company: str | None
    label: str | None
    kind: Literal["headquarters", "registered_office", "branch", "other", "unknown"]
    address: str
    country: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class Offering(StrictModel):
    company: str | None
    kind: Literal["product", "service"]
    name: str
    description: str | None = Field(
        description="Describe the supported work, deliverables and specialization in plain language. Include relevant application areas and technical details, without inventing capabilities. Use null only when the source supplies no description beyond the name."
    )
    url: str | None
    price: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class Person(StrictModel):
    name: str
    role: str | None
    company: str | None
    profile_url: str | None
    as_of: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class Relationship(StrictModel):
    subject: str
    relationship: Literal[
        "parent_of",
        "subsidiary_of",
        "owns_brand",
        "partner_of",
        "customer_of",
        "supplier_of",
        "distributor_of",
        "acquired",
        "shareholder_of",
        "other_explicit",
    ]
    object: str
    subject_kind: Literal["company", "person", "brand", "unknown"]
    object_kind: Literal["company", "person", "brand", "unknown"]
    ownership_percentage: float | None = Field(ge=0, le=100)
    ownership_scope: Literal["direct", "indirect", "unspecified"] | None
    as_of: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)

    @model_validator(mode="after")
    def ownership_fields_require_ownership(self):
        if self.relationship not in {
            "parent_of",
            "subsidiary_of",
            "shareholder_of",
            "acquired",
        } and (
            self.ownership_percentage is not None or self.ownership_scope is not None
        ):
            raise ValueError("Commercial relationships cannot carry an ownership stake")
        return self


class Job(StrictModel):
    employer: str | None
    title: str
    location: str | None
    department: str | None
    employment_type: str | None
    workplace_type: str | None
    job_url: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


type TechnologyCategory = Literal[
    "programming_language",
    "framework_library",
    "database",
    "cloud_platform",
    "infrastructure_devops",
    "data_analytics",
    "business_software",
    "engineering_tool",
    "hardware",
    "protocol_standard",
    "operating_system",
    "other",
]
type TechnologySignalType = Literal[
    "stated_use",
    "advertised_expertise",
    "develops",
    "offers",
    "required_experience",
    "preferred_experience",
    "planned_adoption",
    "past_use",
    "being_replaced",
    "explicitly_not_used",
    "mentioned",
]
type TechnologyScope = Literal["company", "team", "role", "client", "unknown"]


# These known false positives supplement the semantic scope rules in the prompt;
# passing this check is not proof that an arbitrary name identifies a specific tool.
NON_SPECIFIC_TECHNOLOGY_NAMES = frozenset(
    {
        "xml",
        "json",
        "cvat xml",
        "csv",
        "yaml",
        "yml",
        "html",
        "xhtml",
        "xml schema",
        "json schema",
        "extensible markup language",
        "javascript object notation",
        "hypertext markup language",
        "ai",
        "artificial intelligence",
        "ml",
        "machine learning",
        "deep learning",
        "radar",
        "lidar",
        "mmwave",
        "mmwave radar",
        "arm soc",
        "fpga",
        "asic",
        "risc-v",
        "risc v",
        "cad",
        "cfd",
        "gd&t",
        "devops",
        "ci/cd",
        "ci cd",
        "continuous integration",
        "continuous delivery",
        "continuous deployment",
        "iot",
        "internet of things",
        "3d engine",
        "embedded systems",
        "embedded software",
        "embedded hardware",
        "hardware design",
        "software development",
        "cloud",
        "cloud computing",
        "iot cloud",
        "hil",
        "sil",
        "hardware-in-the-loop",
        "software-in-the-loop",
        "mechanical engineering",
        "cmos",
        "bicmos",
        "sige",
        "child presence detection",
        "seat occupancy detection",
        "intrusion & proximity alert",
    }
)


def validate_specific_technology_name(value: str) -> str:
    name = " ".join(unicodedata.normalize("NFKC", value).casefold().split())
    if name in NON_SPECIFIC_TECHNOLOGY_NAMES:
        raise ValueError(
            "Technology must identify a specific named tool, application or platform. "
            "Omit formats, generic capabilities and architectures from technology signals "
            "and proposals. Describe capabilities as services only when explicitly offered."
        )
    return value


type SpecificTechnologyName = Annotated[
    str, AfterValidator(validate_specific_technology_name)
]


class TechnologySignal(StrictModel):
    company: str | None
    technology: SpecificTechnologyName = Field(
        description="Exact source name of a specific application, tool, platform, library, language or hardware product; never a format, broad discipline or generic component category."
    )
    category: TechnologyCategory
    signal: TechnologySignalType
    scope: TechnologyScope
    job_employer: str | None
    job_title: str | None
    job_url: str | None
    alternative_group: str | None
    context: str
    as_of: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class ProposedTechnology(StrictModel):
    name: SpecificTechnologyName = Field(max_length=200)
    description: str = Field(
        max_length=2000,
        description="Write a concise definition and main purpose of this technology for administrator review. This is an LLM-generated draft, not evidence of company usage.",
    )
    website: str | None
    category_ids: list[int] = Field(max_length=20)
    category_suggestion: str | None = Field(
        max_length=4000,
        description="A specific category name, required when category_ids is empty. Do not invent numeric category IDs.",
    )
    saas: bool | None
    oss: bool | None
    pricing: list[str] = Field(max_length=10)

    @field_validator("name", "description", "category_suggestion")
    @classmethod
    def meaningful_proposal_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("Proposal text must not be blank")
        return value.strip()

    @model_validator(mode="after")
    def category_required(self):
        if not self.category_ids and self.category_suggestion is None:
            raise ValueError(
                "A proposal requires at least one category ID or a category suggestion"
            )
        return self


class ModelTechnologyMatch(StrictModel):
    status: Literal["matched", "proposed"]
    canonical_technology: SpecificTechnologyName | None
    proposed_technology: ProposedTechnology | None
    reason: str

    @model_validator(mode="after")
    def consistent_identity(self):
        if self.status == "matched":
            if (
                self.canonical_technology is None
                or self.proposed_technology is not None
            ):
                raise ValueError("Matched technologies need only a canonical identity")
        elif self.status == "proposed":
            if (
                self.canonical_technology is not None
                or self.proposed_technology is None
            ):
                raise ValueError("Proposed technologies need only proposed metadata")
        return self


class CatalogTechnologySignal(TechnologySignal):
    catalog_match: ModelTechnologyMatch


class TechnologyResolution(StrictModel):
    technology: SpecificTechnologyName
    catalog_match: ModelTechnologyMatch


class TechnologyResolutions(StrictModel):
    resolutions: list[TechnologyResolution] = Field(min_length=1)


class ClaimReview(StrictModel):
    record_id: str
    supported: bool
    reason: str
    source_subject: str | None = Field(
        description="Entity actually supported as subject: technology company/employer, relationship subject in the requested direction, company-profile company, credential holder or document company. Null only if unidentified or inapplicable."
    )
    source_object: str | None = Field(
        description="For relationships: object supported by the quotations in the requested predicate direction. Null if unsupported or a technology claim."
    )
    specific_technology: bool | None = Field(
        description="For technologies: is this a specific named tool rather than a generic capability? Null for relationships."
    )
    source_signal: TechnologySignalType | None = Field(
        description="For technologies: signal actually supported by the quotations. Null if unsupported or a relationship."
    )
    identity_basis: (
        Literal[
            "explicit_legal_identity",
            "explicit_trading_identity",
            "regional_operation",
            "other_entity",
            "unconfirmed",
        ]
        | None
    ) = Field(
        description="For company legal/trading-name claims, classify the source's basis for equating the two names. Market expansion into a separately named regional operation is regional_operation, not explicit_legal_identity. Null for other claims."
    )
    source_subject_kind: str | None = Field(
        description="Technology actor or credential holder kind: company/team/service/product/facility/person/unknown. Null for other objectives."
    )
    source_value: str | None = Field(
        description="For company_profile: reconstruct the requested field value from the source. For certifications: standard name. For document_links: actual document URL. Required for these objectives when supported; null only if unsupported or inapplicable."
    )
    source_claim_type: str | None = Field(
        description="For certifications: certification/compliance/working_toward/explicit_negative. For documents: document_type or navigation. Null for other objectives."
    )
    source_scope: str | None = Field(
        description="For technologies: company/team/role/client/unknown scope supported by the source. Null for other objectives."
    )


class ProposalReview(StrictModel):
    record_id: str
    reason: str
    identity_supported: bool
    category_supported: bool
    description_supported: bool


class ProposalReviews(StrictModel):
    reviews: list[ProposalReview]


class ProposalMetadataRepair(StrictModel):
    record_id: str
    description: str = Field(min_length=1, max_length=2000)
    category_ids: list[int] = Field(max_length=20)
    category_suggestion: str | None = Field(max_length=4000)


class ProposalMetadataRepairs(StrictModel):
    repairs: list[ProposalMetadataRepair]


class ClaimReviews(StrictModel):
    reviews: list[ClaimReview] = Field(min_length=1)


class EvidenceRepair(StrictModel):
    record_id: str
    evidence: list[str] = Field(max_length=8)


class EvidenceRepairs(StrictModel):
    repairs: list[EvidenceRepair] = Field(min_length=1)


class TechnologySummary(StrictModel):
    company: str
    technology: str
    canonical_technology: str | None
    catalog_status: Literal["matched", "proposed"] | None
    proposal_id: str | None
    category: TechnologyCategory
    signal: TechnologySignalType
    scope: TechnologyScope
    alternative_group: str | None
    as_of: str | None
    distinct_job_url_count: int
    job_urls: list[str]
    source_urls: list[str]
    record_ids: list[str]


class NegativeClaim(StrictModel):
    objective: Objective
    scope: str = Field(min_length=1)
    evidence: list[str] = Field(min_length=1, max_length=8)


class CertificationClaim(StrictModel):
    subject_name: str | None
    subject_kind: Literal[
        "company", "service", "product", "facility", "person", "unknown"
    ]
    standard_name: str
    standard_version: str | None
    claim_type: Literal[
        "certification", "compliance", "working_toward", "explicit_negative"
    ]
    scope: str | None
    issuer_or_assessor: str | None
    certificate_or_report_id: str | None
    issued_on: str | None
    valid_until: str | None
    document_url: str | None
    document_type: Literal["certificate", "AOC", "ROC", "SAQ", "other"] | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class SiteClassification(StrictModel):
    site_types: list[
        Literal[
            "company",
            "news_media",
            "entertainment",
            "forum_community",
            "marketplace",
            "nonprofit",
            "public_sector",
            "personal",
            "mixed",
            "unknown",
        ]
    ] = Field(min_length=1)
    research_profiles: list[
        Literal[
            "general",
            "service_provider",
            "software_product",
            "manufacturer",
            "media_community",
        ]
    ] = Field(min_length=1)
    purpose: str | None
    operator_name: str | None
    business_activities: list[str]
    evidence: list[str] = Field(min_length=1, max_length=8)


class DocumentLink(StrictModel):
    document_url: str
    label: str | None
    document_type: Literal[
        "annual_report",
        "financial_statement",
        "ownership_disclosure",
        "certificate",
        "other_company_report",
        "product_documentation",
    ]
    company: str | None
    reporting_period: str | None
    evidence: list[str] = Field(min_length=1, max_length=8)


class SummaryStatement(StrictModel):
    text: str
    record_ids: list[str] = Field(min_length=1)


class SummaryReview(StrictModel):
    statement_id: str
    supported: bool
    reason: str


class SummaryReviews(StrictModel):
    reviews: list[SummaryReview]


class CompanyOverview(StrictModel):
    site_description: SummaryStatement | None
    company_name: SummaryStatement | None
    company_description: SummaryStatement | None
    products_services: list[SummaryStatement]
    industries: list[SummaryStatement]
    company_relationships: list[SummaryStatement]
    certifications_compliance: list[SummaryStatement]


class Extraction(StrictModel):
    company_profile: list[CompanyFact]
    company_contacts: list[Contact]
    locations: list[Location]
    products_services: list[Offering]
    people: list[Person]
    company_relationships: list[Relationship]
    jobs: list[Job]
    technology_signals: list[TechnologySignal]
    certifications_compliance: list[CertificationClaim]
    document_links: list[DocumentLink]
    explicit_negatives: list[NegativeClaim]


class CatalogExtraction(Extraction):
    technology_signals: list[CatalogTechnologySignal]


RECORD_TYPES = {
    "company_profile": CompanyFact,
    "company_contacts": Contact,
    "locations": Location,
    "products_services": Offering,
    "people": Person,
    "company_relationships": Relationship,
    "jobs": Job,
    "technology_signals": TechnologySignal,
    "certifications_compliance": CertificationClaim,
    "document_links": DocumentLink,
    "explicit_negatives": NegativeClaim,
}


class ResearchConfig(StrictModel):
    """Operator defaults live here; runtime components receive this configuration."""

    model: str = "deepseek/deepseek-v4-flash-0731"
    provider: str | None = "baidu/fp8"
    reasoning_effort: Literal["none", "low", "medium", "high"] = "low"
    max_pages: int = Field(default=20, ge=1, le=500)
    max_external_pages: int = Field(default=3, ge=0)
    job_detail_reserve: int = Field(default=5, ge=0)
    engineering_page_reserve: int = Field(default=3, ge=0)
    max_extraction_attempts: int = Field(default=2, ge=1, le=5)
    max_saved_extraction_retries: int = Field(default=5, ge=0)
    max_review_attempts: int = Field(default=2, ge=1, le=3)
    max_proposal_corrections: int = Field(default=1, ge=0, le=2)
    statement_batch_size: int = Field(default=20, ge=1, le=100)
    catalog_resolution_batch_size: int = Field(default=12, ge=1, le=100)
    max_candidates: int = Field(default=1000, ge=1)
    max_sitemap_urls: int = Field(default=500, ge=0)
    max_sitemap_files: int = Field(default=20, ge=0)
    selection_batch_size: int = Field(default=20, ge=1, le=100)
    selection_batches_per_page: int = Field(default=1, ge=1)
    max_assessment_attempts: int = Field(default=2, ge=1, le=3)
    external_link_batch_size: int = Field(default=20, ge=1, le=100)
    max_external_link_assessment_calls: int = Field(default=3, ge=0, le=100)
    max_model_calls: int = Field(default=100, ge=1)
    model_timeout_seconds: float = Field(default=180.0, gt=0)
    max_http_attempts: int = Field(default=2, ge=1, le=4)
    max_corrections: int = Field(default=1, ge=0, le=2)
    max_output_tokens: int = Field(default=65536, ge=1)
    max_technology_tool_rounds: int = Field(default=6, ge=1, le=20)
    summary_input_chars: int = Field(default=100000, ge=10000)
    chunk_chars: int = Field(default=60000, ge=1000)
    overlap_chars: int = Field(default=4000, ge=0)
    extraction_concurrency: int = Field(default=3, ge=1, le=10)
    page_timeout_seconds: float = Field(default=45.0, gt=0)
    page_attempts: int = Field(default=2, ge=1, le=3)
    max_browser_restarts: int = Field(default=2, ge=0, le=10)
    exploration_pages: int = Field(default=3, ge=0)
    check_robots_txt: bool = True

    @model_validator(mode="after")
    def check_limits(self):
        if self.overlap_chars >= self.chunk_chars:
            raise ValueError("overlap_chars must be smaller than chunk_chars")
        if self.max_sitemap_urls >= self.max_candidates:
            raise ValueError(
                "Reserve candidate capacity for links: max_sitemap_urls < max_candidates"
            )
        return self


class EvidenceFragment(StrictModel):
    text: str
    matched_in: Literal["text", "html", "not_found"]


class Source(StrictModel):
    url: str
    page_id: str
    fetched_at: str
    html_sha256: str
    chunk_start: int
    chunk_end: int
    evidence: list[EvidenceFragment]
    evidence_status: Literal["source_matched", "needs_review"]
    issues: list[str]


class Finding(StrictModel):
    record_id: str
    data: dict
    sources: list[Source]
    evidence_status: Literal["source_matched", "needs_review"]


class Findings(StrictModel):
    company_profile: list[Finding]
    company_contacts: list[Finding]
    locations: list[Finding]
    products_services: list[Finding]
    people: list[Finding]
    company_relationships: list[Finding]
    jobs: list[Finding]
    technology_signals: list[Finding]
    certifications_compliance: list[Finding]
    document_links: list[Finding]
    explicit_negatives: list[Finding]


class EntitySummary(StrictModel):
    entity_id: str
    identity: dict
    field_values: dict[str, list]
    record_ids: list[str]
    source_urls: list[str]


class EntitySummaries(StrictModel):
    people: list[EntitySummary] = Field(default_factory=list)
    jobs: list[EntitySummary] = Field(default_factory=list)
    technologies: list[EntitySummary] = Field(default_factory=list)


class ObjectiveStatus(StrictModel):
    status: Literal[
        "found", "needs_review", "explicit_negative_found", "not_found", "not_assessed"
    ]
    record_count: int
    source_matched_count: int
    needs_review_count: int
    explicit_negative_count: int
    pages_examined: int
    partial_pages: int
    promising_urls_remaining: int
    note: str


class JobDetailContext(StrictModel):
    url: str
    title: str


class ChunkExtractionAttempt(StrictModel):
    chunk_index: int
    attempt: int
    started_at: str
    finished_at: str | None
    status: Literal["running", "complete", "partial", "retry_pending", "failed"]
    call_ids: list[int]
    errors: list[str]


class Page(StrictModel):
    page_id: str
    requested_url: str
    source_url: str
    selected_for: str
    fetched_at: str
    status_code: int | None
    fetch_status: Literal["pending", "fetched", "failed", "duplicate"]
    extraction_status: Literal["not_assessed", "complete", "partial", "failed"]
    objectives_examined: list[Objective]
    attempts: int
    chunks_planned: int
    chunks_completed: int
    html_sha256: str | None
    html_file: str | None
    errors: list[str]
    job_detail: JobDetailContext | None = None
    extraction_attempts: list[ChunkExtractionAttempt] = Field(default_factory=list)
    external_links_file: str | None = None
    external_link_count: int | None = Field(default=None, ge=0)


class ResearchResult(StrictModel):
    schema_version: Literal["1.4", "1.5", "1.6", "1.7", "1.8", "1.9", "1.10"]
    run_id: str
    technology_catalog: dict | None
    input_url: str
    site_url: str
    started_at: str
    finished_at: str | None
    status: Literal["running", "finished", "partial", "failed"]
    stop_reason: str | None
    config: ResearchConfig
    objectives: dict[Objective, ObjectiveStatus]
    records: Findings
    technology_summary: list[TechnologySummary]
    entities: EntitySummaries = Field(default_factory=EntitySummaries)
    site_profile: Finding | None
    company_overview: dict | None
    pages: list[Page]
    external_links: list[ExternalLink] = Field(default_factory=list)
    discovery: dict
    usage: dict
    errors: list[dict]
    output_directory: str
