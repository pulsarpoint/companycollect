from dataclasses import dataclass, field


@dataclass(frozen=True)
class DomainTarget:
    root_domain: str
    source_rank: int
    open_page_rank: float


@dataclass(frozen=True)
class IndexRecord:
    root_domain: str
    warc_filename: str
    offset: int
    length: int
    url: str
    http_status: int
    crawl_id: str


@dataclass(frozen=True)
class FetchedPage:
    root_domain: str
    final_url: str
    http_status: int
    headers: dict[str, str]
    html: str
    capture_date: str  # ISO date, or "" if unknown
    crawl_id: str


@dataclass(frozen=True)
class Email:
    email: str
    is_role: bool
    source_method: str  # "regex" | "llm"


@dataclass(frozen=True)
class Phone:
    phone_raw: str
    phone_e164: str
    source_method: str  # "regex" | "llm"


@dataclass(frozen=True)
class Social:
    platform: str
    url: str
    handle: str


@dataclass(frozen=True)
class Technology:
    technology: str
    category: str
    version: str
    confidence: int


@dataclass(frozen=True)
class IndustryGuess:
    label: str
    nace_hint: str
    confidence: int
    method: str  # "llm" | "none"


@dataclass
class DomainEnrichment:
    target: DomainTarget
    fetch_status: str  # "ok" | "not_in_index" | "fetch_failed" | "non_html"
    page: FetchedPage | None = None
    title: str = ""
    meta_description: str = ""
    content_language: str = ""
    ico: str = ""
    dic: str = ""
    ico_checksum_valid: bool = False
    industry: IndustryGuess | None = None
    emails: list[Email] = field(default_factory=list)
    phones: list[Phone] = field(default_factory=list)
    socials: list[Social] = field(default_factory=list)
    technologies: list[Technology] = field(default_factory=list)
