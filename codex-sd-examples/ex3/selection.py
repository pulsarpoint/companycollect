"""Deterministic page selection: score company-site URLs before crawling them.

The scorer never consumes LLM tokens. It rewards URL paths that usually carry
company facts (about, contact, management, careers, press, offering), penalizes
utility pages (legal, accounts, search, locators, pagination), and excludes
pages that cannot help (other locales, binary files, external domains).
"""

import re
from collections.abc import Collection, Iterable
from urllib.parse import parse_qsl, urlsplit

from crawl4ai.deep_crawling import URLFilter, URLScorer

from ex3.language import is_english_language
from ex3.models import ScoredUrl
from ex3.urls import canonical_domain, normalize_start_url, same_domain_tree, url_key

HOMEPAGE_SCORE = 60.0
BASE_LOCALE_BONUS = 8.0
BASE_LINK_BONUS = 10.0
ANCESTOR_WEIGHT = 1 / 3
TITLE_CATEGORY_BONUS = 5.0
TITLE_BONUS_CAP = 10.0
QUERY_STRING_PENALTY = 8.0
NUMERIC_SEGMENT_PENALTY = 10.0
EXCLUDED_SCORE = -1000.0

# Path depth bonus by number of path segments (index 0 = homepage).
DEPTH_ADJUSTMENTS = (0.0, 6.0, 3.0, 0.0)
DEEP_PATH_PENALTY_PER_SEGMENT = 3.0

# Weighted URL slug categories. Each category counts once per URL.
POSITIVE_CATEGORIES: dict[str, tuple[float, frozenset[str]]] = {
    "about": (
        30.0,
        frozenset(
            {
                "about",
                "about-us",
                "aboutus",
                "company",
                "who-we-are",
                "our-story",
                "our-company",
                "history",
                "mission",
                "vision",
                "values",
                "profile",
                "organisation",
                "organization",
                "overview",
                "group",
                "group-structure",
                "governance",
                "corporate-governance",
                "subsidiaries",
                "our-companies",
                "om-oss",
                "om-os",
                "om-foretaget",
                "om-virksomheden",
                "ueber-uns",
                "uber-uns",
                "unternehmen",
                "empresa",
                "nosotros",
                "sobre-nosotros",
                "a-propos",
                "entreprise",
                "chi-siamo",
                "azienda",
                "over-ons",
                "bedrijf",
                "tietoa-meista",
                "yritys",
            }
        ),
    ),
    "contact": (
        30.0,
        frozenset(
            {
                "contact",
                "contact-us",
                "contactus",
                "contact-and-support",
                "kontakt",
                "kontakta",
                "kontakta-oss",
                "kontakt-oss",
                "impressum",
                "imprint",
                "legal-notice",
                "mentions-legales",
                "colofon",
                "contacto",
                "contactez-nous",
                "contatti",
                "yhteystiedot",
            }
        ),
    ),
    "people": (
        25.0,
        frozenset(
            {
                "management",
                "leadership",
                "team",
                "our-team",
                "board",
                "board-of-directors",
                "directors",
                "executives",
                "executive-team",
                "people",
                "ledning",
                "styrelse",
                "hallitus",
                "johto",
                "geschaeftsfuehrung",
                "geschaftsfuhrung",
                "vorstand",
                "direction",
                "equipo",
            }
        ),
    ),
    "careers": (
        20.0,
        frozenset(
            {
                "careers",
                "career",
                "jobs",
                "job",
                "vacancies",
                "vacancy",
                "work-with-us",
                "join-us",
                "karriere",
                "jobb",
                "lediga-jobb",
                "jobba-hos-oss",
                "stellenangebote",
                "empleo",
                "carrieres",
                "lavora-con-noi",
                "vacatures",
                "werken-bij",
                "tyopaikat",
                "avoimet-tyopaikat",
            }
        ),
    ),
    "press": (
        15.0,
        frozenset(
            {
                "press",
                "media",
                "news",
                "newsroom",
                "investors",
                "investor-relations",
                "ir",
                "financial-information",
                "financials",
                "annual-report",
                "reports",
                "sustainability",
                "csr",
                "esg",
                "presse",
                "nyheter",
                "nieuws",
                "uutiset",
                "aktuelles",
                "sijoittajat",
            }
        ),
    ),
    "offering": (
        12.0,
        frozenset(
            {
                "products",
                "product",
                "services",
                "service",
                "solutions",
                "offering",
                "offerings",
                "portfolio",
                "pricing",
                "prices",
                "price-list",
                "plans",
                "industries",
                "sectors",
                "brands",
                "customers",
                "cases",
                "case-studies",
                "references",
                "partners",
                "locations",
                "offices",
                "produkter",
                "tjanster",
                "tjenester",
                "produkte",
                "leistungen",
                "dienstleistungen",
                "productos",
                "servicios",
                "produits",
                "prodotti",
                "servizi",
                "producten",
                "diensten",
                "tuotteet",
                "palvelut",
            }
        ),
    ),
}

NEGATIVE_CATEGORIES: dict[str, tuple[float, frozenset[str]]] = {
    "legal": (
        20.0,
        frozenset(
            {
                "privacy",
                "privacy-notice",
                "privacy-policy",
                "cookie",
                "cookies",
                "cookie-policy",
                "terms",
                "terms-and-conditions",
                "conditions",
                "disclaimer",
                "gdpr",
                "policy",
                "policies",
                "accessibility",
                "legal-documents",
                "tillganglighet",
                "digital-tillganglighet",
                "datenschutz",
                "integritetspolicy",
            }
        ),
    ),
    "account": (
        25.0,
        frozenset(
            {
                "login",
                "log-in",
                "signin",
                "sign-in",
                "logout",
                "register",
                "signup",
                "sign-up",
                "account",
                "my-account",
                "cart",
                "basket",
                "checkout",
                "password",
                "logga-in",
                "anmelden",
            }
        ),
    ),
    "utility": (
        20.0,
        frozenset(
            {
                "search",
                "tag",
                "tags",
                "feed",
                "rss",
                "print",
                "sitemap",
                "wp-content",
                "wp-json",
                "wp-admin",
                "cdn",
                "assets",
                "static",
                "sok",
            }
        ),
    ),
    "branch locator": (
        20.0,
        frozenset(
            {
                "find-branch",
                "find-a-branch",
                "branch-finder",
                "branches",
                "store-locator",
                "store-finder",
                "find-store",
                "dealer-locator",
                "hitta-bankkontor",
                "hitta-kontor",
            }
        ),
    ),
}

# ISO 639-1 language codes that mark a non-English locale when they appear as
# the first path segment, a subdomain, or a locale query value. Deliberately
# excluded because they usually denote regions on company sites: uk, us, eu,
# au, ca (Canada vs Catalan), it is kept because /it/ is nearly always Italian.
NON_ENGLISH_LANGUAGE_CODES = frozenset(
    {
        "af",
        "ar",
        "be",
        "bg",
        "bn",
        "bs",
        "cs",
        "cy",
        "da",
        "de",
        "el",
        "es",
        "et",
        "eu",
        "fa",
        "fi",
        "fr",
        "ga",
        "gl",
        "he",
        "hi",
        "hr",
        "hu",
        "hy",
        "id",
        "is",
        "it",
        "ja",
        "ka",
        "kk",
        "ko",
        "lb",
        "lt",
        "lv",
        "mk",
        "ms",
        "mt",
        "nb",
        "nl",
        "nn",
        "no",
        "pl",
        "pt",
        "ro",
        "ru",
        "sk",
        "sl",
        "sq",
        "sr",
        "sv",
        "sw",
        "ta",
        "th",
        "tr",
        "uk",
        "ur",
        "vi",
        "zh",
    }
) - {"eu", "uk"}
# High-precision title phrases; generic words such as "group" or "company"
# appear in almost every corporate page title and are deliberately absent.
TITLE_PHRASES: dict[str, tuple[str, ...]] = {
    "about": (
        "about us",
        "about the company",
        "who we are",
        "our story",
        "our history",
    ),
    "contact": ("contact", "imprint", "impressum", "legal notice"),
    "people": (
        "management",
        "leadership",
        "board of directors",
        "our team",
        "executive",
    ),
    "careers": ("careers", "jobs", "vacancies", "work with us", "join us"),
    "press": ("press", "newsroom", "investor relations", "investors", "annual report"),
    "offering": ("our products", "our services", "products and services", "pricing"),
}
LOCALE_QUERY_KEYS = frozenset({"lang", "language", "locale", "hl"})
EXCLUDED_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".csv",
        ".zip",
        ".gz",
        ".tar",
        ".rar",
        ".7z",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
        ".webp",
        ".ico",
        ".bmp",
        ".tif",
        ".mp3",
        ".mp4",
        ".mov",
        ".avi",
        ".wav",
        ".webm",
        ".css",
        ".js",
        ".json",
        ".xml",
        ".rss",
        ".atom",
        ".txt",
        ".woff",
        ".woff2",
    }
)
LOCALE_SEGMENT_PATTERN = re.compile(r"^([a-z]{2})(?:[-_]([a-z]{2,4}))?$")
NUMERIC_SEGMENT_PATTERN = re.compile(r"^\d{3,}$")
TOKEN_SPLIT_PATTERN = re.compile(r"[^a-z0-9]+")


def assess_url(
    url: str,
    *,
    base_url: str,
    linked_from_base: Collection[str] = (),
) -> ScoredUrl:
    """Score one URL against the selected base URL without fetching it."""
    try:
        normalized = normalize_start_url(url)
    except ValueError:
        return ScoredUrl(url=url, score=EXCLUDED_SCORE, exclusion="invalid URL")

    base = normalize_start_url(base_url)
    parsed = urlsplit(normalized)
    base_parsed = urlsplit(base)
    hostname = parsed.hostname or ""
    domain = canonical_domain(hostname)
    base_domain = canonical_domain(base_parsed.hostname or "")
    segments = [segment for segment in parsed.path.split("/") if segment]
    base_segments = [segment for segment in base_parsed.path.split("/") if segment]

    exclusion = _exclusion_reason(
        parsed_hostname=hostname,
        domain=domain,
        base_domain=base_domain,
        segments=segments,
        query=parsed.query,
    )
    if exclusion is not None:
        return ScoredUrl(url=normalized, score=EXCLUDED_SCORE, exclusion=exclusion)

    reasons: list[str] = []
    score = 0.0
    key = url_key(normalized)

    if key == url_key(base) or not segments:
        score += HOMEPAGE_SCORE
        reasons.append("homepage")

    under_base = _casefold(segments[: len(base_segments)]) == _casefold(base_segments)
    base_locale = _locale_segment(base_segments[0]) if base_segments else None
    if base_locale is not None and base_segments and under_base:
        score += BASE_LOCALE_BONUS
        reasons.append("base locale")

    if any(url_key(link) == key for link in linked_from_base):
        score += BASE_LINK_BONUS
        reasons.append("linked from base page")

    for category, (weight, keywords) in POSITIVE_CATEGORIES.items():
        factor = _category_factor(segments, keywords)
        if factor > 0:
            score += weight * factor
            reasons.append(category)
    for category, (weight, keywords) in NEGATIVE_CATEGORIES.items():
        factor = _category_factor(segments, keywords)
        if factor > 0:
            score -= weight * factor
            reasons.append(category)

    relative_depth = len(segments) - (len(base_segments) if under_base else 0)
    if relative_depth < len(DEPTH_ADJUSTMENTS):
        score += DEPTH_ADJUSTMENTS[relative_depth]
    else:
        score -= DEEP_PATH_PENALTY_PER_SEGMENT * (
            relative_depth - len(DEPTH_ADJUSTMENTS) + 1
        )
        reasons.append("deep path")

    if any(NUMERIC_SEGMENT_PATTERN.match(segment) for segment in segments):
        score -= NUMERIC_SEGMENT_PENALTY
        reasons.append("numeric segment")
    if parsed.query:
        score -= QUERY_STRING_PENALTY
        reasons.append("query string")

    return ScoredUrl(url=normalized, score=score, reasons=reasons)


def apply_head_metadata(
    assessment: ScoredUrl,
    *,
    language: str | None,
    title: str | None,
    description: str | None,
) -> ScoredUrl:
    """Refine an assessment with fetched ``<head>`` metadata."""
    normalized_language = (language or "").strip() or None
    updated = assessment.model_copy(
        update={"language": normalized_language, "title": title or None}
    )
    if updated.exclusion is not None:
        return updated
    if normalized_language is not None and not is_english_language(normalized_language):
        updated.exclusion = f"document language {normalized_language!r}"
        updated.score = EXCLUDED_SCORE
        return updated

    title_text = re.sub(r"\s+", " ", (title or "").casefold())
    bonus = 0.0
    reasons = list(updated.reasons)
    for category, phrases in TITLE_PHRASES.items():
        if bonus >= TITLE_BONUS_CAP:
            break
        if any(
            re.search(rf"(?<![a-z]){re.escape(phrase)}(?![a-z])", title_text)
            for phrase in phrases
        ):
            bonus += TITLE_CATEGORY_BONUS
            reasons.append(f"title mentions {category}")
    updated.score += bonus
    updated.reasons = reasons
    return updated


def rank_urls(
    urls: Iterable[str],
    *,
    base_url: str,
    linked_from_base: Collection[str] = (),
) -> tuple[list[ScoredUrl], list[ScoredUrl]]:
    """Assess unique URLs and split them into ranked eligible and excluded lists."""
    linked_keys = {url_key(link) for link in linked_from_base}
    assessments: dict[str, ScoredUrl] = {}
    for url in urls:
        assessment = assess_url(
            url,
            base_url=base_url,
            linked_from_base=linked_keys,
        )
        assessments.setdefault(assessment.url, assessment)

    eligible = sorted(
        (item for item in assessments.values() if item.exclusion is None),
        key=lambda item: (-item.score, item.url),
    )
    excluded = sorted(
        (item for item in assessments.values() if item.exclusion is not None),
        key=lambda item: item.url,
    )
    return eligible, excluded


class SelectionScorer(URLScorer):
    """Crawl4AI scorer that ranks discovered links with :func:`assess_url`."""

    def __init__(self, *, base_url: str) -> None:
        super().__init__(weight=1.0)
        self._base_url = base_url

    def _calculate_score(self, url: str) -> float:
        return assess_url(url, base_url=self._base_url).score


class SelectionFilter(URLFilter):
    """Crawl4AI filter that drops excluded and already crawled URLs."""

    def __init__(self, *, base_url: str, exclude_urls: Iterable[str] = ()) -> None:
        super().__init__(name="ex3-selection")
        self._base_url = base_url
        self._excluded_keys = {url_key(url) for url in exclude_urls}

    def apply(self, url: str) -> bool:
        if url_key(url) in self._excluded_keys:
            self._update_stats(False)
            return False
        passed = assess_url(url, base_url=self._base_url).exclusion is None
        self._update_stats(passed)
        return passed


def _exclusion_reason(
    *,
    parsed_hostname: str,
    domain: str,
    base_domain: str,
    segments: list[str],
    query: str,
) -> str | None:
    if not same_domain_tree(domain, base_domain):
        return f"external domain {domain!r}"

    host_labels = parsed_hostname.casefold().split(".")
    if len(host_labels) >= 3:
        subdomain_locale = _locale_segment(host_labels[0])
        if subdomain_locale is not None and subdomain_locale != "en":
            return f"locale subdomain {subdomain_locale!r}"

    if segments:
        path_locale = _locale_segment(segments[0])
        if path_locale is not None and path_locale != "en":
            return f"locale prefix {path_locale!r}"
        last_segment = segments[-1].casefold()
        extension = (
            last_segment[last_segment.rfind(".") :] if "." in last_segment else ""
        )
        if extension in EXCLUDED_EXTENSIONS:
            return f"file extension {extension!r}"

    for key, value in parse_qsl(query, keep_blank_values=True):
        if key.casefold() not in LOCALE_QUERY_KEYS:
            continue
        query_locale = _locale_segment(value.casefold())
        if query_locale is not None and query_locale != "en":
            return f"locale query {query_locale!r}"
    return None


def _locale_segment(segment: str) -> str | None:
    """Return the language code when a segment looks like a locale marker."""
    match = LOCALE_SEGMENT_PATTERN.match(segment.casefold())
    if match is None:
        return None
    language = match.group(1)
    if language == "en":
        return "en"
    if language in NON_ENGLISH_LANGUAGE_CODES:
        return language
    return None


def _category_factor(segments: list[str], keywords: frozenset[str]) -> float:
    """Weight a keyword hit fully on the page slug and lightly on its sections."""
    factor = 0.0
    for index, segment in enumerate(segments):
        slug = segment.casefold()
        if slug in keywords or _tokens([segment]) & keywords:
            is_page_slug = index == len(segments) - 1
            factor = max(factor, 1.0 if is_page_slug else ANCESTOR_WEIGHT)
    return factor


def _casefold(segments: list[str]) -> list[str]:
    return [segment.casefold() for segment in segments]


def _tokens(parts: Iterable[str]) -> set[str]:
    tokens: set[str] = set()
    for part in parts:
        for token in TOKEN_SPLIT_PATTERN.split(part.casefold()):
            if token:
                tokens.add(token)
    return tokens
