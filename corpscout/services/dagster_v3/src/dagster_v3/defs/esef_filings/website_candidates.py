"""Deterministic website and registrable-domain extraction from ESEF XHTML."""

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import tldextract
from lxml import etree

from dagster_v3.defs.esef_filings.candidate_context import (
    is_report_production_credit,
)

_URL_PATTERN = re.compile(r"(?:https?://|//|www\.)[^\s<>\"']+", re.IGNORECASE)
_BARE_DOMAIN_PATTERN = re.compile(
    r"(?<![@\w])"
    r"(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+"
    r"[A-Z]{2,63}"
    r"(?::\d{2,5})?"
    r"(?:/[^\s<>\"']*)?",
    re.IGNORECASE,
)
_EMAIL_LIKE_PATTERN = re.compile(r"[^\s@]+@[^\s@]+")
_DOMAIN_PREFIX_PATTERN = re.compile(
    r"(?:https?://|//|www\.)[A-Z0-9.-]*-$",
    re.IGNORECASE,
)
_SPLIT_DOMAIN_PATTERN = re.compile(
    r"((?:https?://|//|www\.)[A-Z0-9.-]*-)\s+"
    r"((?:[A-Z0-9-]+\.)+[A-Z]{2,63}(?::\d{2,5})?(?:/[^\s<>\"']*)?)",
    re.IGNORECASE,
)
_HOST_LABEL_PATTERN = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)$")
_EXCLUDED_ELEMENTS = frozenset(
    {
        "canvas",
        "embed",
        "math",
        "noscript",
        "object",
        "script",
        "style",
        "svg",
        "template",
    }
)
_CONTEXT_ELEMENTS = frozenset(
    {"address", "article", "div", "footer", "li", "p", "section", "td"}
)
_WEBSITE_FACT_ROLES = {
    "WebsitesOfLegalEntity": "company_website",
    "WebsiteAtWhichTheFinancialStatementsOfTheEntityAreDisclosedTogetherWithTheAuditorsReport": "report_disclosure",
    "WebsiteOfTheAuditEntity": "auditor",
}
_INFRASTRUCTURE_DOMAINS = frozenset(
    {"europa.eu", "ifrs.org", "iso.org", "w3.org", "xbrl.org"}
)
_KNOWN_FALSE_DOMAINS = frozenset({"co.ltd"})
_FILE_LIKE_PUBLIC_SUFFIXES = frozenset({"zip"})
_SOCIAL_MEDIA_DOMAINS = frozenset(
    {
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "threads.net",
        "tiktok.com",
        "x.com",
        "youtube.com",
    }
)
_STATIC_ASSET_SUFFIXES = (
    ".avif",
    ".css",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".png",
    ".svg",
    ".webp",
    ".woff",
    ".woff2",
    ".xhtml",
    ".xml",
    ".xsd",
)
_MAX_WEBSITE_BLOCK_LENGTH = 1_000
_BARE_DOMAIN_SIGNAL_PATTERN = re.compile(
    r"\b(?:company\s+website|hemsida|homepage|honlap\w*|internet(?:site|sida)?|"
    r"nettside|site\s+(?:internet|web)|sitio\s+web|splet\w*\s+stran\w*|"
    r"stron\w*\s+internetow\w*|verkkosiv\w*|webbplats\w*|weboldal\w*|"
    r"webov\w*\s+str[aá]nk\w*|websites?)\b",
    re.IGNORECASE,
)
_ROLE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "investor_relations",
        re.compile(
            r"\b(?:actionnaires?|investisseurs?|investor(?:s|\s+relations)?|"
            r"investerarrelationer|relations?\s+investisseurs?|shareholders?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "auditor",
        re.compile(
            r"\b(?:audit(?:ors?|ing)?|revisor\w*|commissaire\s+aux\s+comptes|"
            r"tilintarkastaja)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "corporate_responsibility",
        re.compile(
            r"\b(?:corporate\s+responsibility|csr|sustainability)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "report_disclosure",
        re.compile(
            r"\b(?:annual\s+report|financial\s+statements?|reports?|rapporter?|"
            r"rapport\w*|tulosraportit|tilinpäätös\w*|årsredovisning\w*)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "company_website",
        re.compile(
            r"\b(?:company\s+website|hemsida|homepage|site\s+internet|"
            r"verkkosiv\w*|webbplats\w*|websites?)\b",
            re.IGNORECASE,
        ),
    ),
)
_ROLE_ORDER = (
    "company_website",
    "investor_relations",
    "auditor",
    "corporate_responsibility",
    "report_disclosure",
    "social_media",
)


@dataclass(frozen=True)
class TaggedWebsiteValue:
    report_member: str
    concept_local_name: str
    value: str


@dataclass(frozen=True)
class EsefWebsiteEvidence:
    report_member: str
    extraction_method: str
    raw_value: str
    normalized_url: str
    host: str
    source_concept: str
    xpath: str
    source_line: int | None
    page_id: str
    surrounding_text: str
    suggested_role: str


@dataclass(frozen=True)
class EsefWebsiteCandidate:
    registrable_domain: str
    hosts: list[str]
    normalized_urls: list[str]
    suggested_roles: list[str]
    evidence: list[EsefWebsiteEvidence]


@dataclass(frozen=True)
class _NormalizedWebsite:
    url: str
    host: str
    registrable_domain: str


@dataclass(frozen=True)
class _VisibleBlock:
    value: str
    xpath: str
    source_line: int | None
    page_id: str


@dataclass
class _WebsiteAccumulator:
    registrable_domain: str
    hosts: set[str]
    normalized_urls: set[str]
    evidence: list[EsefWebsiteEvidence]


def extract_website_candidates(
    report_paths: Mapping[str, Path],
    *,
    tagged_values: Iterable[TaggedWebsiteValue],
    known_email_domains: Iterable[str],
) -> list[EsefWebsiteCandidate]:
    """Extract auditable URL evidence grouped by registrable domain."""
    corroborating_domains = {
        domain
        for value in known_email_domains
        if (domain := registrable_domain_for_host(value)) is not None
    }
    accumulators: dict[str, _WebsiteAccumulator] = {}
    for tagged_value in sorted(
        tagged_values,
        key=lambda item: (
            item.report_member,
            item.concept_local_name,
            item.value,
        ),
    ):
        _extract_tagged_value(
            tagged_value,
            corroborating_domains=corroborating_domains,
            accumulators=accumulators,
        )

    for report_member, report_path in sorted(report_paths.items()):
        _extract_report_websites(
            report_member=report_member,
            report_path=report_path,
            corroborating_domains=corroborating_domains,
            accumulators=accumulators,
        )

    return [
        _finalize_candidate(accumulator)
        for _, accumulator in sorted(accumulators.items())
    ]


def _extract_tagged_value(
    tagged_value: TaggedWebsiteValue,
    *,
    corroborating_domains: set[str],
    accumulators: dict[str, _WebsiteAccumulator],
) -> None:
    if tagged_value.concept_local_name not in _WEBSITE_FACT_ROLES:
        return
    for raw_value in _website_values(
        tagged_value.value,
        allow_bare_domains=True,
        corroborating_domains=corroborating_domains,
    ):
        _add_website_candidate(
            raw_value=raw_value,
            evidence_arguments={
                "report_member": tagged_value.report_member,
                "extraction_method": "tagged_fact",
                "source_concept": tagged_value.concept_local_name,
                "xpath": "",
                "source_line": None,
                "page_id": "",
                "surrounding_text": _bounded_context(tagged_value.value),
                "suggested_role": _WEBSITE_FACT_ROLES[tagged_value.concept_local_name],
            },
            accumulators=accumulators,
        )


def _extract_report_websites(
    *,
    report_member: str,
    report_path: Path,
    corroborating_domains: set[str],
    accumulators: dict[str, _WebsiteAccumulator],
) -> None:
    tree = etree.parse(
        report_path,
        etree.XMLParser(
            load_dtd=False,
            no_network=True,
            recover=False,
            remove_comments=True,
            resolve_entities=False,
            huge_tree=True,
        ),
    )
    blocks = _visible_blocks(tree)
    for index, block in enumerate(blocks):
        context = _block_context(blocks, index)
        website_values, extraction_method = _block_website_values(
            blocks,
            index,
            context=context,
            corroborating_domains=corroborating_domains,
        )
        for raw_value in website_values:
            _add_website_candidate(
                raw_value=raw_value,
                evidence_arguments={
                    "report_member": report_member,
                    "extraction_method": extraction_method,
                    "source_concept": "",
                    "xpath": block.xpath,
                    "source_line": block.source_line,
                    "page_id": block.page_id,
                    "surrounding_text": context,
                    "candidate_context": block.value,
                    "role_context": block.value,
                },
                accumulators=accumulators,
            )

    for element in tree.xpath("//*[local-name()='a' and @href]"):
        if not isinstance(element, etree._Element) or not _is_visible(element):
            continue
        href = str(element.get("href", "")).strip()
        context = _element_context(element)
        _add_website_candidate(
            raw_value=href,
            evidence_arguments={
                "report_member": report_member,
                "extraction_method": "visible_link",
                "source_concept": "",
                "xpath": tree.getpath(element),
                "source_line": element.sourceline,
                "page_id": _page_id(element),
                "surrounding_text": context,
            },
            accumulators=accumulators,
        )


def _add_website_candidate(
    *,
    raw_value: str,
    evidence_arguments: Mapping[str, object],
    accumulators: dict[str, _WebsiteAccumulator],
) -> None:
    normalized = _normalize_website(raw_value)
    if normalized is None:
        return
    context = str(evidence_arguments["surrounding_text"])
    candidate_context = str(evidence_arguments.get("candidate_context", context))
    extraction_method = str(evidence_arguments["extraction_method"])
    if extraction_method != "tagged_fact" and is_report_production_credit(
        candidate_context,
        raw_value,
        normalized.host,
        normalized.registrable_domain,
    ):
        return
    role_context = str(evidence_arguments.get("role_context", context))
    suggested_role = str(
        evidence_arguments.get(
            "suggested_role",
            _suggested_role(
                role_context,
                registrable_domain=normalized.registrable_domain,
            ),
        )
    )
    evidence = EsefWebsiteEvidence(
        report_member=str(evidence_arguments["report_member"]),
        extraction_method=extraction_method,
        raw_value=raw_value,
        normalized_url=normalized.url,
        host=normalized.host,
        source_concept=str(evidence_arguments["source_concept"]),
        xpath=str(evidence_arguments["xpath"]),
        source_line=(
            int(evidence_arguments["source_line"])
            if evidence_arguments["source_line"] is not None
            else None
        ),
        page_id=str(evidence_arguments["page_id"]),
        surrounding_text=context,
        suggested_role=suggested_role,
    )
    accumulator = accumulators.setdefault(
        normalized.registrable_domain,
        _WebsiteAccumulator(
            registrable_domain=normalized.registrable_domain,
            hosts=set(),
            normalized_urls=set(),
            evidence=[],
        ),
    )
    accumulator.hosts.add(normalized.host)
    accumulator.normalized_urls.add(normalized.url)
    if evidence not in accumulator.evidence:
        accumulator.evidence.append(evidence)


def _normalize_website(raw_value: str) -> _NormalizedWebsite | None:
    candidate = _clean_candidate(raw_value)
    if candidate == "":
        return None
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    elif not candidate.lower().startswith(("http://", "https://")):
        candidate = f"https://{candidate}"

    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return None
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    host = _normalized_host(parsed.hostname)
    if host == "":
        return None
    registrable_domain = registrable_domain_for_host(host)
    if (
        registrable_domain is None
        or registrable_domain in _INFRASTRUCTURE_DOMAINS
        or registrable_domain in _KNOWN_FALSE_DOMAINS
        or parsed.path.lower().endswith(_STATIC_ASSET_SUFFIXES)
    ):
        return None

    try:
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    include_port = port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    )
    netloc = f"{host}:{port}" if include_port else host
    return _NormalizedWebsite(
        url=urlunsplit((scheme, netloc, parsed.path or "/", "", "")),
        host=host,
        registrable_domain=registrable_domain,
    )


def registrable_domain_for_host(host: str) -> str | None:
    """Return a PSL-validated eTLD+1, or None for a non-domain host."""
    normalized_host = _normalized_host(host)
    if normalized_host == "" or len(normalized_host) > 253:
        return None
    if any(
        _HOST_LABEL_PATTERN.fullmatch(label) is None
        for label in normalized_host.split(".")
    ):
        return None
    extracted = _domain_extractor()(normalized_host)
    registrable_domain = extracted.top_domain_under_public_suffix
    if registrable_domain == "" or extracted.suffix in _FILE_LIKE_PUBLIC_SUFFIXES:
        return None
    return registrable_domain


def _website_values(
    text: str,
    *,
    allow_bare_domains: bool,
    corroborating_domains: set[str],
) -> list[str]:
    values = [
        _clean_candidate(f"{match.group(1)}{match.group(2)}")
        for match in _SPLIT_DOMAIN_PATTERN.finditer(text)
    ]
    url_spans: list[tuple[int, int]] = []
    email_spans = [match.span() for match in _EMAIL_LIKE_PATTERN.finditer(text)]
    for match in _URL_PATTERN.finditer(text):
        values.append(_clean_candidate(match.group(0)))
        url_spans.append(match.span())
    for match in _BARE_DOMAIN_PATTERN.finditer(text):
        if re.search(r"-\s+$", text[: match.start()]) is not None:
            continue
        if any(
            match.start() >= start and match.end() <= end
            for start, end in (*url_spans, *email_spans)
        ):
            continue
        raw_value = _clean_candidate(match.group(0))
        normalized = _normalize_website(raw_value)
        if normalized is None:
            continue
        if (
            allow_bare_domains
            or "/" in raw_value
            or normalized.registrable_domain in corroborating_domains
        ):
            values.append(raw_value)
    return [value for value in values if value != ""]


def _block_website_values(
    blocks: list[_VisibleBlock],
    index: int,
    *,
    context: str,
    corroborating_domains: set[str],
) -> tuple[list[str], str]:
    values = _website_values(
        blocks[index].value,
        allow_bare_domains=_BARE_DOMAIN_SIGNAL_PATTERN.search(context) is not None,
        corroborating_domains=corroborating_domains,
    )
    method = (
        "visible_text_reconstructed"
        if _SPLIT_DOMAIN_PATTERN.search(blocks[index].value) is not None
        else "visible_text"
    )
    if index == 0:
        return values, method
    previous_tokens = blocks[index - 1].value.rstrip().split()
    if not previous_tokens:
        return values, method
    prefix = previous_tokens[-1]
    if _DOMAIN_PREFIX_PATTERN.fullmatch(prefix) is None:
        return values, method
    return (
        _website_values(
            f"{prefix}{blocks[index].value.lstrip()}",
            allow_bare_domains=False,
            corroborating_domains=corroborating_domains,
        ),
        "visible_text_reconstructed",
    )


def _visible_blocks(tree: etree._ElementTree) -> list[_VisibleBlock]:
    context_elements = [
        element
        for element in tree.iter()
        if isinstance(element.tag, str)
        and _local_name(element.tag) in _CONTEXT_ELEMENTS
        and _is_visible(element)
    ]
    context_element_set = set(context_elements)
    non_leaf_context_elements: set[etree._Element] = set()
    for element in context_elements:
        for ancestor in element.iterancestors():
            if ancestor in context_element_set:
                non_leaf_context_elements.add(ancestor)

    blocks: list[_VisibleBlock] = []
    for element in context_elements:
        if element in non_leaf_context_elements:
            continue
        value = _visible_element_text(element)
        if value == "" or len(value) > _MAX_WEBSITE_BLOCK_LENGTH:
            continue
        blocks.append(
            _VisibleBlock(
                value=value,
                xpath=tree.getpath(element),
                source_line=element.sourceline,
                page_id=_page_id(element),
            )
        )
    return blocks


def _block_context(blocks: list[_VisibleBlock], index: int) -> str:
    block = blocks[index]
    nearby = blocks[max(0, index - 2) : index + 2]
    same_page = [
        item
        for item in nearby
        if block.page_id == "" or item.page_id == "" or item.page_id == block.page_id
    ]
    return _bounded_context(" ".join(item.value for item in same_page))


def _element_context(element: etree._Element) -> str:
    for ancestor in (element, *element.iterancestors()):
        if _local_name(ancestor.tag) in _CONTEXT_ELEMENTS:
            return _bounded_context(_visible_element_text(ancestor))
    return _bounded_context(_visible_element_text(element))


def _visible_element_text(element: etree._Element) -> str:
    parts: list[str] = []

    def append_element_text(current: etree._Element) -> None:
        if not _is_visible(current):
            return
        if current.text is not None:
            parts.append(current.text)
        for child in current:
            if isinstance(child.tag, str):
                append_element_text(child)
            if child.tail is not None:
                parts.append(child.tail)

    append_element_text(element)
    return _normalize_space("".join(parts))


def _is_visible(element: etree._Element) -> bool:
    for ancestor in (element, *element.iterancestors()):
        local_name = _local_name(ancestor.tag)
        if local_name in _EXCLUDED_ELEMENTS or (
            _namespace(ancestor.tag) == "http://www.xbrl.org/2013/inlineXBRL"
            and local_name in {"header", "hidden"}
        ):
            return False
        style = str(ancestor.get("style", "")).replace(" ", "").lower()
        if "display:none" in style or "visibility:hidden" in style:
            return False
    return True


def _suggested_role(context: str, *, registrable_domain: str) -> str:
    if registrable_domain in _SOCIAL_MEDIA_DOMAINS:
        return "social_media"
    search_text = f"{context} {registrable_domain}"
    for role, pattern in _ROLE_PATTERNS:
        if pattern.search(search_text) is not None:
            return role
    return "unknown"


def _finalize_candidate(
    accumulator: _WebsiteAccumulator,
) -> EsefWebsiteCandidate:
    evidence = sorted(
        accumulator.evidence,
        key=lambda item: (
            item.report_member,
            item.source_line if item.source_line is not None else -1,
            item.xpath,
            item.extraction_method,
            item.normalized_url,
            item.raw_value,
        ),
    )
    roles = {
        item.suggested_role for item in evidence if item.suggested_role != "unknown"
    }
    return EsefWebsiteCandidate(
        registrable_domain=accumulator.registrable_domain,
        hosts=sorted(accumulator.hosts),
        normalized_urls=sorted(accumulator.normalized_urls),
        suggested_roles=(
            [role for role in _ROLE_ORDER if role in roles] if roles else ["unknown"]
        ),
        evidence=evidence,
    )


@cache
def _domain_extractor() -> tldextract.TLDExtract:
    return tldextract.TLDExtract(
        cache_dir=None,
        suffix_list_urls=(),
        include_psl_private_domains=True,
    )


def _normalized_host(host: str | None) -> str:
    if host is None:
        return ""
    try:
        return host.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError:
        return ""


def _clean_candidate(value: str) -> str:
    return value.strip().rstrip(".,;:!?)]}")


def _page_id(element: etree._Element) -> str:
    for ancestor in (element, *element.iterancestors()):
        identifier = str(ancestor.get("id", "")).strip()
        if identifier != "":
            return identifier
    return ""


def _local_name(tag: object) -> str:
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", maxsplit=1)[-1].lower()


def _namespace(tag: object) -> str:
    if not isinstance(tag, str) or not tag.startswith("{"):
        return ""
    return tag[1:].partition("}")[0]


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _bounded_context(value: str, *, maximum_length: int = 500) -> str:
    normalized = _normalize_space(value)
    if len(normalized) <= maximum_length:
        return normalized
    return normalized[: maximum_length - 1].rstrip() + "…"
