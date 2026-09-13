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
    "external_reference",
)
# Multi-word referral phrases are distinctive enough to search for anywhere in
# the sentence. Bare "se"/"see" are extremely common words (Swedish "se" =
# "see"/"look"), so they only count when the domain follows them within the
# same sentence -- see `_mentions_referral_phrase`.
_REFERRAL_PHRASE_PATTERN = re.compile(
    r"\b(?:läs\s+mer\s+på|read\s+more\s+at|more\s+information\s+at|"
    r"mer\s+information\s+på|available\s+at|finns\s+på)\b",
    re.IGNORECASE,
)
_REFERRAL_SE_SEE_PATTERN = re.compile(r"\b(?:se|see)\b", re.IGNORECASE)
# Known false-positive mode: an uncorroborated, single-mention domain that
# happens to sit after the word "se"/"see" in its sentence, with no
# company-website signal word nearby, is misclassified `external_reference`
# even when it is genuinely the company's own site. Accepted -- the role is
# advisory (it only informs downstream confidence), not a correctness gate.
_SENTENCE_BOUNDARY_PATTERN = re.compile(r"(?<=[.!?])\s+|\s*[|•]\s*")
_LEADING_PUNCTUATION = "([{\"'"


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
    # Every domain in `accumulators` at this checkpoint came from a tagged
    # fact (report bodies haven't been parsed yet) -- used below so a
    # domain independently confirmed by an XBRL website tag is never
    # classified `external_reference`.
    tagged_fact_domains = frozenset(accumulators)

    for report_member, report_path in sorted(report_paths.items()):
        _extract_report_websites(
            report_member=report_member,
            report_path=report_path,
            corroborating_domains=corroborating_domains,
            tagged_fact_domains=tagged_fact_domains,
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
        hyphen_corroborating_domains=corroborating_domains,
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
    tagged_fact_domains: frozenset[str],
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

    # Computed once per report: how many times each registrable domain is
    # mentioned as a genuinely unbroken URL/bare domain anywhere in the
    # report's visible text (excluding a block's own leading hyphen-break
    # fragment, which isn't a real mention). Two uses:
    #   - any presence (>=1) corroborates the dehyphenated reading of a
    #     hyphenated line-break join elsewhere in the same report;
    #   - more than one mention corroborates a candidate against being
    #     classified `external_reference` (a domain seen only once, in a
    #     referral-shaped sentence, is more likely a third-party mention).
    mention_counts = _unbroken_domain_mention_counts(blocks)
    # `tagged_fact_domains` must corroborate a hyphen-drop too -- an XBRL
    # WebsitesOfLegalEntity tag is at least as strong a signal as a plain
    # unbroken text mention, and omitting it here left a tagged domain's own
    # hyphenated line-break split unrejoined (a spurious extra candidate
    # alongside the tagged one).
    hyphen_corroborating_domains = (
        corroborating_domains | tagged_fact_domains | set(mention_counts)
    )
    corroborated_domains = (
        corroborating_domains
        | tagged_fact_domains
        | {domain for domain, count in mention_counts.items() if count > 1}
    )

    for index, block in enumerate(blocks):
        context = _block_context(blocks, index)
        website_values, extraction_method = _block_website_values(
            blocks,
            index,
            context=context,
            corroborating_domains=corroborating_domains,
            hyphen_corroborating_domains=hyphen_corroborating_domains,
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
                    "corroborated_domains": corroborated_domains,
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
                "corroborated_domains": corroborated_domains,
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
    corroborated_domains_argument = evidence_arguments.get("corroborated_domains")
    corroborated_domains = (
        corroborated_domains_argument
        if isinstance(corroborated_domains_argument, (set, frozenset))
        else frozenset[str]()
    )
    suggested_role = str(
        evidence_arguments.get(
            "suggested_role",
            _suggested_role(
                role_context,
                registrable_domain=normalized.registrable_domain,
                corroborated=normalized.registrable_domain in corroborated_domains,
                referral_context=context,
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


def _dehyphenated_or_kept(
    *,
    hyphen_kept: str,
    hyphen_dropped: str,
    corroborating_domains: set[str],
) -> str:
    """Pick the dehyphenated reading of a line-broken domain join only when
    it is corroborated; otherwise keep today's hyphenated reading.

    A PDF-derived line break can land a hyphen either on a genuine
    hyphenated domain (``svenska-handel.se``) or on an artifact of
    reflowing a word across the break (``handels-`` + ``banken.com`` ->
    ``handelsbanken.com``). Without independent evidence the hyphenated
    reading is the safe default.
    """
    dropped_normalized = _normalize_website(hyphen_dropped)
    if (
        dropped_normalized is not None
        and dropped_normalized.registrable_domain in corroborating_domains
    ):
        # Checked first and returned immediately: even if `hyphen_kept` is
        # ALSO a normalizable (or independently corroborated) domain, the
        # dehyphenated reading wins on purpose once it has its own evidence.
        return hyphen_dropped
    return hyphen_kept


def _website_values(
    text: str,
    *,
    allow_bare_domains: bool,
    corroborating_domains: set[str],
    hyphen_corroborating_domains: set[str],
) -> list[str]:
    values = [
        _dehyphenated_or_kept(
            hyphen_kept=_clean_candidate(f"{match.group(1)}{match.group(2)}"),
            hyphen_dropped=_clean_candidate(f"{match.group(1)[:-1]}{match.group(2)}"),
            corroborating_domains=hyphen_corroborating_domains,
        )
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
    hyphen_corroborating_domains: set[str],
) -> tuple[list[str], str]:
    # `hyphen_corroborating_domains` corroborates a dehyphenated line-break
    # join specifically (report-wide unbroken mentions in addition to known
    # contact domains); it is distinct from `corroborating_domains`, which
    # only gates whether an otherwise-unsignalled bare domain is trusted. A
    # caller with no wider report-level signal (e.g. tagged-fact values, or a
    # test exercising a single block in isolation) passes the same set for
    # both.
    current_text = blocks[index].value
    allow_bare_domains = _BARE_DOMAIN_SIGNAL_PATTERN.search(context) is not None
    method = (
        "visible_text_reconstructed"
        if _SPLIT_DOMAIN_PATTERN.search(current_text) is not None
        else "visible_text"
    )
    if index > 0:
        rejoin = _rejoin_hyphenated_block(
            previous_value=blocks[index - 1].value,
            current_value=current_text,
            corroborating_domains=hyphen_corroborating_domains,
        )
        if rejoin is not None:
            joined_values, remainder_text = rejoin
            remainder_values = _website_values(
                remainder_text,
                allow_bare_domains=allow_bare_domains,
                corroborating_domains=corroborating_domains,
                hyphen_corroborating_domains=hyphen_corroborating_domains,
            )
            return [*joined_values, *remainder_values], "visible_text_reconstructed"
    return (
        _website_values(
            current_text,
            allow_bare_domains=allow_bare_domains,
            corroborating_domains=corroborating_domains,
            hyphen_corroborating_domains=hyphen_corroborating_domains,
        ),
        method,
    )


def _hyphen_line_break_prefix(previous_value: str) -> str | None:
    """Return the previous block's trailing hyphenated token, stripped of
    any leading punctuation, when it is a plausible domain-prefix
    continuation -- e.g. ``handels-``, ``www.handels-``,
    ``(www.handels-``, ``https://www.handels-``.

    Returns None for a token whose alphanumeric part is too short to be a
    meaningful prefix (guards against an unrelated trailing dash).
    """
    tokens = previous_value.rstrip().split()
    if not tokens:
        return None
    token = tokens[-1].lstrip(_LEADING_PUNCTUATION)
    if not token.endswith("-"):
        return None
    alphanumeric_length = sum(character.isalnum() for character in token)
    if alphanumeric_length < 3:
        return None
    return token


def _rejoin_hyphenated_block(
    *,
    previous_value: str,
    current_value: str,
    corroborating_domains: set[str],
) -> tuple[list[str], str] | None:
    """Reconstruct a domain split across a PDF-derived line break.

    Returns None when the previous block does not end in a plausible
    domain-prefix token -- the caller should process ``current_value``
    unchanged. Otherwise returns the value(s) to emit for the join (never
    the bare continuation fragment on its own) plus the remainder of
    ``current_value`` still to be scanned for unrelated domains.
    """
    prefix = _hyphen_line_break_prefix(previous_value)
    if prefix is None:
        return None
    stripped_current = current_value.lstrip()
    match = _BARE_DOMAIN_PATTERN.match(stripped_current)
    if match is None:
        match = _URL_PATTERN.match(stripped_current)
    if match is None:
        return None

    fragment = match.group(0)
    remainder = stripped_current[match.end() :]
    hyphen_kept = _clean_candidate(f"{prefix}{fragment}")
    hyphen_dropped = _clean_candidate(f"{prefix[:-1]}{fragment}")

    dropped_normalized = _normalize_website(hyphen_dropped)
    if (
        dropped_normalized is not None
        and dropped_normalized.registrable_domain in corroborating_domains
    ):
        # Checked first and returned immediately: even if `hyphen_kept` is
        # ALSO a normalizable domain, the dehyphenated reading wins on
        # purpose once it has its own corroborating evidence.
        return [hyphen_dropped], remainder
    if _normalize_website(hyphen_kept) is not None:
        return [hyphen_kept], remainder
    if dropped_normalized is not None:
        return [hyphen_dropped], remainder
    # Neither reading normalizes to a domain -- still suppress the bare
    # fragment (it is a line-break artifact, not evidence of anything).
    return [], remainder


def _unbroken_domain_mention_counts(blocks: list[_VisibleBlock]) -> dict[str, int]:
    """Count genuinely unbroken URL/bare-domain mentions per registrable
    domain across a report's visible-text blocks.

    A block whose predecessor ends in a hyphenated prefix candidate has its
    own leading match excluded: that match is the line-break continuation
    fragment (e.g. ``banken.com``), not a real mention of that fragment as
    a domain in its own right.
    """
    counts: dict[str, int] = {}
    for index, block in enumerate(blocks):
        skip_leading_match = index > 0 and (
            _hyphen_line_break_prefix(blocks[index - 1].value) is not None
        )
        for domain in _unbroken_domain_mentions(
            block.value, skip_leading_match=skip_leading_match
        ):
            counts[domain] = counts.get(domain, 0) + 1
    return counts


def _unbroken_domain_mentions(text: str, *, skip_leading_match: bool) -> list[str]:
    domains: list[str] = []
    url_spans: list[tuple[int, int]] = []
    email_spans = [match.span() for match in _EMAIL_LIKE_PATTERN.finditer(text)]
    for match in _URL_PATTERN.finditer(text):
        url_spans.append(match.span())
        if skip_leading_match and match.start() == 0:
            continue
        normalized = _normalize_website(_clean_candidate(match.group(0)))
        if normalized is not None:
            domains.append(normalized.registrable_domain)
    for match in _BARE_DOMAIN_PATTERN.finditer(text):
        if skip_leading_match and match.start() == 0:
            continue
        # Same guard shape as `_website_values`: a hyphen followed by
        # whitespace right before this match means it is the second half of
        # a same-line `_SPLIT_DOMAIN_PATTERN` join, already counted there.
        if re.search(r"-\s+$", text[: match.start()]) is not None:
            continue
        if any(
            match.start() >= start and match.end() <= end
            for start, end in (*url_spans, *email_spans)
        ):
            continue
        normalized = _normalize_website(_clean_candidate(match.group(0)))
        if normalized is not None:
            domains.append(normalized.registrable_domain)
    return domains


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


def _suggested_role(
    role_context: str,
    *,
    registrable_domain: str,
    corroborated: bool,
    referral_context: str,
) -> str:
    if registrable_domain in _SOCIAL_MEDIA_DOMAINS:
        return "social_media"
    search_text = f"{role_context} {registrable_domain}"
    for role, pattern in _ROLE_PATTERNS:
        if pattern.search(search_text) is not None:
            return role
    if not corroborated and _is_external_reference(
        referral_context, registrable_domain=registrable_domain
    ):
        return "external_reference"
    return "unknown"


def _is_external_reference(context: str, *, registrable_domain: str) -> bool:
    sentence = _sentence_mentioning_domain(
        context, registrable_domain=registrable_domain
    )
    if _BARE_DOMAIN_SIGNAL_PATTERN.search(sentence) is not None:
        return False
    return _mentions_referral_phrase(sentence, registrable_domain=registrable_domain)


def _sentence_mentioning_domain(context: str, *, registrable_domain: str) -> str:
    normalized_domain = registrable_domain.lower()
    for sentence in _SENTENCE_BOUNDARY_PATTERN.split(context):
        if normalized_domain in sentence.lower():
            return sentence
    return context


def _mentions_referral_phrase(sentence: str, *, registrable_domain: str) -> bool:
    if _REFERRAL_PHRASE_PATTERN.search(sentence) is not None:
        return True
    domain_match = re.search(re.escape(registrable_domain), sentence, re.IGNORECASE)
    if domain_match is None:
        return False
    # Bare "se"/"see" only count as a referral trigger when they precede the
    # domain within the same sentence (see the comment above
    # `_REFERRAL_SE_SEE_PATTERN` for why they can't be matched anywhere).
    return _REFERRAL_SE_SEE_PATTERN.search(sentence[: domain_match.start()]) is not None


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
