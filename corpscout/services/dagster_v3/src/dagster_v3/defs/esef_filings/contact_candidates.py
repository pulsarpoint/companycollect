"""Deterministic contact-candidate extraction from ESEF report XHTML."""

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

import phonenumbers
from email_validator import EmailNotValidError, validate_email
from lxml import etree

from dagster_v3.defs.esef_filings.candidate_context import (
    is_report_production_credit,
)

_EMAIL_PATTERN = re.compile(
    r"[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?"
    r"(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+",
    re.IGNORECASE,
)
_PHONE_CUE_PATTERN = re.compile(
    r"\b(?:appel|contact|fax|head\s+office|kontakt|mobile|mobil|phone|puhelin|"
    r"reception|switchboard|tel(?:ephone)?|telefon|tfn|tél(?:éphone)?|växel)\b",
    re.IGNORECASE,
)
_STRONG_GENERAL_ROLE_PATTERN = re.compile(
    r"\b(?:head\s+office|reception|switchboard|växel)\b",
    re.IGNORECASE,
)
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
_EMAIL_FACT_CONCEPTS = frozenset({"EMailAddress"})
_PHONE_FACT_CONCEPTS = frozenset({"ContactPhoneNumber"})
_COMBINED_CONTACT_FACT_CONCEPTS = frozenset(
    {"AffiliatesOfReportingEntityAddressesAndPhones"}
)
_MAX_CONTACT_BLOCK_LENGTH = 1_000
_SPECIFIC_CONTACT_ROLES = frozenset(
    {"auditor", "corporate_responsibility", "investor_relations", "media"}
)
_ROLE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "investor_relations",
        re.compile(
            r"\b(?:actionnaires?|investisseurs?|investorrelations|"
            r"investor(?:s|\s+relations)?|"
            r"investerarrelationer|ir\s+contact|relations?\s+investisseurs?|"
            r"shareholders?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "media",
        re.compile(r"\b(?:media|press|presse|presskontakt)\b", re.IGNORECASE),
    ),
    (
        "auditor",
        re.compile(
            r"\b(?:audit(?:or|ing)?|revisor|commissaire\s+aux\s+comptes|"
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
        "fax",
        re.compile(r"\bfax\b", re.IGNORECASE),
    ),
    (
        "general",
        re.compile(
            r"\b(?:appel|contact|head\s+office|kontakt|puhelin|reception|"
            r"switchboard|tel(?:ephone)?|telefon|tfn|tél(?:éphone)?|växel)\b",
            re.IGNORECASE,
        ),
    ),
)


@dataclass(frozen=True)
class TaggedContactValue:
    report_member: str
    concept_local_name: str
    value: str


@dataclass(frozen=True)
class EsefContactEvidence:
    report_member: str
    extraction_method: str
    raw_value: str
    source_concept: str
    xpath: str
    source_line: int | None
    page_id: str
    surrounding_text: str
    suggested_role: str


@dataclass(frozen=True)
class EsefContactCandidate:
    kind: str
    normalized_value: str
    country_code: str
    suggested_roles: list[str]
    evidence: list[EsefContactEvidence]


@dataclass(frozen=True)
class _VisibleFragment:
    value: str
    context_element: etree._Element
    xpath: str
    source_line: int | None
    page_id: str


@dataclass(frozen=True)
class _VisibleBlock:
    value: str
    xpath: str
    source_line: int | None
    page_id: str


@dataclass
class _CandidateAccumulator:
    kind: str
    normalized_value: str
    country_code: str
    evidence: list[EsefContactEvidence]


def extract_contact_candidates(
    report_paths: Mapping[str, Path],
    *,
    tagged_values: Iterable[TaggedContactValue],
    default_region: str = "",
) -> list[EsefContactCandidate]:
    """Extract normalized candidates without deciding which contact is canonical."""
    region = default_region.strip().upper()
    if region != "" and region not in phonenumbers.SUPPORTED_REGIONS:
        raise ValueError(
            "Phone normalization requires a supported ISO alpha-2 region: "
            f"{default_region}"
        )
    accumulators: dict[tuple[str, str], _CandidateAccumulator] = {}

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
            default_region=region,
            accumulators=accumulators,
        )

    for report_member, report_path in sorted(report_paths.items()):
        _extract_report_contacts(
            report_member=report_member,
            report_path=report_path,
            default_region=region,
            accumulators=accumulators,
        )

    return [
        _finalize_candidate(accumulator)
        for _, accumulator in sorted(accumulators.items())
    ]


def _extract_tagged_value(
    tagged_value: TaggedContactValue,
    *,
    default_region: str,
    accumulators: dict[tuple[str, str], _CandidateAccumulator],
) -> None:
    concept = tagged_value.concept_local_name
    evidence_arguments = {
        "report_member": tagged_value.report_member,
        "extraction_method": "tagged_fact",
        "source_concept": concept,
        "xpath": "",
        "source_line": None,
        "page_id": "",
        "surrounding_text": _bounded_context(tagged_value.value),
    }
    if concept in _EMAIL_FACT_CONCEPTS or concept in _COMBINED_CONTACT_FACT_CONCEPTS:
        for raw_email in _email_values(tagged_value.value):
            _add_email_candidate(
                raw_email=raw_email,
                evidence_arguments=evidence_arguments,
                accumulators=accumulators,
            )
    if concept in _PHONE_FACT_CONCEPTS or concept in _COMBINED_CONTACT_FACT_CONCEPTS:
        for raw_phone, number in _phone_values(
            tagged_value.value,
            default_region=default_region,
            allow_without_cue=True,
        ):
            _add_phone_candidate(
                raw_phone=raw_phone,
                number=number,
                evidence_arguments=evidence_arguments,
                accumulators=accumulators,
            )


def _extract_report_contacts(
    *,
    report_member: str,
    report_path: Path,
    default_region: str,
    accumulators: dict[tuple[str, str], _CandidateAccumulator],
) -> None:
    parser = etree.XMLParser(
        load_dtd=False,
        no_network=True,
        recover=False,
        remove_comments=True,
        resolve_entities=False,
        huge_tree=True,
    )
    tree = etree.parse(report_path, parser)
    fragments = _visible_fragments(tree)
    contexts = [_fragment_context(fragments, index) for index in range(len(fragments))]

    for fragment, context in zip(fragments, contexts, strict=True):
        evidence_arguments = {
            "report_member": report_member,
            "extraction_method": "visible_text",
            "source_concept": "",
            "xpath": fragment.xpath,
            "source_line": fragment.source_line,
            "page_id": fragment.page_id,
            "surrounding_text": context,
        }
        for raw_email in _email_values(fragment.value):
            _add_email_candidate(
                raw_email=raw_email,
                evidence_arguments=evidence_arguments,
                accumulators=accumulators,
            )
        if any(character.isdigit() for character in fragment.value):
            for raw_phone, number in _phone_values(
                context,
                default_region=default_region,
                allow_without_cue=_has_phone_signal(context),
            ):
                _add_phone_candidate(
                    raw_phone=raw_phone,
                    number=number,
                    evidence_arguments=evidence_arguments,
                    accumulators=accumulators,
                )

    blocks = _visible_blocks(tree)
    for index, block in enumerate(blocks):
        context, suggested_role = _block_context(blocks, index)
        evidence_arguments = {
            "report_member": report_member,
            "extraction_method": "visible_text",
            "source_concept": "",
            "xpath": block.xpath,
            "source_line": block.source_line,
            "page_id": block.page_id,
            "surrounding_text": context,
            "candidate_context": block.value,
            "suggested_role": suggested_role,
        }
        for raw_email in _email_values(block.value):
            _add_email_candidate(
                raw_email=raw_email,
                evidence_arguments=evidence_arguments,
                accumulators=accumulators,
            )
        for raw_phone, number in _phone_values(
            block.value,
            default_region=default_region,
            allow_without_cue=(
                suggested_role != "unknown"
                or _has_phone_signal(block.value)
                or "+" in block.value
            ),
        ):
            _add_phone_candidate(
                raw_phone=raw_phone,
                number=number,
                evidence_arguments=evidence_arguments,
                accumulators=accumulators,
            )

    for element in tree.xpath("//*[local-name()='a' and @href]"):
        if not isinstance(element, etree._Element) or not _is_visible(element):
            continue
        href = str(element.get("href", "")).strip()
        method, values = _href_contact_values(href)
        if method == "":
            continue
        context = _element_context(element)
        evidence_arguments = {
            "report_member": report_member,
            "extraction_method": method,
            "source_concept": "",
            "xpath": tree.getpath(element),
            "source_line": element.sourceline,
            "page_id": _page_id(element),
            "surrounding_text": context,
        }
        if method == "mailto":
            for raw_email in values:
                _add_email_candidate(
                    raw_email=raw_email,
                    evidence_arguments=evidence_arguments,
                    accumulators=accumulators,
                )
        else:
            for raw_phone in values:
                number = _parse_phone(raw_phone, default_region=default_region)
                if number is not None:
                    _add_phone_candidate(
                        raw_phone=raw_phone,
                        number=number,
                        evidence_arguments=evidence_arguments,
                        accumulators=accumulators,
                    )


def _visible_fragments(tree: etree._ElementTree) -> list[_VisibleFragment]:
    fragments: list[_VisibleFragment] = []
    for element in tree.iter():
        if not isinstance(element.tag, str):
            continue
        if _is_visible(element) and element.text is not None:
            value = _normalize_space(element.text)
            if value != "":
                fragments.append(
                    _VisibleFragment(
                        value=value,
                        context_element=_context_element(element),
                        xpath=tree.getpath(element),
                        source_line=element.sourceline,
                        page_id=_page_id(element),
                    )
                )
        parent = element.getparent()
        if parent is not None and _is_visible(parent) and element.tail is not None:
            value = _normalize_space(element.tail)
            if value != "":
                fragments.append(
                    _VisibleFragment(
                        value=value,
                        context_element=_context_element(parent),
                        xpath=tree.getpath(parent),
                        source_line=element.sourceline,
                        page_id=_page_id(parent),
                    )
                )
    return fragments


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
        if value == "" or len(value) > _MAX_CONTACT_BLOCK_LENGTH:
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


def _block_context(
    blocks: list[_VisibleBlock],
    index: int,
) -> tuple[str, str]:
    block = blocks[index]
    neighboring_blocks = blocks[max(0, index - 3) : index + 2]
    context = _bounded_context(" ".join(item.value for item in neighboring_blocks))
    nearby_roles: list[str] = []
    for role_index in (index, index - 1, index - 2, index - 3, index + 1):
        if role_index < 0 or role_index >= len(blocks):
            continue
        role_block = blocks[role_index]
        if (
            block.page_id != ""
            and role_block.page_id != ""
            and role_block.page_id != block.page_id
        ):
            continue
        nearby_roles.append(_suggested_role(role_block.value))
    if nearby_roles and nearby_roles[0] in _SPECIFIC_CONTACT_ROLES:
        return context, nearby_roles[0]
    if _STRONG_GENERAL_ROLE_PATTERN.search(block.value) is not None:
        return context, "general"
    if len(nearby_roles) > 1 and nearby_roles[1] in _SPECIFIC_CONTACT_ROLES:
        return context, nearby_roles[1]
    if (
        len(nearby_roles) > 1
        and nearby_roles[0] == "general"
        and nearby_roles[1] == "general"
    ):
        return context, "general"
    for suggested_role in nearby_roles:
        if suggested_role in _SPECIFIC_CONTACT_ROLES:
            return context, suggested_role
    for suggested_role in nearby_roles:
        if suggested_role != "unknown":
            return context, suggested_role
    return context, "unknown"


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


def _element_context(element: etree._Element) -> str:
    context_element = _context_element(element)
    return _bounded_context(_visible_element_text(context_element))


def _context_element(element: etree._Element) -> etree._Element:
    for ancestor in (element, *element.iterancestors()):
        if _local_name(ancestor.tag) in _CONTEXT_ELEMENTS:
            return ancestor
    return element


def _fragment_context(
    fragments: list[_VisibleFragment],
    index: int,
) -> str:
    fragment = fragments[index]
    values = [fragment.value]
    if index > 0 and fragments[index - 1].context_element is fragment.context_element:
        values.insert(0, fragments[index - 1].value)
    if (
        index + 1 < len(fragments)
        and fragments[index + 1].context_element is fragment.context_element
    ):
        values.append(fragments[index + 1].value)
    return _bounded_context(" ".join(values))


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


def _href_contact_values(href: str) -> tuple[str, list[str]]:
    lowered = href.lower()
    if lowered.startswith("mailto:"):
        address_part = unquote(href[7:].partition("?")[0])
        return "mailto", [
            value.strip()
            for value in re.split(r"[,;]", address_part)
            if value.strip() != ""
        ]
    if lowered.startswith("tel:"):
        phone_part = unquote(href[4:].partition("?")[0])
        return "tel", [phone_part]
    return "", []


def _email_values(text: str) -> list[str]:
    return [match.group(0) for match in _EMAIL_PATTERN.finditer(text)]


def _phone_values(
    text: str,
    *,
    default_region: str,
    allow_without_cue: bool,
) -> list[tuple[str, phonenumbers.PhoneNumber]]:
    if not allow_without_cue and not text.lstrip().startswith("+"):
        return []
    region = default_region or None
    return [
        (match.raw_string, match.number)
        for match in phonenumbers.PhoneNumberMatcher(
            text,
            region,
            leniency=phonenumbers.Leniency.VALID,
        )
        if phonenumbers.is_valid_number(match.number)
    ]


def _parse_phone(
    raw_phone: str,
    *,
    default_region: str,
) -> phonenumbers.PhoneNumber | None:
    candidate = raw_phone.partition(";")[0].strip()
    try:
        number = phonenumbers.parse(candidate, default_region or None)
    except phonenumbers.NumberParseException:
        return None
    return number if phonenumbers.is_valid_number(number) else None


def _add_email_candidate(
    *,
    raw_email: str,
    evidence_arguments: Mapping[str, object],
    accumulators: dict[tuple[str, str], _CandidateAccumulator],
) -> None:
    try:
        normalized = validate_email(
            raw_email,
            check_deliverability=False,
        ).normalized.lower()
    except EmailNotValidError:
        return
    _add_candidate(
        kind="email",
        normalized_value=normalized,
        country_code="",
        raw_value=raw_email,
        evidence_arguments=evidence_arguments,
        accumulators=accumulators,
    )


def _add_phone_candidate(
    *,
    raw_phone: str,
    number: phonenumbers.PhoneNumber,
    evidence_arguments: Mapping[str, object],
    accumulators: dict[tuple[str, str], _CandidateAccumulator],
) -> None:
    normalized = phonenumbers.format_number(
        number,
        phonenumbers.PhoneNumberFormat.E164,
    )
    _add_candidate(
        kind="phone",
        normalized_value=normalized,
        country_code=phonenumbers.region_code_for_number(number) or "",
        raw_value=raw_phone,
        evidence_arguments=evidence_arguments,
        accumulators=accumulators,
    )


def _add_candidate(
    *,
    kind: str,
    normalized_value: str,
    country_code: str,
    raw_value: str,
    evidence_arguments: Mapping[str, object],
    accumulators: dict[tuple[str, str], _CandidateAccumulator],
) -> None:
    context = str(evidence_arguments["surrounding_text"])
    candidate_context = str(evidence_arguments.get("candidate_context", context))
    extraction_method = str(evidence_arguments["extraction_method"])
    if extraction_method != "tagged_fact" and is_report_production_credit(
        candidate_context,
        raw_value,
        normalized_value,
    ):
        return
    evidence = EsefContactEvidence(
        report_member=str(evidence_arguments["report_member"]),
        extraction_method=extraction_method,
        raw_value=raw_value,
        source_concept=str(evidence_arguments["source_concept"]),
        xpath=str(evidence_arguments["xpath"]),
        source_line=(
            int(evidence_arguments["source_line"])
            if evidence_arguments["source_line"] is not None
            else None
        ),
        page_id=str(evidence_arguments["page_id"]),
        surrounding_text=context,
        suggested_role=str(
            evidence_arguments.get("suggested_role", _suggested_role(context))
        ),
    )
    key = (kind, normalized_value.casefold())
    accumulator = accumulators.setdefault(
        key,
        _CandidateAccumulator(
            kind=kind,
            normalized_value=normalized_value,
            country_code=country_code,
            evidence=[],
        ),
    )
    if evidence not in accumulator.evidence:
        accumulator.evidence.append(evidence)


def _finalize_candidate(
    accumulator: _CandidateAccumulator,
) -> EsefContactCandidate:
    evidence = sorted(
        accumulator.evidence,
        key=lambda item: (
            item.report_member,
            item.source_line if item.source_line is not None else -1,
            item.xpath,
            item.extraction_method,
            item.raw_value,
        ),
    )
    roles = {
        item.suggested_role for item in evidence if item.suggested_role != "unknown"
    }
    return EsefContactCandidate(
        kind=accumulator.kind,
        normalized_value=accumulator.normalized_value,
        country_code=accumulator.country_code,
        suggested_roles=(
            [role for role, _ in _ROLE_PATTERNS if role in roles]
            if roles
            else ["unknown"]
        ),
        evidence=evidence,
    )


def _suggested_role(context: str) -> str:
    for role, pattern in _ROLE_PATTERNS:
        if pattern.search(context) is not None:
            return role
    return "unknown"


def _has_phone_signal(context: str) -> bool:
    return _PHONE_CUE_PATTERN.search(context) is not None


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
