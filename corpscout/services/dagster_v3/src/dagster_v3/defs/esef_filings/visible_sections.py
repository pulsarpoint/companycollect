"""Bounded visible-report evidence omitted by Inline XBRL tagging.

ESEF issuers commonly publish annual reports as absolutely positioned XHTML.
The facts remain machine-readable, but board, management, auditor, signature,
contact, ownership, and overview pages are often visible only. This module
reconstructs those pages from their CSS coordinates without executing report
JavaScript or starting a browser.
"""

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Literal

from lxml import etree

VISIBLE_SECTION_EXTRACTOR_VERSION = "1"
MAX_VISIBLE_SECTION_CHARACTERS = 6_000
MAX_VISIBLE_SECTIONS_PER_TYPE = 2

VisibleSectionType = Literal[
    "board_composition",
    "executive_management",
    "board_committees",
    "auditor_appointment",
    "annual_report_signatures",
    "company_contact",
    "person_profiles",
    "ownership_and_listing",
    "company_overview",
]
VisibleExtractionMethod = Literal["positioned_page", "semantic_section"]

_INLINE_XBRL_NAMESPACE = "http://www.xbrl.org/2013/inlineXBRL"
_XML_LANGUAGE = "{http://www.w3.org/XML/1998/namespace}lang"
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
_SEMANTIC_CONTAINER_ELEMENTS = frozenset(
    {"address", "article", "footer", "main", "section"}
)
_SEMANTIC_BLOCK_ELEMENTS = frozenset(
    {
        "address",
        "article",
        "div",
        "footer",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "li",
        "p",
        "section",
        "td",
        "th",
    }
)
_HEADING_ELEMENTS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})
_CSS_CLASS_RULE = re.compile(r"(?<![-\w])\.([_a-zA-Z][\w-]*)\s*\{([^{}]*)\}")
_CSS_PIXEL_PROPERTY = re.compile(
    r"(?:^|;)\s*([a-z-]+)\s*:\s*(-?[0-9]+(?:\.[0-9]+)?)px",
    re.IGNORECASE,
)
_PRINTED_PAGE_NUMBER = re.compile(r"(?:page\s+|sida\s+)?([0-9]{1,4})", re.IGNORECASE)
_MAX_ANCHOR_CHARACTERS = 220
_MAX_SEMANTIC_BLOCK_CHARACTERS = 3_000
_SEMANTIC_BLOCKS_BEFORE_ANCHOR = 2
_SEMANTIC_BLOCKS_AFTER_ANCHOR = 24


@dataclass(frozen=True)
class EsefVisibleSection:
    section_type: VisibleSectionType
    report_member: str
    heading: str
    text: str
    page_id: str
    printed_page_number: str
    anchor_xpath: str
    anchor_visual_order: int
    extraction_method: VisibleExtractionMethod
    language: str
    original_character_count: int
    included_character_count: int
    truncated: bool
    text_sha256: str


@dataclass(frozen=True)
class _VisibleBlock:
    text: str
    xpath: str
    visual_order: int
    left: float | None
    bottom: float | None
    language: str


@dataclass(frozen=True)
class _Region:
    report_member: str
    page_id: str
    printed_page_number: str
    extraction_method: VisibleExtractionMethod
    blocks: tuple[_VisibleBlock, ...]


@dataclass(frozen=True)
class _SectionRule:
    primary: tuple[re.Pattern[str], ...]
    supporting: tuple[re.Pattern[str], ...]
    minimum_score: int
    allow_support_only: bool = False
    negative: tuple[re.Pattern[str], ...] = ()


def _patterns(*values: str) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(value, re.IGNORECASE) for value in values)


_SECTION_RULES: dict[VisibleSectionType, _SectionRule] = {
    "board_composition": _SectionRule(
        primary=_patterns(
            r"\bboard\s+of\s+directors\b",
            r"\bboard\s+composition\b",
            r"\bstyrelse(?:n|ledam[oö]ter)?\b",
            r"\bhallitus\b",
            r"\baufsichtsrat\b",
            r"\bconseil\s+d['’]administration\b",
        ),
        supporting=_patterns(
            r"\b(?:chair(?:man|person)?|ordf[oö]rande|styrelseledamot|board\s+member)\b",
            r"\b(?:elected|vald|valdes|appointed|uts[aå]gs)\b",
        ),
        minimum_score=5,
    ),
    "executive_management": _SectionRule(
        primary=_patterns(
            r"\b(?:executive|group|senior)\s+management\b",
            r"\bmanagement\s+team\b",
            r"\bexecutive\s+team\b",
            r"\b(?:f[oö]retagsledning|koncernledning|ledningsgrupp|johtoryhm[aä]|vorstand)\b",
            r"\bdirection\s+g[eé]n[eé]rale\b",
        ),
        supporting=_patterns(
            r"\b(?:chief\s+executive|chief\s+financial|ceo|cfo|verkst[aä]llande\s+direkt[oö]r|finanschef|vice\s+vd)\b",
            r"\b(?:management|ledning)\b",
            r"\b(?:education|utbildning|professional\s+experience|arbetslivserfarenhet)\b",
        ),
        minimum_score=5,
    ),
    "board_committees": _SectionRule(
        primary=_patterns(
            r"\b(?:audit|remuneration|nomination|sustainability)\s+committee\b",
            r"\b(?:revisions|ers[aä]ttnings|valberednings|h[aå]llbarhets)utskott(?:et)?\b",
            r"\bcomit[eé]\s+d['’]audit\b",
        ),
        supporting=_patterns(
            r"\bcommittee\s+(?:chair|member)\b",
            r"\butskott(?:ets)?\s+(?:ordf[oö]rande|ledam[oö]ter)\b",
        ),
        minimum_score=5,
        negative=_patterns(
            r"\b(?:independent\s+auditor['’]s\s+report|revisionsber[aä]ttelse)\b"
        ),
    ),
    "auditor_appointment": _SectionRule(
        primary=_patterns(
            r"\b(?:auditor|auditors|external\s+auditor)\b",
            r"\b(?:revisor|revisorer|revision)\b",
            r"\b(?:tilintarkastaja|abschlusspr[uü]fer|commissaire\s+aux\s+comptes)\b",
        ),
        supporting=_patterns(
            r"\b(?:authori[sz]ed|auktoriserad|godk[aä]nd)\s+(?:public\s+)?(?:accountant|auditor|revisor)\b",
            r"\b(?:key\s+audit\s+partner|huvudansvarig\s+revisor|p[aå]skrivande)\b",
            r"\b(?:appointed|elected|selected|uts[aå]gs|valde|valdes)\b",
            r"\b(?:audit\s+firm|revisionsbolag|revisionsbyr[aå])\b",
        ),
        minimum_score=7,
        negative=_patterns(
            r"\b(?:independent\s+auditor['’]s\s+report|revisionsber[aä]ttelse)\b"
        ),
    ),
    "annual_report_signatures": _SectionRule(
        primary=_patterns(
            r"\b(?:signatures?|signing\s+of\s+the\s+annual\s+report)\b",
            r"\b(?:underskrift|underskrifter|undertecknande)\b",
            r"\b(?:styrelsen\s+och\s+verkst[aä]llande\s+direkt[oö]ren|board\s+and\s+(?:the\s+)?chief\s+executive)\b",
            r"\b(?:allekirjoitukset|unterschriften)\b",
        ),
        supporting=_patterns(
            r"\b(?:stockholm|helsinki|gothenburg|g[oö]teborg)\s+(?:den\s+)?[0-9]{1,2}\b",
            r"\b(?:chair(?:man|person)?|ordf[oö]rande|verkst[aä]llande\s+direkt[oö]r|chief\s+executive)\b",
            r"\b(?:authori[sz]ed\s+auditor|auktoriserad\s+revisor)\b",
        ),
        minimum_score=6,
    ),
    "company_contact": _SectionRule(
        primary=_patterns(
            r"\b(?:contact|contact\s+information|contacts)\b",
            r"\b(?:kontakt|kontaktuppgifter|huvudkontor|registered\s+office|head\s+office)\b",
            r"\b(?:org\.?\s*nr|organisation(?:s)?\s+number|company\s+number)\b",
        ),
        supporting=_patterns(
            r"\b(?:phone|telephone|tel(?:efon)?)\b",
            r"\b(?:www\.|https?://)[^\s]+",
            r"\b[\w.+-]+@[\w.-]+\.[a-z]{2,}\b",
            r"\b[0-9]{3}\s*[0-9]{2}\s+[A-ZÅÄÖ][\wÅÄÖåäö-]+\b",
        ),
        minimum_score=6,
        allow_support_only=True,
    ),
    "person_profiles": _SectionRule(
        primary=_patterns(
            r"\b(?:board|management|executive)\s+profiles\b",
            r"\b(?:styrelse|f[oö]retagsledning|koncernledning)\b",
            r"\b(?:personprofiler|presentation\s+av\s+styrelsen)\b",
        ),
        supporting=_patterns(
            r"\b(?:education|utbildning|koulutus)\b",
            r"\b(?:professional\s+experience|career|arbetslivserfarenhet)\b",
            r"\b(?:shareholding|aktieinnehav|innehav\s+i)\b",
            r"\b(?:other\s+board\s+assignments|[oö]vriga\s+styrelseuppdrag)\b",
        ),
        minimum_score=7,
        allow_support_only=True,
    ),
    "ownership_and_listing": _SectionRule(
        primary=_patterns(
            r"\b(?:largest|major|principal)\s+shareholders?\b",
            r"\b(?:st[oö]rsta\s+)?aktie[aä]gare\b",
            r"\b(?:share\s+information|aktieinformation|ownership\s+structure|[aä]garstruktur)\b",
            r"\b(?:stock\s+exchange\s+listing|listed\s+on|noterad\s+p[aå])\b",
        ),
        supporting=_patterns(
            r"\b(?:nasdaq|euronext|london\s+stock\s+exchange|deutsche\s+b[oö]rse)\b",
            r"\b(?:ticker|trading\s+symbol|kortnamn)\b",
            r"\b(?:votes|voting\s+rights|r[oö]ster)\b",
            r"\b(?:shares|aktier)\b",
        ),
        minimum_score=5,
    ),
    "company_overview": _SectionRule(
        primary=_patterns(
            r"\b(?:company|group)\s+(?:overview|at\s+a\s+glance)\b",
            r"\b(?:about\s+(?:the\s+)?(?:company|group)|who\s+we\s+are)\b",
            r"\b(?:i\s+korthet|kort\s+om|verksamhets[oö]versikt|f[oö]retags[oö]versikt)\b",
            r"\b(?:business\s+concept|business\s+model|aff[aä]rsid[eé]|aff[aä]rsmodell)\b",
        ),
        supporting=_patterns(
            r"\b(?:operations|verksamhet|markets?|marknader)\b",
            r"\b(?:customers?|kunder|properties|fastigheter|employees|medarbetare)\b",
            r"\b(?:strategy|strategi|mission|vision)\b",
        ),
        minimum_score=5,
    ),
}
_SECTION_TYPE_ORDER = {
    section_type: index for index, section_type in enumerate(_SECTION_RULES)
}


def extract_visible_sections(
    report_paths: Mapping[str, Path],
) -> list[EsefVisibleSection]:
    """Extract bounded, page-level and semantic visible-report sections."""
    candidates: dict[VisibleSectionType, list[tuple[int, EsefVisibleSection]]] = {
        section_type: [] for section_type in _SECTION_RULES
    }
    for report_member, report_path in sorted(report_paths.items()):
        tree = etree.parse(report_path, _xml_parser())
        regions = _positioned_regions(tree, report_member=report_member)
        if not regions:
            regions = _semantic_regions(tree, report_member=report_member)
        for region in regions:
            for section_type, rule in _SECTION_RULES.items():
                match = _match_region(region, rule=rule)
                if match is None:
                    continue
                score, anchor_index = match
                candidates[section_type].append(
                    (score, _section_from_region(region, section_type, anchor_index))
                )

    selected: list[EsefVisibleSection] = []
    for section_type in _SECTION_RULES:
        ranked = sorted(
            candidates[section_type],
            key=lambda item: (
                -item[0],
                item[1].report_member,
                item[1].page_id,
                item[1].anchor_visual_order,
                item[1].text_sha256,
            ),
        )
        seen_text: set[str] = set()
        for _score, section in ranked:
            normalized_text = section.text.casefold()
            if normalized_text in seen_text:
                continue
            selected.append(section)
            seen_text.add(normalized_text)
            if len(seen_text) >= MAX_VISIBLE_SECTIONS_PER_TYPE:
                break

    return sorted(
        selected,
        key=lambda section: (
            section.report_member,
            _page_sort_key(section.page_id),
            section.anchor_visual_order,
            _SECTION_TYPE_ORDER[section.section_type],
        ),
    )


def _positioned_regions(
    tree: etree._ElementTree,
    *,
    report_member: str,
) -> list[_Region]:
    css_rules = _css_rules(tree)
    page_elements = [
        element
        for element in tree.iter()
        if isinstance(element.tag, str) and _has_class_prefix(element, "pf")
    ]
    regions: list[_Region] = []
    for page_index, page in enumerate(page_elements):
        text_elements = [
            element
            for element in page.iterdescendants()
            if isinstance(element.tag, str)
            and _has_class_prefix(element, "t")
            and not any(
                _has_class_prefix(ancestor, "t")
                for ancestor in element.iterancestors()
                if ancestor is not page
            )
            and _is_visible(element, css_rules=css_rules)
        ]
        unsorted_blocks: list[_VisibleBlock] = []
        for element in text_elements:
            text = _visible_element_text(element, css_rules=css_rules)
            if text == "":
                continue
            properties = _element_pixel_properties(element, css_rules=css_rules)
            unsorted_blocks.append(
                _VisibleBlock(
                    text=text,
                    xpath=tree.getpath(element),
                    visual_order=0,
                    left=properties.get("left"),
                    bottom=properties.get("bottom"),
                    language=_language(element),
                )
            )
        if not unsorted_blocks:
            continue
        ordered_blocks = tuple(
            _with_visual_order(block, visual_order)
            for visual_order, block in enumerate(
                sorted(
                    unsorted_blocks,
                    key=lambda block: (
                        -(block.bottom if block.bottom is not None else 0),
                        block.left if block.left is not None else 0,
                        block.xpath,
                    ),
                )
            )
        )
        page_id = str(page.get("id", "")).strip() or f"page-{page_index + 1}"
        page_height = _element_pixel_properties(page, css_rules=css_rules).get("height")
        regions.append(
            _Region(
                report_member=report_member,
                page_id=page_id,
                printed_page_number=_printed_page_number(
                    ordered_blocks,
                    page_height=page_height,
                ),
                extraction_method="positioned_page",
                blocks=ordered_blocks,
            )
        )
    return regions


def _semantic_regions(
    tree: etree._ElementTree,
    *,
    report_member: str,
) -> list[_Region]:
    css_rules = _css_rules(tree)
    blocks = _semantic_blocks(tree, css_rules=css_rules)
    if not blocks:
        return []
    regions: list[_Region] = []
    seen_ranges: set[tuple[int, int]] = set()
    for block_index, (element, block) in enumerate(blocks):
        local_name = _local_name(element.tag)
        if local_name in _HEADING_ELEMENTS:
            start = block_index
            end = _semantic_heading_end(blocks, block_index)
        elif local_name in _SEMANTIC_CONTAINER_ELEMENTS:
            descendant_xpaths = {
                tree.getpath(descendant)
                for descendant in element.iterdescendants()
                if isinstance(descendant.tag, str)
            }
            contained_indices = [
                index
                for index, (_candidate, candidate_block) in enumerate(blocks)
                if candidate_block.xpath == block.xpath
                or candidate_block.xpath in descendant_xpaths
            ]
            if not contained_indices:
                continue
            start = min(contained_indices)
            end = max(contained_indices) + 1
        elif len(block.text) <= _MAX_ANCHOR_CHARACTERS:
            start = max(0, block_index - _SEMANTIC_BLOCKS_BEFORE_ANCHOR)
            end = min(len(blocks), block_index + _SEMANTIC_BLOCKS_AFTER_ANCHOR)
        else:
            continue
        if (start, end) in seen_ranges:
            continue
        seen_ranges.add((start, end))
        region_blocks = tuple(
            _with_visual_order(candidate_block, visual_order)
            for visual_order, (_candidate, candidate_block) in enumerate(
                blocks[start:end]
            )
        )
        regions.append(
            _Region(
                report_member=report_member,
                page_id="",
                printed_page_number="",
                extraction_method="semantic_section",
                blocks=region_blocks,
            )
        )
    return regions


def _semantic_blocks(
    tree: etree._ElementTree,
    *,
    css_rules: Mapping[str, str],
) -> list[tuple[etree._Element, _VisibleBlock]]:
    elements = [
        element
        for element in tree.iter()
        if isinstance(element.tag, str)
        and _local_name(element.tag) in _SEMANTIC_BLOCK_ELEMENTS
        and _is_visible(element, css_rules=css_rules)
    ]
    element_set = set(elements)
    non_leaf_elements = {
        ancestor
        for element in elements
        for ancestor in element.iterancestors()
        if ancestor in element_set
    }
    blocks: list[tuple[etree._Element, _VisibleBlock]] = []
    for element in elements:
        local_name = _local_name(element.tag)
        if (
            element in non_leaf_elements
            and local_name not in _HEADING_ELEMENTS
            and local_name not in _SEMANTIC_CONTAINER_ELEMENTS
        ):
            continue
        text = _visible_element_text(element, css_rules=css_rules)
        if text == "" or len(text) > _MAX_SEMANTIC_BLOCK_CHARACTERS:
            continue
        blocks.append(
            (
                element,
                _VisibleBlock(
                    text=text,
                    xpath=tree.getpath(element),
                    visual_order=len(blocks),
                    left=None,
                    bottom=None,
                    language=_language(element),
                ),
            )
        )
    return blocks


def _semantic_heading_end(
    blocks: list[tuple[etree._Element, _VisibleBlock]],
    block_index: int,
) -> int:
    heading_name = _local_name(blocks[block_index][0].tag)
    heading_level = int(heading_name[1])
    for index in range(block_index + 1, len(blocks)):
        candidate_name = _local_name(blocks[index][0].tag)
        if (
            candidate_name in _HEADING_ELEMENTS
            and int(candidate_name[1]) <= heading_level
        ):
            return index
    return len(blocks)


def _match_region(
    region: _Region,
    *,
    rule: _SectionRule,
) -> tuple[int, int] | None:
    complete_text = "\n".join(block.text for block in region.blocks)
    primary_matches = sum(
        pattern.search(complete_text) is not None for pattern in rule.primary
    )
    supporting_matches = sum(
        pattern.search(complete_text) is not None for pattern in rule.supporting
    )
    if primary_matches == 0 and not rule.allow_support_only:
        return None
    negative_matches = sum(
        pattern.search(complete_text) is not None for pattern in rule.negative
    )
    score = primary_matches * 4 + supporting_matches * 2 - negative_matches * 8
    anchor_index = 0
    anchor_score = -1
    for index, block in enumerate(region.blocks):
        if len(block.text) > _MAX_ANCHOR_CHARACTERS:
            continue
        block_primary_matches = sum(
            pattern.search(block.text) is not None for pattern in rule.primary
        )
        block_supporting_matches = sum(
            pattern.search(block.text) is not None for pattern in rule.supporting
        )
        block_score = block_primary_matches * 6 + block_supporting_matches
        if any(pattern.fullmatch(block.text) is not None for pattern in rule.primary):
            block_score += 10
        if block_score > anchor_score:
            anchor_index = index
            anchor_score = block_score
    if anchor_score > 0:
        score += min(anchor_score, 8)
    if score < rule.minimum_score:
        return None
    return score, anchor_index


def _section_from_region(
    region: _Region,
    section_type: VisibleSectionType,
    anchor_index: int,
) -> EsefVisibleSection:
    anchor = region.blocks[anchor_index]
    if region.extraction_method == "semantic_section":
        first_block = max(0, anchor_index - _SEMANTIC_BLOCKS_BEFORE_ANCHOR)
        included_blocks = region.blocks[
            first_block : anchor_index + _SEMANTIC_BLOCKS_AFTER_ANCHOR
        ]
    else:
        included_blocks = region.blocks
    complete_text = "\n".join(block.text for block in included_blocks)
    bounded_text = _bounded_text(
        complete_text,
        maximum_length=MAX_VISIBLE_SECTION_CHARACTERS,
    )
    return EsefVisibleSection(
        section_type=section_type,
        report_member=region.report_member,
        heading=anchor.text,
        text=bounded_text,
        page_id=region.page_id,
        printed_page_number=region.printed_page_number,
        anchor_xpath=anchor.xpath,
        anchor_visual_order=anchor.visual_order,
        extraction_method=region.extraction_method,
        language=anchor.language,
        original_character_count=len(complete_text),
        included_character_count=len(bounded_text),
        truncated=len(bounded_text) < len(complete_text),
        text_sha256=sha256(bounded_text.encode("utf-8")).hexdigest(),
    )


def _css_rules(tree: etree._ElementTree) -> dict[str, str]:
    css = "\n".join(
        "".join(element.itertext())
        for element in tree.iter()
        if isinstance(element.tag, str) and _local_name(element.tag) == "style"
    )
    return {match.group(1): match.group(2) for match in _CSS_CLASS_RULE.finditer(css)}


def _element_pixel_properties(
    element: etree._Element,
    *,
    css_rules: Mapping[str, str],
) -> dict[str, float]:
    properties: dict[str, float] = {}
    for class_name in str(element.get("class", "")).split():
        for match in _CSS_PIXEL_PROPERTY.finditer(css_rules.get(class_name, "")):
            properties[match.group(1).lower()] = float(match.group(2))
    for match in _CSS_PIXEL_PROPERTY.finditer(str(element.get("style", ""))):
        properties[match.group(1).lower()] = float(match.group(2))
    return properties


def _has_class_prefix(element: etree._Element, prefix: str) -> bool:
    return any(
        class_name == prefix or class_name.startswith(f"{prefix}_")
        for class_name in str(element.get("class", "")).split()
    )


def _printed_page_number(
    blocks: Iterable[_VisibleBlock],
    *,
    page_height: float | None,
) -> str:
    candidates: list[tuple[float, int, str]] = []
    for block in blocks:
        match = _PRINTED_PAGE_NUMBER.fullmatch(block.text.strip())
        if match is None:
            continue
        number = match.group(1)
        if int(number) > 999:
            continue
        if page_height is None or block.bottom is None:
            edge_distance = float(block.visual_order)
        else:
            edge_distance = min(block.bottom, abs(page_height - block.bottom))
        candidates.append((edge_distance, block.visual_order, number))
    return min(candidates)[2] if candidates else ""


def _visible_element_text(
    element: etree._Element,
    *,
    css_rules: Mapping[str, str],
) -> str:
    parts: list[str] = []

    def append_element_text(current: etree._Element) -> None:
        if not _is_visible(current, css_rules=css_rules):
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


def _is_visible(
    element: etree._Element,
    *,
    css_rules: Mapping[str, str],
) -> bool:
    for ancestor in (element, *element.iterancestors()):
        local_name = _local_name(ancestor.tag)
        if local_name in _EXCLUDED_ELEMENTS or (
            _namespace(ancestor.tag) == _INLINE_XBRL_NAMESPACE
            and local_name in {"header", "hidden"}
        ):
            return False
        declarations = [str(ancestor.get("style", ""))]
        declarations.extend(
            css_rules.get(class_name, "")
            for class_name in str(ancestor.get("class", "")).split()
        )
        compact_style = "".join(declarations).replace(" ", "").lower()
        if "display:none" in compact_style or "visibility:hidden" in compact_style:
            return False
    return True


def _language(element: etree._Element) -> str:
    for ancestor in (element, *element.iterancestors()):
        language = str(ancestor.get(_XML_LANGUAGE, ancestor.get("lang", ""))).strip()
        if language != "":
            return language
    return ""


def _with_visual_order(block: _VisibleBlock, visual_order: int) -> _VisibleBlock:
    return _VisibleBlock(
        text=block.text,
        xpath=block.xpath,
        visual_order=visual_order,
        left=block.left,
        bottom=block.bottom,
        language=block.language,
    )


def _page_sort_key(page_id: str) -> tuple[str, int]:
    prefix, separator, suffix = page_id.rpartition("_")
    if separator != "" and suffix.isdigit():
        return prefix, int(suffix)
    match = re.search(r"([0-9]+)$", page_id)
    if match is not None:
        return page_id[: match.start()], int(match.group(1))
    return page_id, 0


def _xml_parser() -> etree.XMLParser:
    return etree.XMLParser(
        load_dtd=False,
        no_network=True,
        recover=False,
        remove_comments=True,
        resolve_entities=False,
        huge_tree=True,
    )


def _local_name(tag: object) -> str:
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", maxsplit=1)[-1].lower()


def _namespace(tag: object) -> str:
    if not isinstance(tag, str) or not tag.startswith("{"):
        return ""
    return tag[1:].partition("}")[0]


def _normalize_space(value: str) -> str:
    return " ".join(value.replace("\u00a0", " ").replace("\ue000", " ").split())


def _bounded_text(value: str, *, maximum_length: int) -> str:
    if len(value) <= maximum_length:
        return value
    return value[: maximum_length - 1].rstrip() + "…"
