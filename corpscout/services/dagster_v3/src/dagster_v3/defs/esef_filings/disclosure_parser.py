"""Deterministic structure extraction for ESEF narrative facts."""

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from lxml import etree, html

from dagster_v3.defs.company_source_records.identity import file_source_record_uid


DISCLOSURE_PARSER_NAME = "lxml_html_disclosure"
DISCLOSURE_PARSER_VERSION = "1"
VISIBLE_SECTION_PARSER_NAME = "corpscout-visible-sections"
VISIBLE_SECTION_TYPES = frozenset(
    {
        "board_composition",
        "executive_management",
        "board_committees",
        "auditor_appointment",
        "annual_report_signatures",
        "company_contact",
        "person_profiles",
        "ownership_and_listing",
        "company_overview",
    }
)
VISIBLE_SECTION_SEGMENTS = {
    "board_composition": "people_and_audit",
    "executive_management": "people_and_audit",
    "board_committees": "people_and_audit",
    "auditor_appointment": "people_and_audit",
    "annual_report_signatures": "people_and_audit",
    "person_profiles": "people_and_audit",
    "company_overview": "business_profile",
}

_BLOCK_ELEMENTS = frozenset(
    {
        "article",
        "blockquote",
        "div",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "li",
        "p",
        "section",
    }
)
_HEADING_ELEMENTS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})
_IGNORED_ELEMENTS = frozenset(
    {"canvas", "iframe", "noscript", "script", "style", "svg"}
)
_NUMERIC_CELL = re.compile(r"^[+−–—-]?[\d.,%()/]+$")


@dataclass(frozen=True)
class EsefDisclosure:
    blocks: list[dict[str, Any]]
    plain_text: str


def parse_esef_disclosure(raw_value: str) -> EsefDisclosure:
    """Turn one raw XHTML fact value into readable text and table blocks."""
    if raw_value.strip() == "":
        return EsefDisclosure(blocks=[], plain_text="")
    parser = html.HTMLParser(encoding="utf-8", recover=True, no_network=True)
    root = html.fragment_fromstring(raw_value, create_parent="div", parser=parser)
    blocks: list[dict[str, Any]] = []
    _collect_blocks(root, blocks)
    return EsefDisclosure(blocks=blocks, plain_text=_document_plain_text(blocks))


def disclosure_row(
    source: Mapping[str, object],
    *,
    source_run_id: str,
    extracted_at: str,
) -> dict[str, object]:
    """Build the persisted row for one source-linked narrative fact."""
    raw_value = str(source["raw_value"])
    raw_value_sha256 = sha256(raw_value.encode("utf-8")).hexdigest()
    source_document_id = str(source["source_document_id"])
    source_fact_id = str(source["source_fact_id"])
    source_fact_key = str(source["source_fact_key"])
    package_sha256 = str(source["package_sha256"]).lower()
    source_record_uid = file_source_record_uid(
        record_kind="esef_report_package",
        content_sha256=package_sha256,
    )
    disclosure_id = sha256(
        "\0".join(
            (
                source_record_uid,
                source_document_id,
                source_fact_key,
                str(source["segment"]),
                str(source["selection_reason"]),
                raw_value_sha256,
                DISCLOSURE_PARSER_VERSION,
            )
        ).encode("utf-8")
    ).hexdigest()
    disclosure = parse_esef_disclosure(raw_value)
    table_count = sum(block["type"] == "table" for block in disclosure.blocks)
    return {
        "disclosure_id": disclosure_id,
        "disclosure_kind": "tagged_fact",
        "source_document_id": source_document_id,
        "source_record_uid": source_record_uid,
        "source_fact_id": source_fact_id,
        "source_fact_key": source_fact_key,
        "package_sha256": package_sha256,
        "artifact_schema_version": int(source["artifact_schema_version"]),
        "lei": str(source["lei"]),
        "country_iso2": str(source["country_iso2"]).upper(),
        "company_id": str(source["company_id"]),
        "period_end": str(source["period_end"]),
        "fiscal_year": int(source["fiscal_year"]),
        "concept_qname": str(source["concept_qname"]),
        "concept_local_name": str(source["concept_local_name"]),
        "language": str(source["language"]),
        "segment": str(source["segment"]),
        "selection_reason": str(source["selection_reason"]),
        "report_member": str(source["report_member"]),
        "period_json": _json_text(source["period"]),
        "section_type": "",
        "page_id": "",
        "printed_page_number": "",
        "anchor_xpath": "",
        "anchor_visual_order": 0,
        "extraction_method": "tagged_fact",
        "text_sha256": sha256(disclosure.plain_text.encode("utf-8")).hexdigest(),
        "parser_name": DISCLOSURE_PARSER_NAME,
        "parser_version": DISCLOSURE_PARSER_VERSION,
        "blocks_json": _json_text(disclosure.blocks),
        "plain_text": disclosure.plain_text,
        "original_character_count": len(disclosure.plain_text),
        "block_count": len(disclosure.blocks),
        "table_count": table_count,
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }


def visible_section_disclosure_row(
    source: Mapping[str, object],
    section: Mapping[str, object],
    *,
    parser_version: str,
    source_run_id: str,
    extracted_at: str,
) -> dict[str, object]:
    """Build one persisted disclosure row from a visible XHTML section."""
    text = str(section["text"])
    text_sha256 = sha256(text.encode("utf-8")).hexdigest()
    if text_sha256 != str(section["text_sha256"]):
        raise ValueError("ESEF visible-section text SHA-256 does not match text")
    section_type = str(section["section_type"])
    if section_type not in VISIBLE_SECTION_TYPES:
        raise ValueError(f"Unsupported ESEF visible-section type: {section_type}")
    package_sha256 = str(source["package_sha256"]).lower()
    source_document_id = str(source["source_document_id"])
    source_record_uid = file_source_record_uid(
        record_kind="esef_report_package",
        content_sha256=package_sha256,
    )
    disclosure_id = sha256(
        "\0".join(
            (
                source_record_uid,
                source_document_id,
                section_type,
                str(section["report_member"]),
                str(section["anchor_xpath"]),
                text_sha256,
                parser_version,
            )
        ).encode("utf-8")
    ).hexdigest()
    return {
        "disclosure_id": disclosure_id,
        "disclosure_kind": "visible_section",
        "source_document_id": source_document_id,
        "source_record_uid": source_record_uid,
        "source_fact_id": "",
        "source_fact_key": "",
        "package_sha256": package_sha256,
        "artifact_schema_version": int(source["artifact_schema_version"]),
        "lei": str(source["lei"]),
        "country_iso2": str(source["country_iso2"]).upper(),
        "company_id": str(source["company_id"]),
        "period_end": str(source["period_end"]),
        "fiscal_year": int(source["fiscal_year"]),
        "concept_qname": "",
        "concept_local_name": "",
        "language": str(section.get("language", "")),
        "segment": VISIBLE_SECTION_SEGMENTS.get(section_type, ""),
        "selection_reason": f"visible-section:{section_type}",
        "report_member": str(section["report_member"]),
        "period_json": "{}",
        "section_type": section_type,
        "page_id": str(section.get("page_id", "")),
        "printed_page_number": str(section.get("printed_page_number", "")),
        "anchor_xpath": str(section["anchor_xpath"]),
        "anchor_visual_order": int(section["anchor_visual_order"]),
        "extraction_method": str(section["extraction_method"]),
        "text_sha256": text_sha256,
        "parser_name": VISIBLE_SECTION_PARSER_NAME,
        "parser_version": parser_version,
        "blocks_json": "[]",
        "plain_text": text,
        "original_character_count": int(section["original_character_count"]),
        "block_count": 1,
        "table_count": 0,
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }


def _collect_blocks(
    parent: etree._Element,
    blocks: list[dict[str, Any]],
) -> None:
    inline_buffer = parent.text or ""

    def flush_inline() -> None:
        nonlocal inline_buffer
        _append_text_block(blocks, inline_buffer)
        inline_buffer = ""

    for child in parent:
        tag = _tag_name(child)
        if tag in _IGNORED_ELEMENTS:
            inline_buffer += child.tail or ""
            continue
        if tag == "table":
            flush_inline()
            table = _parse_table(child)
            if table is not None:
                blocks.append(table)
            inline_buffer += child.tail or ""
            continue
        if tag == "br":
            inline_buffer += "\n" + (child.tail or "")
            continue
        if tag in _BLOCK_ELEMENTS:
            flush_inline()
            if _has_structural_child(child):
                _collect_blocks(child, blocks)
            else:
                _append_text_block(
                    blocks,
                    _inline_text(child),
                    heading=tag in _HEADING_ELEMENTS,
                )
            inline_buffer += child.tail or ""
            continue
        inline_buffer += _inline_text(child) + (child.tail or "")
    flush_inline()


def _inline_text(element: etree._Element) -> str:
    value = element.text or ""
    for child in element:
        tag = _tag_name(child)
        if tag in _IGNORED_ELEMENTS:
            value += child.tail or ""
            continue
        if tag == "br":
            value += "\n"
        else:
            value += _inline_text(child)
        value += child.tail or ""
    return value


def _has_structural_child(element: etree._Element) -> bool:
    return any(
        _tag_name(child) in _BLOCK_ELEMENTS or _tag_name(child) == "table"
        for child in element
    )


def _append_text_block(
    blocks: list[dict[str, Any]],
    value: str,
    *,
    heading: bool = False,
) -> None:
    text = _normalized_text(value)
    if text == "":
        return
    block_type = "heading" if heading or _looks_like_heading(text) else "paragraph"
    previous = blocks[-1] if blocks else None
    if (
        block_type == "paragraph"
        and previous is not None
        and previous["type"] == "paragraph"
        and len(str(previous["text"])) + len(text) < 2_400
    ):
        previous["text"] = f"{previous['text']} {text}"
        return
    blocks.append({"type": block_type, "text": text})


def _parse_table(table: etree._Element) -> dict[str, Any] | None:
    rows: list[list[dict[str, object]]] = []
    for row in table.iterdescendants():
        if _tag_name(row) != "tr" or _nearest_table(row) is not table:
            continue
        cells = [
            {
                "text": _normalized_text(_inline_text(cell)),
                "colSpan": _positive_span(cell.get("colspan")),
                "rowSpan": _positive_span(cell.get("rowspan")),
            }
            for cell in row
            if _tag_name(cell) in {"td", "th"}
        ]
        if any(cell["text"] != "" for cell in cells):
            rows.append(cells)
    if not rows:
        return None
    first_populated = [cell for cell in rows[0] if cell["text"] != ""]
    title = (
        str(first_populated[0]["text"])
        if len(rows) > 1 and len(first_populated) == 1
        else ""
    )
    content_rows = rows[1:] if title else rows
    return {
        "type": "table",
        "title": title,
        "headerRowCount": _header_row_count(content_rows),
        "rows": content_rows,
    }


def _nearest_table(element: etree._Element) -> etree._Element | None:
    ancestor = element.getparent()
    while ancestor is not None:
        if _tag_name(ancestor) == "table":
            return ancestor
        ancestor = ancestor.getparent()
    return None


def _header_row_count(rows: Sequence[Sequence[Mapping[str, object]]]) -> int:
    column_count = max(
        (sum(int(cell["colSpan"]) for cell in row) for row in rows),
        default=1,
    )
    for index, row in enumerate(rows):
        populated = [cell for cell in row if str(cell["text"]) != ""]
        if len(populated) < max(2, (column_count + 1) // 2):
            continue
        numeric_count = sum(
            _NUMERIC_CELL.fullmatch(re.sub(r"\s+", "", str(cell["text"]))) is not None
            for cell in populated
        )
        if numeric_count / len(populated) >= 0.5:
            return index if index > 0 else 0
    return 0


def _document_plain_text(blocks: Sequence[Mapping[str, Any]]) -> str:
    values: list[str] = []
    for block in blocks:
        if block["type"] != "table":
            values.append(str(block["text"]))
            continue
        table_rows = [
            "\t".join(str(cell["text"]) for cell in row).strip()
            for row in block["rows"]
        ]
        values.append(
            "\n".join(
                value for value in (str(block["title"]), *table_rows) if value != ""
            )
        )
    return "\n\n".join(value for value in values if value != "")


def _looks_like_heading(text: str) -> bool:
    if len(text) > 110:
        return False
    letters = [character for character in text if character.isalpha()]
    if len(letters) < 4:
        return False
    uppercase_count = sum(character == character.upper() for character in letters)
    return uppercase_count / len(letters) >= 0.85


def _positive_span(value: str | None) -> int:
    try:
        parsed = int(value or "1")
    except ValueError:
        return 1
    return parsed if parsed > 0 else 1


def _tag_name(element: etree._Element) -> str:
    if not isinstance(element.tag, str):
        return ""
    return etree.QName(element).localname.lower()


def _normalized_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
