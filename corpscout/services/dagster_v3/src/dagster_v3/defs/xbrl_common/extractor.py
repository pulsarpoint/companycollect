"""Unified full-fidelity XBRL / iXBRL fact extractor (canonical rows).

Emits canonical row dicts per defs/xbrl_common/tables.py. Source adapters
prepend identity columns and append source-specific derived columns.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from lxml import etree

from dagster_v3.defs.xbrl_common.transforms import (
    XBRL_COMMON_PARSER_VERSION,
    UnknownTransform,
    apply_transform,
)

XBRLI_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XML_LANG_ATTR = "{http://www.w3.org/XML/1998/namespace}lang"
IX_NS = "http://www.xbrl.org/2013/inlineXBRL"
IX_2008_NS = "http://www.xbrl.org/2008/inlineXBRL"
_STRUCTURAL_NS = frozenset({XBRLI_NS, XBRLDI_NS, LINK_NS})

_XML_PARSER = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class SourceProfile:
    source_slug: str
    canonical_prefixes: dict[str, str]
    reported_concepts: dict[str, str]


@dataclass
class ExtractedFiling:
    document: dict
    contexts: list[dict] = field(default_factory=list)
    units: list[dict] = field(default_factory=list)
    facts: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _canonical_qname(
    namespace: str | None, local: str, element: etree._Element, profile: SourceProfile
) -> str:
    if not namespace:
        return local
    prefix = profile.canonical_prefixes.get(namespace)
    if prefix is None:
        for cand_prefix, cand_ns in (element.nsmap or {}).items():
            if cand_ns == namespace and cand_prefix:
                prefix = cand_prefix
                break
    return f"{prefix}:{local}" if prefix else local


def _canonical_qname_text(value: str, element: etree._Element, profile: SourceProfile) -> str:
    prefix, sep, local = value.strip().partition(":")
    if not sep:
        return value.strip()
    namespace = (element.nsmap or {}).get(prefix)
    return _canonical_qname(namespace, local, element, profile) if namespace else value.strip()


def _iso(parsed_at: datetime) -> str:
    return parsed_at.isoformat()


def _context_rows(root: etree._Element, profile: SourceProfile, parsed_at: datetime) -> list[dict]:
    rows: list[dict] = []
    for element in root.iter(f"{{{XBRLI_NS}}}context"):
        identifier = element.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}identifier")
        period = element.find(f"{{{XBRLI_NS}}}period")
        instant = period.findtext(f"{{{XBRLI_NS}}}instant") if period is not None else None
        start = period.findtext(f"{{{XBRLI_NS}}}startDate") if period is not None else None
        end = period.findtext(f"{{{XBRLI_NS}}}endDate") if period is not None else None
        dimensions: list[list[str]] = []
        for member in element.findall(f".//{{{XBRLDI_NS}}}explicitMember"):
            dimensions.append(
                [
                    _canonical_qname_text(member.get("dimension", ""), member, profile),
                    _canonical_qname_text((member.text or "").strip(), member, profile),
                    "",
                ]
            )
        for member in element.findall(f".//{{{XBRLDI_NS}}}typedMember"):
            typed_value = ""
            for child in member:
                typed_value = "".join(child.itertext()).strip()
                break
            dimensions.append(
                [
                    _canonical_qname_text(member.get("dimension", ""), member, profile),
                    "",
                    typed_value,
                ]
            )
        rows.append(
            {
                "context_id": element.get("id", ""),
                "entity_identifier": (
                    (identifier.text or "").strip() if identifier is not None else ""
                ),
                "entity_scheme": identifier.get("scheme", "") if identifier is not None else "",
                "period_type": (
                    "instant" if instant else "duration" if (start or end) else "none"
                ),
                "instant_date": (instant or "").strip(),
                "period_start": (start or "").strip(),
                "period_end": (end or "").strip(),
                "dimensions": json.dumps(dimensions, ensure_ascii=False),
                "is_comparative": False,
                "parser_version": XBRL_COMMON_PARSER_VERSION,
                "parsed_at": _iso(parsed_at),
            }
        )
    return rows


def _unit_currency(measures: list[str]) -> str:
    if len(measures) != 1:
        return ""
    prefix, sep, code = measures[0].partition(":")
    if sep and prefix.lower() == "iso4217":
        return code.upper()
    return ""


def _unit_rows(root: etree._Element, profile: SourceProfile, parsed_at: datetime) -> list[dict]:
    rows: list[dict] = []
    for element in root.iter(f"{{{XBRLI_NS}}}unit"):
        direct = [
            _canonical_qname_text((m.text or "").strip(), m, profile)
            for m in element.findall(f"{{{XBRLI_NS}}}measure")
        ]
        numerator = [
            _canonical_qname_text((m.text or "").strip(), m, profile)
            for m in element.findall(
                f"{{{XBRLI_NS}}}divide/{{{XBRLI_NS}}}unitNumerator/{{{XBRLI_NS}}}measure"
            )
        ]
        denominator = [
            _canonical_qname_text((m.text or "").strip(), m, profile)
            for m in element.findall(
                f"{{{XBRLI_NS}}}divide/{{{XBRLI_NS}}}unitDenominator/{{{XBRLI_NS}}}measure"
            )
        ]
        rows.append(
            {
                "unit_id": element.get("id", ""),
                "measures": json.dumps(direct, ensure_ascii=False),
                "numerator_measures": json.dumps(numerator, ensure_ascii=False),
                "denominator_measures": json.dumps(denominator, ensure_ascii=False),
                "is_divide": bool(numerator or denominator),
                "currency": _unit_currency(direct),
                "parser_version": XBRL_COMMON_PARSER_VERSION,
                "parsed_at": _iso(parsed_at),
            }
        )
    return rows


def _decimal_or_none(raw: str) -> Decimal | None:
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def _classify_value(
    *, raw_value: str, is_nil: bool, unit_id: str
) -> tuple[str, str, str, str]:
    """Return (value_kind, numeric_value, date_value, text_value)."""
    if is_nil or not raw_value:
        return "empty", "", "", ""
    if unit_id:
        parsed = _decimal_or_none(raw_value)
        if parsed is not None:
            return "numeric", str(parsed), "", ""
        return "text", "", "", raw_value
    if _ISO_DATE_RE.match(raw_value):
        try:
            date.fromisoformat(raw_value)
            return "date", "", raw_value, ""
        except ValueError:
            pass
    return "text", "", "", raw_value


def _fact_row_base(
    *,
    element: etree._Element,
    namespace: str,
    local: str,
    ordinal: int,
    contexts_by_id: dict[str, dict],
    profile: SourceProfile,
    units_by_id: dict[str, dict],
    parsed_at: datetime,
) -> dict:
    context_id = element.get("contextRef", "")
    context = contexts_by_id.get(context_id)
    unit_id = element.get("unitRef") or ""
    unit = units_by_id.get(unit_id)
    return {
        "fact_ordinal": ordinal,
        "concept_qname": _canonical_qname(namespace, local, element, profile),
        "concept_namespace": namespace or "",
        "concept_local_name": local,
        "context_id": context_id,
        "unit_id": unit_id,
        "currency": unit["currency"] if unit else "",
        "decimals": element.get("decimals") or "",
        "precision": element.get("precision") or "",
        "is_nil": (element.get(f"{{{XSI_NS}}}nil") or "").lower() in {"1", "true"},
        "xml_lang": element.get(XML_LANG_ATTR) or "",
        "value_kind": "",
        "raw_value": "",
        "numeric_value": "",
        "date_value": "",
        "text_value": "",
        "dimensions": context["dimensions"] if context else "[]",
        "is_comparative": False,
        "parser_version": XBRL_COMMON_PARSER_VERSION,
        "parsed_at": _iso(parsed_at),
    }


def _plain_fact_rows(
    root: etree._Element,
    profile: SourceProfile,
    contexts_by_id: dict[str, dict],
    units_by_id: dict[str, dict],
    parsed_at: datetime,
) -> list[dict]:
    rows: list[dict] = []
    for element in root.iter():
        if not isinstance(element.tag, str) or element.get("contextRef") is None:
            continue
        qname = etree.QName(element)
        if qname.namespace in _STRUCTURAL_NS or qname.namespace in (IX_NS, IX_2008_NS):
            continue
        row = _fact_row_base(
            element=element,
            namespace=qname.namespace or "",
            local=qname.localname,
            ordinal=len(rows) + 1,
            contexts_by_id=contexts_by_id,
            profile=profile,
            units_by_id=units_by_id,
            parsed_at=parsed_at,
        )
        raw_value = "".join(element.itertext()).strip()
        row["raw_value"] = raw_value
        kind, numeric, date_value, text = _classify_value(
            raw_value=raw_value, is_nil=row["is_nil"], unit_id=row["unit_id"]
        )
        row.update(
            {"value_kind": kind, "numeric_value": numeric, "date_value": date_value, "text_value": text}
        )
        rows.append(row)
    return rows


def _resolve_reported(document: dict, facts: list[dict], profile: SourceProfile) -> None:
    for fact in facts:
        column = profile.reported_concepts.get(fact["concept_qname"])
        if column and not document.get(column):
            document[column] = fact["raw_value"]


def _apply_comparative(document: dict, contexts: list[dict], facts: list[dict]) -> None:
    reported_end = document.get("reported_period_end") or ""
    by_id: dict[str, bool] = {}
    for context in contexts:
        effective = context["instant_date"] or context["period_end"]
        comparative = bool(reported_end and effective and effective != reported_end)
        context["is_comparative"] = comparative
        by_id[context["context_id"]] = comparative
    for fact in facts:
        fact["is_comparative"] = by_id.get(fact["context_id"], False)


def _is_inline(root: etree._Element) -> bool:
    if etree.QName(root).localname.lower() == "html":
        return True
    for element in root.iter():
        if isinstance(element.tag, str) and element.tag.startswith(f"{{{IX_NS}}}"):
            return True
    return False


def extract_filing(
    body: bytes | str, *, profile: SourceProfile, parsed_at: datetime
) -> ExtractedFiling:
    content = body.encode("utf-8") if isinstance(body, str) else body
    document: dict = {
        "xml_sha256": "",
        "xml_size_bytes": len(content),
        "root_name": "",
        "schema_refs": "[]",
        "taxonomy_entrypoint": "",
        "reported_entity_id": "",
        "reported_company_name": "",
        "reported_period_start": "",
        "reported_period_end": "",
        "contexts_count": 0,
        "units_count": 0,
        "facts_count": 0,
        "validation_warnings": "[]",
        "parser_version": XBRL_COMMON_PARSER_VERSION,
        "parsed_at": _iso(parsed_at),
    }
    filing = ExtractedFiling(document=document)
    try:
        root = etree.fromstring(content, parser=_XML_PARSER)
    except etree.XMLSyntaxError as exc:
        root = None
        filing.warnings.append(f"unparseable XML: {exc}")
    if root is None:
        if not filing.warnings:
            filing.warnings.append("unparseable XML: empty parse result")
        document["validation_warnings"] = json.dumps(filing.warnings, ensure_ascii=False)
        return filing

    document["root_name"] = etree.QName(root).localname
    schema_refs = [
        element.get(f"{{{XLINK_NS}}}href") or ""
        for element in root.iter(f"{{{LINK_NS}}}schemaRef")
    ]
    document["schema_refs"] = json.dumps(schema_refs, ensure_ascii=False)
    document["taxonomy_entrypoint"] = schema_refs[0] if schema_refs else ""

    filing.contexts = _context_rows(root, profile, parsed_at)
    filing.units = _unit_rows(root, profile, parsed_at)
    contexts_by_id = {c["context_id"]: c for c in filing.contexts}
    units_by_id = {u["unit_id"]: u for u in filing.units}

    if _is_inline(root):
        filing.facts = _inline_fact_rows(
            root, profile, contexts_by_id, units_by_id, parsed_at, filing.warnings
        )
    else:
        filing.facts = _plain_fact_rows(root, profile, contexts_by_id, units_by_id, parsed_at)

    _resolve_reported(document, filing.facts, profile)
    _apply_comparative(document, filing.contexts, filing.facts)
    document["contexts_count"] = len(filing.contexts)
    document["units_count"] = len(filing.units)
    document["facts_count"] = len(filing.facts)
    document["validation_warnings"] = json.dumps(filing.warnings, ensure_ascii=False)
    return filing


_IX_FACT_TAGS = (
    f"{{{IX_NS}}}nonFraction",
    f"{{{IX_NS}}}nonNumeric",
    f"{{{IX_NS}}}fraction",
)


def _text_excluding(element: etree._Element) -> str:
    parts: list[str] = [element.text or ""]
    for child in element:
        if isinstance(child.tag, str) and child.tag == f"{{{IX_NS}}}exclude":
            parts.append(child.tail or "")
            continue
        parts.append(_text_excluding(child))
        parts.append(child.tail or "")
    return "".join(parts)


def _continued_text(
    element: etree._Element, continuations: dict[str, etree._Element]
) -> str:
    parts = [_text_excluding(element)]
    seen: set[str] = set()
    next_id = element.get("continuedAt")
    while next_id and next_id not in seen:
        seen.add(next_id)
        continuation = continuations.get(next_id)
        if continuation is None:
            break
        parts.append(_text_excluding(continuation))
        next_id = continuation.get("continuedAt")
    return "".join(parts)


def _inline_fact_rows(
    root: etree._Element,
    profile: SourceProfile,
    contexts_by_id: dict[str, dict],
    units_by_id: dict[str, dict],
    parsed_at: datetime,
    warnings: list[str],
) -> list[dict]:
    continuations = {
        element.get("id", ""): element
        for element in root.iter(f"{{{IX_NS}}}continuation")
        if element.get("id")
    }
    rows: list[dict] = []
    for element in root.iter(*_IX_FACT_TAGS):
        name = element.get("name", "")
        prefix, sep, local = name.partition(":")
        namespace = (element.nsmap or {}).get(prefix if sep else None) or ""
        row = _fact_row_base(
            element=element,
            namespace=namespace,
            local=local if sep else name,
            ordinal=len(rows) + 1,
            contexts_by_id=contexts_by_id,
            profile=profile,
            units_by_id=units_by_id,
            parsed_at=parsed_at,
        )
        tag_local = etree.QName(element).localname

        if tag_local == "fraction":
            numerator = element.findtext(f"{{{IX_NS}}}numerator") or ""
            denominator = element.findtext(f"{{{IX_NS}}}denominator") or ""
            raw_value = f"{numerator.strip()}/{denominator.strip()}"
            warnings.append(f"ix:fraction stored as text: {row['concept_qname']}")
            row.update(
                {"raw_value": raw_value, "value_kind": "text", "text_value": raw_value}
            )
            rows.append(row)
            continue

        raw_text = _continued_text(element, continuations).strip()
        row["raw_value"] = raw_text
        fmt = element.get("format")

        if row["is_nil"] or not raw_text:
            row["value_kind"] = "empty"
            rows.append(row)
            continue

        transformed_kind: str | None = None
        transformed_value = raw_text
        transform_failed = False
        if fmt:
            try:
                result = apply_transform(fmt, raw_text)
                transformed_kind, transformed_value = result.kind, result.value
                row["raw_value"] = transformed_value
            except UnknownTransform:
                warnings.append(f"unknown ixt transform {fmt!r} on {row['concept_qname']}")
                transform_failed = True
            except ValueError as exc:
                warnings.append(
                    f"ixt transform {fmt!r} failed on {row['concept_qname']}: {exc}"
                )
                transform_failed = True

        if transform_failed:
            row.update({"value_kind": "text", "text_value": raw_text})
            rows.append(row)
            continue

        if tag_local == "nonFraction":
            numeric = (
                _decimal_or_none(transformed_value)
                if transformed_kind in (None, "numeric")
                else None
            )
            if numeric is None and transformed_kind is None:
                numeric = _decimal_or_none(raw_text)
            if numeric is not None:
                scale = element.get("scale")
                if scale:
                    try:
                        numeric = numeric * (Decimal(10) ** int(scale))
                    except (ValueError, InvalidOperation):
                        warnings.append(
                            f"invalid scale {scale!r} on {row['concept_qname']}"
                        )
                if element.get("sign") == "-":
                    numeric = -numeric
                row.update({"value_kind": "numeric", "numeric_value": str(numeric)})
            else:
                row.update({"value_kind": "text", "text_value": raw_text})
        else:  # nonNumeric
            if transformed_kind == "date":
                row.update({"value_kind": "date", "date_value": transformed_value})
            elif transformed_kind == "empty":
                row["value_kind"] = "empty"
            elif transformed_kind == "boolean":
                row.update({"value_kind": "text", "text_value": transformed_value})
            else:
                kind, numeric_v, date_v, text_v = _classify_value(
                    raw_value=transformed_value, is_nil=False, unit_id=""
                )
                row.update(
                    {
                        "value_kind": kind,
                        "numeric_value": numeric_v,
                        "date_value": date_v,
                        "text_value": text_v,
                    }
                )
        rows.append(row)
    return rows
