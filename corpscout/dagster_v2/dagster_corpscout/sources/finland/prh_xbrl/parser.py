"""Parse PRH XBRL statement XML into raw-first ClickHouse rows.

Pure module: bytes in, typed rows out. No I/O, no Dagster imports.
Parser rules follow companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md:
keep all dimensions, never filter facts, denormalize MCY/REF, store reported
identity facts (si289/si168/di120/di121) on the document row, warn instead of drop.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from lxml import etree

from dagster_corpscout.sources.finland.prh_xbrl import spec, tables

XBRLI_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"

# Canonical prefixes for the fixed PRH/suomi.fi taxonomy namespaces, so
# concept_qname and member codes are stable even if a document declares
# different prefixes. Anything outside this table falls back to the
# document's own prefix.
CANONICAL_PREFIXES = {
    "http://www.suomi.fi/xbrl/crr/dict/met": "fi_met",
    "http://www.suomi.fi/xbrl/crr/dict/dim": "fi_dim",
    "http://www.suomi.fi/xbrl/crr/dict/dom/MC": "fi_MC",
    "http://www.suomi.fi/xbrl/crr/dict/dom/RF": "fi_RF",
    "http://www.suomi.fi/xbrl/crr/dict/dom/SC": "fi_SC",
    "http://www.xbrl.org/2003/iso4217": "iso4217",
}

_XML_PARSER = etree.XMLParser(resolve_entities=False)

_REPORTED_CONCEPTS = {
    "si289": "reported_business_id",
    "si168": "reported_company_name",
    "di120": "reported_period_start",
    "di121": "reported_period_end",
}


@dataclass(frozen=True)
class ParsedStatement:
    statement_key: str
    rows_by_table: dict[str, list[dict]]
    warnings: list[str]


def statement_key_for(business_id: str, financial_date: str, xml_sha256: str) -> str:
    return hashlib.sha256(
        f"{business_id}:{financial_date}:{xml_sha256}".encode("utf-8")
    ).hexdigest()


def parse_statement_xml(
    *,
    business_id: str,
    financial_date: str,
    registration_date: str | None,
    source_url: str,
    xml_object_key: str,
    source_run_id: str,
    body: bytes,
    parsed_at: datetime,
) -> ParsedStatement:
    root = etree.fromstring(body, parser=_XML_PARSER)
    xml_sha256 = hashlib.sha256(body).hexdigest()
    statement_key = statement_key_for(business_id, financial_date, xml_sha256)
    financial_date_value = date.fromisoformat(financial_date)
    warnings: list[str] = []

    context_rows = [
        _context_row(statement_key, element, parsed_at)
        for element in root.findall(f"{{{XBRLI_NS}}}context")
    ]
    contexts_by_id = {row["context_id"]: row for row in context_rows}
    if len(contexts_by_id) != len(context_rows):
        warnings.append("statement contains duplicate context ids")

    unit_rows = [
        _unit_row(statement_key, element, parsed_at)
        for element in root.findall(f"{{{XBRLI_NS}}}unit")
    ]

    prefix_by_namespace = {
        namespace: prefix for prefix, namespace in (root.nsmap or {}).items() if prefix
    }
    prefix_by_namespace.update(CANONICAL_PREFIXES)

    fact_rows: list[dict] = []
    reported: dict[str, str] = {}
    for element in root.iter():
        if not isinstance(element.tag, str):
            continue  # comments / processing instructions
        if element.get("contextRef") is None:
            continue
        qname = etree.QName(element)
        if qname.namespace == XBRLI_NS:
            continue
        row = _fact_row(
            statement_key=statement_key,
            business_id=business_id,
            financial_date_value=financial_date_value,
            ordinal=len(fact_rows) + 1,
            element=element,
            qname=qname,
            prefix_by_namespace=prefix_by_namespace,
            contexts_by_id=contexts_by_id,
            warnings=warnings,
            parsed_at=parsed_at,
        )
        fact_rows.append(row)
        if qname.localname in _REPORTED_CONCEPTS:
            reported[_REPORTED_CONCEPTS[qname.localname]] = row["raw_value"]

    reported_business_id = reported.get("reported_business_id")
    if reported_business_id and reported_business_id != business_id:
        warnings.append(
            f"reported business id {reported_business_id!r} does not match requested {business_id!r}"
        )
    if not fact_rows:
        warnings.append("statement contains no facts")

    schema_refs = [
        element.get(f"{{{XLINK_NS}}}href") or ""
        for element in root.findall(f"{{{LINK_NS}}}schemaRef")
    ]

    document = {
        "statement_key": statement_key,
        "source_run_id": source_run_id,
        "business_id": business_id,
        "financial_date": financial_date_value,
        "registration_date": _date_or_none(registration_date or ""),
        "source_url": source_url,
        "xml_object_key": xml_object_key,
        "xml_sha256": xml_sha256,
        "xml_size_bytes": len(body),
        "root_name": etree.QName(root).localname,
        "schema_refs": schema_refs,
        "taxonomy_entrypoint": schema_refs[0] if schema_refs else "",
        "reported_business_id": reported_business_id,
        "reported_company_name": reported.get("reported_company_name"),
        "reported_period_start": _date_or_none(reported.get("reported_period_start", "")),
        "reported_period_end": _date_or_none(reported.get("reported_period_end", "")),
        "contexts_count": len(context_rows),
        "units_count": len(unit_rows),
        "facts_count": len(fact_rows),
        "validation_warnings": list(warnings),
        "parser_version": spec.PARSER_VERSION,
        "parsed_at": parsed_at,
    }

    return ParsedStatement(
        statement_key=statement_key,
        rows_by_table={
            tables.STATEMENT_DOCUMENTS_TABLE: [document],
            tables.CONTEXTS_TABLE: context_rows,
            tables.UNITS_TABLE: unit_rows,
            tables.FACTS_TABLE: fact_rows,
        },
        warnings=warnings,
    )


def _date_or_none(value: str) -> date | None:
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


_DECIMAL_MAX_DIGITS = 38
_DECIMAL_MAX_SCALE = 6


def _decimal_or_none(raw_value: str) -> Decimal | None:
    """Parse a fact value if it fits ClickHouse Decimal(38, 6); otherwise None."""
    try:
        value = Decimal(raw_value)
    except InvalidOperation:
        return None
    if not value.is_finite():
        return None
    _sign, digits, exponent = value.as_tuple()
    exponent_int = int(exponent)
    scale = max(0, -exponent_int)
    if exponent_int < 0:
        integral_digits = max(len(digits) + exponent_int, 0)
    else:
        integral_digits = len(digits) + exponent_int
    if scale > _DECIMAL_MAX_SCALE:
        return None
    if integral_digits + _DECIMAL_MAX_SCALE > _DECIMAL_MAX_DIGITS:
        return None
    return value


def _canonical_qname_text(value: str, element) -> str:
    """Re-prefix a document-literal QName string canonically when possible."""
    prefix, sep, local = value.partition(":")
    if not sep:
        return value
    namespace = (element.nsmap or {}).get(prefix)
    canonical = CANONICAL_PREFIXES.get(namespace) if namespace else None
    return f"{canonical}:{local}" if canonical else value


def _member_for(dimensions: list[tuple[str, str, str]], suffix: str) -> str | None:
    for dimension_code, member_code, _label in dimensions:
        if dimension_code == suffix or dimension_code.endswith(f":{suffix}"):
            return member_code
    return None


def _context_row(statement_key: str, element, parsed_at: datetime) -> dict:
    entity = element.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}identifier")
    period = element.find(f"{{{XBRLI_NS}}}period")
    instant = period.findtext(f"{{{XBRLI_NS}}}instant") if period is not None else None
    period_start = period.findtext(f"{{{XBRLI_NS}}}startDate") if period is not None else None
    period_end = period.findtext(f"{{{XBRLI_NS}}}endDate") if period is not None else None
    if instant:
        period_type = "instant"
    elif period_start or period_end:
        period_type = "duration"
    else:
        period_type = "none"

    dimensions = [
        (
            _canonical_qname_text(member.get("dimension", ""), member),
            _canonical_qname_text((member.text or "").strip(), member),
            "",
        )
        for member in element.findall(f".//{{{XBRLDI_NS}}}explicitMember")
    ]
    ref_member = _member_for(dimensions, "REF")

    return {
        "statement_key": statement_key,
        "context_id": element.get("id", ""),
        "entity_identifier": (entity.text or "").strip() if entity is not None else "",
        "entity_scheme": entity.get("scheme", "") if entity is not None else "",
        "period_type": period_type,
        "instant_date": _date_or_none(instant) if instant else None,
        "period_start": _date_or_none(period_start) if period_start else None,
        "period_end": _date_or_none(period_end) if period_end else None,
        "dimensions": dimensions,
        "mcy_member_code": _member_for(dimensions, "MCY"),
        "mcy_member_label_fi": None,
        "ref_member_code": ref_member,
        "ref_member_label_fi": None,
        "is_comparative": 1 if ref_member is not None else 0,
        "parsed_at": parsed_at,
    }


def _unit_row(statement_key: str, element, parsed_at: datetime) -> dict:
    measures = [
        (measure.text or "").strip()
        for measure in element.findall(f".//{{{XBRLI_NS}}}measure")
    ]
    return {
        "statement_key": statement_key,
        "unit_id": element.get("id", ""),
        "measures": measures,
        "is_divide": 1 if element.find(f"{{{XBRLI_NS}}}divide") is not None else 0,
        "raw_xml": etree.tostring(element, encoding="unicode"),
        "parsed_at": parsed_at,
    }


def _fact_row(
    *,
    statement_key: str,
    business_id: str,
    financial_date_value: date,
    ordinal: int,
    element,
    qname,
    prefix_by_namespace: dict[str, str],
    contexts_by_id: dict[str, dict],
    warnings: list[str],
    parsed_at: datetime,
) -> dict:
    raw_value = (element.text or "").strip()
    unit_id = element.get("unitRef")
    numeric_value = None
    date_value = None
    text_value = None
    if not raw_value:
        value_kind = "empty"
    elif unit_id is not None:
        numeric_value = _decimal_or_none(raw_value)
        if numeric_value is not None:
            value_kind = "numeric"
        else:
            text_value = raw_value
            value_kind = "text"
            warnings.append(
                f"fact value {raw_value!r} is not representable as Decimal(38,6); stored as text"
            )
    else:
        date_value = _date_or_none(raw_value)
        if date_value is not None:
            value_kind = "date"
        else:
            text_value = raw_value
            value_kind = "text"

    prefix = prefix_by_namespace.get(qname.namespace) or element.prefix
    if prefix:
        concept_qname = f"{prefix}:{qname.localname}"
    else:
        concept_qname = qname.localname
        warnings.append(
            f"no namespace prefix resolved for concept {qname.localname!r} ({qname.namespace!r})"
        )

    context_ref = element.get("contextRef", "")
    context = contexts_by_id.get(context_ref)
    if context is None:
        warnings.append(f"fact {concept_qname} references unknown context {context_ref!r}")
        context = {
            "dimensions": [],
            "mcy_member_code": None,
            "ref_member_code": None,
            "is_comparative": 0,
        }

    return {
        "statement_key": statement_key,
        "business_id": business_id,
        "financial_date": financial_date_value,
        "fact_ordinal": ordinal,
        "concept_qname": concept_qname,
        "concept_namespace": qname.namespace or "",
        "concept_local_name": qname.localname,
        "context_id": context_ref,
        "unit_id": unit_id,
        "decimals": element.get("decimals"),
        "precision": element.get("precision"),
        "value_kind": value_kind,
        "raw_value": raw_value,
        "numeric_value": numeric_value,
        "date_value": date_value,
        "text_value": text_value,
        "mcy_member_code": context["mcy_member_code"],
        "mcy_member_label_fi": None,
        "ref_member_code": context["ref_member_code"],
        "ref_member_label_fi": None,
        "is_comparative": context["is_comparative"],
        "dimensions": context["dimensions"],
        "parser_version": spec.PARSER_VERSION,
        "parsed_at": parsed_at,
    }
