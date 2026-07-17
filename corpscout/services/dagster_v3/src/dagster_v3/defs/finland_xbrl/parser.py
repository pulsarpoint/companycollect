from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from lxml import etree

from dagster_v3.defs.finland_xbrl import tables

XBRLI_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XML_NS = "http://www.w3.org/XML/1998/namespace"
PARSER_VERSION = "2.0.0"

CANONICAL_PREFIXES = {
    "http://www.suomi.fi/xbrl/crr/dict/met": "fi_met",
    "http://www.suomi.fi/xbrl/crr/dict/dim": "fi_dim",
    "http://www.suomi.fi/xbrl/crr/dict/dom/MC": "fi_MC",
    "http://www.suomi.fi/xbrl/crr/dict/dom/RF": "fi_RF",
    "http://www.suomi.fi/xbrl/crr/dict/dom/SC": "fi_SC",
    "http://www.xbrl.org/2003/iso4217": "iso4217",
}
REPORTED_CONCEPTS = {
    "si289": "reported_business_id",
    "si168": "reported_company_name",
    "di120": "reported_period_start",
    "di121": "reported_period_end",
}
XML_PARSER = etree.XMLParser(resolve_entities=False)


@dataclass(frozen=True)
class ParsedStatement:
    statement_key: str
    rows_by_table: dict[str, list[dict[str, Any]]]
    warnings: list[str]


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
    root = etree.fromstring(body, parser=XML_PARSER)
    xml_sha256 = hashlib.sha256(body).hexdigest()
    statement_key = statement_key_for(
        business_id,
        financial_date,
        registration_date or "",
        xml_sha256,
    )
    warnings: list[str] = []

    contexts = [
        _context_row(
            statement_key=statement_key,
            element=element,
            parsed_at=parsed_at,
        )
        for element in root.findall(f"{{{XBRLI_NS}}}context")
    ]
    contexts_by_id = {context["context_id"]: context for context in contexts}
    units = [
        _unit_row(
            statement_key=statement_key,
            element=element,
            parsed_at=parsed_at,
        )
        for element in root.findall(f"{{{XBRLI_NS}}}unit")
    ]
    units_by_id = {unit["unit_id"]: unit for unit in units}
    prefix_by_namespace = {
        namespace: prefix for prefix, namespace in (root.nsmap or {}).items() if prefix
    }
    prefix_by_namespace.update(CANONICAL_PREFIXES)

    fact_rows: list[dict[str, Any]] = []
    reported: dict[str, str] = {}
    for element in root.iter():
        if not isinstance(element.tag, str) or element.get("contextRef") is None:
            continue
        qname = etree.QName(element)
        if qname.namespace == XBRLI_NS:
            continue
        fact = _fact_row(
            statement_key=statement_key,
            business_id=business_id,
            financial_date=financial_date,
            ordinal=len(fact_rows) + 1,
            element=element,
            qname=qname,
            prefix_by_namespace=prefix_by_namespace,
            contexts_by_id=contexts_by_id,
            units_by_id=units_by_id,
            warnings=warnings,
            parsed_at=parsed_at,
        )
        fact_rows.append(fact)
        if qname.localname in REPORTED_CONCEPTS:
            reported[REPORTED_CONCEPTS[qname.localname]] = fact["raw_value"]

    reported_business_id = reported.get("reported_business_id")
    if reported_business_id and reported_business_id != business_id:
        warnings.append(
            f"reported business id {reported_business_id!r} does not match requested {business_id!r}"
        )
    if not fact_rows:
        warnings.append("statement contains no facts")

    reported_period_end = reported.get("reported_period_end") or financial_date
    for context in contexts:
        effective_date = context["instant_date"] or context["period_end"]
        context["is_comparative"] = bool(
            effective_date and reported_period_end and effective_date != reported_period_end
        )
    for fact in fact_rows:
        context = contexts_by_id.get(fact["context_id"])
        fact["is_comparative"] = bool(context and context["is_comparative"])

    schema_refs = [
        element.get(f"{{{XLINK_NS}}}href") or ""
        for element in root.findall(f"{{{LINK_NS}}}schemaRef")
    ]
    document = {
        "statement_key": statement_key,
        "source_run_id": source_run_id,
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date or "",
        "source_url": source_url,
        "xml_object_key": xml_object_key,
        "xml_sha256": xml_sha256,
        "xml_size_bytes": len(body),
        "root_name": etree.QName(root).localname,
        "schema_refs": json.dumps(schema_refs, ensure_ascii=False),
        "taxonomy_entrypoint": schema_refs[0] if schema_refs else "",
        "reported_business_id": reported_business_id or "",
        "reported_company_name": reported.get("reported_company_name", ""),
        "reported_period_start": reported.get("reported_period_start", ""),
        "reported_period_end": reported.get("reported_period_end", ""),
        "contexts_count": len(contexts),
        "units_count": len(units),
        "facts_count": len(fact_rows),
        "validation_warnings": json.dumps(warnings, ensure_ascii=False),
        "parser_version": PARSER_VERSION,
        "parsed_at": parsed_at.isoformat(),
    }
    return ParsedStatement(
        statement_key=statement_key,
        rows_by_table={
            tables.STATEMENT_DOCUMENTS_TABLE: [document],
            tables.CONTEXTS_TABLE: contexts,
            tables.UNITS_TABLE: units,
            tables.FACTS_TABLE: fact_rows,
        },
        warnings=warnings,
    )


def statement_key_for(
    business_id: str,
    financial_date: str,
    registration_date: str,
    xml_sha256: str,
) -> str:
    identity = f"{business_id}:{financial_date}:{registration_date}:{xml_sha256}"
    return hashlib.sha256(identity.encode()).hexdigest()


def _context_row(
    *,
    statement_key: str,
    element: etree._Element,
    parsed_at: datetime,
) -> dict[str, Any]:
    identifier = element.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}identifier")
    period = element.find(f"{{{XBRLI_NS}}}period")
    instant = period.findtext(f"{{{XBRLI_NS}}}instant") if period is not None else None
    period_start = period.findtext(f"{{{XBRLI_NS}}}startDate") if period is not None else None
    period_end = period.findtext(f"{{{XBRLI_NS}}}endDate") if period is not None else None
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
        "entity_identifier": (identifier.text or "").strip() if identifier is not None else "",
        "entity_scheme": identifier.get("scheme", "") if identifier is not None else "",
        "period_type": "instant" if instant else "duration" if period_start or period_end else "none",
        "instant_date": instant or "",
        "period_start": period_start or "",
        "period_end": period_end or "",
        "dimensions": json.dumps(dimensions, ensure_ascii=False),
        "mcy_member_code": _member_for(dimensions, "MCY") or "",
        "ref_member_code": ref_member or "",
        "is_comparative": ref_member is not None,
        "raw_xml": etree.tostring(element, encoding="unicode"),
        "parser_version": PARSER_VERSION,
        "parsed_at": parsed_at.isoformat(),
    }


def _unit_row(
    *,
    statement_key: str,
    element: etree._Element,
    parsed_at: datetime,
) -> dict[str, Any]:
    direct_measures = [
        _canonical_qname_text((measure.text or "").strip(), measure)
        for measure in element.findall(f"{{{XBRLI_NS}}}measure")
    ]
    numerator_measures = [
        _canonical_qname_text((measure.text or "").strip(), measure)
        for measure in element.findall(
            f"{{{XBRLI_NS}}}divide/{{{XBRLI_NS}}}unitNumerator/{{{XBRLI_NS}}}measure"
        )
    ]
    denominator_measures = [
        _canonical_qname_text((measure.text or "").strip(), measure)
        for measure in element.findall(
            f"{{{XBRLI_NS}}}divide/{{{XBRLI_NS}}}unitDenominator/{{{XBRLI_NS}}}measure"
        )
    ]
    return {
        "statement_key": statement_key,
        "unit_id": element.get("id", ""),
        "measures": json.dumps(direct_measures, ensure_ascii=False),
        "numerator_measures": json.dumps(numerator_measures, ensure_ascii=False),
        "denominator_measures": json.dumps(denominator_measures, ensure_ascii=False),
        "is_divide": bool(numerator_measures or denominator_measures),
        "raw_xml": etree.tostring(element, encoding="unicode"),
        "parser_version": PARSER_VERSION,
        "parsed_at": parsed_at.isoformat(),
    }


def _fact_row(
    *,
    statement_key: str,
    business_id: str,
    financial_date: str,
    ordinal: int,
    element: etree._Element,
    qname: etree.QName,
    prefix_by_namespace: dict[str, str],
    contexts_by_id: dict[str, dict[str, Any]],
    units_by_id: dict[str, dict[str, Any]],
    warnings: list[str],
    parsed_at: datetime,
) -> dict[str, Any]:
    raw_value = "".join(element.itertext()).strip()
    unit_id = element.get("unitRef") or ""
    is_nil = (element.get(f"{{{XSI_NS}}}nil") or "").lower() in {"1", "true"}
    numeric_value = ""
    date_value = ""
    text_value = ""
    if is_nil or not raw_value:
        value_kind = "empty"
    elif unit_id:
        parsed_decimal = _decimal_or_none(raw_value)
        if parsed_decimal is not None:
            value_kind = "numeric"
            numeric_value = str(parsed_decimal)
        else:
            value_kind = "text"
            text_value = raw_value
            warnings.append(f"fact value {raw_value!r} is not a finite decimal")
    else:
        if _date_or_none(raw_value) is not None:
            value_kind = "date"
            date_value = raw_value
        else:
            value_kind = "text"
            text_value = raw_value

    prefix = prefix_by_namespace.get(qname.namespace) or element.prefix
    concept_qname = f"{prefix}:{qname.localname}" if prefix else qname.localname
    context_id = element.get("contextRef", "")
    context = contexts_by_id.get(
        context_id,
        {"dimensions": "[]", "mcy_member_code": "", "ref_member_code": "", "is_comparative": False},
    )
    unit = units_by_id.get(unit_id)
    return {
        "statement_key": statement_key,
        "business_id": business_id,
        "financial_date": financial_date,
        "fact_ordinal": ordinal,
        "concept_qname": concept_qname,
        "concept_namespace": qname.namespace or "",
        "concept_local_name": qname.localname,
        "context_id": context_id,
        "unit_id": unit_id,
        "currency": _currency_for_unit(unit),
        "decimals": element.get("decimals") or "",
        "precision": element.get("precision") or "",
        "is_nil": is_nil,
        "xml_lang": element.get(f"{{{XML_NS}}}lang") or "",
        "value_kind": value_kind,
        "raw_value": raw_value,
        "numeric_value": numeric_value,
        "date_value": date_value,
        "text_value": text_value,
        "mcy_member_code": context["mcy_member_code"],
        "mcy_member_label_fi": "",
        "ref_member_code": context["ref_member_code"],
        "ref_member_label_fi": "",
        "is_comparative": context["is_comparative"],
        "dimensions": context["dimensions"],
        "parser_version": PARSER_VERSION,
        "parsed_at": parsed_at.isoformat(),
    }


def _currency_for_unit(unit: dict[str, Any] | None) -> str:
    if unit is None:
        return ""
    measures = json.loads(unit["measures"])
    if len(measures) != 1:
        return ""
    prefix, separator, currency = measures[0].partition(":")
    if separator and prefix.lower() == "iso4217":
        return currency.upper()
    return ""


def _decimal_or_none(raw_value: str) -> Decimal | None:
    try:
        value = Decimal(raw_value)
    except InvalidOperation:
        return None
    if not value.is_finite():
        return None
    return value


def _date_or_none(value: str) -> date | None:
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _canonical_qname_text(value: str, element: etree._Element) -> str:
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
