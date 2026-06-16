from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from lxml import etree

XBRLI_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
PARSER_VERSION = "arelle-1.0.0"
XML_PARSER = etree.XMLParser(resolve_entities=False)

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


@dataclass(frozen=True)
class ArelleParsedStatement:
    statement_document: dict[str, Any]
    facts: list[dict[str, Any]]
    warnings: list[str]


ArelleStatementParser = Callable[..., ArelleParsedStatement]


def parse_statement_xml_with_arelle(
    *,
    business_id: str,
    financial_date: str,
    registration_date: str | None,
    source_url: str,
    xml_object_key: str,
    source_run_id: str,
    body: bytes,
    parsed_at: datetime,
) -> ArelleParsedStatement:
    root = etree.fromstring(body, parser=XML_PARSER)
    xml_sha256 = hashlib.sha256(body).hexdigest()
    statement_key = statement_key_for(business_id, financial_date, xml_sha256)
    schema_refs = [
        element.get(f"{{{XLINK_NS}}}href") or ""
        for element in root.findall(f"{{{LINK_NS}}}schemaRef")
    ]
    prefix_by_namespace = {
        namespace: prefix for prefix, namespace in (root.nsmap or {}).items() if prefix
    }
    prefix_by_namespace.update(CANONICAL_PREFIXES)

    model_xbrl, close_model = _load_arelle_model(body)
    warnings: list[str] = []
    try:
        fact_rows, reported = _fact_rows_from_model(
            model_xbrl=model_xbrl,
            statement_key=statement_key,
            business_id=business_id,
            financial_date=financial_date,
            prefix_by_namespace=prefix_by_namespace,
            parsed_at=parsed_at,
        )
        if not fact_rows:
            return _fallback_instance_parse(
                business_id=business_id,
                financial_date=financial_date,
                registration_date=registration_date,
                source_url=source_url,
                xml_object_key=xml_object_key,
                source_run_id=source_run_id,
                body=body,
                parsed_at=parsed_at,
            )

        reported_business_id = reported.get("reported_business_id")
        if reported_business_id and reported_business_id != business_id:
            warnings.append(
                f"reported business id {reported_business_id!r} does not match requested {business_id!r}"
            )

        statement_document = {
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
            "contexts_count": len(getattr(model_xbrl, "contexts", {}) or {}),
            "units_count": len(getattr(model_xbrl, "units", {}) or {}),
            "facts_count": len(fact_rows),
            "validation_warnings": json.dumps(warnings, ensure_ascii=False),
            "parser_version": PARSER_VERSION,
            "parsed_at": parsed_at.isoformat(),
        }
        return ArelleParsedStatement(
            statement_document=statement_document,
            facts=fact_rows,
            warnings=warnings,
        )
    finally:
        close_model()


def statement_key_for(business_id: str, financial_date: str, xml_sha256: str) -> str:
    return hashlib.sha256(f"{business_id}:{financial_date}:{xml_sha256}".encode()).hexdigest()


def _fallback_instance_parse(
    *,
    business_id: str,
    financial_date: str,
    registration_date: str | None,
    source_url: str,
    xml_object_key: str,
    source_run_id: str,
    body: bytes,
    parsed_at: datetime,
) -> ArelleParsedStatement:
    from dagster_v3.defs.finland_xbrl import tables
    from dagster_v3.defs.finland_xbrl.parser import parse_statement_xml

    parsed = parse_statement_xml(
        business_id=business_id,
        financial_date=financial_date,
        registration_date=registration_date,
        source_url=source_url,
        xml_object_key=xml_object_key,
        source_run_id=source_run_id,
        body=body,
        parsed_at=parsed_at,
    )
    parser_version = f"{PARSER_VERSION}+lxml-fallback"
    warnings = ["Arelle returned no facts", *parsed.warnings]
    statement_document = dict(parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE][0])
    statement_document["validation_warnings"] = json.dumps(warnings, ensure_ascii=False)
    statement_document["parser_version"] = parser_version
    facts = [dict(row) for row in parsed.rows_by_table[tables.FACTS_TABLE]]
    for fact in facts:
        fact["parser_version"] = parser_version
    return ArelleParsedStatement(
        statement_document=statement_document,
        facts=facts,
        warnings=warnings,
    )


def _load_arelle_model(body: bytes) -> tuple[Any, Callable[[], None]]:
    from arelle import Cntlr

    temp_directory = tempfile.TemporaryDirectory()
    xml_path = Path(temp_directory.name) / "statement.xml"
    xml_path.write_bytes(body)
    cntlr = Cntlr.Cntlr(logFileName=os.devnull, disable_persistent_config=True)
    model_xbrl = cntlr.modelManager.load(str(xml_path))

    def close_model() -> None:
        if model_xbrl is not None:
            model_xbrl.close()
        cntlr.close()
        temp_directory.cleanup()

    if model_xbrl is None:
        close_model()
        raise ValueError("Arelle could not load XBRL model")
    return model_xbrl, close_model


def _fact_rows_from_model(
    *,
    model_xbrl: Any,
    statement_key: str,
    business_id: str,
    financial_date: str,
    prefix_by_namespace: dict[str, str],
    parsed_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    rows: list[dict[str, Any]] = []
    reported: dict[str, str] = {}
    for fact in getattr(model_xbrl, "facts", []) or []:
        concept = getattr(fact, "concept", None)
        if concept is None:
            continue
        qname = getattr(concept, "qname", None)
        namespace = str(getattr(qname, "namespaceURI", "") or "")
        if namespace == XBRLI_NS:
            continue
        local_name = str(getattr(qname, "localName", "") or "")
        row = _fact_row(
            fact=fact,
            statement_key=statement_key,
            business_id=business_id,
            financial_date=financial_date,
            ordinal=len(rows) + 1,
            namespace=namespace,
            local_name=local_name,
            prefix_by_namespace=prefix_by_namespace,
            parsed_at=parsed_at,
        )
        rows.append(row)
        if local_name in REPORTED_CONCEPTS:
            reported[REPORTED_CONCEPTS[local_name]] = row["raw_value"]
    return rows, reported


def _fact_row(
    *,
    fact: Any,
    statement_key: str,
    business_id: str,
    financial_date: str,
    ordinal: int,
    namespace: str,
    local_name: str,
    prefix_by_namespace: dict[str, str],
    parsed_at: datetime,
) -> dict[str, Any]:
    raw_value = str(getattr(fact, "value", "") or "").strip()
    unit_id = str(getattr(fact, "unitID", "") or "")
    numeric_value = ""
    date_value = ""
    text_value = ""
    if not raw_value:
        value_kind = "empty"
    elif unit_id:
        parsed_decimal = _decimal_or_none(raw_value)
        if parsed_decimal is None:
            value_kind = "text"
            text_value = raw_value
        else:
            value_kind = "numeric"
            numeric_value = str(parsed_decimal)
    elif _date_or_none(raw_value) is not None:
        value_kind = "date"
        date_value = raw_value
    else:
        value_kind = "text"
        text_value = raw_value

    context = getattr(fact, "context", None)
    dimensions = _context_dimensions(context)
    ref_member = _member_for(dimensions, "REF")
    prefix = prefix_by_namespace.get(namespace) or str(
        getattr(getattr(fact, "qname", None), "prefix", "") or ""
    )
    concept_qname = f"{prefix}:{local_name}" if prefix else local_name

    return {
        "statement_key": statement_key,
        "business_id": business_id,
        "financial_date": financial_date,
        "fact_ordinal": ordinal,
        "concept_qname": concept_qname,
        "concept_namespace": namespace,
        "concept_local_name": local_name,
        "context_id": str(getattr(fact, "contextID", "") or ""),
        "unit_id": unit_id,
        "decimals": str(getattr(fact, "decimals", "") or ""),
        "precision": str(getattr(fact, "precision", "") or ""),
        "value_kind": value_kind,
        "raw_value": raw_value,
        "numeric_value": numeric_value,
        "date_value": date_value,
        "text_value": text_value,
        "mcy_member_code": _member_for(dimensions, "MCY") or "",
        "mcy_member_label_fi": "",
        "ref_member_code": ref_member or "",
        "ref_member_label_fi": "",
        "is_comparative": ref_member is not None,
        "dimensions": json.dumps(dimensions, ensure_ascii=False),
        "parser_version": PARSER_VERSION,
        "parsed_at": parsed_at.isoformat(),
    }


def _context_dimensions(context: Any) -> list[tuple[str, str, str]]:
    if context is None:
        return []
    dimensions: list[tuple[str, str, str]] = []
    for dimension_qname, dimension_value in (getattr(context, "qnameDims", {}) or {}).items():
        member_qname = getattr(dimension_value, "memberQname", None)
        dimensions.append(
            (
                _canonical_qname_text(dimension_qname),
                _canonical_qname_text(member_qname),
                "",
            )
        )
    return dimensions


def _canonical_qname_text(value: Any) -> str:
    namespace = str(getattr(value, "namespaceURI", "") or "")
    local_name = str(getattr(value, "localName", "") or "")
    if local_name:
        prefix = CANONICAL_PREFIXES.get(namespace) or str(getattr(value, "prefix", "") or "")
        return f"{prefix}:{local_name}" if prefix else local_name
    return str(value or "")


def _member_for(dimensions: list[tuple[str, str, str]], suffix: str) -> str | None:
    for dimension_code, member_code, _label in dimensions:
        if dimension_code == suffix or dimension_code.endswith(f":{suffix}"):
            return member_code
    return None


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
