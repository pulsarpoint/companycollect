"""Finland adapter: unified extractor behind the legacy StatementParser signature."""

import hashlib
import json
from datetime import datetime

from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.parser import (
    CANONICAL_PREFIXES,
    ParsedStatement,
    statement_key_for,
)
from dagster_v3.defs.xbrl_common.extractor import SourceProfile, extract_filing
from dagster_v3.defs.xbrl_common.tables import XbrlRowContract

FINLAND_PROFILE = SourceProfile(
    source_slug="finland_prh",
    canonical_prefixes=dict(CANONICAL_PREFIXES),
    reported_concepts={
        "fi_met:si289": "reported_entity_id",
        "fi_met:si168": "reported_company_name",
        "fi_met:di120": "reported_period_start",
        "fi_met:di121": "reported_period_end",
    },
)

FINLAND_UNIFIED_CONTRACT = XbrlRowContract.build(
    document_identity=(
        "statement_key",
        "source_run_id",
        "business_id",
        "financial_date",
        "registration_date",
        "source_url",
        "xml_object_key",
    ),
    row_identity=("statement_key",),
    fact_identity=("statement_key", "business_id", "financial_date"),
    context_extras=("mcy_member_code", "ref_member_code"),
    fact_extras=("mcy_member_code", "ref_member_code"),
)


def _member_for(dimensions_json: str, suffix: str) -> str:
    for dimension_qname, member_qname, _typed in json.loads(dimensions_json):
        if dimension_qname == suffix or dimension_qname.endswith(f":{suffix}"):
            return member_qname
    return ""


def parse_statement_xml_unified(
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
    filing = extract_filing(body, profile=FINLAND_PROFILE, parsed_at=parsed_at)
    xml_sha256 = hashlib.sha256(body).hexdigest()
    statement_key = statement_key_for(
        business_id, financial_date, registration_date or "", xml_sha256
    )

    if not filing.document["reported_period_end"]:
        # Legacy behavior: fall back to the requested financial_date for
        # comparative flagging when the filing does not report a period end.
        reported_end = financial_date
        by_id: dict[str, bool] = {}
        for context in filing.contexts:
            effective = context["instant_date"] or context["period_end"]
            context["is_comparative"] = bool(effective and effective != reported_end)
            by_id[context["context_id"]] = context["is_comparative"]
        for fact in filing.facts:
            fact["is_comparative"] = by_id.get(fact["context_id"], False)

    document = {
        "statement_key": statement_key,
        "source_run_id": source_run_id,
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date or "",
        "source_url": source_url,
        "xml_object_key": xml_object_key,
        **filing.document,
        "xml_sha256": xml_sha256,
    }
    document = {name: document[name] for name in FINLAND_UNIFIED_CONTRACT.documents.columns}

    contexts = []
    for row in filing.contexts:
        contexts.append(
            {
                "statement_key": statement_key,
                **row,
                "mcy_member_code": _member_for(row["dimensions"], "MCY"),
                "ref_member_code": _member_for(row["dimensions"], "REF"),
            }
        )
    units = [{"statement_key": statement_key, **row} for row in filing.units]
    facts = []
    for row in filing.facts:
        facts.append(
            {
                "statement_key": statement_key,
                "business_id": business_id,
                "financial_date": financial_date,
                **row,
                "mcy_member_code": _member_for(row["dimensions"], "MCY"),
                "ref_member_code": _member_for(row["dimensions"], "REF"),
            }
        )

    return ParsedStatement(
        statement_key=statement_key,
        rows_by_table={
            tables.STATEMENT_DOCUMENTS_TABLE: [document],
            tables.CONTEXTS_TABLE: contexts,
            tables.UNITS_TABLE: units,
            tables.FACTS_TABLE: facts,
        },
        warnings=list(filing.warnings),
    )
