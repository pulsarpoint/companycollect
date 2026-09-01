import json
from datetime import UTC, datetime

from dagster_v3.defs.finland_xbrl import parser as legacy_parser
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.unified_adapter import (
    FINLAND_UNIFIED_CONTRACT,
    parse_statement_xml_unified,
)

PARSED_AT = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)

FINLAND_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
      xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
      xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC">
  <xbrli:context id="cur">
    <xbrli:entity><xbrli:identifier scheme="http://www.prh.fi">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cur_mc">
    <xbrli:entity><xbrli:identifier scheme="http://www.prh.fi">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
    <xbrli:scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
    </xbrli:scenario>
  </xbrli:context>
  <xbrli:unit id="eur"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
  <fi_met:si289 contextRef="cur">1234567-8</fi_met:si289>
  <fi_met:si168 contextRef="cur">Testi Oy</fi_met:si168>
  <fi_met:di121 contextRef="cur">2024-12-31</fi_met:di121>
  <fi_met:md103 contextRef="cur_mc" unitRef="eur" decimals="0">500000</fi_met:md103>
</xbrl>
"""

KWARGS = dict(
    business_id="1234567-8",
    financial_date="2024-12-31",
    registration_date="2025-04-01",
    source_url="https://example.fi/statement",
    xml_object_key="finland/xml/1234567-8.xml",
    source_run_id="run-1",
    body=FINLAND_XML,
    parsed_at=PARSED_AT,
)


def test_unified_rows_have_contract_columns():
    parsed = parse_statement_xml_unified(**KWARGS)
    doc = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE][0]
    assert list(doc) == FINLAND_UNIFIED_CONTRACT.documents.columns
    fact = parsed.rows_by_table[tables.FACTS_TABLE][0]
    assert list(fact) == FINLAND_UNIFIED_CONTRACT.facts.columns


def test_unified_document_identity_and_reported():
    parsed = parse_statement_xml_unified(**KWARGS)
    doc = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE][0]
    assert doc["business_id"] == "1234567-8"
    assert doc["reported_entity_id"] == "1234567-8"
    assert doc["reported_company_name"] == "Testi Oy"
    assert doc["reported_period_end"] == "2024-12-31"
    assert doc["statement_key"] == legacy_parser.statement_key_for(
        "1234567-8", "2024-12-31", "2025-04-01", doc["xml_sha256"]
    )


def test_unified_mcy_member_derived_from_dimensions():
    parsed = parse_statement_xml_unified(**KWARGS)
    revenue = next(
        f for f in parsed.rows_by_table[tables.FACTS_TABLE]
        if f["concept_qname"] == "fi_met:md103"
    )
    assert revenue["mcy_member_code"] == "fi_MC:x673"
    assert revenue["value_kind"] == "numeric"
    assert revenue["numeric_value"] == "500000"
    assert revenue["currency"] == "EUR"


def test_mini_parity_against_legacy_parser():
    """Same input through both parsers: same fact count and identical
    (concept_qname, context_id, numeric_value) triples for numeric facts."""
    unified = parse_statement_xml_unified(**KWARGS)
    legacy = legacy_parser.parse_statement_xml(**KWARGS)

    def triples(rows):
        return sorted(
            (r["concept_qname"], r["context_id"], r["numeric_value"])
            for r in rows
            if r["value_kind"] == "numeric"
        )

    unified_facts = unified.rows_by_table[tables.FACTS_TABLE]
    legacy_facts = legacy.rows_by_table[tables.FACTS_TABLE]
    assert len(unified_facts) == len(legacy_facts)
    assert triples(unified_facts) == triples(legacy_facts)
