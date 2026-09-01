import json
from datetime import UTC, datetime

from dagster_v3.defs.xbrl_common.extractor import SourceProfile, extract_filing

PARSED_AT = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)

PROFILE = SourceProfile(
    source_slug="testland",
    canonical_prefixes={
        "http://example.org/met": "t_met",
        "http://example.org/dim": "t_dim",
        "http://example.org/dom": "t_dom",
        "http://www.xbrl.org/2003/iso4217": "iso4217",
    },
    reported_concepts={
        "t_met:entityId": "reported_entity_id",
        "t_met:companyName": "reported_company_name",
        "t_met:periodStart": "reported_period_start",
        "t_met:periodEnd": "reported_period_end",
    },
)

PLAIN_XBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:link="http://www.xbrl.org/2003/linkbase"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:met="http://example.org/met"
      xmlns:dim="http://example.org/dim"
      xmlns:dom="http://example.org/dom"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <link:schemaRef xlink:type="simple" xlink:href="http://example.org/entry.xsd"/>
  <xbrli:context id="cur">
    <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="prior">
    <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2023-01-01</xbrli:startDate><xbrli:endDate>2023-12-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cur_dim">
    <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2024-12-31</xbrli:instant></xbrli:period>
    <xbrli:scenario>
      <xbrldi:explicitMember dimension="dim:Segment">dom:Retail</xbrldi:explicitMember>
    </xbrli:scenario>
  </xbrli:context>
  <xbrli:unit id="eur"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
  <met:entityId contextRef="cur">1234567-8</met:entityId>
  <met:companyName contextRef="cur">Testland Oy</met:companyName>
  <met:periodEnd contextRef="cur">2024-12-31</met:periodEnd>
  <met:revenue contextRef="cur" unitRef="eur" decimals="0">1000000</met:revenue>
  <met:revenue contextRef="prior" unitRef="eur" decimals="0">900000</met:revenue>
  <met:assets contextRef="cur_dim" unitRef="eur" decimals="0">555</met:assets>
  <met:note contextRef="cur" xsi:nil="true"/>
</xbrl>
"""


def test_plain_xbrl_document_fields():
    filing = extract_filing(PLAIN_XBRL, profile=PROFILE, parsed_at=PARSED_AT)
    doc = filing.document
    assert doc["root_name"] == "xbrl"
    assert json.loads(doc["schema_refs"]) == ["http://example.org/entry.xsd"]
    assert doc["taxonomy_entrypoint"] == "http://example.org/entry.xsd"
    assert doc["reported_entity_id"] == "1234567-8"
    assert doc["reported_company_name"] == "Testland Oy"
    assert doc["reported_period_end"] == "2024-12-31"
    assert doc["contexts_count"] == 3
    assert doc["units_count"] == 1
    assert doc["facts_count"] == 7
    assert doc["parser_version"] == "xbrl-common-1.0.0"


def test_plain_xbrl_contexts_and_comparative():
    filing = extract_filing(PLAIN_XBRL, profile=PROFILE, parsed_at=PARSED_AT)
    by_id = {c["context_id"]: c for c in filing.contexts}
    assert by_id["cur"]["period_type"] == "duration"
    assert by_id["cur"]["is_comparative"] is False
    assert by_id["prior"]["is_comparative"] is True
    assert by_id["cur_dim"]["period_type"] == "instant"
    assert by_id["cur_dim"]["is_comparative"] is False
    assert json.loads(by_id["cur_dim"]["dimensions"]) == [["t_dim:Segment", "t_dom:Retail", ""]]


def test_plain_xbrl_facts():
    filing = extract_filing(PLAIN_XBRL, profile=PROFILE, parsed_at=PARSED_AT)
    revenue = [f for f in filing.facts if f["concept_qname"] == "t_met:revenue"]
    assert len(revenue) == 2
    current = next(f for f in revenue if f["context_id"] == "cur")
    assert current["value_kind"] == "numeric"
    assert current["numeric_value"] == "1000000"
    assert current["currency"] == "EUR"
    assert current["is_comparative"] is False
    prior = next(f for f in revenue if f["context_id"] == "prior")
    assert prior["is_comparative"] is True
    nil = next(f for f in filing.facts if f["concept_qname"] == "t_met:note")
    assert nil["is_nil"] is True and nil["value_kind"] == "empty"
    period_end = next(f for f in filing.facts if f["concept_qname"] == "t_met:periodEnd")
    assert period_end["value_kind"] == "date"
    assert period_end["date_value"] == "2024-12-31"
    assert [f["fact_ordinal"] for f in filing.facts] == list(range(1, 8))


def test_malformed_body_yields_empty_filing_with_warning():
    filing = extract_filing(b"this is not xml at all \x00", profile=PROFILE, parsed_at=PARSED_AT)
    assert filing.facts == []
    assert filing.warnings  # at least one warning explains the failure


IXBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2015-02-26"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:link="http://www.xbrl.org/2003/linkbase"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:met="http://example.org/met">
<head><title>t</title></head>
<body>
<div style="display:none">
  <ix:header>
    <ix:hidden>
      <ix:nonNumeric name="met:entityId" contextRef="cur">1234567-8</ix:nonNumeric>
      <ix:nonNumeric name="met:periodEnd" contextRef="cur" format="ixt:date-day-month-year">31.12.2024</ix:nonNumeric>
    </ix:hidden>
    <ix:resources>
      <xbrli:context id="cur">
        <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
      </xbrli:context>
      <xbrli:context id="prior">
        <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:startDate>2023-01-01</xbrli:startDate><xbrli:endDate>2023-12-31</xbrli:endDate></xbrli:period>
      </xbrli:context>
      <xbrli:unit id="eur"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
    </ix:resources>
    <ix:references>
      <link:schemaRef xlink:type="simple" xlink:href="http://example.org/entry.xsd"/>
    </ix:references>
  </ix:header>
</div>
<p>Revenue was
  <ix:nonFraction name="met:revenue" contextRef="cur" unitRef="eur"
                  decimals="0" format="ixt:num-comma-decimal" scale="3">1.234,5</ix:nonFraction>
 thousand euro.</p>
<p>Loss:
  <ix:nonFraction name="met:profit" contextRef="cur" unitRef="eur"
                  decimals="0" sign="-" format="ixt:num-dot-decimal">2,500</ix:nonFraction></p>
<p><ix:nonNumeric name="met:description" contextRef="cur" continuedAt="c1">Part one
  <ix:exclude><span>IGNORED</span></ix:exclude></ix:nonNumeric></p>
<p><ix:continuation id="c1"> and part two.</ix:continuation></p>
<p><ix:nonFraction name="met:weird" contextRef="cur" unitRef="eur"
                   format="ixt:num-tolkien">999</ix:nonFraction></p>
</body>
</html>
"""


def test_ixbrl_transform_scale_and_sign():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    revenue = next(f for f in filing.facts if f["concept_qname"] == "t_met:revenue")
    assert revenue["value_kind"] == "numeric"
    assert revenue["numeric_value"] == "1234500.0"
    assert revenue["currency"] == "EUR"
    profit = next(f for f in filing.facts if f["concept_qname"] == "t_met:profit")
    assert profit["numeric_value"] == "-2500"


def test_ixbrl_hidden_and_reported_and_schema_refs():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    assert filing.document["reported_entity_id"] == "1234567-8"
    assert filing.document["reported_period_end"] == "2024-12-31"
    assert filing.document["taxonomy_entrypoint"] == "http://example.org/entry.xsd"
    period_end = next(f for f in filing.facts if f["concept_qname"] == "t_met:periodEnd")
    assert period_end["value_kind"] == "date"
    assert period_end["date_value"] == "2024-12-31"
    assert period_end["raw_value"] == "31.12.2024"


def test_ixbrl_continuation_and_exclude():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    description = next(f for f in filing.facts if f["concept_qname"] == "t_met:description")
    assert "IGNORED" not in description["text_value"]
    assert "Part one" in description["text_value"]
    assert "and part two." in description["text_value"]


def test_ixbrl_unknown_transform_degrades_to_text_with_warning():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    weird = next(f for f in filing.facts if f["concept_qname"] == "t_met:weird")
    assert weird["value_kind"] == "text"
    assert weird["text_value"] == "999"
    assert any("num-tolkien" in w for w in filing.warnings)


def test_ixbrl_contexts_parsed_from_resources():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    assert {c["context_id"] for c in filing.contexts} == {"cur", "prior"}
    prior = next(c for c in filing.contexts if c["context_id"] == "prior")
    assert prior["is_comparative"] is True
