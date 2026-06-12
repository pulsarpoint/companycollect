from datetime import date, datetime, timezone
from decimal import Decimal

from dagster_corpscout.sources.finland.prh_xbrl import tables
from dagster_corpscout.sources.finland.prh_xbrl.parser import parse_statement_xml

SAMPLE_XML = b"""<xbrl xmlns="http://www.xbrl.org/2003/instance"
    xmlns:link="http://www.xbrl.org/2003/linkbase"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
    xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
    xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
    xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC"
    xmlns:fi_RF="http://www.suomi.fi/xbrl/crr/dict/dom/RF"
    xmlns:iso4217="http://www.xbrl.org/2003/iso4217">
  <link:schemaRef xlink:href="http://www.valtiokonttori.fi/fi/fr/xbrl/crr/fws/oytp/kpl-2016-12/2019-03-28/mod/oytp_gaap_ind.xsd" xlink:type="simple"/>
  <context id="ctx_base">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2023-09-30</instant></period>
  </context>
  <context id="ctx_mcy">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2023-09-30</instant></period>
    <scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
    </scenario>
  </context>
  <context id="ctx_prev">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2022-09-30</instant></period>
    <scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
      <xbrldi:explicitMember dimension="fi_dim:REF">fi_RF:x4</xbrldi:explicitMember>
    </scenario>
  </context>
  <unit id="EUR"><measure>iso4217:EUR</measure></unit>
  <fi_met:si289 contextRef="ctx_base">0176460-0</fi_met:si289>
  <fi_met:si168 contextRef="ctx_base">Testi Oy</fi_met:si168>
  <fi_met:di120 contextRef="ctx_base">2022-10-01</fi_met:di120>
  <fi_met:di121 contextRef="ctx_base">2023-09-30</fi_met:di121>
  <fi_met:md103 contextRef="ctx_mcy" unitRef="EUR" decimals="0">125000</fi_met:md103>
  <fi_met:md103 contextRef="ctx_prev" unitRef="EUR" decimals="0">110000</fi_met:md103>
</xbrl>"""

PARSED_AT = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)


def _parse(business_id: str = "0176460-0"):
    return parse_statement_xml(
        business_id=business_id,
        financial_date="2023-09-30",
        registration_date="2025-01-23",
        source_url="https://example.test/financial?businessId=0176460-0&financialDate=2023-09-30",
        xml_object_key="companies/0176460-0/2023-09-30.xml",
        source_run_id="dagster-run-1",
        body=SAMPLE_XML,
        parsed_at=PARSED_AT,
    )


def test_document_row_has_identity_counts_and_reported_metadata():
    parsed = _parse()
    [document] = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE]

    assert len(parsed.statement_key) == 64
    assert document["statement_key"] == parsed.statement_key
    assert document["business_id"] == "0176460-0"
    assert document["financial_date"] == date(2023, 9, 30)
    assert document["registration_date"] == date(2025, 1, 23)
    assert document["root_name"] == "xbrl"
    assert document["taxonomy_entrypoint"].endswith("oytp_gaap_ind.xsd")
    assert document["reported_business_id"] == "0176460-0"
    assert document["reported_company_name"] == "Testi Oy"
    assert document["reported_period_start"] == date(2022, 10, 1)
    assert document["reported_period_end"] == date(2023, 9, 30)
    assert document["contexts_count"] == 3
    assert document["units_count"] == 1
    assert document["facts_count"] == 6
    assert document["validation_warnings"] == []
    assert document["parser_version"] == "1.0.0"


def test_contexts_capture_dimensions_and_comparative_flag():
    parsed = _parse()
    contexts = {row["context_id"]: row for row in parsed.rows_by_table[tables.CONTEXTS_TABLE]}

    assert contexts["ctx_base"]["period_type"] == "instant"
    assert contexts["ctx_base"]["instant_date"] == date(2023, 9, 30)
    assert contexts["ctx_base"]["entity_scheme"] == "http://ytj.fi"
    assert contexts["ctx_mcy"]["mcy_member_code"] == "fi_MC:x673"
    assert contexts["ctx_mcy"]["is_comparative"] == 0
    assert contexts["ctx_prev"]["ref_member_code"] == "fi_RF:x4"
    assert contexts["ctx_prev"]["is_comparative"] == 1
    assert ("fi_dim:MCY", "fi_MC:x673", "") in contexts["ctx_prev"]["dimensions"]


def test_facts_are_typed_and_denormalized_with_context_members():
    parsed = _parse()
    facts = parsed.rows_by_table[tables.FACTS_TABLE]
    by_concept = {}
    for fact in facts:
        by_concept.setdefault(fact["concept_qname"], []).append(fact)

    revenue_current = next(
        f for f in by_concept["fi_met:md103"] if f["context_id"] == "ctx_mcy"
    )
    assert revenue_current["value_kind"] == "numeric"
    assert revenue_current["numeric_value"] == Decimal("125000")
    assert revenue_current["mcy_member_code"] == "fi_MC:x673"
    assert revenue_current["unit_id"] == "EUR"
    assert revenue_current["decimals"] == "0"

    business_id_fact = by_concept["fi_met:si289"][0]
    assert business_id_fact["value_kind"] == "text"
    assert business_id_fact["text_value"] == "0176460-0"

    period_start_fact = by_concept["fi_met:di120"][0]
    assert period_start_fact["value_kind"] == "date"
    assert period_start_fact["date_value"] == date(2022, 10, 1)

    ordinals = [fact["fact_ordinal"] for fact in facts]
    assert ordinals == sorted(ordinals) and len(set(ordinals)) == len(ordinals)


def test_reported_business_id_mismatch_produces_warning_not_failure():
    parsed = _parse(business_id="9999999-9")
    [document] = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE]

    assert any("reported business id" in warning for warning in parsed.warnings)
    assert document["facts_count"] == 6


def test_row_keys_match_table_contract_for_every_table():
    parsed = _parse()
    for table, rows in parsed.rows_by_table.items():
        for row in rows:
            assert set(row.keys()) == set(tables.TABLE_COLUMNS[table]), table


def test_non_representable_numeric_values_degrade_to_text_with_warning():
    xml = b"""<xbrl xmlns="http://www.xbrl.org/2003/instance"
        xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met">
      <context id="c1">
        <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
        <period><instant>2023-09-30</instant></period>
      </context>
      <unit id="EUR"><measure>iso4217:EUR</measure></unit>
      <fi_met:md103 contextRef="c1" unitRef="EUR">NaN</fi_met:md103>
      <fi_met:md103 contextRef="c1" unitRef="EUR">1.5E+45</fi_met:md103>
      <fi_met:md103 contextRef="c1" unitRef="EUR">0.123456789</fi_met:md103>
      <fi_met:md103 contextRef="c1" unitRef="EUR">125000.50</fi_met:md103>
    </xbrl>"""

    parsed = parse_statement_xml(
        business_id="0176460-0",
        financial_date="2023-09-30",
        registration_date=None,
        source_url="test",
        xml_object_key="test",
        source_run_id="test",
        body=xml,
        parsed_at=PARSED_AT,
    )

    facts = parsed.rows_by_table[tables.FACTS_TABLE]
    kinds = [fact["value_kind"] for fact in facts]
    assert kinds == ["text", "text", "text", "numeric"]
    assert facts[3]["numeric_value"] == Decimal("125000.50")
    assert sum("not representable" in w for w in parsed.warnings) == 3


def test_concept_qname_is_canonical_even_with_nonstandard_prefixes():
    xml = b"""<xbrl xmlns="http://www.xbrl.org/2003/instance"
        xmlns:met="http://www.suomi.fi/xbrl/crr/dict/met"
        xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
        xmlns:d="http://www.suomi.fi/xbrl/crr/dict/dim"
        xmlns:mc="http://www.suomi.fi/xbrl/crr/dict/dom/MC">
      <context id="c1">
        <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
        <period><instant>2023-09-30</instant></period>
        <scenario>
          <xbrldi:explicitMember dimension="d:MCY">mc:x673</xbrldi:explicitMember>
        </scenario>
      </context>
      <unit id="EUR"><measure>iso4217:EUR</measure></unit>
      <met:md103 contextRef="c1" unitRef="EUR">125000</met:md103>
    </xbrl>"""

    parsed = parse_statement_xml(
        business_id="0176460-0",
        financial_date="2023-09-30",
        registration_date=None,
        source_url="test",
        xml_object_key="test",
        source_run_id="test",
        body=xml,
        parsed_at=PARSED_AT,
    )

    [fact] = parsed.rows_by_table[tables.FACTS_TABLE]
    assert fact["concept_qname"] == "fi_met:md103"
    assert fact["mcy_member_code"] == "fi_MC:x673"
    [context] = parsed.rows_by_table[tables.CONTEXTS_TABLE]
    assert ("fi_dim:MCY", "fi_MC:x673", "") in context["dimensions"]
