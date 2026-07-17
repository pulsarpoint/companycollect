from datetime import UTC, datetime
from pathlib import Path

import dagster as dg

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.financial_metrics import (
    build_finland_financial_metrics_insert_sql,
)
from dagster_v3.defs.finland_xbrl.parser import parse_statement_xml, statement_key_for
from dagster_v3.defs.finland_xbrl.taxonomy import read_taxonomy_catalog


SAMPLE_XML = b"""<xbrl xmlns="http://www.xbrl.org/2003/instance"
    xmlns:link="http://www.xbrl.org/2003/linkbase"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
    xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
    xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
    xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC"
    xmlns:iso4217="http://www.xbrl.org/2003/iso4217">
  <link:schemaRef xlink:href="https://taxonomy.test/oytp_gaap_ind.xsd" xlink:type="simple"/>
  <context id="current-revenue">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2025-12-31</instant></period>
    <scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
    </scenario>
  </context>
  <context id="previous-revenue">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2024-12-31</instant></period>
    <scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
    </scenario>
  </context>
  <context id="metadata">
    <entity><identifier scheme="http://ytj.fi">0176460-0</identifier></entity>
    <period><instant>2025-12-31</instant></period>
  </context>
  <unit id="ISO4217_EUR"><measure>iso4217:EUR</measure></unit>
  <fi_met:si289 contextRef="metadata">0176460-0</fi_met:si289>
  <fi_met:si168 contextRef="metadata" xml:lang="fi">Testi Oy</fi_met:si168>
  <fi_met:di120 contextRef="metadata">2025-01-01</fi_met:di120>
  <fi_met:di121 contextRef="metadata">2025-12-31</fi_met:di121>
  <fi_met:md103 contextRef="current-revenue" unitRef="ISO4217_EUR" decimals="2">125000.25</fi_met:md103>
  <fi_met:md103 contextRef="previous-revenue" unitRef="ISO4217_EUR" decimals="2">110000.10</fi_met:md103>
</xbrl>"""


def test_parser_preserves_contexts_units_and_complete_fact_provenance() -> None:
    parsed = parse_statement_xml(
        business_id="0176460-0",
        financial_date="2025-12-31",
        registration_date="2026-05-20",
        source_url="https://avoindata.prh.fi/financial",
        xml_object_key="financial_data/xml_snapshot/window/companies/0176460-0/2025-12-31.xml",
        source_run_id="test-run",
        body=SAMPLE_XML,
        parsed_at=datetime(2026, 7, 17, tzinfo=UTC),
    )

    assert set(parsed.rows_by_table) == {
        tables.STATEMENT_DOCUMENTS_TABLE,
        tables.CONTEXTS_TABLE,
        tables.UNITS_TABLE,
        tables.FACTS_TABLE,
    }

    contexts = {
        row["context_id"]: row for row in parsed.rows_by_table[tables.CONTEXTS_TABLE]
    }
    assert contexts["current-revenue"]["entity_identifier"] == "0176460-0"
    assert contexts["current-revenue"]["instant_date"] == "2025-12-31"
    assert contexts["current-revenue"]["is_comparative"] is False
    assert contexts["previous-revenue"]["is_comparative"] is True

    unit = parsed.rows_by_table[tables.UNITS_TABLE][0]
    assert unit["unit_id"] == "ISO4217_EUR"
    assert unit["measures"] == '["iso4217:EUR"]'

    revenue_facts = [
        row
        for row in parsed.rows_by_table[tables.FACTS_TABLE]
        if row["concept_qname"] == "fi_met:md103"
    ]
    assert revenue_facts[0]["currency"] == "EUR"
    assert revenue_facts[0]["numeric_value"] == "125000.25"
    assert revenue_facts[0]["is_comparative"] is False
    assert revenue_facts[1]["is_comparative"] is True
    assert revenue_facts[0]["xml_lang"] == ""


def test_statement_key_preserves_separate_registration_events() -> None:
    first = statement_key_for(
        "0176460-0", "2025-12-31", "2026-05-20", "a" * 64
    )
    second = statement_key_for(
        "0176460-0", "2025-12-31", "2026-05-21", "a" * 64
    )

    assert first != second


def test_taxonomy_catalog_explains_generic_finland_fact_codes() -> None:
    rows = {row.code: row for row in read_taxonomy_catalog()}

    assert rows["fi_MC:x673"].label_fi == "Liikevaihto"
    assert rows["fi_MC:x360"].label_fi
    assert rows["fi_met:ii52"].label_fi == "Henkilöstön lukumäärä"
    assert all(row.source_url.startswith("https://") for row in rows.values())


def test_finland_metrics_sql_selects_current_context_and_declared_precision() -> None:
    sql = build_finland_financial_metrics_insert_sql("corpscout.metrics_stage")

    assert "facts.is_comparative = 0" in sql
    assert "JOIN corpscout.fi_xbrl_contexts" not in sql
    assert "argMaxIf" in sql
    assert "toUInt64OrNull(toString(toInt256(facts.numeric_value)))" in sql
    assert "toUInt64(facts.numeric_value)" not in sql
    assert "precision_rank" in sql
    assert "Float64" not in sql
    assert "xml_source_uri" in sql
    assert "fi_met:ii52" in sql


def test_finland_publish_graph_uses_clickhouse_facts_directly() -> None:
    graph = load_project_defs().resolve_asset_graph()
    keys = graph.get_all_asset_keys()

    for key in (
        "fi_financial_statements_ch",
        "fi_xbrl_contexts_ch",
        "fi_xbrl_units_ch",
        "fi_xbrl_facts_ch",
        "fi_xbrl_taxonomy_codes_ch",
        "fi_financial_metrics_ch",
    ):
        assert dg.AssetKey(key) in keys

    assert dg.AssetKey("fi_financial_metrics_parquet") not in keys
    assert dg.AssetKey("fi_financial_metrics_usd_parquet") not in keys
    assert graph.get(dg.AssetKey("fi_financial_metrics_ch")).parent_keys >= {
        dg.AssetKey("fi_financial_statements_ch"),
        dg.AssetKey("fi_xbrl_contexts_ch"),
        dg.AssetKey("fi_xbrl_facts_ch"),
    }


def test_finland_comprehensive_clickhouse_migration() -> None:
    migration = (
        Path(__file__).parents[3]
        / "clickhouse"
        / "migrations"
        / "000135_corpscout_finland_xbrl_comprehensive.up.sql"
    ).read_text(encoding="utf-8")

    for table in (
        "fi_xbrl_contexts",
        "fi_xbrl_units",
        "fi_xbrl_facts_raw",
        "fi_xbrl_taxonomy_codes",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in migration
    assert "CREATE OR REPLACE VIEW corpscout.fi_financial_facts_with_source" in migration
    assert "xml_source_uri" in migration
    assert "taxonomy_entrypoint" in migration


def test_finland_provenance_view_exposes_clean_column_names() -> None:
    migration = (
        Path(__file__).parents[3]
        / "clickhouse"
        / "migrations"
        / "000136_corpscout_finland_xbrl_provenance_view_columns.up.sql"
    ).read_text(encoding="utf-8")

    for expression in (
        "facts.statement_key AS statement_key",
        "facts.mcy_member_code AS mcy_member_code",
        "facts.ref_member_code AS ref_member_code",
        "reports.source_url AS source_url",
        "reports.parser_version AS parser_version",
    ):
        assert expression in migration
