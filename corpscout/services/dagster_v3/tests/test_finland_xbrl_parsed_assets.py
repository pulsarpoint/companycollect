import json
from datetime import UTC, datetime
from io import BytesIO

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets import run_finland_xbrl_parse
from dagster_v3.defs.finland_xbrl.parser import parse_statement_xml
from dagster_v3.defs.finland_xbrl.tables import FACTS_TABLE, STATEMENT_DOCUMENTS_TABLE


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


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}


def test_lxml_parser_extracts_statement_and_facts_from_prh_instance_xml() -> None:
    parsed = parse_statement_xml(
        business_id="0176460-0",
        financial_date="2023-09-30",
        registration_date="2026-05-20",
        source_url="https://example.test/financial",
        xml_object_key="companies/0176460-0/2023-09-30.xml",
        source_run_id="test-run",
        body=SAMPLE_XML,
        parsed_at=datetime(2026, 6, 29, tzinfo=UTC),
    )

    statement = parsed.rows_by_table[STATEMENT_DOCUMENTS_TABLE][0]
    facts = parsed.rows_by_table[FACTS_TABLE]

    assert statement["business_id"] == "0176460-0"
    assert statement["reported_company_name"] == "Testi Oy"
    assert statement["facts_count"] == 6
    assert statement["parser_version"] == "2.0.0"

    revenue_facts = [fact for fact in facts if fact["concept_qname"] == "fi_met:md103"]
    assert [fact["numeric_value"] for fact in revenue_facts] == ["125000", "110000"]
    assert revenue_facts[0]["mcy_member_code"] == "fi_MC:x673"
    assert revenue_facts[0]["is_comparative"] is False
    assert revenue_facts[1]["ref_member_code"] == "fi_RF:x4"
    assert revenue_facts[1]["is_comparative"] is True
    assert json.loads(revenue_facts[1]["dimensions"]) == [
        ["fi_dim:MCY", "fi_MC:x673", ""],
        ["fi_dim:REF", "fi_RF:x4", ""],
    ]


def test_run_finland_xbrl_parse_reads_existing_s3_xml_documents() -> None:
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    s3_client.objects[
        ("source-finland-prh-xbrl", "companies/0176460-0/2023-09-30.xml")
    ] = SAMPLE_XML

    result = run_finland_xbrl_parse(
        object_store=object_store,
        documents=[
            {
                "business_id": "0176460-0",
                "financial_date": "2023-09-30",
                "registration_date": "2026-05-20",
                "source_url": "https://example.test/financial",
                "xml_object_key": "companies/0176460-0/2023-09-30.xml",
            }
        ],
        run_id="test-run",
    )

    assert len(result.statement_rows) == 1
    assert len(result.fact_rows) == 6
    assert result.failed_rows == []
    assert result.statement_rows[0]["reported_company_name"] == "Testi Oy"


def test_run_finland_xbrl_parse_records_failed_documents() -> None:
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    s3_client.objects[
        ("source-finland-prh-xbrl", "companies/good/2023-09-30.xml")
    ] = SAMPLE_XML
    s3_client.objects[("source-finland-prh-xbrl", "companies/bad/2023-09-30.xml")] = (
        b"<xbrl"
    )

    result = run_finland_xbrl_parse(
        object_store=object_store,
        documents=[
            {
                "business_id": "good",
                "financial_date": "2023-09-30",
                "registration_date": "2026-05-20",
                "source_url": "",
                "xml_object_key": "companies/good/2023-09-30.xml",
            },
            {
                "business_id": "bad",
                "financial_date": "2023-09-30",
                "registration_date": "2026-05-21",
                "source_url": "",
                "xml_object_key": "companies/bad/2023-09-30.xml",
            },
        ],
        run_id="test-run",
    )

    assert len(result.statement_rows) == 1
    assert len(result.fact_rows) == 6
    assert len(result.failed_rows) == 1
    assert result.failed_rows[0]["xml_object_key"] == "companies/bad/2023-09-30.xml"
