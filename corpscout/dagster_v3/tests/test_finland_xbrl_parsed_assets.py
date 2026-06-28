import json
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pytest
from dagster import AssetKey

import dagster_v3.defs.finland_xbrl.arelle_parser as arelle_parser
import dagster_v3.defs.finland_xbrl.assets as xbrl
from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.finland_xbrl.arelle_parser import ArelleParsedStatement
from dagster_v3.defs.finland_xbrl.assets import (
    RAW_XML_DOCUMENTS_OBJECT_KEY,
    XBRL_DLT_DATASET_NAME,
    build_concept_profile_rows,
    build_parse_quality_row,
    load_xbrl_document_manifest,
    parsed_duckdb_observability_metadata,
    run_finland_xbrl_arelle_dlt_pipeline,
)
from dagster_v3.defs.finland_xbrl.tables import FACTS_TABLE, STATEMENT_DOCUMENTS_TABLE
from dagster_v3.defs.common.duckdb_resources import duckdb_database_path, duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource


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

    def create_bucket(self, Bucket: str) -> None:
        pass

    def head_object(self, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            raise FakeS3Error("404")

    def put_object(self, Bucket: str, Key: str, Body: bytes | str) -> None:
        body = Body.encode("utf-8") if isinstance(Body, str) else Body
        self.objects[(Bucket, Key)] = body

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


def test_parsed_xbrl_asset_loads_statement_and_fact_duckdb_tables(tmp_path: Path) -> None:
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    s3_client.objects[("source-finland-prh-xbrl", "companies/0176460-0/2023-09-30.xml")] = SAMPLE_XML
    output = BytesIO()
    pl.DataFrame(
        [
            {
                "business_id": "0176460-0",
                "financial_date": "2023-09-30",
                "registration_date": "2026-05-20",
                "source_url": "https://example.test/financial",
                "xml_object_key": "companies/0176460-0/2023-09-30.xml",
            }
        ]
    ).write_parquet(output)
    s3_client.objects[("source-finland-prh-xbrl", RAW_XML_DOCUMENTS_OBJECT_KEY)] = output.getvalue()

    documents, _ = load_xbrl_document_manifest(
        object_store=object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY
    )
    run_finland_xbrl_arelle_dlt_pipeline(
        database_path=tmp_path / "source.duckdb",
        object_store=object_store,
        documents=documents,
        run_id="test-run",
        parser=_fake_arelle_parser,
    )

    with duckdb.connect(str(tmp_path / "source.duckdb"), read_only=True) as connection:
        statement_frame = connection.execute(
            f"select * from {XBRL_DLT_DATASET_NAME}.{STATEMENT_DOCUMENTS_TABLE}"
        ).pl()
        facts_frame = connection.execute(
            f"select * from {XBRL_DLT_DATASET_NAME}.{FACTS_TABLE}"
        ).pl()

    assert statement_frame.row(0, named=True)["business_id"] == "0176460-0"
    assert statement_frame.row(0, named=True)["reported_company_name"] == "Testi Oy"
    assert statement_frame.row(0, named=True)["facts_count"] == 6
    assert facts_frame.height == 1
    revenue_current = facts_frame.filter(
        (pl.col("concept_qname") == "fi_met:md103") & (pl.col("context_id") == "ctx_mcy")
    ).row(0, named=True)
    assert revenue_current["value_kind"] == "numeric"
    assert revenue_current["numeric_value"] == "125000"
    assert revenue_current["mcy_member_code"] == "fi_MC:x673"


def test_parsed_xbrl_pipeline_logs_manifest_progress_and_summary(tmp_path: Path) -> None:
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    s3_client.objects[("source-finland-prh-xbrl", "companies/0176460-0/2023-09-30.xml")] = SAMPLE_XML
    output = BytesIO()
    pl.DataFrame(
        [
            {
                "business_id": "0176460-0",
                "financial_date": "2023-09-30",
                "registration_date": "2026-05-20",
                "source_url": "https://example.test/financial",
                "xml_object_key": "companies/0176460-0/2023-09-30.xml",
            }
        ]
    ).write_parquet(output)
    s3_client.objects[("source-finland-prh-xbrl", RAW_XML_DOCUMENTS_OBJECT_KEY)] = output.getvalue()
    log_messages: list[str] = []

    documents, _ = load_xbrl_document_manifest(
        object_store=object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY
    )
    run_finland_xbrl_arelle_dlt_pipeline(
        database_path=tmp_path / "source.duckdb",
        object_store=object_store,
        documents=documents,
        run_id="test-run",
        parser=_fake_arelle_parser,
        log_info=log_messages.append,
        progress_interval=1,
    )

    assert any("Parsing 1 XBRL XML documents" in message for message in log_messages)
    assert any("Parsed XBRL XML document 1/1" in message for message in log_messages)
    assert any("Parsed XBRL XML documents complete" in message for message in log_messages)
    assert any("dlt loaded parsed XBRL tables" in message for message in log_messages)


def test_parsed_xbrl_observability_metadata_summarizes_quality_without_extra_tables(
    tmp_path: Path,
) -> None:
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    s3_client.objects[("source-finland-prh-xbrl", "companies/0176460-0/2023-09-30.xml")] = SAMPLE_XML
    output = BytesIO()
    pl.DataFrame(
        [
            {
                "business_id": "0176460-0",
                "financial_date": "2023-09-30",
                "registration_date": "2026-05-20",
                "source_url": "https://example.test/financial",
                "xml_object_key": "companies/0176460-0/2023-09-30.xml",
            }
        ]
    ).write_parquet(output)
    s3_client.objects[("source-finland-prh-xbrl", RAW_XML_DOCUMENTS_OBJECT_KEY)] = output.getvalue()
    database_path = tmp_path / "source.duckdb"
    documents, _ = load_xbrl_document_manifest(
        object_store=object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY
    )
    run_finland_xbrl_arelle_dlt_pipeline(
        database_path=database_path,
        object_store=object_store,
        documents=documents,
        run_id="test-run",
        parser=_fake_arelle_parser,
    )

    metadata = parsed_duckdb_observability_metadata(
        object_store=object_store,
        source_duckdb=duckdb_resource(database_path),
        documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY,
    )

    assert metadata["xml_documents_count"] == 1
    assert metadata["statement_documents_count"] == 1
    assert metadata["facts_count"] == 1
    assert metadata["parser_warning_statements_count"] == 0
    assert metadata["concept_count"] == 1
    assert metadata["top_concepts"] == [{"concept_qname": "fi_met:md103", "count": 1}]


def test_parsed_xbrl_asset_reports_missing_xml_documents_manifest() -> None:
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)

    with pytest.raises(ValueError, match="finland_xbrl_raw_xml_documents"):
        load_xbrl_document_manifest(
            object_store=object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY
        )


def test_pipeline_appends_documents_across_calls(tmp_path: Path) -> None:
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    s3_client.objects[("source-finland-prh-xbrl", "companies/a/2023-12-31.xml")] = SAMPLE_XML
    s3_client.objects[("source-finland-prh-xbrl", "companies/b/2023-12-31.xml")] = SAMPLE_XML
    doc_a = {
        "business_id": "a",
        "financial_date": "2023-12-31",
        "registration_date": "2024-03-10",
        "source_url": "",
        "xml_object_key": "companies/a/2023-12-31.xml",
    }
    doc_b = {
        "business_id": "b",
        "financial_date": "2023-12-31",
        "registration_date": "2024-04-02",
        "source_url": "",
        "xml_object_key": "companies/b/2023-12-31.xml",
    }
    db = tmp_path / "source.duckdb"
    run_finland_xbrl_arelle_dlt_pipeline(
        database_path=db,
        object_store=object_store,
        documents=[doc_a],
        run_id="r1",
        parser=_fake_arelle_parser,
    )
    run_finland_xbrl_arelle_dlt_pipeline(
        database_path=db,
        object_store=object_store,
        documents=[doc_b],
        run_id="r2",
        parser=_fake_arelle_parser,
    )

    with duckdb.connect(str(db), read_only=True) as conn:
        n = conn.execute(
            f"select count(*) from {XBRL_DLT_DATASET_NAME}.{STATEMENT_DOCUMENTS_TABLE}"
        ).fetchone()[0]
    assert n == 2  # append, not replace


def test_arelle_parser_falls_back_to_instance_facts_when_taxonomy_facts_are_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptyArelleModel:
        contexts: dict[str, object] = {}
        units: dict[str, object] = {}
        facts: list[object] = []

    monkeypatch.setattr(
        arelle_parser,
        "_load_arelle_model",
        lambda body: (EmptyArelleModel(), lambda: None),
    )

    parsed = arelle_parser.parse_statement_xml_with_arelle(
        business_id="0176460-0",
        financial_date="2023-09-30",
        registration_date="2026-05-20",
        source_url="https://example.test/financial",
        xml_object_key="companies/0176460-0/2023-09-30.xml",
        source_run_id="test-run",
        body=SAMPLE_XML,
        parsed_at=datetime.now(UTC),
    )

    assert parsed.statement_document["facts_count"] == 6
    assert parsed.statement_document["parser_version"] == "arelle-1.0.0+lxml-fallback"
    assert parsed.facts[0]["parser_version"] == "arelle-1.0.0+lxml-fallback"
    assert "Arelle returned no facts" in parsed.warnings


def test_parse_quality_summary_reports_counts_and_common_concepts() -> None:
    row = build_parse_quality_row(
        xml_documents_frame=pl.DataFrame(
            [
                {"business_id": "0176460-0", "financial_date": "2023-09-30"},
                {"business_id": "1234567-8", "financial_date": "2023-12-31"},
            ]
        ),
        statement_documents_frame=pl.DataFrame(
            [
                {
                    "statement_key": "s1",
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "facts_count": 2,
                    "validation_warnings": "",
                    "parsed_at": "2026-06-16T00:00:00+00:00",
                },
                {
                    "statement_key": "s2",
                    "business_id": "",
                    "financial_date": "2023-12-31",
                    "facts_count": 0,
                    "validation_warnings": "[\"missing reported company name\"]",
                    "parsed_at": "2026-06-16T00:01:00+00:00",
                },
                {
                    "statement_key": "s2",
                    "business_id": "duplicate",
                    "financial_date": "",
                    "facts_count": 1,
                    "validation_warnings": "",
                    "parsed_at": "2026-06-16T00:02:00+00:00",
                },
            ]
        ),
        facts_frame=pl.DataFrame(
            [
                {
                    "statement_key": "s1",
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "concept_qname": "fi_met:md103",
                    "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                    "concept_local_name": "md103",
                },
                {
                    "statement_key": "s1",
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "concept_qname": "fi_met:md103",
                    "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                    "concept_local_name": "md103",
                },
                {
                    "statement_key": "s2",
                    "business_id": "",
                    "financial_date": "",
                    "concept_qname": "fi_met:si168",
                    "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                    "concept_local_name": "si168",
                },
            ]
        ),
        generated_at="2026-06-16T00:03:00+00:00",
    )

    assert row["xml_documents_count"] == 2
    assert row["statement_documents_count"] == 3
    assert row["facts_count"] == 3
    assert row["statements_with_zero_facts_count"] == 1
    assert row["duplicate_statement_keys_count"] == 1
    assert row["parser_warning_statements_count"] == 1
    assert row["missing_statement_business_ids_count"] == 1
    assert row["missing_statement_financial_dates_count"] == 1
    assert row["missing_fact_business_ids_count"] == 1
    assert row["missing_fact_financial_dates_count"] == 1
    assert row["facts_per_statement_min"] == 0
    assert row["facts_per_statement_avg"] == 1.0
    assert row["facts_per_statement_max"] == 2
    assert row["latest_parsed_at"] == "2026-06-16T00:02:00+00:00"
    assert json.loads(row["top_concept_namespaces"]) == [
        {"concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met", "count": 3}
    ]
    assert json.loads(row["top_concepts"]) == [
        {"concept_qname": "fi_met:md103", "count": 2},
        {"concept_qname": "fi_met:si168", "count": 1},
    ]


def test_concept_profile_groups_facts_by_concept() -> None:
    rows = build_concept_profile_rows(
        pl.DataFrame(
            [
                {
                    "statement_key": "s1",
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "concept_qname": "fi_met:md103",
                    "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                    "concept_local_name": "md103",
                    "value_kind": "numeric",
                    "numeric_value": "125000",
                    "date_value": "",
                    "text_value": "",
                    "is_comparative": False,
                },
                {
                    "statement_key": "s1",
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "concept_qname": "fi_met:md103",
                    "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                    "concept_local_name": "md103",
                    "value_kind": "numeric",
                    "numeric_value": "110000",
                    "date_value": "",
                    "text_value": "",
                    "is_comparative": True,
                },
                {
                    "statement_key": "s2",
                    "business_id": "1234567-8",
                    "financial_date": "2023-12-31",
                    "concept_qname": "fi_met:md103",
                    "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                    "concept_local_name": "md103",
                    "value_kind": "numeric",
                    "numeric_value": "220000",
                    "date_value": "",
                    "text_value": "",
                    "is_comparative": False,
                },
                {
                    "statement_key": "s1",
                    "business_id": "0176460-0",
                    "financial_date": "2023-09-30",
                    "concept_qname": "fi_met:si168",
                    "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                    "concept_local_name": "si168",
                    "value_kind": "text",
                    "numeric_value": "",
                    "date_value": "",
                    "text_value": "Testi Oy",
                    "is_comparative": False,
                },
            ]
        ),
        example_limit=2,
    )

    assert [row["concept_qname"] for row in rows] == ["fi_met:md103", "fi_met:si168"]
    revenue = rows[0]
    assert revenue["facts_count"] == 3
    assert revenue["statement_count"] == 2
    assert revenue["business_count"] == 2
    assert revenue["financial_date_count"] == 2
    assert revenue["numeric_count"] == 3
    assert revenue["date_count"] == 0
    assert revenue["text_count"] == 0
    assert revenue["current_period_count"] == 2
    assert revenue["comparative_count"] == 1
    assert json.loads(revenue["example_numeric_values"]) == ["125000", "110000"]

    company_name = rows[1]
    assert company_name["facts_count"] == 1
    assert company_name["text_count"] == 1
    assert json.loads(company_name["example_text_values"]) == ["Testi Oy"]


def _fake_arelle_parser(**kwargs: Any) -> ArelleParsedStatement:
    parsed_at = kwargs["parsed_at"].isoformat()
    statement_key = f"{kwargs['business_id']}-{kwargs['financial_date']}"
    return ArelleParsedStatement(
        statement_document={
            "statement_key": statement_key,
            "source_run_id": kwargs["source_run_id"],
            "business_id": kwargs["business_id"],
            "financial_date": kwargs["financial_date"],
            "registration_date": kwargs["registration_date"] or "",
            "source_url": kwargs["source_url"],
            "xml_object_key": kwargs["xml_object_key"],
            "xml_sha256": "sample-sha256",
            "xml_size_bytes": len(kwargs["body"]),
            "root_name": "xbrl",
            "schema_refs": json.dumps(["https://example.test/taxonomy.xsd"]),
            "taxonomy_entrypoint": "https://example.test/taxonomy.xsd",
            "reported_business_id": "0176460-0",
            "reported_company_name": "Testi Oy",
            "reported_period_start": "2022-10-01",
            "reported_period_end": "2023-09-30",
            "contexts_count": 3,
            "units_count": 1,
            "facts_count": 6,
            "validation_warnings": "[]",
            "parser_version": "arelle-test",
            "parsed_at": parsed_at,
        },
        facts=[
            {
                "statement_key": statement_key,
                "business_id": kwargs["business_id"],
                "financial_date": kwargs["financial_date"],
                "fact_ordinal": 1,
                "concept_qname": "fi_met:md103",
                "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
                "concept_local_name": "md103",
                "context_id": "ctx_mcy",
                "unit_id": "EUR",
                "decimals": "0",
                "precision": "",
                "value_kind": "numeric",
                "raw_value": "125000",
                "numeric_value": "125000",
                "date_value": "",
                "text_value": "",
                "mcy_member_code": "fi_MC:x673",
                "mcy_member_label_fi": "",
                "ref_member_code": "",
                "ref_member_label_fi": "",
                "is_comparative": False,
                "dimensions": json.dumps([["fi_dim:MCY", "fi_MC:x673", ""]]),
                "parser_version": "arelle-test",
                "parsed_at": parsed_at,
            }
        ],
        warnings=[],
    )


def _raising_parser(**kwargs):
    if kwargs["business_id"] == "bad":
        raise ValueError("boom: unparseable XBRL")
    return _fake_arelle_parser(**kwargs)


def test_parse_skips_bad_document_and_keeps_good_ones(tmp_path):
    s3 = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3)
    s3.objects[("source-finland-prh-xbrl", "companies/good/2023-12-31.xml")] = SAMPLE_XML
    s3.objects[("source-finland-prh-xbrl", "companies/bad/2023-12-31.xml")] = SAMPLE_XML
    good = {"business_id": "good", "financial_date": "2023-12-31", "registration_date": "2024-03-10",
            "source_url": "", "xml_object_key": "companies/good/2023-12-31.xml"}
    bad = {"business_id": "bad", "financial_date": "2023-12-31", "registration_date": "2024-03-11",
           "source_url": "", "xml_object_key": "companies/bad/2023-12-31.xml"}
    db = tmp_path / "source.duckdb"

    result = run_finland_xbrl_arelle_dlt_pipeline(
        database_path=db, object_store=object_store,
        documents=[good, bad], run_id="r1", parser=_raising_parser,
    )

    assert result.parsed == 1
    assert result.failed == 1
    with duckdb.connect(str(db), read_only=True) as conn:
        keys = [r[0] for r in conn.execute(
            f"select xml_object_key from {XBRL_DLT_DATASET_NAME}.{STATEMENT_DOCUMENTS_TABLE}"
        ).fetchall()]
    assert keys == ["companies/good/2023-12-31.xml"]


def _doc(business_id, financial_date, registration_date):
    return {
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date,
        "xml_object_key": f"companies/{business_id}/{financial_date}.xml",
    }


def test_documents_in_registration_window_filters_by_month():
    docs = [
        _doc("a", "2023-12-31", "2024-03-10"),
        _doc("b", "2023-12-31", "2024-04-02"),
        _doc("c", "2023-12-31", ""),
    ]
    got = xbrl.documents_in_registration_window(
        docs, window_start=date(2024, 3, 1), window_end=date(2024, 4, 1)
    )
    assert [d["business_id"] for d in got] == ["a"]


def test_documents_missing_registration_date_flags_empty_and_invalid():
    docs = [
        _doc("a", "2023-12-31", "2024-03-10"),
        _doc("b", "2023-12-31", ""),
        _doc("c", "2023-12-31", "not-a-date"),
    ]
    missing = xbrl.documents_missing_registration_date(docs)
    assert [d["business_id"] for d in missing] == ["b", "c"]


def test_unparsed_documents_skips_by_object_key():
    docs = [
        _doc("a", "2023-12-31", "2024-03-10"),
        _doc("b", "2023-12-31", "2024-03-11"),
    ]
    already = {"companies/a/2023-12-31.xml"}
    got = xbrl.unparsed_documents(docs, parsed_object_keys=already)
    assert [d["business_id"] for d in got] == ["b"]


def _xbrl_resource(tmp_path):
    return duckdb_resource(tmp_path / "finland_ytj.duckdb")


def test_load_parsed_object_keys_empty_when_no_table(tmp_path):
    res = _xbrl_resource(tmp_path)
    assert xbrl.load_parsed_object_keys(res) == set()


def test_parsed_duckdb_row_counts_zero_on_missing_db(tmp_path):
    # An empty partition skips the parse, so the tables/db may not exist yet.
    res = _xbrl_resource(tmp_path)
    assert xbrl.parsed_duckdb_row_counts(res) == {
        xbrl.tables.STATEMENT_DOCUMENTS_TABLE: 0,
        xbrl.tables.FACTS_TABLE: 0,
    }


def test_load_parsed_object_keys_returns_distinct_keys(tmp_path):
    res = _xbrl_resource(tmp_path)
    with res.get_connection() as conn:
        conn.execute(f"create schema if not exists {xbrl.XBRL_DLT_DATASET_NAME}")
        conn.execute(
            f"create table {xbrl.XBRL_DLT_DATASET_NAME}.{xbrl.tables.STATEMENT_DOCUMENTS_TABLE} "
            "(statement_key varchar, xml_object_key varchar)"
        )
        conn.execute(
            f"insert into {xbrl.XBRL_DLT_DATASET_NAME}.{xbrl.tables.STATEMENT_DOCUMENTS_TABLE} values "
            "('k1','companies/a/2023-12-31.xml'),"
            "('k2','companies/b/2023-12-31.xml'),"
            "('k1b','companies/a/2023-12-31.xml')"
        )
    assert xbrl.load_parsed_object_keys(res) == {
        "companies/a/2023-12-31.xml",
        "companies/b/2023-12-31.xml",
    }


def test_parse_writer_assets_are_partitioned_and_table_assets_are_canonical():
    graph = load_project_defs().get_repository_def().asset_graph
    backfill = graph.get(AssetKey(["finland_xbrl_parse_backfill"]))
    assert type(backfill.partitions_def).__name__ == "MonthlyPartitionsDefinition"
    incremental = graph.get(AssetKey(["finland_xbrl_parse_incremental"]))
    assert type(incremental.partitions_def).__name__ == "DailyPartitionsDefinition"
    node = graph.get(AssetKey(["fi_prh_xbrl_statement_documents"]))
    assert node.partitions_def is None
    assert node.parent_keys == {
        AssetKey(["finland_xbrl_parse_backfill"]),
        AssetKey(["finland_xbrl_parse_incremental"]),
    }
    metrics = graph.get(AssetKey(["fi_prh_xbrl_financial_metrics"]))
    assert metrics.partitions_def is None


def _seed_catalog(s3_client, object_store, docs):
    for d in docs:
        s3_client.objects[("source-finland-prh-xbrl", d["xml_object_key"])] = SAMPLE_XML
    output = BytesIO()
    pl.DataFrame(docs).write_parquet(output)
    s3_client.objects[("source-finland-prh-xbrl", RAW_XML_DOCUMENTS_OBJECT_KEY)] = output.getvalue()


def _run_partition(res, object_store, window_start, window_end):
    documents, _ = xbrl.load_xbrl_document_manifest(
        object_store=object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY
    )
    in_window = xbrl.documents_in_registration_window(
        documents, window_start=window_start, window_end=window_end
    )
    to_parse = xbrl.unparsed_documents(
        in_window, parsed_object_keys=xbrl.load_parsed_object_keys(res)
    )
    if to_parse:
        xbrl.run_finland_xbrl_arelle_dlt_pipeline(
            database_path=duckdb_database_path(res),
            object_store=object_store,
            documents=to_parse,
            run_id="t",
            parser=_fake_arelle_parser,
        )
    return len(to_parse)


def _statement_count(res):
    with duckdb.connect(str(duckdb_database_path(res)), read_only=True) as conn:
        return conn.execute(
            f"select count(*) from {XBRL_DLT_DATASET_NAME}.{STATEMENT_DOCUMENTS_TABLE}"
        ).fetchone()[0]


def test_parse_metadata_reports_failed_and_parsed_counts(tmp_path):
    # The body of finland_xbrl_parsed_tables maps the runner result onto the
    # per-table MaterializeResult metadata:
    #   documents_parsed_this_run -> result.parsed
    #   documents_failed_this_run -> result.failed
    # Drive the same runner with one good and one poison doc and assert the
    # counts that feed that metadata are accurate (parsed != attempted).
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    res = _xbrl_resource(tmp_path)
    good = {
        "business_id": "good",
        "financial_date": "2023-12-31",
        "registration_date": "2024-03-10",
        "source_url": "",
        "xml_object_key": "companies/good/2023-12-31.xml",
    }
    bad = {
        "business_id": "bad",
        "financial_date": "2023-12-31",
        "registration_date": "2024-03-11",
        "source_url": "",
        "xml_object_key": "companies/bad/2023-12-31.xml",
    }
    _seed_catalog(s3_client, object_store, [good, bad])

    documents, _ = xbrl.load_xbrl_document_manifest(
        object_store=object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY
    )
    to_parse = xbrl.documents_in_registration_window(
        documents, window_start=date(2024, 3, 1), window_end=date(2024, 4, 1)
    )
    assert len(to_parse) == 2  # both attempted

    result = xbrl.run_finland_xbrl_arelle_dlt_pipeline(
        database_path=duckdb_database_path(res),
        object_store=object_store,
        documents=to_parse,
        run_id="t",
        parser=_raising_parser,
    )

    # Metadata semantics: parsed count reflects rows actually written, not
    # attempts (len(to_parse) would overcount), and failures are surfaced.
    documents_parsed_this_run = result.parsed
    documents_failed_this_run = result.failed
    assert documents_parsed_this_run == 1
    assert documents_failed_this_run == 1
    assert documents_parsed_this_run != len(to_parse)


def test_partitioned_parse_is_incremental_and_resumable(tmp_path):
    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    res = _xbrl_resource(tmp_path)
    march = {
        "business_id": "a",
        "financial_date": "2023-12-31",
        "registration_date": "2024-03-10",
        "source_url": "",
        "xml_object_key": "companies/a/2023-12-31.xml",
    }
    april = {
        "business_id": "b",
        "financial_date": "2023-12-31",
        "registration_date": "2024-04-02",
        "source_url": "",
        "xml_object_key": "companies/b/2023-12-31.xml",
    }
    _seed_catalog(s3_client, object_store, [march, april])

    # March partition: only the March document is parsed.
    assert _run_partition(res, object_store, date(2024, 3, 1), date(2024, 4, 1)) == 1
    assert _statement_count(res) == 1
    # Re-running March is a no-op (the March document is already parsed).
    assert _run_partition(res, object_store, date(2024, 3, 1), date(2024, 4, 1)) == 0
    assert _statement_count(res) == 1
    # April partition: adds the April document while the March row remains.
    assert _run_partition(res, object_store, date(2024, 4, 1), date(2024, 5, 1)) == 1
    assert _statement_count(res) == 2
