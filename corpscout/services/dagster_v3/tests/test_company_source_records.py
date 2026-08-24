import pytest

from dagster_v3.defs.company_source_records.identity import (
    clickhouse_file_source_record_uid_sql,
    clickhouse_structured_source_record_uid_sql,
    file_source_record_uid,
    observation_uid,
    structured_source_record_uid,
)
from dagster_v3.defs.company_source_records.sql import (
    esef_source_record_sql,
    finland_financial_source_record_sql,
    sweden_financial_source_record_sql,
    sweden_registry_source_record_sql,
    wikidata_source_record_sql,
)


def test_file_identity_deduplicates_origins_by_content() -> None:
    package_hash = "A" * 64

    filings_xbrl_uid = file_source_record_uid(
        record_kind="esef_report_package",
        content_sha256=package_hash,
    )
    bolagsverket_uid = file_source_record_uid(
        record_kind="esef_report_package",
        content_sha256=package_hash.lower(),
    )

    assert filings_xbrl_uid == bolagsverket_uid
    assert len(filings_xbrl_uid) == 64


def test_structured_identity_versions_changed_payloads() -> None:
    first = structured_source_record_uid(
        source_slug="sweden_bolagsverket",
        record_kind="registry_company",
        source_record_key="5566692850",
        payload_sha256="a" * 64,
    )
    repeated = structured_source_record_uid(
        source_slug="sweden_bolagsverket",
        record_kind="registry_company",
        source_record_key="5566692850",
        payload_sha256="a" * 64,
    )
    changed = structured_source_record_uid(
        source_slug="sweden_bolagsverket",
        record_kind="registry_company",
        source_record_key="5566692850",
        payload_sha256="b" * 64,
    )

    assert first == repeated
    assert changed != first


def test_observation_identity_is_scoped_to_source_record() -> None:
    source_record_uid = file_source_record_uid(
        record_kind="esef_report_package",
        content_sha256="a" * 64,
    )

    assert observation_uid(
        source_record_uid=source_record_uid,
        observation_kind="company_description",
        natural_key="en",
    ) != observation_uid(
        source_record_uid=source_record_uid,
        observation_kind="company_description",
        natural_key="sv",
    )


@pytest.mark.parametrize("value", ["", "abc", "z" * 64, "a" * 63])
def test_source_record_identity_rejects_invalid_hashes(value: str) -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        file_source_record_uid(
            record_kind="esef_report_package",
            content_sha256=value,
        )


def test_clickhouse_identity_expressions_share_python_identity_version() -> None:
    file_sql = clickhouse_file_source_record_uid_sql(
        record_kind="esef_report_package",
        content_sha256_expression="package_sha256",
    )
    structured_sql = clickhouse_structured_source_record_uid_sql(
        source_slug="sweden_bolagsverket",
        record_kind="registry_company",
        source_record_key_expression="source_record_id",
        payload_sha256_expression="source_payload_hash",
    )

    assert "company-source-record-v1\\nfile\\nesef_report_package\\n" in file_sql
    assert (
        "company-source-record-v1\\nstructured\\nsweden_bolagsverket" in structured_sql
    )
    assert "lowerUTF8" in file_sql
    assert "lowerUTF8" in structured_sql


def test_selected_source_sql_preserves_independent_origins_and_company_links() -> None:
    registry = "\n".join(sweden_registry_source_record_sql())
    financial = "\n".join(sweden_financial_source_record_sql())
    finland_financial = "\n".join(finland_financial_source_record_sql())
    esef = "\n".join(esef_source_record_sql())
    wikidata = "\n".join(wikidata_source_record_sql())

    assert "sweden_bolagsverket" in registry
    assert "sweden_scb" in registry
    assert "registry_subject" in registry
    assert "corpscout.se_financial_reports" in financial
    assert "annual_report_xhtml" in financial
    assert "corpscout.fi_financial_statements" in finland_financial
    assert "annual_report_xml" in finland_financial
    assert "finland_prh_xbrl" in finland_financial
    assert "'FI'" in finland_financial
    assert "verified_lei_registry_map" in esef
    assert "corpscout.esef_facts FINAL" in esef
    assert "corpscout.esef_filings AS filings FINAL" in esef
    assert "esef_source_documents" not in esef
    assert "identifier_type = 'se_orgnr'" in wikidata
    assert "wikidata_verified_lei" in wikidata


def test_wikidata_uses_compound_company_snapshot_and_separate_person_records() -> None:
    combined_sql = "\n".join(wikidata_source_record_sql())

    for table_name in (
        "wikidata_companies",
        "wikidata_company_listings",
        "wikidata_company_identifiers",
        "wikidata_company_websites",
        "wikidata_company_relationships",
        "wikidata_company_people",
    ):
        assert table_name in combined_sql
    assert "wikidata_company_source_snapshots" in combined_sql
    assert "arraySort(groupArray" in combined_sql
    assert "wikidata_company_item" in combined_sql
    assert "wikidata_person_item" in combined_sql
