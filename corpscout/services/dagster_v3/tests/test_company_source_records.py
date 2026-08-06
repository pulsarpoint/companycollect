import pytest

from dagster_v3.defs.company_source_records.assets import (
    _delete_superseded_esef_observations,
)
from dagster_v3.defs.company_source_records.identity import (
    clickhouse_file_source_record_uid_sql,
    clickhouse_structured_source_record_uid_sql,
    file_source_record_uid,
    observation_uid,
    structured_source_record_uid,
)
from dagster_v3.defs.company_source_records.sql import (
    esef_document_observation_sql,
    esef_source_record_sql,
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
    esef = "\n".join(esef_source_record_sql())
    wikidata = "\n".join(wikidata_source_record_sql())

    assert "sweden_bolagsverket" in registry
    assert "sweden_scb" in registry
    assert "registry_subject" in registry
    assert "corpscout.se_financial_reports" in financial
    assert "annual_report_xhtml" in financial
    assert "verified_lei_registry_map" in esef
    assert "identifier_type = 'se_orgnr'" in wikidata
    assert "wikidata_verified_lei" in wikidata


def test_esef_llm_arrays_are_normalized_into_typed_observations() -> None:
    statements = esef_document_observation_sql()
    combined_sql = "\n".join(statements)

    assert len(statements) == 4
    assert "company_description_observations" in statements[0]
    assert "esef_document_people" in statements[1]
    assert "esef_document_business_items" in statements[2]
    assert "esef_document_group_relationships" in statements[3]
    for json_column in (
        "people_json",
        "products_and_services_json",
        "customer_markets_json",
        "operating_geographies_json",
        "business_segments_json",
        "material_group_relationships_json",
    ):
        assert json_column in combined_sql
    assert "evidence_ids" in combined_sql
    assert "prompt_version" in combined_sql
    assert "model_name" in combined_sql
    assert "ARRAY JOIN" in combined_sql
    assert "parseDate32BestEffortOrNull" not in combined_sql
    assert combined_sql.count("toDate32(parseDateTimeBestEffortOrNull(") == 3


def test_esef_observation_publication_deletes_only_superseded_run_versions() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.cleanup_calls: list[tuple[str, dict[str, object], dict[str, int]]] = []

        def execute(
            self,
            query: str,
            parameters: dict[str, object] | None = None,
            settings: dict[str, int] | None = None,
        ) -> list[tuple[str, str]]:
            if query.startswith("SELECT DISTINCT"):
                return [("a" * 64, "current-run")]
            assert parameters is not None
            assert settings is not None
            self.cleanup_calls.append((query, parameters, settings))
            return []

    client = FakeClient()

    assert _delete_superseded_esef_observations(client) == 1
    assert len(client.cleanup_calls) == 4
    for query, parameters, settings in client.cleanup_calls:
        assert "source_record_uid IN %(source_record_uids)s" in query
        assert "source_run_id) NOT IN %(current_versions)s" in query
        assert parameters["source_record_uids"] == ("a" * 64,)
        assert parameters["current_versions"] == (("a" * 64, "current-run"),)
        assert settings == {"mutations_sync": 2}
    description_query = client.cleanup_calls[0][0]
    assert "extraction_method = 'llm_extraction'" in description_query
    assert "corpscout.esef_document_company_information:" in description_query


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
