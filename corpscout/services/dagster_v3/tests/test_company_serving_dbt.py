import re
from pathlib import Path

import dagster as dg
from dbt.cli.main import dbtRunner
from dagster_dbt import DbtProjectComponent

from dagster_v3.components.company_serving_dbt import CompanyServingDbtComponent


DBT_DIR = (
    Path(__file__).parents[1]
    / "src"
    / "dagster_v3"
    / "defs"
    / "company_serving"
    / "dbt"
)


def test_company_serving_dbt_project_parses() -> None:
    result = dbtRunner().invoke(
        [
            "parse",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--no-partial-parse",
        ]
    )

    assert result.success, result.exception
    assert result.result is not None
    model_names = {
        node.name
        for node in result.result.nodes.values()
        if node.resource_type.value == "model"
    }
    assert model_names == {
        "company_external_identifier_current_build",
        "company_gleif_current_build",
        "company_gleif_relationship_current_build",
        "company_wikidata_current_build",
        "company_description_current_build",
        "company_contact_current_build",
        "company_domains_build",
        "company_domain_current_build",
        "company_contract_current_build",
        "company_contract_summary_current_build",
        "se_company_industry_display_current_build",
        "company_section_item_source_links_build",
        "company_section_presence_current_build",
    }


def test_shared_dbt_sources_are_dependency_only_specs(monkeypatch) -> None:
    base_spec = dg.AssetSpec(
        key=["corpscout", "se_company_basic_info"],
        metadata={
            "dagster/table_name": "corpscout.se_company_basic_info",
            "dagster/code_references": "generated component path",
        },
    )
    monkeypatch.setattr(
        DbtProjectComponent,
        "get_asset_spec",
        lambda _self, _manifest, _unique_id, _project: base_spec,
    )
    component = object.__new__(CompanyServingDbtComponent)

    source_spec = component.get_asset_spec(
        {}, "source.company_serving.se_companies", None
    )
    model_spec = component.get_asset_spec(
        {}, "model.company_serving.company_gleif_current_build", None
    )

    assert source_spec.metadata == {}
    assert model_spec.metadata == base_spec.metadata


def test_partitioned_esef_sources_feed_serving_models_from_all_weeks(
    monkeypatch,
) -> None:
    base_spec = dg.AssetSpec(
        key="company_contact_current_build",
        deps=[
            "esef_document_contact_candidates_clickhouse",
            "esef_filings_clickhouse",
        ],
    )
    monkeypatch.setattr(
        DbtProjectComponent,
        "get_asset_spec",
        lambda _self, _manifest, _unique_id, _project: base_spec,
    )
    component = object.__new__(CompanyServingDbtComponent)

    model_spec = component.get_asset_spec(
        {}, "model.company_serving.company_contact_current_build", None
    )
    dependencies = {dependency.asset_key: dependency for dependency in model_spec.deps}

    assert isinstance(
        dependencies[
            dg.AssetKey("esef_document_contact_candidates_clickhouse")
        ].partition_mapping,
        dg.AllPartitionMapping,
    )
    assert (
        dependencies[
            dg.AssetKey("esef_filings_clickhouse")
        ].partition_mapping
        is None
    )


def test_serving_models_resolve_identity_and_evidence_offline() -> None:
    models = DBT_DIR / "models"
    external_ids = (
        models / "company_external_identifier_current_build.sql"
    ).read_text()
    gleif = (models / "company_gleif_current_build.sql").read_text()
    wikidata = (models / "company_wikidata_current_build.sql").read_text()
    source_links = (models / "company_section_item_source_links_build.sql").read_text()
    presence = (models / "company_section_presence_current_build.sql").read_text()

    assert "ref('company_external_identifier_current_build')" in gleif
    assert "ref('company_external_identifier_current_build')" in wikidata
    assert "country_person_match" not in source_links
    assert "company_section_item_source_links" not in source_links
    assert "ref('company_section_item_source_links_build')" in presence
    assert "FROM evidence_links AS links" in source_links
    assert "INNER JOIN company_anchors AS anchors" in source_links
    assert (
        "FROM section_rows AS rows\nINNER JOIN company_anchors AS anchors" in presence
    )
    assert "ref('company_domains_build')" in source_links
    assert "has(current.source_names, 'wikidata')" in source_links
    assert "has(current.source_names, 'esef_filing')" in source_links
    assert "annual_report_website" in source_links
    assert "'management'" not in source_links
    assert "ref('company_management_current_build')" not in source_links
    company_domains = (models / "company_domains_build.sql").read_text()
    assert "source('corpscout', 'se_company_domain')" in company_domains
    assert "reviewed_evidence_fingerprint" in company_domains
    assert "domains.active AS is_active" in company_domains
    assert "existing.review_status != 'unreviewed'" not in company_domains
    assert "gleif_lei_record" in source_links
    assert "record_kind" in source_links
    assert "payload_sha256" in source_links
    assert "concat('SE', company_id, '01')" not in external_ids
    assert "issuer_scheme = 'vat'" in external_ids
    assert (
        external_ids.count(
            "INNER JOIN companies\n        ON companies.company_id = identifiers.company_id"
        )
        == 2
    )

    assert "AS companies" in company_domains

    for model_name in (
        "company_description_current_build.sql",
        "company_contract_current_build.sql",
        "se_company_industry_display_current_build.sql",
    ):
        model = (models / model_name).read_text()
        assert "source('corpscout', 'se_company_basic_info')" in model
        assert "INNER JOIN company_anchors AS anchors" in model

    # ESEF slice 1 Task 5: every Swedish ESEF leg reads a se_esef_* view -- never the
    # country-agnostic esef_document_* product directly, and never the retired
    # esef_entity_registry_map join the views now do themselves. A view is already a FINAL
    # read of its ReplacingMergeTree product, so re-adding FINAL after se_esef_document_people
    # would be a ClickHouse error.
    for model in ("company_contact_current_build", "company_description_current_build", "company_section_item_source_links_build", "company_domains_build"):
        text = (models / f"{model}.sql").read_text()
        # Only the se_esef_document_* views remain: any esef_document_* NOT preceded by
        # "se_" is the country-agnostic product itself, read directly.
        assert re.search(r"(?<!se_)esef_document_", text) is None
        assert "esef_entity_registry_map" not in text
        assert "se_esef_document_people FINAL" not in text

    combined_serving_sql = "\n".join(path.read_text() for path in models.glob("*.sql"))
    for retired_table in (
        "company_source_records",
        "company_source_record_origins",
        "company_source_record_links",
        "company_description_observations",
    ):
        assert retired_table not in combined_serving_sql


def test_serving_project_declares_integrity_tests() -> None:
    tests = {path.name for path in (DBT_DIR / "tests").glob("*.sql")}
    assert tests == {
        "company_lei_gleif_is_consistent.sql",
        "company_domains_source_arrays_align.sql",
        "section_presence_uses_supported_names.sql",
        "section_source_links_have_records.sql",
    }

    generic_tests = (DBT_DIR / "macros" / "company_serving_tests.sql").read_text()
    schema = (DBT_DIR / "models" / "schema.yml").read_text()
    assert "test company_serving_unique_key" in generic_tests
    assert "test company_serving_sweden_anchor" in generic_tests
    assert schema.count("company_serving_unique_key:") == 12
    assert schema.count("company_serving_sweden_anchor") == 12


MODELS_DIR = DBT_DIR / "models"


def test_company_domains_build_reads_the_folded_entity() -> None:
    sql = (MODELS_DIR / "company_domains_build.sql").read_text(encoding="utf-8")
    assert "source('corpscout', 'se_company_domain')" in sql
    assert "domains.evidence_hash AS evidence_fingerprint" in sql
    assert "domains.active AS is_active" in sql
    assert "domains.is_primary AS suggested_primary" in sql
    assert "company_domain_suggestions_active" not in sql


def test_source_links_read_esef_domain_provenance_from_the_domains_view() -> None:
    sql = (MODELS_DIR / "company_section_item_source_links_build.sql").read_text(
        encoding="utf-8"
    )
    cte = sql.split("esef_domains AS (", 1)[1].split("\n),\n", 1)[0]
    assert "source('corpscout', 'se_esef_domains') }} AS domains" in cte
    assert "se_esef_document_contact_candidates" not in cte
    assert "domains.extraction_status = 'ok'" in cte
    assert "domains.registrable_domain != ''" in cte
    assert "role -> role IN ('auditor', 'social_media', 'external_reference')" in cte
    assert "domains.source_record_uid" in cte
    assert "domains.resolved_at AS linked_at" in cte
    assert "candidate_kind = 'website'" not in sql


def test_company_contact_current_build_excludes_website_rows() -> None:
    sql = (MODELS_DIR / "company_contact_current_build.sql").read_text(encoding="utf-8")
    assert "WHERE candidate_kind != 'website'" in sql


def test_sources_declare_the_esef_domains_view_with_its_asset_key() -> None:
    text = (MODELS_DIR / "sources.yml").read_text(encoding="utf-8")
    assert "- name: se_esef_domains" in text
    assert "asset_key: [esef_domains_clickhouse]" in text
