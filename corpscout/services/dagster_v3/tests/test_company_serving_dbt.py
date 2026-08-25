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
        "company_management_current_build",
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
        key=["corpscout", "se_companies"],
        metadata={
            "dagster/table_name": "corpscout.se_companies",
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


def test_serving_models_resolve_identity_and_evidence_offline() -> None:
    models = DBT_DIR / "models"
    external_ids = (
        models / "company_external_identifier_current_build.sql"
    ).read_text()
    gleif = (models / "company_gleif_current_build.sql").read_text()
    wikidata = (models / "company_wikidata_current_build.sql").read_text()
    management = (models / "company_management_current_build.sql").read_text()
    source_links = (models / "company_section_item_source_links_build.sql").read_text()
    presence = (models / "company_section_presence_current_build.sql").read_text()

    assert "ref('company_external_identifier_current_build')" in gleif
    assert "ref('company_external_identifier_current_build')" in wikidata
    assert "country_person_match" in management
    assert "GROUP BY country_code, company_id, identity_person_id" in management
    assert "identity_person_id AS person_id" in management
    assert "'registry-person|'" in management
    assert "observed_name_normalized" not in management
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
    assert "annual_report_signature" in source_links
    assert "public_knowledge_graph_company_role" in source_links
    company_domains = (models / "company_domains_build.sql").read_text()
    assert "source('corpscout', 'wikidata_company_domains')" in company_domains
    assert "source('corpscout', 'esef_document_contact_candidates')" in company_domains
    assert "source('corpscout', 'company_domain_suggestions_active')" in company_domains
    assert "reviewed_evidence_fingerprint" in company_domains
    assert "domains_without_current_source" in company_domains
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

    assert company_domains.count("INNER JOIN companies") == 4

    for model_name in (
        "company_description_current_build.sql",
        "company_contract_current_build.sql",
        "se_company_industry_display_current_build.sql",
    ):
        model = (models / model_name).read_text()
        assert "source('corpscout', 'se_companies')" in model
        assert "INNER JOIN company_anchors AS anchors" in model

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
        "management_identity_requires_accepted_evidence.sql",
        "section_presence_uses_supported_names.sql",
        "section_source_links_have_records.sql",
    }

    generic_tests = (DBT_DIR / "macros" / "company_serving_tests.sql").read_text()
    schema = (DBT_DIR / "models" / "schema.yml").read_text()
    assert "test company_serving_unique_key" in generic_tests
    assert "test company_serving_sweden_anchor" in generic_tests
    assert schema.count("company_serving_unique_key:") == 13
    assert schema.count("company_serving_sweden_anchor") == 13
