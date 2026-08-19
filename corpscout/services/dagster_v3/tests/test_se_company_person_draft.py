from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.company_people.draft import (
    PERSON_DRAFT_COLUMNS,
    _company_filter,
    build_person_draft_insert_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def test_migration_columns_match_source_observation_insert_contract() -> None:
    sql = (
        MIGRATIONS_DIR
        / "000290_corpscout_company_person_source_observations_and_role_types.up.sql"
    ).read_text(encoding="utf-8")

    for column in PERSON_DRAFT_COLUMNS:
        assert f"    {column} " in sql

    assert "se_company_person_draft_legacy" in sql
    assert "ENGINE = ReplacingMergeTree(created_at)" in sql
    assert "ORDER BY (company_id, source, draft_id)" in sql


def test_draft_sql_appends_unchanged_source_observations() -> None:
    sql = build_person_draft_insert_sql("`corpscout`.`_tmp_person_draft`")

    for table in (
        "corpscout.se_financial_report_signatories",
        "corpscout.esef_document_people",
        "corpscout.wikidata_company_people",
        "corpscout.wikidata_persons",
    ):
        assert table in sql
    for source in ("bolagsverket", "esef", "wikidata"):
        assert f"'{source}' AS source" in sql

    assert "toJSONString(CAST(tuple(" in sql
    assert "signatories.role_original" in sql
    assert "people.role_category" in sql
    assert "links.role_property" in sql
    assert "persons.description" in sql


def test_draft_id_uses_source_entity_and_semantic_hashes() -> None:
    sql = build_person_draft_insert_sql("`corpscout`.`_tmp_person_draft`")

    assert "se-company-person-source-observation-v1" in sql
    assert "source_entity_id" in sql
    assert "toString(person_profile_hash)" in sql
    assert "toString(person_role_hash)" in sql
    assert "FROM corpscout.se_company_person_draft FINAL" in sql
    assert "WHERE draft_id NOT IN" in sql


def test_draft_sql_does_not_match_merge_or_normalize_people() -> None:
    sql = build_person_draft_insert_sql("`corpscout`.`_tmp_person_draft`")

    for forbidden in (
        "country_person_match",
        "registry_name_anchors",
        "same_company_name_tokens",
        "multiple_sources_pending_llm",
        "name_normalized",
        "match_confidence",
        "company_person_role",
    ):
        assert forbidden not in sql


def test_company_scope_accepts_only_sweden_company_ids() -> None:
    assert _company_filter("rows.company_id", []) == "1"
    assert (
        _company_filter(
            "rows.company_id",
            ["5565200028", "5565200028", "5560003575"],
        )
        == "rows.company_id IN ('5560003575', '5565200028')"
    )

    with pytest.raises(ValueError, match="exactly 10 digits"):
        _company_filter("rows.company_id", ["SE5565200028"])


def test_draft_asset_is_wired_directly_to_source_assets() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    draft = repository.asset_graph.get(
        dg.AssetKey("se_company_person_draft_clickhouse")
    )

    assert draft.parent_keys == {
        dg.AssetKey("se_financial_report_signatories_clickhouse"),
        dg.AssetKey("esef_document_observations_clickhouse"),
        dg.AssetKey("sweden_company_companies_clickhouse"),
        dg.AssetKey("company_identifier_clickhouse"),
        dg.AssetKey("wikidata_company_identifiers"),
        dg.AssetKey("wikidata_company_people"),
        dg.AssetKey("wikidata_persons"),
    }

    job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "se_company_person_draft_job"
        ).asset_layer.executable_asset_keys
    }
    assert job_keys == {"se_company_person_draft_clickhouse"}
