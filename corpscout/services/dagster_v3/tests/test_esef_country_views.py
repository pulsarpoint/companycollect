"""The se_esef_* views: rendered from the module's column tuples, pinned to migration 000395."""

import re
from pathlib import Path

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse" / "migrations" / "000395_corpscout_esef_country_agnostic_products.up.sql"
)


def _normalized(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().rstrip(";")


def test_eight_views_one_per_swedish_consumer() -> None:
    assert [v.view for v in tables.SE_ESEF_VIEWS] == [
        "se_esef_filings", "se_esef_facts", "se_esef_disclosures",
        "se_esef_document_contact_candidates", "se_esef_document_company_information",
        "se_esef_document_people", "se_esef_document_business_items",
        "se_esef_document_group_relationships",
    ]
    for view in tables.SE_ESEF_VIEWS:
        assert view.view == f"se_{view.table}"  # se_ + esef_<table>
        assert "country_iso2" not in view.columns and "company_id" not in view.columns
        assert "lei" in view.columns


def test_view_sql_joins_the_verified_swedish_link_and_reads_replacing_tables_final() -> None:
    people = next(v for v in tables.SE_ESEF_VIEWS if v.table == "esef_document_people")
    sql = _normalized(build_se_esef_view_sql(people))
    assert sql.startswith("CREATE OR REPLACE VIEW corpscout.se_esef_document_people AS SELECT m.registry_id AS company_id,")
    assert "FROM corpscout.esef_document_people AS t FINAL" in sql
    assert ("INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL "
            "WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei") in sql
    disclosures = next(v for v in tables.SE_ESEF_VIEWS if v.table == "esef_disclosures")
    assert "AS t INNER JOIN" in _normalized(build_se_esef_view_sql(disclosures))  # MergeTree: no FINAL


def test_migration_000395_embeds_every_rendered_view() -> None:
    up = _normalized(MIGRATION.read_text(encoding="utf-8"))
    for view in tables.SE_ESEF_VIEWS:
        assert _normalized(build_se_esef_view_sql(view)) in up, view.view
    assert "ALTER TABLE corpscout.esef_entity_registry_map ADD COLUMN IF NOT EXISTS link_status LowCardinality(String) DEFAULT 'gleif' AFTER match_source" in up


def test_tuples_lost_the_stamps_and_the_model_outputs_gained_lei() -> None:
    for columns in (
        tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_EXPORT_COLUMNS,
        tables.ESEF_DOCUMENT_CONCEPT_LABELS_EXPORT_COLUMNS,
        tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS,
        tables.ESEF_DISCLOSURES_EXPORT_COLUMNS,
    ):
        assert "country_iso2" not in columns and "company_id" not in columns
    for columns in (
        tables.ESEF_DOCUMENT_PEOPLE_COLUMNS,
        tables.ESEF_DOCUMENT_BUSINESS_ITEM_COLUMNS,
        tables.ESEF_DOCUMENT_GROUP_RELATIONSHIP_COLUMNS,
    ):
        assert columns[:4] == ("candidate_uid", "source_record_uid", "source_document_id", "lei")
        assert "country_code" not in columns and "company_id" not in columns
    assert tables.ESEF_ENTITY_MAP_EXPORT_COLUMNS == (
        "lei", "country_iso2", "registry_id_raw", "registry_id", "match_source", "link_status", "source_run_id",
    )
