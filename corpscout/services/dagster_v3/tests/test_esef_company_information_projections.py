from dagster import AssetKey

from dagster_v3.defs.esef_filings.company_information_projections import (
    esef_company_description_observation_sql,
    esef_document_business_items_sql,
    esef_document_group_relationships_sql,
    esef_document_people_sql,
)


def test_esef_company_information_has_one_sql_projection_per_serving_table() -> None:
    statements = {
        "company_description_observations": (
            esef_company_description_observation_sql()
        ),
        "esef_document_people": esef_document_people_sql(),
        "esef_document_business_items": esef_document_business_items_sql(),
        "esef_document_group_relationships": (esef_document_group_relationships_sql()),
    }

    for table_name, statement in statements.items():
        assert f"INSERT INTO corpscout.{table_name}" in statement
        assert "FROM corpscout.esef_document_company_information AS info" in statement
        assert "source_record_uid" in statement
        assert "evidence_ids" in statement
        assert "DELETE" not in statement
        assert "ALTER TABLE" not in statement

    assert "people_json" in statements["esef_document_people"]
    for json_column in (
        "products_and_services_json",
        "customer_markets_json",
        "operating_geographies_json",
        "business_segments_json",
    ):
        assert json_column in statements["esef_document_business_items"]
    assert (
        "material_group_relationships_json"
        in statements["esef_document_group_relationships"]
    )


def test_esef_company_information_projections_are_separate_esef_assets() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    expected_assets = {
        AssetKey("esef_company_description_observations_clickhouse"),
        AssetKey("esef_document_people_clickhouse"),
        AssetKey("esef_document_business_items_clickhouse"),
        AssetKey("esef_document_group_relationships_clickhouse"),
    }

    for asset_key in expected_assets:
        node = repository.asset_graph.get(asset_key)
        assert node.group_name == "esef_filings"
        assert node.parent_keys == {
            AssetKey("esef_document_company_information_clickhouse")
        }

    assert not repository.asset_graph.has(
        AssetKey("esef_document_observations_clickhouse")
    )
