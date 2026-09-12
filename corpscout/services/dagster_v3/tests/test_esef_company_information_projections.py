from dagster import AssetKey

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.company_information_projections import (
    _replace_projection,
    esef_document_business_items_sql,
    esef_document_group_relationships_sql,
    esef_document_people_sql,
)
from tests.test_esef_llm_enrichment import _FakeClickHouse


def test_esef_company_information_has_one_sql_projection_per_serving_table() -> None:
    statements = {
        "esef_document_people": esef_document_people_sql(),
        "esef_document_business_items": esef_document_business_items_sql(),
        "esef_document_group_relationships": (esef_document_group_relationships_sql()),
    }

    for table_name, statement in statements.items():
        assert f"INSERT INTO corpscout.{table_name}" in statement
        assert "source_record_uid" in statement
        assert "evidence_ids" in statement
        assert "DELETE" not in statement
        assert "ALTER TABLE" not in statement

    assert (
        "FROM corpscout.esef_document_people_extraction AS info"
        in statements["esef_document_people"]
    )
    for business_table in (
        "esef_document_business_items",
        "esef_document_group_relationships",
    ):
        assert (
            "FROM corpscout.esef_document_company_information AS info"
            in statements[business_table]
        )

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


def test_projections_write_lei_and_no_stamps() -> None:
    for sql in (
        esef_document_people_sql(),
        esef_document_business_items_sql(),
        esef_document_group_relationships_sql(),
    ):
        assert "info.lei," in sql
        assert "info.country_iso2" not in sql and "info.company_id" not in sql


def test_esef_company_information_projections_are_separate_esef_assets() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    business_assets = {
        AssetKey("esef_document_business_items_clickhouse"),
        AssetKey("esef_document_group_relationships_clickhouse"),
    }

    for asset_key in business_assets:
        node = repository.asset_graph.get(asset_key)
        assert node.group_name == "esef"
        assert node.parent_keys == {
            AssetKey("esef_document_company_information_clickhouse")
        }

    people_node = repository.asset_graph.get(
        AssetKey("esef_document_people_clickhouse")
    )
    assert people_node.group_name == "esef"
    assert people_node.parent_keys == {
        AssetKey("esef_document_people_extraction_clickhouse")
    }

    assert not repository.asset_graph.has(
        AssetKey("esef_document_observations_clickhouse")
    )
    for retired_asset in (
        "sweden_registry_company_source_records_clickhouse",
        "sweden_financial_company_source_records_clickhouse",
        "finland_financial_company_source_records_clickhouse",
        "esef_company_source_records_clickhouse",
        "wikidata_company_source_records_clickhouse",
        "esef_company_description_observations_clickhouse",
    ):
        assert not repository.asset_graph.has(AssetKey(retired_asset))


def test_people_projection_uses_the_spec_identity_and_one_row_per_key() -> None:
    sql = esef_document_people_sql()
    assert "'\\nesef_person\\n'" in sql
    assert "lowerUTF8(trim(replaceRegexpAll(JSONExtractString(item_json, 'name'), '\\\\s+', ' ')))" in sql
    assert "JSONExtractString(item_json, 'role_category')" in sql
    # 2026-09-12 ruling: the normalised role text joins the identity too -- a person's
    # committee seat is its own row, not merged into their other roles.
    assert "lowerUTF8(trim(replaceRegexpAll(JSONExtractString(item_json, 'role'), '\\\\s+', ' ')))" in sql
    assert "esef_typed_candidate" not in sql
    assert "info.extraction_status IN ('extracted', 'reused')" in sql
    assert "LIMIT 1 BY info.lei, info.fiscal_year, info.source_record_uid, candidate_uid" in sql
    assert "multiIf(JSONExtractString(item_json, 'status') = 'current', 0, JSONExtractString(item_json, 'status') = 'historical', 1, 2)" in sql
    # The identity is (name, role_category, role text); every item of one extraction row
    # shares extracted_at, so a tie is broken by the model's own list order, not
    # ClickHouse's unstable sort -- item_index must be zipped alongside item_json in the
    # ARRAY JOIN and be the ORDER BY's tail.
    assert (
        "arrayEnumerate(JSONExtractArrayRaw(info.people_json)) AS item_index" in sql
    )
    assert (
        "ORDER BY multiIf(JSONExtractString(item_json, 'status') = 'current', 0, "
        "JSONExtractString(item_json, 'status') = 'historical', 1, 2), "
        "info.extracted_at DESC, item_index"
    ) in sql
    staged = esef_document_people_sql(target="corpscout._tmp_esef_document_people_abc")
    assert staged.startswith("INSERT INTO corpscout._tmp_esef_document_people_abc")
    assert "info.extracted_at" in sql and "parseDateTime64BestEffortOrNull" not in sql


def test_people_projection_replace_stages_and_exchanges_in_order() -> None:
    clickhouse = _FakeClickHouse(
        [
            [
                (tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,),
                (tables.ESEF_DOCUMENT_PEOPLE_TABLE,),
            ],
            [],  # CREATE TABLE stage
            [],  # INSERT INTO stage
            [(3,)],  # SELECT count() FROM stage (the empty-rebuild guard)
            [(2,)],  # SELECT count() FROM target FINAL (the guard's pre-check)
            [],  # EXCHANGE TABLES
            [],  # DROP TABLE IF EXISTS
            [(3,)],  # SELECT count() FROM target FINAL (the returned row_count)
        ]
    )

    result = _replace_projection(
        clickhouse=clickhouse,
        table_name=tables.ESEF_DOCUMENT_PEOPLE_TABLE,
        statement_for=lambda target: esef_document_people_sql(target=target),
        source_table=tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,
    )

    statements = [sql for sql, _parameters in clickhouse.client.calls]
    create_index = next(
        index for index, sql in enumerate(statements) if sql.startswith("CREATE TABLE")
    )
    insert_index = next(
        index for index, sql in enumerate(statements) if sql.startswith("INSERT INTO")
    )
    count_indexes = [
        index
        for index, sql in enumerate(statements)
        if sql.startswith("SELECT count()")
    ]
    exchange_index = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("EXCHANGE TABLES")
    )
    drop_index = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("DROP TABLE IF EXISTS")
    )

    assert create_index < insert_index < exchange_index < drop_index
    # The guard's two counts are queried after the INSERT and before EXCHANGE; the
    # final row_count is queried once more, after the DROP.
    assert len(count_indexes) == 3
    assert insert_index < count_indexes[0] < count_indexes[1] < exchange_index
    assert count_indexes[2] > drop_index
    assert statements[insert_index].startswith(
        f"INSERT INTO {tables.ESEF_DATABASE}._tmp_{tables.ESEF_DOCUMENT_PEOPLE_TABLE}_"
    )
    assert result.metadata["row_count"] == 3
    assert result.metadata["table"] == tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE


def test_people_projection_replace_refuses_to_empty_a_populated_table() -> None:
    clickhouse = _FakeClickHouse(
        [
            [
                (tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,),
                (tables.ESEF_DOCUMENT_PEOPLE_TABLE,),
            ],
            [],  # CREATE TABLE stage
            [],  # INSERT INTO stage (rebuilds nothing this run)
            [(0,)],  # SELECT count() FROM stage -- empty
            [(5,)],  # SELECT count() FROM target FINAL -- 5 published rows
            [],  # DROP TABLE IF EXISTS (still issued from the finally block)
        ]
    )

    try:
        _replace_projection(
            clickhouse=clickhouse,
            table_name=tables.ESEF_DOCUMENT_PEOPLE_TABLE,
            statement_for=lambda target: esef_document_people_sql(target=target),
            source_table=tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,
        )
    except ValueError as error:
        assert "refusing to replace" in str(error)
        assert tables.ESEF_DOCUMENT_PEOPLE_TABLE in str(error)
        assert "5 rows" in str(error)
    else:
        raise AssertionError("an empty rebuild over a populated table must be refused")

    statements = [sql for sql, _parameters in clickhouse.client.calls]
    assert not any(sql.startswith("EXCHANGE TABLES") for sql in statements)
    assert any(sql.startswith("DROP TABLE IF EXISTS") for sql in statements)
