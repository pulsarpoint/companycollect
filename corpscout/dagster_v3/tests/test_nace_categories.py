from dagster_v3.defs.nace import tables


def test_dlt_clickhouse_destination_dependencies_are_available() -> None:
    import clickhouse_connect
    import dlt

    assert clickhouse_connect
    assert hasattr(dlt.destinations, "clickhouse")


def test_nace_categories_clickhouse_schema_contract() -> None:
    assert tables.NACE_DATABASE == "reference"
    assert tables.NACE_CATEGORIES_TABLE == "nace_categories"
    assert tables.QUALIFIED_NACE_CATEGORIES_TABLE == "reference.nace_categories"
    assert tables.NACE_CATEGORIES_COLUMNS == (
        "classification_version",
        "code",
        "normalized_code",
        "parent_code",
        "level",
        "section_code",
        "description_en",
        "concept_uri",
        "parent_concept_uri",
        "source_scheme_uri",
        "source_url",
        "source_payload_hash",
        "valid_from",
        "valid_to",
        "is_current",
        "source_run_id",
        "pulled_at",
    )
    assert "CREATE TABLE IF NOT EXISTS reference.nace_categories" in tables.NACE_CATEGORIES_DDL
    assert "ORDER BY (classification_version, normalized_code)" in tables.NACE_CATEGORIES_DDL
