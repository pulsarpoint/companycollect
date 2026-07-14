from pathlib import Path

from dagster_v3.defs.gleif import tables


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"


def test_gleif_table_names_are_current_reference_tables() -> None:
    assert tables.GLEIF_TABLES == (
        "gleif_lei_records",
        "gleif_lei_names",
        "gleif_lei_addresses",
        "gleif_lei_identifiers",
        "gleif_lei_relationships",
        "gleif_lei_relationship_periods",
        "gleif_lei_reporting_exceptions",
        "gleif_lei_issuers",
        "gleif_code_list_entries",
    )


def test_gleif_clickhouse_column_contracts_are_defined_for_all_tables() -> None:
    assert set(tables.GLEIF_TABLE_COLUMNS) == set(tables.GLEIF_TABLES)
    for table_name, columns in tables.GLEIF_TABLE_COLUMNS.items():
        assert columns
        assert len(columns) == len(set(columns)), table_name
        assert "source_run_id" in columns
        assert "resolved_at" in columns


def test_gleif_migration_does_not_create_raw_manifest_table() -> None:
    up_sql = (MIGRATIONS_DIR / "000023_corpscout_gleif_reference_data.up.sql").read_text()
    assert "CREATE DATABASE IF NOT EXISTS corpscout" in up_sql
    assert "corpscout.gleif_raw_file_manifest" not in up_sql
    for table_name in tables.GLEIF_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in up_sql
