from pathlib import Path

from dagster_v3.defs.latvia_ur import tables

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "clickhouse"
    / "migrations"
    / "000016_corpscout_lv_financial_statements.up.sql"
).read_text()


def test_numeric_groups_have_expected_counts():
    assert len(tables.BALANCE_NUMERIC_COLUMNS) == 16
    assert len(tables.INCOME_NUMERIC_COLUMNS) == 26
    assert len(tables.CASHFLOW_NUMERIC_COLUMNS) == 35


def test_wide_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE}"
        in MIGRATION
    )
    for column in tables.LV_FINANCIAL_STATEMENTS_COLUMNS:
        assert f"    {column} " in MIGRATION, f"missing {column} in migration"


def test_wide_columns_are_unique_and_ordered():
    cols = tables.LV_FINANCIAL_STATEMENTS_COLUMNS
    assert len(cols) == len(set(cols))
    assert cols[0] == "country_iso2"
    assert cols[6:9] == ("statement_id", "file_id", "regcode")
    assert cols[-2:] == ("source_url", "raw_financial_record")


METRICS_MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "clickhouse"
    / "migrations"
    / "000019_corpscout_lv_financial_metrics.up.sql"
).read_text()


def test_metric_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE}"
        in METRICS_MIGRATION
    )
    for column in tables.LV_FINANCIAL_METRICS_COLUMNS:
        assert f"    {column} " in METRICS_MIGRATION, f"missing {column}"


def test_each_metric_source_column_exists_in_wide_schema():
    for source_col in tables.FINANCIAL_METRIC_SOURCE_COLUMNS.values():
        assert source_col in tables.LV_FINANCIAL_STATEMENTS_COLUMNS


def test_export_columns_drop_large_provenance_columns():
    assert tables.CLICKHOUSE_EXCLUDED_COLUMNS == {
        "raw_entity",
        "raw_financial_record",
        "source_payload_hash",
    }
    # the ClickHouse export omits the big/incompressible provenance columns ...
    for export in (
        tables.LV_COMPANIES_EXPORT_COLUMNS,
        tables.LV_FINANCIAL_STATEMENTS_EXPORT_COLUMNS,
        tables.LV_FINANCIAL_METRICS_EXPORT_COLUMNS,
    ):
        assert not (set(export) & tables.CLICKHOUSE_EXCLUDED_COLUMNS)
    # ... but they remain in the full DuckDB/migration tuples (staging keeps them).
    assert "raw_entity" in tables.LV_COMPANIES_COLUMNS
    assert "raw_financial_record" in tables.LV_FINANCIAL_STATEMENTS_COLUMNS
    assert "source_payload_hash" in tables.LV_FINANCIAL_METRICS_COLUMNS


def test_raw_sources_cover_four_files():
    assert set(tables.FINANCIAL_RAW_SOURCES) == {
        tables.FINANCIAL_STATEMENTS_RAW_TABLE,
        tables.BALANCE_SHEETS_RAW_TABLE,
        tables.INCOME_STATEMENTS_RAW_TABLE,
        tables.CASH_FLOW_STATEMENTS_RAW_TABLE,
    }
    assert all(url.startswith("https://data.gov.lv/") for url in tables.FINANCIAL_RAW_SOURCES.values())
