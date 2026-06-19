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


def test_raw_sources_cover_four_files():
    assert set(tables.FINANCIAL_RAW_SOURCES) == {
        tables.FINANCIAL_STATEMENTS_RAW_TABLE,
        tables.BALANCE_SHEETS_RAW_TABLE,
        tables.INCOME_STATEMENTS_RAW_TABLE,
        tables.CASH_FLOW_STATEMENTS_RAW_TABLE,
    }
    assert all(url.startswith("https://data.gov.lv/") for url in tables.FINANCIAL_RAW_SOURCES.values())
