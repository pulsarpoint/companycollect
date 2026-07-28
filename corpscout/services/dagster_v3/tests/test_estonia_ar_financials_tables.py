from pathlib import Path

from dagster_v3.defs.estonia_ar import tables

MIG_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
STATEMENTS_MIGRATION = (
    MIG_DIR / "000025_corpscout_ee_financial_statements.up.sql"
).read_text()
METRICS_MIGRATION = (MIG_DIR / "000026_corpscout_ee_financial_metrics.up.sql").read_text()


def test_nine_files_report_general_plus_seven_years():
    assert tables.EE_FINANCIAL_YEARS == (2019, 2020, 2021, 2022, 2023, 2024, 2025)
    assert len(tables.EE_FINANCIAL_RAW_SOURCES) == 8  # report-general + 7 years
    assert tables.REPORT_GENERAL_RAW_TABLE in tables.EE_FINANCIAL_RAW_SOURCES
    for year in tables.EE_FINANCIAL_YEARS:
        assert tables.key_indicators_raw_table(year) in tables.EE_FINANCIAL_RAW_SOURCES
        assert str(year) in tables.key_indicators_url(year)


def test_metric_element_map_matches_xbrl_vocabulary():
    assert tables.FINANCIAL_METRIC_NAMES == (
        "revenue",
        "gross_profit",
        "pretax_result",
        "net_result",
        "total_assets",
        "current_assets",
        "non_current_assets",
        "equity",
        "current_liabilities",
        "non_current_liabilities",
    )
    assert tables.EE_FINANCIAL_METRIC_ELEMENTS["revenue"] == "Revenue"
    assert tables.EE_FINANCIAL_METRIC_ELEMENTS["net_result"] == "TotalAnnualPeriodProfitLoss"
    assert tables.EE_FINANCIAL_METRIC_ELEMENTS["total_assets"] == "Assets"
    assert tables.EE_FINANCIAL_METRIC_ELEMENTS["gross_profit"] is None  # no Estonian element


def test_export_columns_drop_raw_and_hash():
    for full, export in (
        (tables.EE_FINANCIAL_STATEMENTS_COLUMNS, tables.EE_FINANCIAL_STATEMENTS_EXPORT_COLUMNS),
        (tables.EE_FINANCIAL_METRICS_COLUMNS, tables.EE_FINANCIAL_METRICS_EXPORT_COLUMNS),
    ):
        assert "source_payload_hash" in full
        assert not (set(export) & tables.CLICKHOUSE_EXCLUDED_COLUMNS)
    assert "raw_financial_record" in tables.EE_FINANCIAL_STATEMENTS_COLUMNS
    assert "raw_financial_record" not in tables.EE_FINANCIAL_STATEMENTS_EXPORT_COLUMNS


def test_statements_translation_and_usd_columns_present():
    # translatable enum carries an _en column
    assert "report_category_original" in tables.EE_FINANCIAL_STATEMENTS_EXPORT_COLUMNS
    assert "report_category_en" in tables.EE_FINANCIAL_STATEMENTS_EXPORT_COLUMNS
    # every metric carries native + USD on the metrics table
    for metric in tables.FINANCIAL_METRIC_NAMES:
        assert f"{metric}_amount_original" in tables.EE_FINANCIAL_METRICS_EXPORT_COLUMNS
        assert f"{metric}_amount_usd" in tables.EE_FINANCIAL_METRICS_EXPORT_COLUMNS
    for fx in ("fx_rate_to_usd", "fx_rate_date", "fx_source"):
        assert fx in tables.EE_FINANCIAL_METRICS_EXPORT_COLUMNS


def test_statements_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE}"
        in STATEMENTS_MIGRATION
    )
    for column in tables.EE_FINANCIAL_STATEMENTS_EXPORT_COLUMNS:
        assert f"    {column} " in STATEMENTS_MIGRATION, f"missing {column} in 000025"


def test_metrics_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_EE_FINANCIAL_METRICS_TABLE}"
        in METRICS_MIGRATION
    )
    for column in tables.EE_FINANCIAL_METRICS_EXPORT_COLUMNS:
        assert f"    {column} " in METRICS_MIGRATION, f"missing {column} in 000026"
