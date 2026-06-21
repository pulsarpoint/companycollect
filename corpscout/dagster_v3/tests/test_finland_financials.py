from pathlib import Path

from dagster_v3.defs.finland_financials import metrics, tables

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "clickhouse" / "migrations" / "000009_corpscout_fi_financial_metrics.up.sql"
).read_text()

# Synthetic Finnish plain dimensional XBRL: revenue (md103+x673, P&L duration) and
# total_assets (mi53+x360, balance-sheet instant).
STATEMENT = """<?xml version="1.0"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
            xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
            xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
            xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC">
  <xbrli:context id="c_dur"><xbrli:entity>
    <xbrli:identifier scheme="http://ytj.fi">0202458-3</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2023-10-01</xbrli:startDate>
    <xbrli:endDate>2024-09-30</xbrli:endDate></xbrli:period>
    <xbrli:scenario><xbrldi:explicitMember dimension="fi_dim:d">fi_MC:x673</xbrldi:explicitMember>
    </xbrli:scenario></xbrli:context>
  <xbrli:context id="c_inst"><xbrli:entity>
    <xbrli:identifier scheme="http://ytj.fi">0202458-3</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2024-09-30</xbrli:instant></xbrli:period>
    <xbrli:scenario><xbrldi:explicitMember dimension="fi_dim:d">fi_MC:x360</xbrldi:explicitMember>
    </xbrli:scenario></xbrli:context>
  <fi_met:md103 contextRef="c_dur" unitRef="ISO4217_EUR">43384.81</fi_met:md103>
  <fi_met:mi53 contextRef="c_inst" unitRef="ISO4217_EUR">52897.68</fi_met:mi53>
</xbrli:xbrl>
"""


def test_metric_map_loads():
    mapping = metrics.load_metric_map()
    assert mapping[("fi_met:md103", "fi_MC:x673")] == "revenue"
    assert mapping[("fi_met:mi53", "fi_MC:x360")] == "total_assets"


def test_extract_statement_metrics():
    from datetime import date
    from decimal import Decimal

    mapping = metrics.load_metric_map()
    result = metrics.extract_statement_metrics(STATEMENT.encode("utf-8"), mapping)
    assert result["period_end"] == date(2024, 9, 30)
    assert result["period_start"] == date(2023, 10, 1)
    assert result["currency"] == "EUR"
    assert result["metrics"]["revenue"] == Decimal("43384.81")
    assert result["metrics"]["total_assets"] == Decimal("52897.68")
    assert result["metrics"]["equity"] is None


def test_export_columns_match_migration():
    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_financial_metrics" in MIGRATION
    for column in tables.FI_FINANCIAL_METRICS_COLUMNS:
        assert f"    {column} " in MIGRATION, f"missing {column} in migration 000009"
    for metric in tables.FI_METRIC_NAMES:
        assert f"{metric}_amount_original" in tables.FI_FINANCIAL_METRICS_COLUMNS
        assert f"{metric}_amount_usd" in tables.FI_FINANCIAL_METRICS_COLUMNS


def test_job_registered():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {
        k.path[-1]
        for k in repo.get_job("finland_financials_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "finland_financials_metrics_duckdb",
        "finland_financials_metrics_usd_duckdb",
        "finland_financials_clickhouse_metrics",
    }
