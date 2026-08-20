from pathlib import Path


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"

SOURCE_VIEW_COLUMNS = (
    "source_id",
    "accounting_scope",
    "company_id",
    "source_document_id",
    "fiscal_year",
    "report_period_start",
    "report_period_end",
    "currency",
    "revenue_amount_original",
    "revenue_amount_usd",
    "operating_result_amount_original",
    "operating_result_amount_usd",
    "net_result_amount_original",
    "net_result_amount_usd",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "equity_amount_original",
    "equity_amount_usd",
    "liabilities_amount_original",
    "liabilities_amount_usd",
    "cash_and_bank_amount_original",
    "cash_and_bank_amount_usd",
    "current_assets_amount_original",
    "current_assets_amount_usd",
    "current_liabilities_amount_original",
    "current_liabilities_amount_usd",
    "personnel_expenses_amount_original",
    "personnel_expenses_amount_usd",
    "wages_and_salaries_amount_original",
    "wages_and_salaries_amount_usd",
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "observation",
    "source_fiscal_year",
    "source_record_uids",
    "source_url",
    "viewer_url",
)


def test_source_views_have_the_same_canonical_contract() -> None:
    sql = _migration_sql("000286_corpscout_se_financial_source_views.up.sql")
    bolagsverket_sql, esef_sql = _view_bodies(sql)

    for column in SOURCE_VIEW_COLUMNS:
        assert column in bolagsverket_sql, f"Bolagsverket view missing {column}"
        assert column in esef_sql, f"ESEF view missing {column}"


def test_source_views_keep_sources_separate_and_preserve_esef_amendments() -> None:
    sql = _migration_sql("000286_corpscout_se_financial_source_views.up.sql")
    bolagsverket_sql, esef_sql = _view_bodies(sql)

    assert (
        "FROM corpscout.se_bolagsverket_financial_metrics AS metrics FINAL"
        in bolagsverket_sql
    )
    assert "corpscout.esef_financial_metrics" not in bolagsverket_sql
    assert "argMax(" in bolagsverket_sql
    assert "GROUP BY metrics.company_id, metrics.fiscal_year" in bolagsverket_sql
    assert "LIMIT 1 BY" not in bolagsverket_sql
    assert "FROM corpscout.esef_financial_metrics AS metrics FINAL" in esef_sql
    assert "corpscout.se_bolagsverket_financial_metrics" not in esef_sql
    assert "UNION" not in sql.upper()
    assert "argMaxIf" in esef_sql
    assert "arrayDistinct(arrayFilter" in esef_sql
    assert "ProfitFromPropertyManagement" not in sql


def test_source_view_down_migration_removes_both_views() -> None:
    down_sql = _migration_sql("000286_corpscout_se_financial_source_views.down.sql")

    assert "DROP VIEW IF EXISTS corpscout.se_financials_esef_current" in down_sql
    assert (
        "DROP VIEW IF EXISTS corpscout.se_financials_bolagsverket_current" in down_sql
    )


def _view_bodies(sql: str) -> tuple[str, str]:
    marker = "CREATE OR REPLACE VIEW corpscout.se_financials_esef_current AS"
    bolagsverket_sql, esef_body = sql.split(marker, maxsplit=1)
    return bolagsverket_sql, f"{marker}{esef_body}"


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")
