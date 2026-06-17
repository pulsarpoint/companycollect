from pathlib import Path

from dagster_v3.defs.exchange_rates import tables as exchange_rate_tables
from dagster_v3.defs.finland_resolved import tables as finland_resolved_tables
from dagster_v3.defs.nace import tables as nace_tables
from dagster_v3.defs.norway_brreg import tables as norway_brreg_tables


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"

EXPECTED_MIGRATIONS = (
    "0001_reference_nace_categories.sql",
    "0002_reference_exchange_rates.sql",
    "0003_norway_brreg_companies.sql",
    "0004_norway_brreg_financial_statements.sql",
    "0005_corpscout_fi_companies.sql",
    "0006_corpscout_fi_websites.sql",
    "0007_corpscout_fi_industries.sql",
    "0008_corpscout_fi_financial_statements.sql",
    "0009_corpscout_fi_financial_metrics.sql",
)

FINLAND_FINANCIAL_STATEMENT_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "registration_date",
    "source_url",
    "xml_object_key",
    "xml_sha256",
    "xml_size_bytes",
    "reported_business_id",
    "reported_company_name",
    "period_start",
    "period_end",
    "contexts_count",
    "units_count",
    "facts_count",
    "validation_warnings",
    "parser_version",
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
)

FINLAND_FINANCIAL_METRIC_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "period_start",
    "period_end",
    "currency_original",
    "revenue_amount_original",
    "revenue_amount_usd",
    "operating_profit_loss_amount_original",
    "operating_profit_loss_amount_usd",
    "profit_loss_amount_original",
    "profit_loss_amount_usd",
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
    "current_receivables_amount_original",
    "current_receivables_amount_usd",
    "current_liabilities_amount_original",
    "current_liabilities_amount_usd",
    "personnel_expenses_amount_original",
    "personnel_expenses_amount_usd",
    "wages_and_salaries_amount_original",
    "wages_and_salaries_amount_usd",
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "metric_warnings",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_converted_at",
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
)


def test_clickhouse_migration_files_are_explicit() -> None:
    migration_files = tuple(path.name for path in sorted(MIGRATIONS_DIR.glob("*.sql")))

    assert migration_files == EXPECTED_MIGRATIONS


def test_clickhouse_migrations_create_databases_and_tables() -> None:
    for migration_file in EXPECTED_MIGRATIONS:
        sql = _migration_sql(migration_file)

        assert "CREATE DATABASE IF NOT EXISTS" in sql
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "TRUNCATE" not in sql.upper()


def test_finland_resolved_migrations_use_corpscout_database() -> None:
    for migration_file in EXPECTED_MIGRATIONS:
        sql = _migration_sql(migration_file)

        assert "corpscout_resolved" not in sql

    for migration_file in EXPECTED_MIGRATIONS[4:]:
        assert "CREATE DATABASE IF NOT EXISTS corpscout" in _migration_sql(migration_file)


def test_clickhouse_migrations_match_existing_python_ddl_constants() -> None:
    expected_ddl_by_file = {
        "0001_reference_nace_categories.sql": nace_tables.NACE_CATEGORIES_DDL,
        "0002_reference_exchange_rates.sql": exchange_rate_tables.EXCHANGE_RATES_DDL,
        "0003_norway_brreg_companies.sql": norway_brreg_tables.COMPANIES_DDL,
        "0004_norway_brreg_financial_statements.sql": (
            norway_brreg_tables.FINANCIAL_STATEMENTS_DDL
        ),
    }

    for migration_file, expected_ddl in expected_ddl_by_file.items():
        assert _normalize_sql(expected_ddl) in _normalize_sql(_migration_sql(migration_file))


def test_finland_resolved_migrations_cover_exported_columns() -> None:
    migration_file_by_table = {
        finland_resolved_tables.FI_COMPANIES_TABLE: (
            "0005_corpscout_fi_companies.sql"
        ),
        finland_resolved_tables.FI_WEBSITES_TABLE: (
            "0006_corpscout_fi_websites.sql"
        ),
        finland_resolved_tables.FI_INDUSTRIES_TABLE: (
            "0007_corpscout_fi_industries.sql"
        ),
    }

    assert set(migration_file_by_table) == set(finland_resolved_tables.FINLAND_YTJ_RESOLVED_TABLES)

    for table_name, migration_file in migration_file_by_table.items():
        sql = _migration_sql(migration_file)

        for column_name in finland_resolved_tables.RESOLVED_TABLE_COLUMNS[table_name]:
            assert f"    {column_name} " in sql


def test_finland_financial_migrations_cover_statements_and_usd_metrics() -> None:
    financial_statements_sql = _migration_sql(
        "0008_corpscout_fi_financial_statements.sql"
    )
    financial_metrics_sql = _migration_sql("0009_corpscout_fi_financial_metrics.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_financial_statements" in (
        financial_statements_sql
    )
    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_financial_metrics" in (
        financial_metrics_sql
    )

    for column_name in FINLAND_FINANCIAL_STATEMENT_COLUMNS:
        assert f"    {column_name} " in financial_statements_sql

    for column_name in FINLAND_FINANCIAL_METRIC_COLUMNS:
        assert f"    {column_name} " in financial_metrics_sql


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text()


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.replace(";", "").split())
