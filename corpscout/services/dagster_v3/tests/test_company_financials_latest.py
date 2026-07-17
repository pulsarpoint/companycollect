from pathlib import Path

from dagster_v3.defs.company_financials_latest.tables import (
    COMPANY_FINANCIALS_LATEST_COLUMNS,
    COMPANY_FINANCIALS_LATEST_TABLES,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _migration_sql() -> str:
    return (MIGRATIONS_DIR / "000137_corpscout_company_financials_latest.up.sql").read_text()


def test_migration_creates_every_summary_table_with_full_schema() -> None:
    sql = _migration_sql()
    assert len(COMPANY_FINANCIALS_LATEST_TABLES) == 8
    for table in COMPANY_FINANCIALS_LATEST_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in sql
    for column in COMPANY_FINANCIALS_LATEST_COLUMNS:
        assert f"    {column} " in sql
