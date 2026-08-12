from pathlib import Path

from dagster_v3.defs.sweden_company import tables


def test_sweden_profile_history_migration_owns_history_and_current_snapshots() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000257_corpscout_se_company_profile_history.up.sql"
    ).read_text(encoding="utf-8")

    for table_name in (
        "se_company_registry_observations",
        "se_company_registry_current",
        "se_company_proceeding_observations",
        "se_company_proceedings_current",
        "se_company_industry_observations",
        "se_company_industry_current",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in migration

    assert migration.count("PARTITION BY toYear(observed_at)") == 3
    assert migration.count("has_observation UInt8 DEFAULT 1") == 3
    for columns in (
        tables.SE_COMPANY_REGISTRY_OBSERVATION_COLUMNS,
        tables.SE_COMPANY_PROCEEDING_OBSERVATION_COLUMNS,
        tables.SE_COMPANY_INDUSTRY_OBSERVATION_COLUMNS,
    ):
        assert all(f"\n    {column} " in migration for column in columns)
