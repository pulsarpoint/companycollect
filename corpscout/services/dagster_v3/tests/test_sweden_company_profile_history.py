from pathlib import Path

from dagster_v3.defs.sweden_company import tables


def test_sweden_profile_history_migration_owns_history_and_current_snapshots() -> None:
    """000257's file is unchanged: it still declares the registry pair, which stays on the
    server until migration 000375 retires it. What changed is that nothing in Dagster
    writes that pair any more (2026-09-03 SE basic-info design, section 3.1)."""
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
        tables.SE_COMPANY_PROCEEDING_OBSERVATION_COLUMNS,
        tables.SE_COMPANY_INDUSTRY_OBSERVATION_COLUMNS,
    ):
        assert all(f"\n    {column} " in migration for column in columns)


def test_the_sweden_module_no_longer_names_the_retired_registry_tables() -> None:
    """The register sources are published by se_scb_companies / se_bolagsverket_companies.
    Leaving the old constants behind would invite a reader to believe something still
    writes them."""
    # COMPANY_REGISTRY is the stem every retired constant shared; the unrelated
    # WIKIDATA_REGISTRY_SEED_SPEC stays.
    assert [name for name in dir(tables) if "COMPANY_REGISTRY" in name] == []
    assert tables.SCB_COMPANIES_TABLE_CH == "se_scb_companies"
    assert tables.BOLAGSVERKET_COMPANIES_TABLE_CH == "se_bolagsverket_companies"
