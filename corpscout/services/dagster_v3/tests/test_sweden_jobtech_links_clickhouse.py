from pathlib import Path

from dagster_v3.defs.sweden_jobtech_links import tables
from dagster_v3.defs.sweden_jobtech_links.clickhouse import (
    active_intervals_insert_sql,
    append_stage_insert_sql,
    job_ads_insert_sql,
)


MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000363_corpscout_se_jobtech_links_jobs.up.sql"
)
SERVING_MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000367_corpscout_se_jobtech_links_job_ads.up.sql"
)


def test_partition_replay_is_idempotent_by_each_table_uid() -> None:
    for _, target_table, columns, uid_column in tables.CLICKHOUSE_APPEND_TABLES:
        sql = append_stage_insert_sql(
            target=f"corpscout.{target_table}",
            stage=f"corpscout._stage_{target_table}",
            uid_column=uid_column,
            columns=columns,
        )

        assert f"LEFT ANTI JOIN corpscout.{target_table} AS existing FINAL" in sql
        assert f"existing.{uid_column} = incoming.{uid_column}" in sql
        assert "FINAL AS" not in sql


def test_clickhouse_export_maps_all_normalized_duckdb_tables() -> None:
    assert tables.CLICKHOUSE_APPEND_TABLES == (
        (
            tables.SNAPSHOTS_TABLE,
            tables.CLICKHOUSE_SNAPSHOTS_TABLE,
            tables.SNAPSHOT_COLUMNS,
            "snapshot_uid",
        ),
        (
            tables.VERSIONS_TABLE,
            tables.CLICKHOUSE_VERSIONS_TABLE,
            tables.VERSION_COLUMNS,
            "version_uid",
        ),
        (
            tables.OBSERVATIONS_TABLE,
            tables.CLICKHOUSE_OBSERVATIONS_TABLE,
            tables.OBSERVATION_COLUMNS,
            "observation_uid",
        ),
        (
            tables.LOCATIONS_TABLE,
            tables.CLICKHOUSE_LOCATIONS_TABLE,
            tables.LOCATION_COLUMNS,
            "location_uid",
        ),
        (
            tables.ENRICHMENTS_TABLE,
            tables.CLICKHOUSE_ENRICHMENTS_TABLE,
            tables.ENRICHMENT_COLUMNS,
            "enrichment_uid",
        ),
    )


def test_migration_owns_every_exported_clickhouse_column() -> None:
    migration_sql = MIGRATION.read_text(encoding="utf-8")

    for _, target_table, columns, _ in tables.CLICKHOUSE_APPEND_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{target_table}" in migration_sql
        for column in columns:
            assert f"    {column} " in migration_sql


def test_intervals_are_derived_from_presence_in_consecutive_snapshots() -> None:
    sql = active_intervals_insert_sql(
        intervals_stage="corpscout._stage_job_ad_active_intervals"
    )

    assert "argMax(snapshot_uid, tuple(retrieved_at, snapshot_uid))" in sql
    assert "snapshot_number != previous_snapshot_number + 1" in sql
    assert "next_snapshot.snapshot_number = interval.last_snapshot_number + 1" in sql
    assert "'first_absent_snapshot'" in sql
    assert "application_deadline" not in sql


def test_unified_job_ads_uses_latest_version_and_derived_status() -> None:
    sql = job_ads_insert_sql(
        intervals_stage="corpscout._stage_job_ad_active_intervals",
        job_ads_stage="corpscout._stage_job_ads",
    )

    assert "if(interval.active_to IS NULL, 'active', 'expired') AS status" in sql
    assert "ORDER BY interval_number DESC" in sql
    assert "version.version_uid = observation.version_uid" in sql
    assert "latest_snapshot.snapshot_date AS resolved_against_snapshot_date" in sql
    assert "application_deadline" in sql


def test_serving_migration_replaces_current_only_table_with_unified_job_ads() -> None:
    migration_sql = SERVING_MIGRATION.read_text(encoding="utf-8")

    assert "DROP TABLE IF EXISTS corpscout.se_jobtech_links_job_ad_current" in (
        migration_sql
    )
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.se_jobtech_links_job_ads" in migration_sql
    )
    for column in tables.JOB_AD_COLUMNS:
        assert f"    {column} " in migration_sql
    assert "CHECK status IN ('active', 'expired')" in migration_sql
