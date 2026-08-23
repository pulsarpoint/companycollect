from pathlib import Path

from dagster_v3.defs.sweden_platsbanken import tables
from dagster_v3.defs.sweden_platsbanken.clickhouse import (
    append_stage_insert_sql,
    company_history_insert_sql,
    intervals_insert_sql,
    monthly_insert_sql,
)


INITIAL_MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000302_corpscout_se_platsbanken_jobs.up.sql"
)
CONTACTS_MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000303_corpscout_se_platsbanken_job_contacts.up.sql"
)


def test_history_append_is_idempotent_by_deterministic_uid() -> None:
    sql = append_stage_insert_sql(
        target="corpscout.se_platsbanken_job_ad_versions",
        stage="corpscout._stage_versions",
        uid_column="version_uid",
        columns=tables.VERSION_COLUMNS,
    )

    assert "LEFT ANTI JOIN corpscout.se_platsbanken_job_ad_versions FINAL" in sql
    assert "existing.version_uid = incoming.version_uid" in sql


def test_active_intervals_are_derived_from_status_changes() -> None:
    sql = intervals_insert_sql("corpscout._interval_stage")

    assert "FROM corpscout.se_platsbanken_job_ad_events FINAL" in sql
    assert "lagInFrame" in sql
    assert "state_group" in sql
    assert "end_group.state_group = active_group.state_group + 1" in sql
    assert "active_to_basis" in sql
    assert "is_end_estimated" in sql


def test_company_history_uses_exact_organization_number_only() -> None:
    sql = company_history_insert_sql(
        intervals_stage="corpscout._interval_stage",
        history_stage="corpscout._history_stage",
    )

    assert "INNER ANY JOIN" in sql
    assert "company.company_id = latest.employer_org_number" in sql
    assert "employer_name =" not in sql
    assert "corpscout.se_platsbanken_job_ad_versions FINAL AS version" in sql
    assert "GROUP BY\n            interval.source_job_ad_id" in sql


def test_monthly_rollup_keeps_hiring_activity_semantics() -> None:
    sql = monthly_insert_sql(
        history_stage="corpscout._history_stage",
        monthly_stage="corpscout._monthly_stage",
    )

    for metric in (
        "ads_published",
        "advertised_positions",
        "ads_closed",
        "active_ads_end_of_month",
        "active_positions_end_of_month",
        "median_open_days",
        "distinct_occupation_groups",
    ):
        assert metric in sql
    assert "arrayJoin" in sql


def test_migration_owns_every_exported_source_table_column() -> None:
    sql = INITIAL_MIGRATION.read_text(encoding="utf-8")
    contacts_sql = CONTACTS_MIGRATION.read_text(encoding="utf-8")
    complete_sql = f"{sql}\n{contacts_sql}"

    contracts = {
        tables.VERSIONS_TABLE: tables.VERSION_COLUMNS,
        tables.EVENTS_TABLE: tables.EVENT_COLUMNS,
        tables.REQUIREMENTS_TABLE: tables.REQUIREMENT_COLUMNS,
        tables.CONTACTS_TABLE: tables.CONTACT_COLUMNS,
    }
    for table_name, columns in contracts.items():
        table_sql = contacts_sql if table_name == tables.CONTACTS_TABLE else complete_sql
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in table_sql
        for column in columns:
            assert (
                f"    {column} " in table_sql
                or f"ADD COLUMN IF NOT EXISTS {column} " in table_sql
            )

    for column in (
        "application_email",
        "application_url",
        "application_other",
        "application_reference",
        "application_information",
        "application_via_af",
        "employer_email",
        "employer_phone",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column} " in contacts_sql
