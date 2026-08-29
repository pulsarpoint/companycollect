"""Execute Platsbanken-generated SQL against a real ClickHouse parser."""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_platsbanken import tables
from dagster_v3.defs.sweden_platsbanken.clickhouse import (
    append_stage_insert_sql,
    company_history_insert_sql,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command


pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
INITIAL_MIGRATION = MIGRATIONS_DIR / "000302_corpscout_se_platsbanken_jobs.up.sql"
CONTACTS_MIGRATION = (
    MIGRATIONS_DIR / "000303_corpscout_se_platsbanken_job_contacts.up.sql"
)


def test_generated_final_alias_queries_execute_in_clickhouse() -> None:
    versions_stage = "corpscout._stage_versions"
    intervals_stage = "corpscout._stage_intervals"
    history_stage = "corpscout._stage_history"
    statements = (
        INITIAL_MIGRATION.read_text(encoding="utf-8"),
        CONTACTS_MIGRATION.read_text(encoding="utf-8"),
        """
        CREATE TABLE corpscout.se_companies (company_id String)
        ENGINE = ReplacingMergeTree
        ORDER BY company_id
        """,
        f"CREATE TABLE {versions_stage} AS corpscout.{tables.VERSIONS_TABLE}",
        f"CREATE TABLE {intervals_stage} AS corpscout.{tables.INTERVALS_TABLE}",
        f"CREATE TABLE {history_stage} AS corpscout.{tables.COMPANY_HISTORY_TABLE}",
        append_stage_insert_sql(
            target=f"corpscout.{tables.VERSIONS_TABLE}",
            stage=versions_stage,
            uid_column="version_uid",
            columns=tables.VERSION_COLUMNS,
        ),
        company_history_insert_sql(
            intervals_stage=intervals_stage,
            history_stage=history_stage,
        ),
    )
    script = (
        ";\n".join(statement.strip().rstrip(";") for statement in statements) + ";\n"
    )

    try:
        completed = subprocess.run(
            _clickhouse_local_command(),
            input=script,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")

    assert completed.returncode == 0, completed.stderr or completed.stdout
