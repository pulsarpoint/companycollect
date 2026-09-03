"""The register source tables' change-only publish, executed on a real ClickHouse.

Two claims a fake client cannot settle:

1. The anti-join's right side is the CURRENT row per company. A company whose payload goes
   A -> B -> A must get its third row inserted; a whole-table anti-join would suppress it
   and ReplacingMergeTree(observed_at) would keep the stale B row as current.
2. registration_date is Date32, not 000257's Date. A pre-1970 registration date has to
   survive insertion and read-back unclamped -- Date starts at 1970-01-01.

The script applies the real migration DDL and the real anti-join text
(sweden_company_source_anti_join_sql), never a paraphrase.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import (
    sweden_company_source_anti_join_sql,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000374_corpscout_se_bolagsverket_companies.up.sql",
)
APPLIED_PREFIXES = ("CREATE DATABASE", "CREATE TABLE")


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(APPLIED_PREFIXES):
                statements.append(statement)
    return statements


def _run(script_statements: list[str]) -> list[str]:
    script = ";\n".join(script_statements) + ";\n"
    completed = subprocess.run(
        _clickhouse_local_command(),
        input=script,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _scb_row(payload_hash: str, legal_name: str, observed_at: str) -> str:
    """One se_scb_companies row: only legal_name, source_payload_hash and observed_at vary."""
    return (
        "('5560000000', '5560000000', "
        f"'{legal_name}', NULL, '49', '1', '1', toDate32('1962-03-04'), "
        "'62010', NULL, NULL, NULL, NULL, "
        "NULL, 'Main Street 1', '11122', 'STOCKHOLM', '1', "
        f"'run-1', '5560000000', '{payload_hash}', "
        f"toDateTime64('{observed_at}', 3, 'UTC'))"
    )


def _publish(stage_rows: str) -> list[str]:
    """Stage the given rows and run the real anti-join insert against the real target."""
    columns = ", ".join(tables.SE_SCB_COMPANIES_EXPORT_COLUMNS)
    anti_join = sweden_company_source_anti_join_sql(
        qualified_stage="corpscout.stage_scb",
        qualified_target=tables.QUALIFIED_SCB_COMPANIES_TABLE,
    )
    selected = ", ".join(
        f"candidate.{column}" for column in tables.SE_SCB_COMPANIES_EXPORT_COLUMNS
    )
    return [
        "DROP TABLE IF EXISTS corpscout.stage_scb",
        f"CREATE TABLE corpscout.stage_scb AS {tables.QUALIFIED_SCB_COMPANIES_TABLE}",
        f"INSERT INTO corpscout.stage_scb ({columns}) VALUES {stage_rows}",
        f"SELECT count() AS new_versions {anti_join}",
        f"INSERT INTO {tables.QUALIFIED_SCB_COMPANIES_TABLE} ({columns})\n"
        f"SELECT {selected} {anti_join}",
    ]


def _bolagsverket_row(payload_hash: str, observed_at: str) -> str:
    """One se_bolagsverket_companies row with both dates before 1970."""
    return (
        "('5561111111', '5561111111$ORGNR-IDORG', '1', 'SE-LAND', "
        "'Closed AB', 'Closed AB$FORETAGSNAMN-ORGNAM$1955-01-01', 'AB-ORGFO', "
        "toDate32('1955-01-01'), toDate32('1968-12-31'), 'OVERK-AVORG', "
        "NULL, 'Closed activity', 'Closed Street 2$$GOTEBORG$41111$SE-LAND', "
        f"'run-1', '5561111111$ORGNR-IDORG', '{payload_hash}', "
        f"toDateTime64('{observed_at}', 3, 'UTC'))"
    )


def _publish_bolagsverket(stage_rows: str) -> list[str]:
    columns = ", ".join(tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS)
    anti_join = sweden_company_source_anti_join_sql(
        qualified_stage="corpscout.stage_bolagsverket",
        qualified_target=tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE,
    )
    selected = ", ".join(
        f"candidate.{column}"
        for column in tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS
    )
    return [
        "DROP TABLE IF EXISTS corpscout.stage_bolagsverket",
        "CREATE TABLE corpscout.stage_bolagsverket AS "
        f"{tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE}",
        f"INSERT INTO corpscout.stage_bolagsverket ({columns}) VALUES {stage_rows}",
        f"SELECT count() AS new_versions {anti_join}",
        f"INSERT INTO {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} ({columns})\n"
        f"SELECT {selected} {anti_join}",
    ]


def test_the_anti_join_reinserts_a_reverted_payload_and_skips_an_unchanged_one() -> None:
    output = _run(
        [
            *_schema_statements(),
            # Publish A, then B, then A again, then B again unchanged.
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-01 06:00:00")),
            *_publish(_scb_row("hash-b", "Acme Bolag AB", "2026-09-02 06:00:00")),
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-03 06:00:00")),
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-04 06:00:00")),
            f"SELECT count() FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE}",
            "SELECT legal_name, toString(source_payload_hash) "
            f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL",
        ]
    )

    # The four anti-join counts, then the row count, then the current row.
    assert output[:4] == ["1", "1", "1", "0"]
    assert output[4] == "3"
    assert output[5] == "Acme AB\thash-a"


def test_a_pre_1970_registration_date_survives_the_round_trip() -> None:
    output = _run(
        [
            *_schema_statements(),
            *_publish(_scb_row("hash-a", "Acme AB", "2026-09-01 06:00:00")),
            *_publish_bolagsverket(_bolagsverket_row("bv-hash-a", "2026-09-01 06:00:00")),
            "SELECT toString(registration_date) "
            f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL",
            "SELECT toString(registration_date), toString(deregistration_date) "
            f"FROM {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} FINAL",
        ]
    )

    assert output[0] == "1"  # the SCB anti-join count
    assert output[1] == "1"  # the Bolagsverket anti-join count
    assert output[2] == "1962-03-04"
    assert output[3] == "1955-01-01\t1968-12-31"
