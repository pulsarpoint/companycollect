"""The register source tables' change-only publish, executed on a real ClickHouse.

Three claims a fake client cannot settle:

1. The anti-join's right side is the CURRENT row per company. A company whose payload goes
   A -> B -> A must get its third row inserted; a whole-table anti-join would suppress it
   and ReplacingMergeTree(observed_at) would keep the stale B row as current.
2. registration_date is Date32, not 000257's Date. A pre-1970 registration date has to
   survive insertion and read-back unclamped -- Date starts at 1970-01-01.
3. A date the DuckDB producer bounds away (final review Important-1: a pre-1900 value is
   outside Date32's own range, and ClickHouse's native driver silently flattens an
   out-of-range Date32 to 1970-01-01 instead of NULL) must publish as NULL, not that
   fabricated 1970-01-01, while an in-range pre-1970 value in the same batch survives --
   and registration_date_raw keeps the source string either way.
4. A company a delivery drops is tombstoned exactly once (has_company = 0) however many
   deliveries it stays absent from, and is inserted again when it returns.

The script applies the real migration DDL and the real anti-join, removal and tombstone
texts (sweden_company_source_anti_join_sql, sweden_company_source_removal_sql,
sweden_company_source_tombstone_insert_sql), never a paraphrase; the pre-1900 case runs
the real producer bound (normalized_duckdb._date32_bounded_sql) against a real DuckDB
connection rather than paraphrasing it too.
"""

import subprocess
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import (
    sweden_company_source_anti_join_sql,
    sweden_company_source_removal_sql,
    sweden_company_source_tombstone_insert_sql,
)
from dagster_v3.defs.sweden_company.normalized_duckdb import _date32_bounded_sql
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
        f"'{legal_name}', NULL, '49', '1', '1', toDate32('1962-03-04'), '19620304', "
        "'62010', NULL, NULL, NULL, NULL, "
        "NULL, 'Main Street 1', '11122', 'STOCKHOLM', '1', "
        f"'run-1', '5560000000', '{payload_hash}', "
        f"toDateTime64('{observed_at}', 3, 'UTC'))"
    )


def _scb_company_row(company_id: str, payload_hash: str, observed_at: str) -> str:
    """One se_scb_companies row for an arbitrary company: for the removal cases, where two
    companies have to come and go independently."""
    return (
        f"('{company_id}', '{company_id}', "
        f"'Company {company_id}', NULL, '49', '1', '1', toDate32('1962-03-04'), "
        "'19620304', "
        "'62010', NULL, NULL, NULL, NULL, "
        "NULL, 'Main Street 1', '11122', 'STOCKHOLM', '1', "
        f"'run-1', '{company_id}', '{payload_hash}', "
        f"toDateTime64('{observed_at}', 3, 'UTC'))"
    )


def _publish(stage_rows: str) -> list[str]:
    """Stage the given rows and run the real anti-join insert against the real target."""
    columns = ", ".join(tables.SE_SCB_COMPANIES_EXPORT_COLUMNS)
    anti_join = sweden_company_source_anti_join_sql(
        qualified_stage="corpscout.stage_scb",
        qualified_target=tables.QUALIFIED_SCB_COMPANIES_TABLE,
    )
    removal = sweden_company_source_removal_sql(
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
        f"SELECT count() AS removals {removal}",
        sweden_company_source_tombstone_insert_sql(
            qualified_stage="corpscout.stage_scb",
            qualified_target=tables.QUALIFIED_SCB_COMPANIES_TABLE,
        ),
    ]


def _bolagsverket_row(payload_hash: str, observed_at: str) -> str:
    """One se_bolagsverket_companies row with both dates before 1970."""
    return (
        "('5561111111', '5561111111$ORGNR-IDORG', '1', 'SE-LAND', "
        "'Closed AB', 'Closed AB$FORETAGSNAMN-ORGNAM$1955-01-01', 'AB-ORGFO', "
        "toDate32('1955-01-01'), '1955-01-01', "
        "toDate32('1968-12-31'), '1968-12-31', 'OVERK-AVORG', "
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
    removal = sweden_company_source_removal_sql(
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
        f"SELECT count() AS removals {removal}",
        sweden_company_source_tombstone_insert_sql(
            qualified_stage="corpscout.stage_bolagsverket",
            qualified_target=tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE,
        ),
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

    # (new_versions, removals) per publish -- one company, delivered every time, so never
    # removed -- then the row count, then the current row.
    assert output[:8] == ["1", "0", "1", "0", "1", "0", "0", "0"]
    assert output[8] == "3"
    assert output[9] == "Acme AB\thash-a"


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
    assert output[1] == "0"  # nothing dropped from the SCB delivery
    assert output[2] == "1"  # the Bolagsverket anti-join count
    assert output[3] == "0"  # nothing dropped from the Bolagsverket delivery
    assert output[4] == "1962-03-04"
    assert output[5] == "1955-01-01\t1968-12-31"


def _date32_bound(date_literal: str) -> str:
    """Run the producer's exact Date32 bound (normalized_duckdb._date32_bounded_sql)
    against a real DuckDB connection and translate the result to a ClickHouse literal:
    ``toDate32('...')`` for an in-range date, ``NULL`` when the producer bounds it away.
    """
    bounded_sql = _date32_bounded_sql(f"date '{date_literal}'")
    with duckdb.connect() as connection:
        bounded = connection.execute(f"select ({bounded_sql})::varchar").fetchone()[0]
    return "NULL" if bounded is None else f"toDate32('{bounded}')"


def _bolagsverket_row_with_registration_date(
    *,
    company_digits: str,
    payload_hash: str,
    observed_at: str,
    registration_date_sql: str,
    registration_date_raw: str,
) -> str:
    """One se_bolagsverket_companies row whose registration_date is whatever the caller
    supplies -- so the value published to ClickHouse is the real DuckDB producer's output
    (see _date32_bound), not a hand-typed paraphrase of it.

    ``registration_date_raw`` is registreringsdatum as delivered, which is what the
    producer keeps in the raw twin whether or not the Date32 bound nulls the typed column
    out (owner decision 2026-09-03)."""
    return (
        f"('{company_digits}', '{company_digits}$ORGNR-IDORG', '1', 'SE-LAND', "
        f"'Company {company_digits}', "
        f"'Company {company_digits}$FORETAGSNAMN-ORGNAM$2020-01-01', 'AB-ORGFO', "
        f"{registration_date_sql}, '{registration_date_raw}', NULL, NULL, NULL, "
        "NULL, 'Some activity', 'Some Street 1$$SOMETOWN$11111$SE-LAND', "
        f"'run-1', '{company_digits}$ORGNR-IDORG', '{payload_hash}', "
        f"toDateTime64('{observed_at}', 3, 'UTC'))"
    )


def test_a_pre_1900_registration_date_is_published_as_null_alongside_a_pre_1970_one() -> (
    None
):
    """Final review Important-1: a pre-1900 registreringsdatum is outside Date32's own
    range, and ClickHouse's native driver silently flattens an out-of-range Date32 to
    1970-01-01 rather than NULL. The DuckDB producer must bound it away before it ever
    reaches ClickHouse, so it publishes as NULL here -- while a genuine pre-1970,
    post-1900 date in the same batch keeps its exact value (companion to the pre-1970
    survival case above)."""
    ancient = _date32_bound("1878-05-24")
    century = _date32_bound("1955-06-15")
    assert ancient == "NULL"
    assert century == "toDate32('1955-06-15')"

    rows = ", ".join(
        [
            _bolagsverket_row_with_registration_date(
                company_digits="5559990000",
                payload_hash="bolag-hash-ancient",
                observed_at="2026-09-01 06:00:00",
                registration_date_sql=ancient,
                registration_date_raw="1878-05-24",
            ),
            _bolagsverket_row_with_registration_date(
                company_digits="5559990001",
                payload_hash="bolag-hash-century",
                observed_at="2026-09-01 06:00:00",
                registration_date_sql=century,
                registration_date_raw="1955-06-15",
            ),
        ]
    )

    output = _run(
        [
            *_schema_statements(),
            *_publish_bolagsverket(rows),
            "SELECT company_id, toString(registration_date), registration_date_raw "
            f"FROM {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} FINAL "
            "ORDER BY company_id",
        ]
    )

    assert output[0] == "2"  # both companies are new -> both counted by the anti-join
    assert output[1] == "0"  # nothing dropped: the target was empty before this delivery
    # The bounded-away date is NULL in the typed column and still readable in the twin.
    assert output[2:] == [
        "5559990000\t\\N\t1878-05-24",
        "5559990001\t1955-06-15\t1955-06-15",
    ]


def test_a_company_the_delivery_drops_is_tombstoned_and_returns_with_a_fresh_row() -> None:
    """The removal is computed against the CURRENT row per company: a dropped company
    gets exactly one tombstone however many deliveries it stays absent from, and comes
    back as a delivered row because its hash differs from the tombstone's empty one."""
    both = (
        _scb_company_row("5560000000", "h-a", "2026-01-01 00:00:00")
        + ", "
        + _scb_company_row("5561111111", "h-b", "2026-01-01 00:00:00")
    )
    script = _schema_statements()
    script += _publish(both)
    script += _publish(_scb_company_row("5560000000", "h-a", "2026-01-08 00:00:00"))
    # The reader contract while B's tombstone is the current row, not after B returns.
    script += [
        f"SELECT count() FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL "
        "WHERE has_company = 1"
    ]
    script += _publish(_scb_company_row("5560000000", "h-a", "2026-01-15 00:00:00"))
    script += _publish(
        _scb_company_row("5560000000", "h-a", "2026-01-22 00:00:00")
        + ", "
        + _scb_company_row("5561111111", "h-b", "2026-01-22 00:00:00")
    )
    script += [
        "SELECT company_id, has_company, source_payload_hash, toString(observed_at) "
        f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL ORDER BY company_id",
        f"SELECT count() FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} "
        "WHERE company_id = '5561111111'",
        f"SELECT count() FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} FINAL "
        "WHERE has_company = 1",
        "SELECT company_id_raw, legal_name, registration_date, source_record_id "
        f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} "
        "WHERE company_id = '5561111111' AND has_company = 0",
        "SELECT toString(observed_at) "
        f"FROM {tables.QUALIFIED_SCB_COMPANIES_TABLE} WHERE has_company = 0",
    ]
    lines = _run(script)
    # (new_versions, removals) per publish: both new / B dropped.
    assert lines[:4] == ["2", "0", "0", "1"]
    # The delivered set while the tombstone is live: A alone, B read as removed.
    assert lines[4] == "1"
    # B still gone, no second tombstone / B back with its old payload, inserted again.
    assert lines[5:9] == ["0", "0", "1", "0"]
    assert lines[9] == "5560000000\t1\th-a\t2026-01-01 00:00:00.000"
    assert lines[10] == "5561111111\t1\th-b\t2026-01-22 00:00:00.000"
    assert lines[11] == "3"
    assert lines[12] == "2"
    # The tombstone carries no values at all: empty record id and hash, NULL everywhere
    # else, so a returning company never matches it on source_payload_hash.
    assert lines[13] == "\t\\N\t\\N\t"
    # It is stamped with the delivery that dropped the company, not the first one.
    assert lines[14] == "2026-01-08 00:00:00.000"
