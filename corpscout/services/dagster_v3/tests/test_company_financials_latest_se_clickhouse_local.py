"""The Sweden leg of company_financials_latest against a real ClickHouse (clickhouse-local).

`toDate()` of a `Date32` outside the `Date` range (1970-01-01 to 2149-06-06) does not clamp
on 26.5, it WRAPS: `toDate(toDate32('1919-09-30'))` is `2099-03-05`. The entity carries
comparative period ends back to 1919, so the leg's `period_end_date` is guarded with a NULL
for an out-of-range period end (the target column is `Nullable(Date)`), and the ORDER BY
still ranks by the unguarded `period_end`, so an old period never outranks a newer one. A text
assertion cannot see a wraparound; this runs the SELECT on the engine over migration 000401's
own DDL.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.company_financials_latest.sql import build_latest_insert_sql
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_FILE = "000401_corpscout_se_company_financial_entity.up.sql"
ENTITY = "corpscout.se_company_financial"


def _schema_statements() -> list[str]:
    statements: list[str] = []
    text = (MIGRATIONS_DIR / MIGRATION_FILE).read_text(encoding="utf-8")
    for raw in text.split(";"):
        statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        if statement.upper().startswith("CREATE DATABASE") or statement.startswith("CREATE TABLE"):
            statements.append(statement)
    return statements


def _row(company_id: str, period_end: str, fiscal_year: int, folded_at: str, active: int = 1, scope: str = "standalone") -> str:
    return (
        f"('{company_id}', '{scope}', toDate32('{period_end}'), '{scope}:{period_end}', {fiscal_year}, 'SEK', "
        f"['ratsit'], {active}, toDateTime64('{folded_at}', 3, 'UTC'))"
    )


ROWS = (
    # Modern company: two active standalone periods, one comparative row outranking nothing.
    _row("5560000001", "2023-12-31", 2023, "2026-09-12 10:00:00.000"),
    _row("5560000001", "2022-12-31", 2022, "2026-09-12 10:00:00.000"),
    _row("5560000001", "2024-12-31", 2024, "2026-09-12 10:00:00.000", scope="consolidated"),
    # Pre-1970 only: its newest active standalone period end is outside the Date range.
    _row("5560000002", "1919-09-30", 1919, "2026-09-12 10:00:00.000"),
    # A hidden (active = 0) newer row must not win, and a 1919 row must not outrank 1971.
    _row("5560000003", "1971-06-30", 1971, "2026-09-12 10:00:00.000"),
    _row("5560000003", "1919-09-30", 1919, "2026-09-12 10:00:00.000"),
    _row("5560000003", "2020-12-31", 2020, "2026-09-12 10:00:00.000", active=0),
)


def _read(query: str) -> list[list[str]]:
    insert = (
        f"INSERT INTO {ENTITY} (company_id, scope, period_end, period_key, fiscal_year, currency, sources, active, folded_at) VALUES "
        + ", ".join(ROWS)
    )
    script = ";\n".join([*_schema_statements(), insert, query]) + ";\n"
    try:
        completed = subprocess.run(clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900)
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line.split("\t") for line in completed.stdout.splitlines() if line.strip()]


def test_the_sweden_leg_nulls_a_pre_1970_period_end_instead_of_wrapping_it() -> None:
    rows = _read(
        "SELECT company_id, toString(period_end_date), toString(fiscal_year), toString(years_count) "
        f"FROM ({build_latest_insert_sql('se')}) AS latest ORDER BY company_id FORMAT TSV"
    )

    assert rows == [
        ["5560000001", "2023-12-31", "2023", "2"],
        ["5560000002", "\\N", "1919", "1"],
        ["5560000003", "1971-06-30", "1971", "2"],
    ]


def test_the_wraparound_the_guard_exists_for_is_real_on_this_engine() -> None:
    """If a future ClickHouse clamps or raises instead, this fails and the guard can go."""
    [[wrapped]] = _read("SELECT toString(toDate(toDate32('1919-09-30'))) FORMAT TSV")
    assert wrapped != "1919-09-30"
    assert wrapped > "2090-01-01"
