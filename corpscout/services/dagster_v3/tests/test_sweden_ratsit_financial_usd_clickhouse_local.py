"""The USD mutation of financial_usd.py on a real ClickHouse (spec 2026-09-11 section 3):
the table as migrations 000343 + 000346 + 000400 leave it, four kinds of row, the mutation
run twice. Needs clickhouse-local or Docker; skips otherwise."""

import subprocess
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_ratsit.financial_usd import (
    join_table_ddl,
    pending_rate_dates_sql,
    usd_update_sql,
)
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
TABLE = "corpscout.se_ratsit_financial_periods"
JOIN = "corpscout._tmp_ratsit_fx_test"


def _statements_for_periods_table() -> list[str]:
    """CREATE DATABASE, the periods CREATE TABLE from 000343, and every ALTER of the periods
    table from 000346 and 000400, in order; comments stripped."""
    out: list[str] = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    for name in (
        "000343_corpscout_se_ratsit_normalized_segments.up.sql",
        "000346_corpscout_se_ratsit_normalization_v2.up.sql",
        "000400_corpscout_se_ratsit_financial_periods_usd.up.sql",
    ):
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.startswith(f"CREATE TABLE IF NOT EXISTS {TABLE}") or statement.startswith(
                f"ALTER TABLE {TABLE}"
            ):
                out.append(statement)
    assert sum(s.startswith("CREATE TABLE") for s in out) == 1
    return out


ROWS = """INSERT INTO corpscout.se_ratsit_financial_periods
(company_id, result_sha256, normalizer_version, financial_report_index, period_index, period_kind,
 scope, monetary_unit, fiscal_year, period_start, period_end, period_months,
 revenue_amount, equity_amount, personnel_cost_per_employee_msek, employee_count, normalized_at)
VALUES
('5567081699', repeat('a', 64), 'ratsit-normalizer-v2', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 57.1, 3.4, 0.9, 21, now64(6)),
('5567081699', repeat('a', 64), 'ratsit-normalizer-v2', 0, 1, 'financial_only', 'company', 'TSEK', 2022, NULL, NULL, NULL, 1234.5, NULL, NULL, NULL, now64(6)),
('5560000001', repeat('b', 64), 'ratsit-normalizer-v2', 0, 0, 'financial_only', 'company', 'MSEK', 2021, '2020-07-01', '2021-06-30', 12, 10, NULL, NULL, NULL, now64(6)),
('5560000002', repeat('c', 64), 'ratsit-normalizer-v2', 0, 0, 'employment_only', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, NULL, NULL, NULL, 7, now64(6))"""

RATES = f"""INSERT INTO {JOIN} VALUES
('2023-12-31', 0.099123456789, '2023-12-29', 'ecb'),
('2022-12-31', 0.095, '2022-12-30', 'ecb')"""

READ = f"""SELECT company_id, period_index, monetary_unit, revenue_amount, revenue_amount_usd,
       equity_amount_usd, personnel_cost_per_employee_usd, fx_rate_to_usd, fx_rate_date, fx_source
FROM {TABLE} FINAL ORDER BY company_id, period_index FORMAT TSV"""


def _run(statements: list[str]) -> list[str]:
    script = ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _cells(line: str) -> tuple:
    """A TSV line as a tuple: `\\N` is None, a numeric cell is a Decimal (ClickHouse trims a
    Decimal's trailing zeros, so 89211.111110 prints as 89211.11111), anything else a str."""
    out = []
    for cell in line.split("\t"):
        if cell == "\\N":
            out.append(None)
        else:
            try:
                out.append(Decimal(cell))
            except InvalidOperation:
                out.append(cell)
    return tuple(out)


def test_the_mutation_scales_derives_dates_and_leaves_unrated_rows_for_next_time() -> None:
    schema = _statements_for_periods_table()
    before = pending_rate_dates_sql() + " FORMAT TSV"
    lines = _run(
        schema
        + [ROWS, before, join_table_ddl(JOIN), RATES, usd_update_sql(JOIN, mutations_sync=2), READ]
        + [before]
    )
    # Pending before: three dates (the employment_only row has no money and is not pending).
    assert lines[:3] == ["2021-06-30\t1", "2022-12-31\t1", "2023-12-31\t1"]
    rows = [_cells(line) for line in lines[3:7]]
    D = Decimal
    # No rate for 2021-06-30 in the join table: untouched, still pending.
    assert rows[0] == (D("5560000001"), D(0), "MSEK", D(10), None, None, None, None, None, "")
    # Employees only: never money, never pending, never converted.
    assert rows[1] == (D("5560000002"), D(0), "MSEK", None, None, None, None, None, None, "")
    # MSEK 57.1 at 0.099123456789: 57.1e6 * rate to six decimals; equity and the per-employee
    # figure (always MSEK) with the same rate; fx columns filled.
    # multiplyDecimal(..., 6) truncates to the sixth decimal; it does not round.
    assert rows[2] == (
        D("5567081699"), D(0), "MSEK", D("57.1"), D("5659949.382651"), D("337019.753082"),
        D("89211.111110"), D("0.099123456789"), "2023-12-29", "ecb",
    )
    # Undated TSEK row: rate date Dec 31 2022, 1234.5e3 * 0.095.
    assert rows[3] == (
        D("5567081699"), D(1), "TSEK", D("1234.5"), D("117277.5"), None, None,
        D("0.095"), "2022-12-30", "ecb",
    )
    assert lines[7:] == ["2021-06-30\t1"]


def test_the_mutation_is_idempotent() -> None:
    schema = _statements_for_periods_table()
    once = _run(schema + [ROWS, join_table_ddl(JOIN), RATES, usd_update_sql(JOIN, mutations_sync=2), READ])
    twice = _run(
        schema
        + [ROWS, join_table_ddl(JOIN), RATES, usd_update_sql(JOIN, mutations_sync=2),
           usd_update_sql(JOIN, mutations_sync=2), READ]
    )
    assert once == twice
