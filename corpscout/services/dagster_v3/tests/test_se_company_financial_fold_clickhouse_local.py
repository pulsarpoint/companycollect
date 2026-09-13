"""The financial fold end to end against a real ClickHouse (clickhouse-local).

Claims a fake client cannot settle (spec 2026-09-11 sections 6 and 11):

1. Migration 000401's tables accept every shape the fold writes: an 80-value main tuple
   (Nullable(Decimal(38, 6)), Date32, Nullable(UInt16), Nullable(UInt64),
   Array(LowCardinality(String))), an 84-value history tuple, a company precedence rule and a
   hide rule -- and every main row passes the table's CHECKs.
2. A company folds end to end through the REAL batch SQL -- selection, the five page reads
   under FINAL, history then main -- and the row reads back as the fold built it: Ratsit's
   rounded revenue over Bolagsverket's exact one with Ratsit's own USD twin, Bolagsverket's
   employees, SEK from Ratsit, both sources; the consolidated ESEF period stands on its own.
3. Re-running the same fold selects NOTHING: the rewrite advanced max(folded_at) past every
   watermark.
4. A hide rule deactivates its period, a company-wide precedence rule flips revenue to
   Bolagsverket for the period where both compete, and a tombstone (every value NULL, newer
   than the last fold) withdraws its period with the last values kept.
5. A changed_only=False refold reproduces every row the driver wrote -- Decimal(38, 6) money,
   Date32 dates, the UInt16/UInt64 counts and the sources array round-trip EQUAL -- so nothing
   turns `updated` and no history row is written.

`_LocalClient` is a clickhouse-driver-shaped client over `clickhouse-local`: the process is
stateless, so the session keeps every statement it has run and replays the whole script for
each SELECT. Decimals come back quoted (output_format_json_quote_decimals) and parse exactly.
Both `join_use_nulls` settings run: none of these statements joins, so the parametrization
guards a future one, not a live risk.
"""

import json
import subprocess
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.financial import batch, tables
from dagster_v3.defs.se_company.financial.assets import export_precedence
from dagster_v3.defs.se_company.financial.fold import FOLD_VERSION
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_FILE = "000401_corpscout_se_company_financial_entity.up.sql"

A, B = "5567081699", "5560000002"     # A: Bolagsverket + Ratsit + ESEF; B: Ratsit only
P23, P22, C23 = "standalone:2023-12-31", "standalone:2022-12-31", "consolidated:2023-12-31"
SUGGESTED_AT = datetime(2026, 9, 12, 20, 0, tzinfo=UTC)
EXPORTED_AT = datetime(2026, 9, 12, 21, 0, tzinfo=UTC)
FIRST_FOLD_AT = datetime(2026, 9, 13, 8, 0, tzinfo=UTC)
SECOND_FOLD_AT = datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
RULE_AT = datetime(2026, 9, 13, 9, 30, tzinfo=UTC)          # after FIRST_FOLD_AT, so round 3 selects A
TOMBSTONED_AT = datetime(2026, 9, 13, 9, 40, tzinfo=UTC)
THIRD_FOLD_AT = datetime(2026, 9, 13, 10, 0, tzinfo=UTC)
FOURTH_FOLD_AT = datetime(2026, 9, 13, 11, 0, tzinfo=UTC)
COMPANY_IDS = [A, B]


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        return f"toDateTime64('{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', 3, 'UTC')"
    if isinstance(value, date):
        return f"toDate32('{value.isoformat()}')"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _schema_statements() -> list[str]:
    statements: list[str] = []
    text = (MIGRATIONS_DIR / MIGRATION_FILE).read_text(encoding="utf-8")
    for raw in text.split(";"):
        statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        if statement.upper().startswith("CREATE DATABASE") or statement.startswith("CREATE TABLE"):
            statements.append(statement)
    return statements


_QUERY_COLUMNS: dict[str, tuple[str, ...]] = {
    batch.bucket_company_ids_sql(): ("company_id",),
    batch.suggestion_watermarks_sql(): ("company_id", "suggested_at", "live"),
    batch.main_watermarks_sql(): ("company_id", "folded_at"),
    batch.rule_watermarks_sql(): ("company_id", "decided_at"),
    batch.hide_watermarks_sql(): ("company_id", "decided_at"),
    batch.current_suggestions_sql(): batch.SUGGESTION_SELECT_COLUMNS,
    batch.current_main_rows_sql(): batch.MAIN_COMPARE_COLUMNS,
    batch.company_rules_sql(): batch.RULE_SELECT_COLUMNS,
    batch.hidden_periods_sql(): ("company_id", "period_key"),
}
_DATETIME_COLUMNS = frozenset({"suggested_at", "folded_at", "decided_at"})
_DATE_COLUMNS = frozenset({"period_end", "period_start"})
_DECIMAL_COLUMNS = frozenset(tables.MONETARY_SUGGESTION_COLUMNS)
_INT_COLUMNS = frozenset({"employees", "live", "precedence", "fiscal_year", "period_months", "active"})


def _value(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in _DATETIME_COLUMNS:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    if column in _DATE_COLUMNS:
        return date.fromisoformat(value)
    if column in _DECIMAL_COLUMNS:
        return Decimal(value)
    if column in _INT_COLUMNS:
        return int(value)
    return value


class _LocalClient:
    """A clickhouse-driver-shaped client over clickhouse-local. Writes are remembered and
    replayed before every read, because each invocation starts with an empty server."""

    def __init__(self, setting: int) -> None:
        self.statements: list[str] = [f"SET join_use_nulls = {setting}", *_schema_statements()]

    def add(self, statement: str) -> None:
        self.statements.append(statement)

    def _run(self, query: str) -> list[str]:
        script = ";\n".join([*self.statements, query]) + ";\n"
        try:
            completed = subprocess.run(clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900)
        except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
            pytest.skip(f"clickhouse-local is unusable here: {exc}")
        assert completed.returncode == 0, completed.stderr or completed.stdout
        return [line for line in completed.stdout.splitlines() if line.strip()]

    def execute(self, sql, params=None, settings=None):
        if sql.startswith("INSERT"):
            values = ", ".join(_literal(tuple(row)) for row in params)
            self.add(f"{sql} {values}")
            return []
        columns = _QUERY_COLUMNS.get(sql)
        rendered = sql
        for name, value in (params or {}).items():
            rendered = rendered.replace(f"%({name})s", _literal(tuple(value) if isinstance(value, list) else value))
        assert "%(" not in rendered, rendered
        lines = self._run(
            f"SELECT * FROM ({rendered}) AS q FORMAT JSONCompactEachRow SETTINGS output_format_json_quote_decimals = 1"
        )
        if columns is None:   # a one-off read (export_precedence's stored rows): raw
            return [tuple(json.loads(line)) for line in lines]
        return [tuple(_value(column, item) for column, item in zip(columns, json.loads(line), strict=True)) for line in lines]

    def read(self, query: str) -> list[list[str]]:
        """A read the batch does not make: the test's own assertions, TSV."""
        return [line.split("\t") for line in self._run(query)]


def _suggestion(company_id: str, source: str, period_key: str, *, suggested_at: datetime = SUGGESTED_AT,
                currency: str | None = "SEK", employees: int | None = None, amount_scale: int = 1,
                money: dict[str, tuple[str, str]] | None = None) -> tuple:
    """One row in tables.SUGGESTION_COLUMNS order. `money` maps a field to (original, usd) as
    decimal strings; an omitted `money` with no employees is a tombstone."""
    scope, end = period_key.split(":")
    values: dict[str, Any] = {
        "company_id": company_id, "source": source, "period_key": period_key, "suggestion_id": "f" * 64,
        "suggested_at": suggested_at, "source_record_uid": f"{source}:{period_key}", "scope": scope,
        "period_end": date.fromisoformat(end), "period_end_derived": 0,
        "period_start": None if money is None and employees is None else date(int(end[:4]), 1, 1),
        "fiscal_year": None if money is None and employees is None else int(end[:4]),
        "period_months": None if money is None and employees is None else 12,
        "filing_fiscal_year": None, "currency": currency, "amount_scale": amount_scale,
        "employees": employees, "fx_rate_to_usd": None, "fx_rate_date": None, "fx_source": "",
        "decided_by": "", "note": "", "source_run_id": "r", "extractor_version": "v",
    }
    for column in tables.MONETARY_SUGGESTION_COLUMNS:
        values[column] = None
    for field, (original, usd) in (money or {}).items():
        values[tables.original_column(field)] = Decimal(original)
        values[tables.usd_column(field)] = Decimal(usd)
    return tuple(values[column] for column in tables.SUGGESTION_COLUMNS)


def _insert(table: str, columns: tuple[str, ...], rows: list[tuple]) -> str:
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES " + ", ".join(_literal(row) for row in rows)


SUGGESTIONS = [
    _suggestion(A, "bolagsverket", P23, employees=2100, money={"revenue": ("59016040", "5877138.085783"), "equity": ("3379581", "336000")}),
    _suggestion(A, "ratsit", P23, amount_scale=1000000, money={"revenue": ("60300000", "6005001.802437"), "equity": ("3400000", "338000")}),
    _suggestion(A, "ratsit", P22, amount_scale=1000000, money={"revenue": ("57100000", "5475989.498063")}),
    _suggestion(A, "esef", C23, money={"revenue": ("1296506000", "129113115.53"), "equity": ("2030344000", "202000000")}),
    _suggestion(B, "ratsit", P23, amount_scale=1000000, employees=5, money={"revenue": ("100000000", "10000000")}),
]
TOMBSTONE = _suggestion(A, "ratsit", P22, suggested_at=TOMBSTONED_AT, currency=None)


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def folded(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Four rounds against one clickhouse-local session: the first fold, the re-run, a fold
    under a hide rule, a company-wide precedence rule and a tombstone, and a changed_only=False
    refold that proves the round trip through real ClickHouse reproduces every row exactly."""
    client = _LocalClient(request.param)
    client.add(_insert(tables.QUALIFIED_SUGGESTION_TABLE, tables.SUGGESTION_COLUMNS, SUGGESTIONS))
    export_precedence(client, EXPORTED_AT)              # the 82 global rows, through the real code

    first = batch.fold_companies(client, COMPANY_IDS, changed_only=True, source_run_id="run-1", folded_at=FIRST_FOLD_AT)
    # Captured HERE: round 3 changes A's 2023 row (hidden, revenue flipped), so a later read
    # against the live table would not show what round 1 published.
    first_rows = client.read(
        "SELECT period_key, currency, currency_source, toString(revenue_amount_original), toString(revenue_amount_usd), "
        "revenue_source, toString(equity_amount_original), equity_source, ifNull(toString(employees), 'NULL'), employees_source, "
        "arrayStringConcat(sources, ','), toString(active), inactive_reason, toString(period_start), toString(fiscal_year) "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{A}' ORDER BY period_key"
    )
    rerun = batch.fold_companies(client, COMPANY_IDS, changed_only=True, source_run_id="run-2", folded_at=SECOND_FOLD_AT)

    client.add(_insert(tables.QUALIFIED_RULE_TABLE, tables.RULE_COLUMNS, [(A, P23, "hide", 0, "backoffice", "", RULE_AT)]))
    client.add(_insert(tables.QUALIFIED_PRECEDENCE_TABLE, tables.PRECEDENCE_COLUMNS, [(A, "", "revenue", "bolagsverket", 5000, 0, "backoffice", "", RULE_AT)]))
    client.add(_insert(tables.QUALIFIED_SUGGESTION_TABLE, tables.SUGGESTION_COLUMNS, [TOMBSTONE]))
    third = batch.fold_companies(client, COMPANY_IDS, changed_only=True, source_run_id="run-3", folded_at=THIRD_FOLD_AT)
    # Captured HERE too: round 4 rewrites every row with its own folded_at and run id.
    third_rows = client.read(
        "SELECT period_key, toString(active), inactive_reason, toString(revenue_amount_original), toString(revenue_amount_usd), revenue_source, "
        "toString(equity_amount_original), equity_source, fold_version, source_run_id, toString(folded_at) "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{A}' ORDER BY period_key"
    )
    fourth = batch.fold_companies(client, COMPANY_IDS, changed_only=False, source_run_id="run-4", folded_at=FOURTH_FOLD_AT)
    return {"client": client, "first": first, "first_rows": first_rows, "rerun": rerun, "third": third, "third_rows": third_rows, "fourth": fourth}


def test_the_first_fold_publishes_every_period_ratsit_first_with_its_own_twin(folded) -> None:
    counts = folded["first"]
    assert (counts.companies, counts.considered, counts.pages) == (2, 2, 1)
    assert (counts.periods, counts.published, counts.created, counts.unchanged, counts.unpublished) == (4, 4, 4, 0, 0)
    rows = {row[0]: row for row in folded["first_rows"]}
    assert set(rows) == {C23, P22, P23}
    standalone = rows[P23]
    assert standalone[1:6] == ["SEK", "ratsit", "60300000", "6005001.802437", "ratsit"]   # 1000 over 900, twin travels
    assert standalone[6:8] == ["3400000", "ratsit"]
    assert standalone[8:10] == ["2100", "bolagsverket"]                                    # ungated, Ratsit has none
    assert standalone[10:15] == ["bolagsverket,ratsit", "1", "", "2023-01-01", "2023"]
    consolidated = rows[C23]
    assert consolidated[1:6] == ["SEK", "esef", "1296506000", "129113115.53", "esef"] and consolidated[10] == "esef"
    assert rows[P22][3:6] == ["57100000", "5475989.498063", "ratsit"]
    b_rows = folded["client"].read(
        f"SELECT toString(revenue_amount_original), toString(employees), fold_version FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{B}'"
    )
    assert b_rows == [["100000000", "5", FOLD_VERSION]]


def test_the_history_of_the_first_fold_is_one_created_row_per_period_with_its_valued_fields(folded) -> None:
    rows = folded["client"].read(
        f"SELECT period_key, change_kind, arrayStringConcat(changed_fields, ','), toString(changed_at) FROM {tables.QUALIFIED_HISTORY_TABLE} "
        f"WHERE fold_run_id = 'run-1' AND company_id = '{A}' ORDER BY period_key"
    )
    stamp = FIRST_FOLD_AT.strftime("%Y-%m-%d %H:%M:%S.000")
    assert rows == [
        [C23, "created", "period_start,fiscal_year,period_months,currency,revenue,equity", stamp],
        [P22, "created", "period_start,fiscal_year,period_months,currency,revenue", stamp],
        [P23, "created", "period_start,fiscal_year,period_months,currency,revenue,equity,employees", stamp],
    ]


def test_re_running_the_fold_selects_nothing(folded) -> None:
    counts = folded["rerun"]
    assert counts.considered == 0 and counts.periods == 0 and counts.created == 0
    assert folded["client"].read(f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE fold_run_id = 'run-2'") == [["0"]]


def test_a_hide_a_company_rule_and_a_tombstone_all_move_their_periods(folded) -> None:
    counts = folded["third"]
    assert counts.considered == 1                                   # A only: B has nothing newer than its fold
    assert (counts.periods, counts.published, counts.hidden, counts.withdrawn, counts.unchanged) == (3, 1, 1, 1, 1)
    client = folded["client"]
    rows = {row[0]: row for row in folded["third_rows"]}
    # Hidden, and the company-wide rule (5000) flips revenue to Bolagsverket WITH Bolagsverket's USD; equity stays Ratsit's.
    assert rows[P23][1:8] == ["0", "hidden", "59016040", "5877138.085783", "bolagsverket", "3400000", "ratsit"]
    # Withdrawn keeps the last values; the writer's stamps are this run's.
    assert rows[P22][1:6] == ["0", "withdrawn", "57100000", "5475989.498063", "ratsit"] and rows[P22][8:10] == [FOLD_VERSION, "run-3"]
    assert rows[C23][1:3] == ["1", ""]                              # the ESEF-only period: unchanged, still rewritten
    kinds = client.read(
        f"SELECT period_key, change_kind, arrayStringConcat(changed_fields, ',') FROM {tables.QUALIFIED_HISTORY_TABLE} "
        "WHERE fold_run_id = 'run-3' ORDER BY period_key"
    )
    assert kinds == [[P22, "withdrawn", ""], [P23, "hidden", "revenue"]]
    assert {row[10] for row in rows.values()} == {THIRD_FOLD_AT.strftime("%Y-%m-%d %H:%M:%S.000")}   # every period of A carries run-3's folded_at


def test_a_stable_refold_reproduces_every_row_the_driver_wrote(folded) -> None:
    """A changed_only=False refold with nothing different since round 3 forces every row
    through the real read-compare-write cycle: Decimal(38, 6), Date32, Nullable(UInt16),
    Nullable(UInt64) and Array(LowCardinality(String)) must come back EQUAL, or a row turns
    `updated` and writes a spurious history entry."""
    counts = folded["fourth"]
    assert counts.considered == 2
    for kind in ("created", "updated", "hidden", "withdrawn", "reactivated"):
        assert getattr(counts, kind) == 0, kind
    client = folded["client"]
    rows = client.read(f"SELECT count() FROM {tables.QUALIFIED_MAIN_TABLE} FINAL")
    assert counts.unchanged == counts.periods == int(rows[0][0]) == 4
    assert client.read(f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE fold_run_id = 'run-4'") == [["0"]]
    stamps = client.read(f"SELECT count(DISTINCT folded_at) FROM {tables.QUALIFIED_MAIN_TABLE} FINAL")
    assert stamps == [["1"]]                                        # the rewrite stamped FOURTH_FOLD_AT on every row


def test_the_precedence_export_wrote_the_global_rows(folded) -> None:
    rows = folded["client"].read(
        f"SELECT count(), uniqExact(field) FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL WHERE company_id = '' AND period_key = ''"
    )
    assert rows == [["82", "25"]]                                   # 3 period fields + currency + 20 money + employees
