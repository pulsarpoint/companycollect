"""Slice 0 of the financial entity (spec 2026-09-11 section 3): the USD twins of
se_ratsit_financial_periods. Column pairs, the SQL the asset runs, and the run function
against a fake client and fake rates."""

from datetime import date
from decimal import Decimal

import pytest
from exchange_rates import ExchangeRateRequest
from exchange_rates.models import UsdExchangeRate

from dagster_v3.defs.sweden_ratsit import financial_usd
from dagster_v3.defs.sweden_ratsit.financial_usd import (
    AMOUNT_COLUMNS,
    JOIN_TABLE_PREFIX,
    MONETARY_PRESENT_SQL,
    PER_EMPLOYEE_COLUMNS,
    QUALIFIED_PERIODS_TABLE,
    RATE_DATE_SQL,
    USD_PAIRS,
    UsdCounts,
    convert_ratsit_financial_periods,
    join_insert_sql,
    join_table_ddl,
    join_table_name,
    load_usd_rates,
    mutation_status_sql,
    pending_rate_dates_sql,
    usd_update_sql,
    wait_for_mutation,
)


def test_usd_pairs_cover_the_twenty_monetary_columns_and_nothing_else() -> None:
    assert len(AMOUNT_COLUMNS) == 18
    assert AMOUNT_COLUMNS[0] == "revenue_amount"
    assert "balance_sheet_total_amount" in AMOUNT_COLUMNS
    assert PER_EMPLOYEE_COLUMNS == (
        ("personnel_cost_per_employee_msek", "personnel_cost_per_employee_usd"),
        ("revenue_per_employee_msek", "revenue_per_employee_usd"),
    )
    assert len(USD_PAIRS) == 20
    natives = [native for native, _, _ in USD_PAIRS]
    assert "average_salary" not in natives
    assert not any(native.endswith("_percent") for native in natives)
    for native, usd, scale_sql in USD_PAIRS:
        if native.endswith("_msek"):
            assert usd == native.removesuffix("_msek") + "_usd"
            assert scale_sql == "1000000"
        else:
            assert usd == f"{native}_usd"
            assert scale_sql == financial_usd.UNIT_SCALE_SQL
    assert financial_usd.UNIT_SCALE_SQL == (
        "multiIf(monetary_unit = 'MSEK', 1000000, monetary_unit = 'TSEK', 1000, 1)"
    )


def test_rate_date_and_pending_scan() -> None:
    assert RATE_DATE_SQL == "ifNull(period_end, makeDate32(fiscal_year, 12, 31))"
    assert MONETARY_PRESENT_SQL.startswith("(revenue_amount IS NOT NULL OR ")
    assert MONETARY_PRESENT_SQL.endswith("revenue_per_employee_msek IS NOT NULL)")
    sql = pending_rate_dates_sql()
    assert sql.startswith(f"SELECT {RATE_DATE_SQL} AS rate_date, count() AS rows")
    assert f"FROM {QUALIFIED_PERIODS_TABLE} FINAL" in sql
    assert f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL}" in sql
    assert sql.endswith("GROUP BY rate_date\nORDER BY rate_date")


def test_join_table_is_qualified_run_scoped_and_a_join_engine() -> None:
    name = join_table_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    assert name == f"{JOIN_TABLE_PREFIX}a1b2c3d4e5f67890abcdef1234567890"
    assert name.startswith("corpscout.")
    ddl = join_table_ddl(name)
    assert ddl == (
        f"CREATE TABLE {name} (rate_date Date32, fx_rate Decimal(38, 12), "
        "fx_rate_date Date32, fx_source String) ENGINE = Join(ANY, LEFT, rate_date)"
    )
    assert join_insert_sql(name) == (
        f"INSERT INTO {name} (rate_date, fx_rate, fx_rate_date, fx_source) VALUES"
    )


def test_update_sql_names_every_pair_the_fx_columns_and_the_pending_guard() -> None:
    name = join_table_name("run-1")
    sql = usd_update_sql(name)
    assert sql.startswith(f"ALTER TABLE {QUALIFIED_PERIODS_TABLE} UPDATE\n")
    for native, usd, scale_sql in USD_PAIRS:
        assert (
            f"{usd} = multiplyDecimal({native} * {scale_sql}, "
            f"joinGet('{name}', 'fx_rate', {RATE_DATE_SQL}), 6)"
        ) in sql, usd
    assert f"fx_rate_to_usd = joinGet('{name}', 'fx_rate', {RATE_DATE_SQL})" in sql
    assert f"fx_rate_date = joinGet('{name}', 'fx_rate_date', {RATE_DATE_SQL})" in sql
    assert f"fx_source = joinGet('{name}', 'fx_source', {RATE_DATE_SQL})" in sql
    assert (
        f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL} "
        f"AND {RATE_DATE_SQL} IN (SELECT rate_date FROM {name})"
    ) in sql
    assert sql.endswith("SETTINGS mutations_sync = 0")
    assert usd_update_sql(name, mutations_sync=2).endswith("SETTINGS mutations_sync = 2")
    # Every assignment is one line and the fx columns come last, so a reader can diff it.
    assert sql.count("\n") == 20 + 3 + 1


def test_mutation_status_sql_is_bound_by_table_and_join_name() -> None:
    sql = mutation_status_sql()
    assert "FROM system.mutations" in sql
    assert "database = %(database)s AND table = %(table)s AND command LIKE %(pattern)s" in sql
    assert "ORDER BY create_time DESC LIMIT 1" in sql


class FakeRates:
    """usd_rates over a dict of known ISO dates; a batch with one unknown date raises, as the
    real client does, so the loader must fall back to one request at a time."""

    def __init__(self, known: dict[str, Decimal]):
        self.known = known
        self.calls: list[list[ExchangeRateRequest]] = []

    def usd_rates(self, requests):
        requests = list(requests)
        self.calls.append(requests)
        out = {}
        for request in requests:
            if request.rate_date not in self.known:
                raise LookupError(request.rate_date)
            out[(request.currency, request.rate_date)] = UsdExchangeRate(
                currency=request.currency,
                requested_rate_date=request.rate_date,
                rate_date=request.rate_date,
                rate=self.known[request.rate_date],
                eur_to_usd=Decimal("1.1"),
                eur_to_currency=Decimal("11"),
                source="ecb",
                components=(),
            )
        return out


class FakeClient:
    """Answers the pending scan from `pending` (one list per call), the mutation poll from
    `statuses` (one row per call), and records every other statement."""

    def __init__(self, *, pending, statuses=((1, ""),)):
        self.pending = [list(p) for p in pending]
        self.statuses = list(statuses)
        self.statements: list[tuple[str, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params))
        if sql.startswith("SELECT ifNull(period_end"):
            return self.pending.pop(0)
        if "FROM system.mutations" in sql:
            row = self.statuses.pop(0)
            return [row] if row is not None else []
        if sql.startswith(("CREATE TABLE", "DROP TABLE", "INSERT INTO", "ALTER TABLE")):
            return []
        raise AssertionError(sql)


PENDING = [(date(2023, 12, 31), 3), (date(2021, 6, 30), 2), (date(2004, 12, 31), 1)]


def test_load_usd_rates_batches_at_fifty_and_skips_only_the_missing_date() -> None:
    known = {f"2023-01-{day:02d}": Decimal("0.1") for day in range(1, 32)}
    rates = FakeRates(known)
    requests = [
        ExchangeRateRequest(currency="SEK", rate_date=f"2023-01-{day:02d}") for day in range(1, 32)
    ] + [ExchangeRateRequest(currency="SEK", rate_date="1999-12-31")] + [
        ExchangeRateRequest(currency="SEK", rate_date=f"2023-01-{day:02d}") for day in range(1, 20)
    ]
    assert len(requests) == 51
    found = load_usd_rates(rates, requests)
    assert len(found) == 31
    assert ("SEK", "1999-12-31") not in found
    # First batch of 50 raised on the unknown date and was retried one by one (50 single
    # calls), the second batch of 1 succeeded whole: 1 + 50 + 1 calls.
    assert [len(call) for call in rates.calls] == [50] + [1] * 50 + [1]


def test_preview_reports_counts_and_writes_nothing() -> None:
    client = FakeClient(pending=[PENDING])
    rates = FakeRates({"2023-12-31": Decimal("0.099"), "2021-06-30": Decimal("0.117")})
    counts = convert_ratsit_financial_periods(client, rates, run_id="run-1", execute=False)
    assert counts == UsdCounts(
        rows_pending=6, rate_dates_needed=3, rates_found=2, rows_convertible=5,
        rows_converted=0, rows_still_without_rate=6, executed=False,
    )
    assert counts.as_metadata()["rows_convertible"] == 5
    assert [s for s, _ in client.statements] == [
        "SELECT ifNull(period_end, makeDate32(fiscal_year, 12, 31)) AS rate_date, count() AS rows\n"
        "FROM corpscout.se_ratsit_financial_periods FINAL\n"
        "WHERE fx_rate_to_usd IS NULL AND " + MONETARY_PRESENT_SQL + "\n"
        "GROUP BY rate_date\nORDER BY rate_date"
    ]


def test_execute_loads_the_join_table_runs_the_mutation_waits_and_drops() -> None:
    client = FakeClient(pending=[PENDING, [(date(2004, 12, 31), 1)]], statuses=[(0, ""), (1, "")])
    rates = FakeRates({"2023-12-31": Decimal("0.099"), "2021-06-30": Decimal("0.117")})
    slept: list[float] = []
    counts = convert_ratsit_financial_periods(
        client, rates, run_id="a1b2-c3", execute=True, sleep=slept.append,
    )
    assert counts == UsdCounts(
        rows_pending=6, rate_dates_needed=3, rates_found=2, rows_convertible=5,
        rows_converted=5, rows_still_without_rate=1, executed=True,
    )
    name = "corpscout._tmp_ratsit_fx_a1b2c3"
    statements = [s for s, _ in client.statements]
    assert statements[1] == join_table_ddl(name)
    assert statements[2] == join_insert_sql(name)
    rows = client.statements[2][1]
    assert rows == [
        (date(2023, 12, 31), Decimal("0.099"), date(2023, 12, 31), "ecb"),
        (date(2021, 6, 30), Decimal("0.117"), date(2021, 6, 30), "ecb"),
    ]
    assert statements[3] == usd_update_sql(name)
    assert "FROM system.mutations" in statements[4] and "FROM system.mutations" in statements[5]
    assert client.statements[4][1] == {
        "database": "corpscout", "table": "se_ratsit_financial_periods", "pattern": f"%{name}%",
    }
    assert slept == [5.0]  # one poll came back not done
    assert statements[6] == f"DROP TABLE IF EXISTS {name}"
    assert statements[7].startswith("SELECT ifNull(period_end")  # the after-count


def test_execute_with_no_rate_found_writes_nothing() -> None:
    client = FakeClient(pending=[PENDING])
    counts = convert_ratsit_financial_periods(client, FakeRates({}), run_id="r", execute=True)
    assert counts.rates_found == 0 and counts.rows_converted == 0 and counts.executed is True
    assert len(client.statements) == 1


def test_wait_for_mutation_raises_on_failure_and_on_timeout() -> None:
    failed = FakeClient(pending=[], statuses=[(0, "Code: 241. DB::Exception: memory")])
    with pytest.raises(RuntimeError, match="memory"):
        wait_for_mutation(failed, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None)
    slow = FakeClient(pending=[], statuses=[(0, ""), (0, ""), (0, "")])
    with pytest.raises(TimeoutError):
        wait_for_mutation(
            slow, join_table="corpscout._tmp_ratsit_fx_x", poll_seconds=5.0,
            timeout_seconds=10.0, sleep=lambda _: None,
        )
    # A mutation row that has not appeared yet is not a failure: keep polling.
    late = FakeClient(pending=[], statuses=[None, (1, "")])
    wait_for_mutation(late, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None)


def test_the_join_table_is_dropped_when_the_mutation_fails() -> None:
    client = FakeClient(pending=[PENDING], statuses=[(0, "boom")])
    rates = FakeRates({"2023-12-31": Decimal("0.099")})
    with pytest.raises(RuntimeError, match="boom"):
        convert_ratsit_financial_periods(client, rates, run_id="r", execute=True, sleep=lambda _: None)
    assert [s for s, _ in client.statements][-1] == "DROP TABLE IF EXISTS corpscout._tmp_ratsit_fx_r"
