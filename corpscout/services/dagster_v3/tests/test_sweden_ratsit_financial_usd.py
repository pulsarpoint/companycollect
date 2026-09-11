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
    PENDING_PREDICATE_SQL,
    PER_EMPLOYEE_COLUMNS,
    QUALIFIED_PERIODS_TABLE,
    RATE_DATE_SQL,
    UNFINISHED_MUTATION_PATTERN,
    USD_PAIRS,
    UsdCounts,
    convert_ratsit_financial_periods,
    join_insert_sql,
    join_table_ddl,
    join_table_name,
    load_usd_rates,
    mutation_status_sql,
    pending_rate_dates_sql,
    rate_series_bounds_sql,
    unfinished_usd_mutation_sql,
    unknown_unit_rows_sql,
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
    # A NULL unit would scale as SEK in the mutation's multiIf, so such a row is not pending.
    assert PENDING_PREDICATE_SQL == (
        f"fx_rate_to_usd IS NULL AND monetary_unit IS NOT NULL AND {MONETARY_PRESENT_SQL}"
    )
    sql = pending_rate_dates_sql()
    assert sql.startswith(f"SELECT {RATE_DATE_SQL} AS rate_date, count() AS rows")
    assert f"FROM {QUALIFIED_PERIODS_TABLE} FINAL" in sql
    assert f"WHERE {PENDING_PREDICATE_SQL}" in sql
    assert "monetary_unit IS NOT NULL" in sql
    assert sql.endswith("GROUP BY rate_date\nORDER BY rate_date")


def test_unknown_unit_rows_sql_counts_the_rows_the_pending_scan_now_skips() -> None:
    assert unknown_unit_rows_sql() == (
        f"SELECT count() FROM {QUALIFIED_PERIODS_TABLE} FINAL "
        f"WHERE fx_rate_to_usd IS NULL AND monetary_unit IS NULL AND {MONETARY_PRESENT_SQL}"
    )


def test_rate_series_bounds_sql_takes_the_window_both_legs_cover() -> None:
    assert rate_series_bounds_sql() == (
        "SELECT greatest(minIf(rate_date, quote_currency = 'SEK'), "
        "minIf(rate_date, quote_currency = 'USD')) AS first_date, "
        "least(maxIf(rate_date, quote_currency = 'SEK'), "
        "maxIf(rate_date, quote_currency = 'USD')) AS last_date "
        "FROM corpscout.exchange_rates "
        "WHERE base_currency = 'EUR' AND quote_currency IN ('SEK', 'USD')"
    )


def test_unfinished_usd_mutation_sql_finds_any_earlier_runs_mutation() -> None:
    assert unfinished_usd_mutation_sql() == (
        "SELECT mutation_id, command FROM system.mutations "
        "WHERE database = %(database)s AND table = %(table)s AND is_done = 0 "
        "AND command LIKE %(pattern)s"
    )
    assert UNFINISHED_MUTATION_PATTERN == "%_tmp_ratsit_fx_%"


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
        f"WHERE {PENDING_PREDICATE_SQL} AND {RATE_DATE_SQL} IN (SELECT rate_date FROM {name})"
    ) in sql
    assert "monetary_unit IS NOT NULL" in sql
    assert sql.endswith("SETTINGS mutations_sync = 0")
    assert usd_update_sql(name, mutations_sync=2).endswith("SETTINGS mutations_sync = 2")
    # Every assignment is one line and the fx columns come last, so a reader can diff it.
    assert sql.count("\n") == 20 + 3 + 1


def test_mutation_status_sql_is_bound_by_table_and_join_name() -> None:
    sql = mutation_status_sql()
    assert sql.startswith(
        "SELECT mutation_id, is_done, latest_fail_reason, parts_to_do, is_killed "
    )
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

    def requested_dates(self) -> list[str]:
        return [request.rate_date for call in self.calls for request in call]


SERIES = (date(2006, 1, 2), date(2026, 7, 6))


class FakeClient:
    """Answers the pre-flight scan from `unfinished`, the pending scan from `pending` (one list
    per call), the rate-series bounds from `bounds`, the unknown-unit count from `unknown_unit`
    and the mutation poll from `statuses` (one 5-tuple, or None for "no row yet", per call);
    records every statement."""

    def __init__(
        self, *, pending, statuses=(("m1", 1, "", 0, 0),), bounds=SERIES, unknown_unit=0,
        unfinished=(),
    ):
        self.pending = [list(p) for p in pending]
        self.statuses = list(statuses)
        self.bounds = bounds
        self.unknown_unit = unknown_unit
        self.unfinished = list(unfinished)
        self.statements: list[tuple[str, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params))
        if sql.startswith("SELECT ifNull(period_end"):
            return self.pending.pop(0)
        if sql.startswith("SELECT greatest(minIf"):
            return [self.bounds]
        if "AND monetary_unit IS NULL" in sql:
            return [(self.unknown_unit,)]
        if "is_done = 0 AND command LIKE" in sql:
            return list(self.unfinished)
        if "FROM system.mutations" in sql:
            row = self.statuses.pop(0)
            return [row] if row is not None else []
        if sql.startswith(("CREATE TABLE", "DROP TABLE", "INSERT INTO", "ALTER TABLE")):
            return []
        raise AssertionError(sql)

    def kinds(self) -> list[str]:
        return [sql.split("\n")[0][:12] for sql, _ in self.statements]


# Two of these dates are outside SERIES: 2004-12-31 is before the ECB series starts and
# 2026-09-30 is after its newest rate. Three rows in total wait for a later run.
PENDING = [
    (date(2023, 12, 31), 3),
    (date(2021, 6, 30), 2),
    (date(2004, 12, 31), 1),
    (date(2026, 9, 30), 2),
]
OUT_OF_SERIES_PENDING = [(date(2004, 12, 31), 1), (date(2026, 9, 30), 2)]


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
        rows_pending=8, rate_dates_needed=4, rates_found=2, rows_convertible=5,
        rows_rate_date_outside_series=3, rows_unknown_unit=0,
        rows_converted=0, rows_still_without_rate=8, executed=False,
    )
    assert counts.as_metadata()["rows_convertible"] == 5
    assert counts.as_metadata()["rows_rate_date_outside_series"] == 3
    assert counts.as_metadata()["rows_unknown_unit"] == 0
    # The two dates outside the ECB series are never asked of the client.
    assert rates.requested_dates() == ["2023-12-31", "2021-06-30"]
    assert [s for s, _ in client.statements] == [
        unfinished_usd_mutation_sql(),
        pending_rate_dates_sql(),
        rate_series_bounds_sql(),
        unknown_unit_rows_sql(),
    ]


def test_preview_counts_the_rows_whose_unit_is_unknown() -> None:
    client = FakeClient(pending=[PENDING], unknown_unit=4)
    counts = convert_ratsit_financial_periods(client, FakeRates({}), run_id="r", execute=False)
    assert counts.rows_unknown_unit == 4


def test_an_empty_rate_series_converts_nothing() -> None:
    client = FakeClient(pending=[PENDING], bounds=(None, None))
    rates = FakeRates({"2023-12-31": Decimal("0.099"), "2021-06-30": Decimal("0.117")})
    counts = convert_ratsit_financial_periods(client, rates, run_id="r", execute=True)
    assert counts.rows_rate_date_outside_series == 8
    assert counts.rows_convertible == 0 and counts.rows_converted == 0
    assert rates.requested_dates() == []
    assert not any(
        s.startswith(("CREATE TABLE", "INSERT INTO", "ALTER TABLE")) for s, _ in client.statements
    )


def test_execute_loads_the_join_table_runs_the_mutation_waits_and_drops() -> None:
    client = FakeClient(
        pending=[PENDING, OUT_OF_SERIES_PENDING],
        statuses=[("m1", 0, "", 3, 0), ("m1", 1, "", 0, 0)],
    )
    rates = FakeRates({"2023-12-31": Decimal("0.099"), "2021-06-30": Decimal("0.117")})
    slept: list[float] = []
    logged: list[tuple] = []
    counts = convert_ratsit_financial_periods(
        client, rates, run_id="a1b2-c3", execute=True, sleep=slept.append,
        log=lambda message, *args: logged.append((message, args)),
    )
    assert counts == UsdCounts(
        rows_pending=8, rate_dates_needed=4, rates_found=2, rows_convertible=5,
        rows_rate_date_outside_series=3, rows_unknown_unit=0,
        rows_converted=5, rows_still_without_rate=3, executed=True,
    )
    name = "corpscout._tmp_ratsit_fx_a1b2c3"
    statements = [s for s, _ in client.statements]
    assert statements[4] == join_table_ddl(name)
    assert statements[5] == join_insert_sql(name)
    rows = client.statements[5][1]
    # Only the in-series dates reach the join table, so the mutation's IN (...) guard leaves
    # the out-of-series rows untouched.
    assert rows == [
        (date(2023, 12, 31), Decimal("0.099"), date(2023, 12, 31), "ecb"),
        (date(2021, 6, 30), Decimal("0.117"), date(2021, 6, 30), "ecb"),
    ]
    assert statements[6] == usd_update_sql(name)
    assert "FROM system.mutations" in statements[7] and "FROM system.mutations" in statements[8]
    assert client.statements[7][1] == {
        "database": "corpscout", "table": "se_ratsit_financial_periods", "pattern": f"%{name}%",
    }
    assert slept == [5.0]  # one poll came back not done
    # The drop comes only after the last status poll said the mutation is done.
    last_poll = max(i for i, s in enumerate(statements) if "FROM system.mutations" in s)
    drop = statements.index(f"DROP TABLE IF EXISTS {name}")
    assert drop > last_poll
    assert statements[9] == f"DROP TABLE IF EXISTS {name}"
    assert statements[10].startswith("SELECT ifNull(period_end")  # the after-count
    # wait_for_mutation returned the id the poll reported, and the run logged it.
    assert logged[-1][1] == (5, 3, "m1")


def test_execute_with_no_rate_found_writes_nothing() -> None:
    client = FakeClient(pending=[PENDING])
    counts = convert_ratsit_financial_periods(client, FakeRates({}), run_id="r", execute=True)
    assert counts.rates_found == 0 and counts.rows_converted == 0 and counts.executed is True
    assert not any(
        s.startswith(("CREATE TABLE", "INSERT INTO", "ALTER TABLE")) for s, _ in client.statements
    )


def test_wait_for_mutation_returns_the_id_and_waits_out_a_transient_failure() -> None:
    # parts_to_do fell from 3 to 2 while the fail reason stood: parts are still completing.
    progressing = FakeClient(
        pending=[],
        statuses=[("m1", 0, "memory", 3, 0), ("m1", 0, "", 2, 0), ("m1", 1, "", 0, 0)],
    )
    assert wait_for_mutation(
        progressing, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None
    ) == "m1"
    # Three consecutive failing polls without progress: terminal.
    stuck = FakeClient(
        pending=[],
        statuses=[
            ("m1", 0, "memory", 3, 0), ("m1", 0, "memory", 2, 0),
            ("m1", 0, "memory", 2, 0), ("m1", 0, "memory", 2, 0),
        ],
    )
    with pytest.raises(RuntimeError, match="memory"):
        wait_for_mutation(stuck, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None)
    assert stuck.statuses == []  # it polled all four times


def test_wait_for_mutation_raises_on_a_killed_mutation_and_on_timeout() -> None:
    killed = FakeClient(pending=[], statuses=[("m1", 0, "", 3, 1)])
    with pytest.raises(RuntimeError, match="killed"):
        wait_for_mutation(killed, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None)
    slow = FakeClient(
        pending=[], statuses=[("m1", 0, "", 3, 0), ("m1", 0, "", 3, 0), ("m1", 0, "", 3, 0)],
    )
    with pytest.raises(TimeoutError):
        wait_for_mutation(
            slow, join_table="corpscout._tmp_ratsit_fx_x", poll_seconds=5.0,
            timeout_seconds=10.0, sleep=lambda _: None,
        )
    # A mutation row that has not appeared yet is not a failure: keep polling.
    late = FakeClient(pending=[], statuses=[None, ("m1", 1, "", 0, 0)])
    assert wait_for_mutation(
        late, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None
    ) == "m1"


def test_a_wait_failure_keeps_the_join_table_and_names_the_recovery() -> None:
    client = FakeClient(pending=[PENDING], statuses=[("m9", 0, "boom", 5, 0)] * 3)
    rates = FakeRates({"2023-12-31": Decimal("0.099")})
    with pytest.raises(RuntimeError) as excinfo:
        convert_ratsit_financial_periods(
            client, rates, run_id="r", execute=True, sleep=lambda _: None
        )
    message = str(excinfo.value)
    assert message.startswith(
        "USD mutation m9 on corpscout.se_ratsit_financial_periods did not finish ("
    )
    assert message.endswith(
        "); the join table corpscout._tmp_ratsit_fx_r is kept. Recover with: "
        "KILL MUTATION WHERE database = 'corpscout' AND table = 'se_ratsit_financial_periods' "
        "AND mutation_id = 'm9'; DROP TABLE corpscout._tmp_ratsit_fx_r"
    )
    # Dropping the join table under a queued mutation wedges every later mutation: never.
    assert not any(s.startswith("DROP TABLE") for s, _ in client.statements)


def test_a_failed_submission_drops_the_join_table_there_is_no_mutation_yet() -> None:
    class RefusingClient(FakeClient):
        def execute(self, sql, params=None, settings=None):
            if sql.startswith("ALTER TABLE"):
                self.statements.append((sql, params))
                raise RuntimeError("Code: 47. DB::Exception: Missing column")
            return super().execute(sql, params, settings)

    client = RefusingClient(pending=[PENDING])
    rates = FakeRates({"2023-12-31": Decimal("0.099")})
    with pytest.raises(RuntimeError, match="Missing column"):
        convert_ratsit_financial_periods(
            client, rates, run_id="r", execute=True, sleep=lambda _: None
        )
    assert [s for s, _ in client.statements][-1] == (
        "DROP TABLE IF EXISTS corpscout._tmp_ratsit_fx_r"
    )


@pytest.mark.parametrize("execute", [False, True])
def test_an_unfinished_usd_mutation_stops_the_run_before_anything_is_written(
    execute: bool,
) -> None:
    client = FakeClient(
        pending=[PENDING],
        unfinished=[
            ("m0", "UPDATE ... joinGet('corpscout._tmp_ratsit_fx_old', 'fx_rate', ...) WHERE ..."),
        ],
    )
    rates = FakeRates({"2023-12-31": Decimal("0.099")})
    with pytest.raises(RuntimeError) as excinfo:
        convert_ratsit_financial_periods(
            client, rates, run_id="r", execute=execute, sleep=lambda _: None
        )
    message = str(excinfo.value)
    assert "m0" in message and "corpscout._tmp_ratsit_fx_old" in message
    assert "KILL MUTATION" in message and "DROP TABLE corpscout._tmp_ratsit_fx_old" in message
    assert client.statements[0][0] == unfinished_usd_mutation_sql()
    assert client.statements[0][1] == {
        "database": "corpscout",
        "table": "se_ratsit_financial_periods",
        "pattern": UNFINISHED_MUTATION_PATTERN,
    }
    assert not any(
        s.startswith(("CREATE TABLE", "INSERT INTO", "ALTER TABLE", "DROP TABLE"))
        for s, _ in client.statements
    )
