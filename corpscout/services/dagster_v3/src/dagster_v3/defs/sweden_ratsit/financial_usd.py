"""USD twins for se_ratsit_financial_periods (financial entity spec 2026-09-11 section 3,
slice 0).

Ratsit publishes twenty monetary figures per period in MSEK (one decimal), TSEK or SEK and no
dollar value. The currency standard wants every monetary figure stored WITH its USD twin in the
source table, converted as a separate re-runnable step. This module is that step:

1. `pending_rate_dates_sql` finds the distinct rate dates of rows whose fx_rate_to_usd is
   still NULL (the rate date is the period end, or Dec 31 of the fiscal year for the 7,503
   undated rows). A row whose monetary_unit is NULL is not pending: the mutation's multiIf
   would scale it as SEK and stamp it. `unknown_unit_rows_sql` counts those rows instead.
2. The rates come from the shared ExchangeRateClient (SEK to USD through EUR, the latest ECB
   date at or before the requested one). Only dates the ECB series actually covers are asked
   for: the client never refuses a date, so a date before the series starts (2006) or after
   its newest rate would be answered with a far-off rate and stamped for good.
   `rate_series_bounds_sql` reads the window both legs cover and a pending date outside it is
   left alone, counted as `rows_rate_date_outside_series` and converted by a later run once
   the rate table has caught up. The rates that were found go into a per-run Join table.
3. ONE `ALTER TABLE ... UPDATE` mutation fills the twins and the three fx columns of every
   pending row whose date got a rate, with `joinGet` against the join table. The join table
   name is always database-qualified: a mutation has no default database (proven 2026-09-11).
4. The mutation runs asynchronously (`mutations_sync = 0`) and `wait_for_mutation` polls
   system.mutations, because the first run rewrites 3.1M rows and a synchronous wait would sit
   on the driver's socket timeout. A transient part failure keeps the wait going while parts
   still complete; three failing polls without progress, or a killed mutation, give up.
5. The join table is dropped ONLY once the mutation reported `is_done = 1`. Dropping it while
   the mutation is queued or running leaves the mutation retrying forever against a missing
   table and every later mutation of the periods table queues behind it (reproduced on
   ClickHouse 26.5). A wait that fails therefore KEEPS the join table and raises with the
   `KILL MUTATION` recipe, and every run refuses to start while an earlier run's USD mutation
   is still unfinished (`unfinished_usd_mutation_sql`).

`_amount` stays the published figure in the row's monetary_unit; `_usd` is the FULL-UNIT dollar
value: scale first (MSEK 1e6, TSEK 1e3, SEK 1; the two per-employee figures are always MSEK),
then FX, in decimal arithmetic (`multiplyDecimal(..., 6)` keeps the Decimal(38, 6) result).
A row is converted once (the guard is `fx_rate_to_usd IS NULL`); a row whose rate date lies
outside the ECB series is left native-only and waits; the normalizer only ever inserts NEW
report versions, so it never overwrites a converted row.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from exchange_rates import ExchangeRateRequest

from dagster_v3.defs.sweden_ratsit.normalization import (
    RATSIT_CLICKHOUSE_DATABASE,
    RATSIT_FINANCIAL_PERIODS_TABLE,
)

DATABASE = RATSIT_CLICKHOUSE_DATABASE
QUALIFIED_PERIODS_TABLE = f"{DATABASE}.{RATSIT_FINANCIAL_PERIODS_TABLE}"
EXCHANGE_RATES_TABLE = f"{DATABASE}.exchange_rates"
RATSIT_FINANCIAL_CURRENCY = "SEK"
RATE_REQUEST_BATCH = 50
JOIN_TABLE_PREFIX = f"{DATABASE}._tmp_ratsit_fx_"
# Any join table of any run, for the pre-flight scan of system.mutations.
UNFINISHED_MUTATION_PATTERN = "%_tmp_ratsit_fx_%"
_JOIN_TABLE_IN_COMMAND = re.compile(re.escape(JOIN_TABLE_PREFIX) + r"[0-9A-Za-z]+")
# A mutation that failed on a part it is retrying still reports latest_fail_reason, so a fail
# reason is terminal only after this many polls in a row without a part completing.
FAILING_POLLS_BEFORE_GIVING_UP = 3

# The eighteen *_amount columns of se_ratsit_financial_periods, in DDL order, every one in the
# row's monetary_unit. balance_sheet_total_amount is kept for fidelity even though Ratsit's own
# field rule makes total_assets_amount the canonical balance-sheet total.
AMOUNT_COLUMNS: tuple[str, ...] = (
    "revenue_amount",
    "operating_costs_amount",
    "operating_profit_amount",
    "profit_after_financial_items_amount",
    "net_income_amount",
    "current_assets_amount",
    "fixed_assets_amount",
    "share_capital_amount",
    "equity_amount",
    "untaxed_reserves_amount",
    "provisions_amount",
    "long_term_liabilities_amount",
    "current_liabilities_amount",
    "liabilities_amount",
    "total_assets_amount",
    "balance_sheet_total_amount",
    "ebitda_amount",
    "dividend_amount",
)
# The two per-employee figures Ratsit publishes in MSEK regardless of the row's unit.
PER_EMPLOYEE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("personnel_cost_per_employee_msek", "personnel_cost_per_employee_usd"),
    ("revenue_per_employee_msek", "revenue_per_employee_usd"),
)
UNIT_SCALE_SQL = "multiIf(monetary_unit = 'MSEK', 1000000, monetary_unit = 'TSEK', 1000, 1)"
MSEK_SCALE_SQL = "1000000"
# (native column, usd column, scale expression) for the twenty pairs.
USD_PAIRS: tuple[tuple[str, str, str], ...] = tuple(
    (native, f"{native}_usd", UNIT_SCALE_SQL) for native in AMOUNT_COLUMNS
) + tuple((native, usd, MSEK_SCALE_SQL) for native, usd in PER_EMPLOYEE_COLUMNS)

# makeDate32 clamps a year below 1900 to 1970-01-01; such a date then falls outside the rate
# series bound and the row is left for a later run rather than converted at the 2006 rate.
RATE_DATE_SQL = "ifNull(period_end, makeDate32(fiscal_year, 12, 31))"
MONETARY_PRESENT_SQL = "(" + " OR ".join(f"{native} IS NOT NULL" for native, _, _ in USD_PAIRS) + ")"
# A NULL monetary_unit is not "SEK": multiIf treats the NULL condition as false and would scale
# such a row by 1, so it is excluded here and counted by unknown_unit_rows_sql instead.
PENDING_PREDICATE_SQL = (
    f"fx_rate_to_usd IS NULL AND monetary_unit IS NOT NULL AND {MONETARY_PRESENT_SQL}"
)
FX_COLUMNS: tuple[str, ...] = ("fx_rate_to_usd", "fx_rate_date", "fx_source")


def pending_rate_dates_sql() -> str:
    """The distinct rate dates of rows still without a rate, with their row counts."""
    return (
        f"SELECT {RATE_DATE_SQL} AS rate_date, count() AS rows\n"
        f"FROM {QUALIFIED_PERIODS_TABLE} FINAL\n"
        f"WHERE {PENDING_PREDICATE_SQL}\n"
        "GROUP BY rate_date\n"
        "ORDER BY rate_date"
    )


def unknown_unit_rows_sql() -> str:
    """Monetary rows the pending scan skips because their monetary_unit is NULL."""
    return (
        f"SELECT count() FROM {QUALIFIED_PERIODS_TABLE} FINAL "
        f"WHERE fx_rate_to_usd IS NULL AND monetary_unit IS NULL AND {MONETARY_PRESENT_SQL}"
    )


def rate_series_bounds_sql() -> str:
    """The first and last date the ECB series covers for BOTH legs of SEK to USD through EUR."""
    return (
        "SELECT greatest(minIf(rate_date, quote_currency = 'SEK'), "
        "minIf(rate_date, quote_currency = 'USD')) AS first_date, "
        "least(maxIf(rate_date, quote_currency = 'SEK'), "
        "maxIf(rate_date, quote_currency = 'USD')) AS last_date "
        f"FROM {EXCHANGE_RATES_TABLE} "
        "WHERE base_currency = 'EUR' AND quote_currency IN ('SEK', 'USD')"
    )


def unfinished_usd_mutation_sql() -> str:
    """Any USD mutation of the periods table, from any run, that has not finished."""
    return (
        "SELECT mutation_id, command FROM system.mutations "
        "WHERE database = %(database)s AND table = %(table)s AND is_done = 0 "
        "AND command LIKE %(pattern)s"
    )


def join_table_name(run_id: str) -> str:
    """A database-qualified, run-scoped Join table name (only [0-9A-Za-z] of the run id)."""
    return f"{JOIN_TABLE_PREFIX}{re.sub(r'[^0-9A-Za-z]', '', run_id)}"


def join_table_ddl(join_table: str) -> str:
    return (
        f"CREATE TABLE {join_table} (rate_date Date32, fx_rate Decimal(38, 12), "
        "fx_rate_date Date32, fx_source String) ENGINE = Join(ANY, LEFT, rate_date)"
    )


def join_insert_sql(join_table: str) -> str:
    return f"INSERT INTO {join_table} (rate_date, fx_rate, fx_rate_date, fx_source) VALUES"


def usd_update_sql(join_table: str, *, mutations_sync: int = 0) -> str:
    """One mutation over the pending rows whose rate date is in the join table."""
    rate = f"joinGet('{join_table}', 'fx_rate', {RATE_DATE_SQL})"
    assignments = [
        f"{usd} = multiplyDecimal({native} * {scale_sql}, {rate}, 6)"
        for native, usd, scale_sql in USD_PAIRS
    ] + [
        f"fx_rate_to_usd = {rate}",
        f"fx_rate_date = joinGet('{join_table}', 'fx_rate_date', {RATE_DATE_SQL})",
        f"fx_source = joinGet('{join_table}', 'fx_source', {RATE_DATE_SQL})",
    ]
    return (
        f"ALTER TABLE {QUALIFIED_PERIODS_TABLE} UPDATE\n"
        + ",\n".join(f"    {assignment}" for assignment in assignments)
        + f"\nWHERE {PENDING_PREDICATE_SQL} "
        f"AND {RATE_DATE_SQL} IN (SELECT rate_date FROM {join_table}) "
        f"SETTINGS mutations_sync = {mutations_sync}"
    )


def mutation_status_sql() -> str:
    """The newest mutation of the table whose command names the run's join table."""
    return (
        "SELECT mutation_id, is_done, latest_fail_reason, parts_to_do, is_killed "
        "FROM system.mutations "
        "WHERE database = %(database)s AND table = %(table)s AND command LIKE %(pattern)s "
        "ORDER BY create_time DESC LIMIT 1"
    )


@dataclass(frozen=True)
class UsdCounts:
    rows_pending: int
    rate_dates_needed: int
    rates_found: int
    rows_convertible: int
    rows_rate_date_outside_series: int
    rows_unknown_unit: int
    rows_converted: int
    rows_still_without_rate: int
    executed: bool

    def as_metadata(self) -> dict[str, int | bool]:
        return {
            "rows_pending": self.rows_pending,
            "rate_dates_needed": self.rate_dates_needed,
            "rates_found": self.rates_found,
            "rows_convertible": self.rows_convertible,
            "rows_rate_date_outside_series": self.rows_rate_date_outside_series,
            "rows_unknown_unit": self.rows_unknown_unit,
            "rows_converted": self.rows_converted,
            "rows_still_without_rate": self.rows_still_without_rate,
            "executed": self.executed,
        }


class MutationNotDoneError(RuntimeError):
    """A mutation that will not finish on its own: killed, or failing without progress.
    `mutation_id` is the id from the last status row, for the caller's KILL recipe."""

    def __init__(self, message: str, *, mutation_id: str) -> None:
        super().__init__(message)
        self.mutation_id = mutation_id


class MutationTimeoutError(TimeoutError):
    """A mutation still running when the wait budget ran out; carries the same mutation id."""

    def __init__(self, message: str, *, mutation_id: str) -> None:
        super().__init__(message)
        self.mutation_id = mutation_id


def load_usd_rates(
    exchange_rates: Any, requests: Sequence[ExchangeRateRequest]
) -> dict[tuple[str, str], Any]:
    """Batches of RATE_REQUEST_BATCH; a batch the client refuses (LookupError on any date)
    is retried one request at a time so only the missing dates are dropped. Same shape as
    sweden_financial.usd_conversion._load_rates."""
    rates: dict[tuple[str, str], Any] = {}
    for start in range(0, len(requests), RATE_REQUEST_BATCH):
        batch = list(requests[start : start + RATE_REQUEST_BATCH])
        try:
            rates.update(exchange_rates.usd_rates(batch))
        except LookupError:
            for request in batch:
                try:
                    rates.update(exchange_rates.usd_rates([request]))
                except LookupError:
                    continue
    return rates


def _pending(client: Any) -> list[tuple[date, int]]:
    return [(row[0], int(row[1])) for row in client.execute(pending_rate_dates_sql())]


def _rate_series_bounds(client: Any) -> tuple[date | None, date | None]:
    """The ECB window, or (None, None) when the rate table has no usable series."""
    rows = client.execute(rate_series_bounds_sql())
    if not rows:
        return None, None
    return rows[0][0], rows[0][1]


def _unknown_unit_rows(client: Any) -> int:
    rows = client.execute(unknown_unit_rows_sql())
    return int(rows[0][0]) if rows else 0


def _recovery_recipe(mutation_id: str, join_table: str) -> str:
    return (
        f"Recover with: KILL MUTATION WHERE database = '{DATABASE}' AND "
        f"table = '{RATSIT_FINANCIAL_PERIODS_TABLE}' AND mutation_id = '{mutation_id}'; "
        f"DROP TABLE {join_table}"
    )


def _join_table_in(command: str) -> str:
    match = _JOIN_TABLE_IN_COMMAND.search(command)
    return match.group(0) if match else f"{JOIN_TABLE_PREFIX}<see the mutation command>"


def _refuse_while_a_usd_mutation_is_unfinished(client: Any) -> None:
    """A USD mutation left over from an earlier run blocks this one: its join table may already
    be gone, and a second mutation would only queue behind it. Report it, write nothing."""
    rows = client.execute(
        unfinished_usd_mutation_sql(),
        {
            "database": DATABASE,
            "table": RATSIT_FINANCIAL_PERIODS_TABLE,
            "pattern": UNFINISHED_MUTATION_PATTERN,
        },
    )
    if not rows:
        return
    mutation_id, command = str(rows[0][0]), str(rows[0][1])
    join_table = _join_table_in(command)
    raise RuntimeError(
        f"A USD mutation of an earlier run is still unfinished on {QUALIFIED_PERIODS_TABLE}: "
        f"mutation {mutation_id}, join table {join_table}. This run would queue behind it. "
        + _recovery_recipe(mutation_id, join_table)
    )


def wait_for_mutation(
    client: Any,
    *,
    join_table: str,
    poll_seconds: float = 5.0,
    timeout_seconds: float = 7200.0,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Poll system.mutations for the mutation whose command names `join_table` until it is
    done, and return its mutation_id. A killed mutation raises MutationNotDoneError at once;
    a latest_fail_reason raises only after FAILING_POLLS_BEFORE_GIVING_UP polls in a row
    without parts_to_do falling, because ClickHouse reports the reason of a part it then
    retries. No completion within the timeout raises MutationTimeoutError. A poll that finds
    no row yet keeps waiting (the row appears right after the ALTER returns, but a replica lag
    is possible)."""
    waited = 0.0
    mutation_id = ""
    previous_parts: int | None = None
    failing_polls = 0
    params = {
        "database": DATABASE,
        "table": RATSIT_FINANCIAL_PERIODS_TABLE,
        "pattern": f"%{join_table}%",
    }
    while True:
        rows = client.execute(mutation_status_sql(), params)
        if rows:
            mutation_id = str(rows[0][0])
            is_done, fail_reason = int(rows[0][1]), rows[0][2]
            parts_to_do, is_killed = int(rows[0][3]), int(rows[0][4])
            if is_killed == 1:
                raise MutationNotDoneError(
                    f"USD mutation {mutation_id} was killed (join table {join_table})",
                    mutation_id=mutation_id,
                )
            if is_done == 1:
                return mutation_id
            if fail_reason:
                progressed = previous_parts is not None and parts_to_do < previous_parts
                failing_polls = 1 if progressed else failing_polls + 1
                if failing_polls >= FAILING_POLLS_BEFORE_GIVING_UP:
                    raise MutationNotDoneError(
                        f"USD mutation {mutation_id} failed {failing_polls} polls in a row with "
                        f"{parts_to_do} parts left: {fail_reason}",
                        mutation_id=mutation_id,
                    )
            else:
                failing_polls = 0
            previous_parts = parts_to_do
        if waited >= timeout_seconds:
            raise MutationTimeoutError(
                f"USD mutation still running after {timeout_seconds:.0f} s "
                f"(join table {join_table})",
                mutation_id=mutation_id,
            )
        sleep(poll_seconds)
        waited += poll_seconds


def convert_ratsit_financial_periods(
    client: Any,
    exchange_rates: Any,
    *,
    run_id: str,
    execute: bool,
    log: Callable[..., object] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> UsdCounts:
    """Preview (execute=False) counts the pending rows and the rates that exist for them and
    writes nothing. Execute loads the rates into a run-scoped Join table, runs the mutation,
    waits for it, drops the join table once it is done, and re-counts what is still pending.
    Both refuse to start while an earlier run's USD mutation is unfinished.

    `rows_converted` is the pending delta measured across the run, so it is approximate when a
    normalize run inserts rows meanwhile."""
    _refuse_while_a_usd_mutation_is_unfinished(client)
    pending = _pending(client)
    rows_pending = sum(rows for _, rows in pending)
    first_date, last_date = _rate_series_bounds(client)
    in_series = [
        (rate_date, rows)
        for rate_date, rows in pending
        if first_date is not None and last_date is not None and first_date <= rate_date <= last_date
    ]
    rows_outside_series = rows_pending - sum(rows for _, rows in in_series)
    rows_unknown_unit = _unknown_unit_rows(client)
    requests = [
        ExchangeRateRequest(currency=RATSIT_FINANCIAL_CURRENCY, rate_date=rate_date.isoformat())
        for rate_date, _ in in_series
    ]
    rates = load_usd_rates(exchange_rates, requests)
    rows_convertible = sum(
        rows for rate_date, rows in in_series
        if (RATSIT_FINANCIAL_CURRENCY, rate_date.isoformat()) in rates
    )
    counts = UsdCounts(
        rows_pending=rows_pending,
        rate_dates_needed=len(pending),
        rates_found=len(rates),
        rows_convertible=rows_convertible,
        rows_rate_date_outside_series=rows_outside_series,
        rows_unknown_unit=rows_unknown_unit,
        rows_converted=0,
        rows_still_without_rate=rows_pending,
        executed=execute,
    )
    if log is not None:
        log(
            "Ratsit financial USD %s: rows_pending=%s rate_dates_needed=%s rates_found=%s "
            "rows_convertible=%s rows_rate_date_outside_series=%s rows_unknown_unit=%s",
            "execute" if execute else "preview",
            rows_pending, len(pending), len(rates), rows_convertible,
            rows_outside_series, rows_unknown_unit,
        )
    if not execute or not rates:
        return counts

    join_table = join_table_name(run_id)
    client.execute(join_table_ddl(join_table))
    try:
        client.execute(
            join_insert_sql(join_table),
            [
                (
                    date.fromisoformat(rate.requested_rate_date),
                    rate.rate,
                    date.fromisoformat(rate.rate_date),
                    rate.source,
                )
                for rate in rates.values()
            ],
        )
        client.execute(usd_update_sql(join_table))
    except Exception:
        # Nothing was submitted, so no mutation can reference the join table: clean it up.
        client.execute(f"DROP TABLE IF EXISTS {join_table}")
        raise
    try:
        mutation_id = wait_for_mutation(client, join_table=join_table, sleep=sleep)
    except (RuntimeError, TimeoutError) as error:
        # The mutation is still queued or retrying: dropping the join table now would leave it
        # retrying against a missing table and block every later mutation of this table.
        mutation_id = getattr(error, "mutation_id", "") or "<none seen>"
        raise RuntimeError(
            f"USD mutation {mutation_id} on {QUALIFIED_PERIODS_TABLE} did not finish ({error}); "
            f"the join table {join_table} is kept. " + _recovery_recipe(mutation_id, join_table)
        ) from error
    client.execute(f"DROP TABLE IF EXISTS {join_table}")

    still = sum(rows for _, rows in _pending(client))
    counts = replace(counts, rows_converted=rows_pending - still, rows_still_without_rate=still)
    if log is not None:
        log(
            "Ratsit financial USD converted rows_converted=%s rows_still_without_rate=%s "
            "mutation_id=%s",
            counts.rows_converted, counts.rows_still_without_rate, mutation_id,
        )
    return counts
