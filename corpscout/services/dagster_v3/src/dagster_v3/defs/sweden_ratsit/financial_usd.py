"""USD twins for se_ratsit_financial_periods (financial entity spec 2026-09-11 section 3,
slice 0).

Ratsit publishes twenty monetary figures per period in MSEK (one decimal), TSEK or SEK and no
dollar value. The currency standard wants every monetary figure stored WITH its USD twin in the
source table, converted as a separate re-runnable step. This module is that step:

1. `pending_rate_dates_sql` finds the distinct rate dates of rows whose fx_rate_to_usd is
   still NULL (the rate date is the period end, or Dec 31 of the fiscal year for the 7,503
   undated rows).
2. The rates come from the shared ExchangeRateClient (SEK to USD through EUR, the latest ECB
   date at or before the requested one; for dates before the 2006 start of the rate table the
   earliest date after). They are written into a per-run Join table.
3. ONE `ALTER TABLE ... UPDATE` mutation fills the twins and the three fx columns of every
   pending row whose date got a rate, with `joinGet` against the join table. The join table
   name is always database-qualified: a mutation has no default database (proven 2026-09-11).
4. The mutation runs asynchronously (`mutations_sync = 0`) and `wait_for_mutation` polls
   system.mutations, because the first run rewrites 3.1M rows and a synchronous wait would sit
   on the driver's socket timeout. The join table is dropped only after the mutation is done.

`_amount` stays the published figure in the row's monetary_unit; `_usd` is the FULL-UNIT dollar
value: scale first (MSEK 1e6, TSEK 1e3, SEK 1; the two per-employee figures are always MSEK),
then FX, in decimal arithmetic (`multiplyDecimal(..., 6)` keeps the Decimal(38, 6) result).
A row is converted once (the guard is `fx_rate_to_usd IS NULL`); a row whose rate does not
exist yet is picked up on a later run; the normalizer only ever inserts NEW report versions, so
it never overwrites a converted row.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from exchange_rates import ExchangeRateRequest

from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_FINANCIAL_PERIODS_TABLE

DATABASE = "corpscout"
QUALIFIED_PERIODS_TABLE = f"{DATABASE}.{RATSIT_FINANCIAL_PERIODS_TABLE}"
RATSIT_FINANCIAL_CURRENCY = "SEK"
RATE_REQUEST_BATCH = 50
JOIN_TABLE_PREFIX = f"{DATABASE}._tmp_ratsit_fx_"

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

RATE_DATE_SQL = "ifNull(period_end, makeDate32(fiscal_year, 12, 31))"
MONETARY_PRESENT_SQL = "(" + " OR ".join(f"{native} IS NOT NULL" for native, _, _ in USD_PAIRS) + ")"
FX_COLUMNS: tuple[str, ...] = ("fx_rate_to_usd", "fx_rate_date", "fx_source")


def pending_rate_dates_sql() -> str:
    """The distinct rate dates of rows still without a rate, with their row counts."""
    return (
        f"SELECT {RATE_DATE_SQL} AS rate_date, count() AS rows\n"
        f"FROM {QUALIFIED_PERIODS_TABLE} FINAL\n"
        f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL}\n"
        "GROUP BY rate_date\n"
        "ORDER BY rate_date"
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
        + f"\nWHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL} "
        f"AND {RATE_DATE_SQL} IN (SELECT rate_date FROM {join_table}) "
        f"SETTINGS mutations_sync = {mutations_sync}"
    )


def mutation_status_sql() -> str:
    """The newest mutation of the table whose command names the run's join table."""
    return (
        "SELECT is_done, latest_fail_reason FROM system.mutations "
        "WHERE database = %(database)s AND table = %(table)s AND command LIKE %(pattern)s "
        "ORDER BY create_time DESC LIMIT 1"
    )


@dataclass(frozen=True)
class UsdCounts:
    rows_pending: int
    rate_dates_needed: int
    rates_found: int
    rows_convertible: int
    rows_converted: int
    rows_still_without_rate: int
    executed: bool

    def as_metadata(self) -> dict[str, int | bool]:
        return {
            "rows_pending": self.rows_pending,
            "rate_dates_needed": self.rate_dates_needed,
            "rates_found": self.rates_found,
            "rows_convertible": self.rows_convertible,
            "rows_converted": self.rows_converted,
            "rows_still_without_rate": self.rows_still_without_rate,
            "executed": self.executed,
        }


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


def wait_for_mutation(
    client: Any,
    *,
    join_table: str,
    poll_seconds: float = 5.0,
    timeout_seconds: float = 7200.0,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Poll system.mutations for the mutation whose command names `join_table` until it is
    done. A failure reason raises RuntimeError; no completion within the timeout raises
    TimeoutError. A poll that finds no row yet keeps waiting (the row appears right after
    the ALTER returns, but a replica lag is possible)."""
    waited = 0.0
    params = {
        "database": DATABASE,
        "table": RATSIT_FINANCIAL_PERIODS_TABLE,
        "pattern": f"%{join_table}%",
    }
    while True:
        rows = client.execute(mutation_status_sql(), params)
        if rows:
            is_done, fail_reason = rows[0][0], rows[0][1]
            if fail_reason:
                raise RuntimeError(f"USD mutation failed: {fail_reason}")
            if int(is_done) == 1:
                return
        if waited >= timeout_seconds:
            raise TimeoutError(
                f"USD mutation still running after {timeout_seconds:.0f} s (join table {join_table})"
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
    waits for it, drops the join table, and re-counts what is still pending."""
    pending = _pending(client)
    rows_pending = sum(rows for _, rows in pending)
    requests = [
        ExchangeRateRequest(currency=RATSIT_FINANCIAL_CURRENCY, rate_date=rate_date.isoformat())
        for rate_date, _ in pending
    ]
    rates = load_usd_rates(exchange_rates, requests)
    rows_convertible = sum(
        rows for rate_date, rows in pending
        if (RATSIT_FINANCIAL_CURRENCY, rate_date.isoformat()) in rates
    )
    counts = UsdCounts(
        rows_pending=rows_pending,
        rate_dates_needed=len(pending),
        rates_found=len(rates),
        rows_convertible=rows_convertible,
        rows_converted=0,
        rows_still_without_rate=rows_pending,
        executed=execute,
    )
    if log is not None:
        log(
            "Ratsit financial USD %s: rows_pending=%s rate_dates_needed=%s rates_found=%s rows_convertible=%s",
            "execute" if execute else "preview",
            rows_pending, len(pending), len(rates), rows_convertible,
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
                    date.fromisoformat(str(rate.rate_date)),
                    str(rate.source),
                )
                for rate in rates.values()
            ],
        )
        client.execute(usd_update_sql(join_table))
        wait_for_mutation(client, join_table=join_table, sleep=sleep)
    finally:
        client.execute(f"DROP TABLE IF EXISTS {join_table}")

    still = sum(rows for _, rows in _pending(client))
    counts = replace(counts, rows_converted=rows_pending - still, rows_still_without_rate=still)
    if log is not None:
        log(
            "Ratsit financial USD converted rows_converted=%s rows_still_without_rate=%s",
            counts.rows_converted, counts.rows_still_without_rate,
        )
    return counts
