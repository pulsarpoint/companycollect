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
