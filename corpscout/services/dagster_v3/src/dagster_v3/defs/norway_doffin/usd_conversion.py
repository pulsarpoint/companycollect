"""NOK -> USD for Doffin notices, as a step of its own.

All three figures are converted, not just the one the view reads. Converting a
subset would leave the rest answerable in NOK alone, which for a cross-country
product is the same loss as not storing them.

Keyed on ``issue_date``, falling back to ``publication_date``. Issue date is the
one Doffin filters on and the one partitions key on, so it is the date that
definitely exists; the fallback covers the handful where it does not.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any, Protocol

import pyarrow as pa

from dagster_v3.defs.norway_doffin import tables

# Bounds the per-request fallback: usd_rates raises LookupError if ANY requested
# pair is missing, so a batch keeps one absent date from discarding every rate
# fetched beside it.
_RATE_REQUEST_BATCH = 50
_FX_BATCH_RELATION = "_no_doffin_fx_batch"
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("currency", pa.string(), nullable=False),
        pa.field("rate_date", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.string(), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
        pa.field("fx_source", pa.string()),
    ]
)

RATE_DATE_EXPRESSION = "coalesce(issue_date, publication_date)"


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def apply_norway_doffin_usd_conversion(
    *,
    duckdb_connection: Any,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"

    # Each amount carries its own currency, so the pairs are collected per
    # metric rather than assuming one currency per notice. Every sample is NOK,
    # but a register that publishes a currency code per amount is entitled to
    # use it.
    pair_query = "\nunion\n".join(
        f"""
        select distinct {metric}_currency as currency,
               cast({RATE_DATE_EXPRESSION} as varchar) as rate_date
        from {qualified}
        where coalesce({metric}_currency, '') <> ''
          and {metric}_amount_original is not null
          and {RATE_DATE_EXPRESSION} is not null
        """
        for metric, _ in tables.VALUE_METRICS
    )
    pairs = [
        (str(currency), str(rate_date))
        for currency, rate_date in duckdb_connection.execute(pair_query).fetchall()
    ]
    rates = _load_rates(exchange_rates, [_request(*pair) for pair in pairs])
    fx_rows = [
        {
            "currency": currency,
            "rate_date": date.fromisoformat(rate_date),
            "fx_rate": str(rate.rate),
            "fx_rate_date": date.fromisoformat(str(rate.rate_date)),
            "fx_source": rate.source,
        }
        for currency, rate_date in pairs
        if (rate := rates.get((currency, rate_date))) is not None
    ]

    duckdb_connection.execute(
        "create or replace temp table _no_doffin_fx ("
        "currency varchar, rate_date date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        duckdb_connection.register(
            _FX_BATCH_RELATION, pa.Table.from_pylist(fx_rows, schema=_FX_ARROW_SCHEMA)
        )
        try:
            duckdb_connection.execute(
                "insert into _no_doffin_fx select currency, rate_date, "
                "cast(fx_rate as decimal(38, 12)), fx_rate_date, fx_source "
                f"from {_FX_BATCH_RELATION}"
            )
        finally:
            duckdb_connection.unregister(_FX_BATCH_RELATION)

    # Cleared first so a re-run after a rate correction cannot leave a stale
    # conversion beside an amount whose rate has since changed.
    cleared = ",\n            ".join(
        f"{metric}_amount_usd = NULL" for metric, _ in tables.VALUE_METRICS
    )
    duckdb_connection.execute(
        f"""
        update {qualified}
        set {cleared}, fx_rate_to_usd = NULL, fx_rate_date = NULL, fx_source = ''
        """
    )
    counts: dict[str, int] = {"rate_pairs": len(pairs), "rates_found": len(fx_rows)}
    for metric, _ in tables.VALUE_METRICS:
        duckdb_connection.execute(
            f"""
            update {qualified} as records
            set {metric}_amount_usd = try_cast(
                    cast(records.{metric}_amount_original as double)
                    * cast(fx.fx_rate as double) as decimal(38, 2)
                ),
                fx_rate_to_usd = fx.fx_rate,
                fx_rate_date = fx.fx_rate_date,
                fx_source = fx.fx_source
            from _no_doffin_fx as fx
            where records.{metric}_currency = fx.currency
              and {RATE_DATE_EXPRESSION} = fx.rate_date
              and records.{metric}_amount_original is not null
            """
        )
        counts[f"converted_{metric}"] = _count(
            duckdb_connection,
            f"select count(*) from {qualified} where {metric}_amount_usd is not null",
        )
        counts[f"present_{metric}"] = _count(
            duckdb_connection,
            f"select count(*) from {qualified} "
            f"where {metric}_amount_original is not null",
        )
    if log is not None:
        log("Doffin USD conversion: %s", counts)
    return counts


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def _load_rates(
    exchange_rates: ExchangeRates, requests: list[Any]
) -> dict[tuple[str, str], Any]:
    """LookupError only -- a connection error is a real failure and must surface
    rather than be swallowed as "no rate available", which would blank every
    USD figure while looking like a clean run."""
    rates: dict[tuple[str, str], Any] = {}
    for start in range(0, len(requests), _RATE_REQUEST_BATCH):
        batch = requests[start : start + _RATE_REQUEST_BATCH]
        try:
            rates.update(exchange_rates.usd_rates(batch))
        except LookupError:
            for request in batch:
                try:
                    rates.update(exchange_rates.usd_rates([request]))
                except LookupError:
                    continue
    return rates


def _count(duckdb_connection: Any, sql: str) -> int:
    return int(duckdb_connection.execute(sql).fetchone()[0])
