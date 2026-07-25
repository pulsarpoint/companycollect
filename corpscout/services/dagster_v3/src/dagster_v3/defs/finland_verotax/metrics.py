"""USD conversion for the Finland verotax tax_records table."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any, Protocol

import pyarrow as pa
from duckdb import DuckDBPyConnection

from dagster_v3.defs.finland_verotax import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
_RATE_REQUEST_BATCH = 50
_FX_BATCH_RELATION = "_fi_verotax_fx_batch"
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("currency", pa.string(), nullable=False),
        pa.field("period_end", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.string(), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
        pa.field("fx_source", pa.string()),
    ]
)


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def _load_rates(
    exchange_rates: ExchangeRates, requests: list[Any]
) -> dict[tuple[str, str], Any]:
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


def apply_finland_verotax_usd_conversion(
    *,
    duckdb_connection: DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd and fx_* columns on tax_records, keyed on period_end_date."""
    qualified = f"{DLT_DATASET_NAME}.{tables.TAX_RECORDS_TABLE}"
    pairs = duckdb_connection.execute(
        f"""
        select distinct upper(currency) as currency,
                        cast(period_end_date as varchar) as period_end
        from {qualified}
        where coalesce(currency, '') <> '' and period_end_date is not null
        """
    ).fetchall()

    requests = [_request(currency, end) for currency, end in pairs]
    rates = _load_rates(exchange_rates, requests)
    fx_rows = [
        {
            "currency": currency,
            "period_end": date.fromisoformat(end),
            "fx_rate": str(rate.rate),
            "fx_rate_date": _date_value(rate.rate_date),
            "fx_source": rate.source,
        }
        for currency, end in pairs
        if (rate := rates.get((currency, end))) is not None
    ]

    reset_usd = ", ".join(
        f"{metric}_amount_usd = NULL" for metric in tables.TAX_METRIC_NAMES
    )
    set_usd = ", ".join(
        f"{metric}_amount_usd = cast({metric}_amount_original * fx.fx_rate "
        f"as decimal(38, 2))"
        for metric in tables.TAX_METRIC_NAMES
    )
    duckdb_connection.execute(
        "create or replace temp table _fi_verotax_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        fx_table = pa.Table.from_pylist(fx_rows, schema=_FX_ARROW_SCHEMA)
        duckdb_connection.register(_FX_BATCH_RELATION, fx_table)
        try:
            duckdb_connection.execute(
                "insert into _fi_verotax_fx "
                "select currency, period_end, "
                "cast(fx_rate as decimal(38, 12)), fx_rate_date, fx_source "
                f"from {_FX_BATCH_RELATION}"
            )
        finally:
            duckdb_connection.unregister(_FX_BATCH_RELATION)
    duckdb_connection.execute(
        f"update {qualified} set fx_rate_to_usd = NULL, fx_rate_date = NULL, "
        f"fx_source = '', {reset_usd}"
    )
    duckdb_connection.execute(
        f"""
        update {qualified} as records
        set fx_rate_to_usd = fx.fx_rate,
            fx_rate_date = fx.fx_rate_date,
            fx_source = fx.fx_source,
            {set_usd}
        from _fi_verotax_fx as fx
        where upper(records.currency) = fx.currency
          and records.period_end_date = fx.period_end
        """
    )
    converted = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified} where fx_rate_to_usd is not null"
        ).fetchone()[0]
    )

    counts = {
        "rate_pairs": len(pairs),
        "rates_found": len(fx_rows),
        "rows_converted": converted,
    }
    if log is not None:
        log(
            "Applied Finland verotax USD conversion: rate_pairs=%s rates_found=%s "
            "rows_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts


def _date_value(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
