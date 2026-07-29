"""EUR to USD conversion for France financial metrics."""

from collections.abc import Callable
from datetime import date
from typing import Any, Protocol

import pyarrow as pa
from duckdb import DuckDBPyConnection

from dagster_v3.defs.france_financial import tables

_RATE_REQUEST_BATCH = 50
_FX_BATCH_RELATION = "_fr_financial_fx_batch"
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
    exchange_rates: ExchangeRates,
    requests: list[Any],
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


def apply_france_financial_usd_conversion(
    *,
    duckdb_connection: DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill every monetary USD companion and the shared FX metadata."""
    qualified = f"{tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
    pairs = duckdb_connection.execute(
        f"""
        select distinct upper(currency), cast(period_end_date as varchar)
        from {qualified}
        where coalesce(currency, '') <> '' and period_end_date is not null
        """
    ).fetchall()
    requests = [_request(currency, period_end) for currency, period_end in pairs]
    rates = _load_rates(exchange_rates, requests)
    fx_rows = [
        {
            "currency": currency,
            "period_end": date.fromisoformat(period_end),
            "fx_rate": str(rate.rate),
            "fx_rate_date": _date_value(rate.rate_date),
            "fx_source": rate.source,
        }
        for currency, period_end in pairs
        if (rate := rates.get((currency, period_end))) is not None
    ]

    duckdb_connection.execute(
        "create or replace temp table _fr_financial_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        fx_table = pa.Table.from_pylist(fx_rows, schema=_FX_ARROW_SCHEMA)
        duckdb_connection.register(_FX_BATCH_RELATION, fx_table)
        try:
            duckdb_connection.execute(
                "insert into _fr_financial_fx "
                "select currency, period_end, cast(fx_rate as decimal(38, 12)), "
                f"fx_rate_date, fx_source from {_FX_BATCH_RELATION}"
            )
        finally:
            duckdb_connection.unregister(_FX_BATCH_RELATION)

    reset_usd = ", ".join(
        f"{metric}_amount_usd = NULL" for metric in tables.MONETARY_METRICS
    )
    set_usd = ", ".join(
        f"{metric}_amount_usd = cast("
        f"{metric}_amount_original * fx.fx_rate as decimal(38, 2))"
        for metric in tables.MONETARY_METRICS
    )
    duckdb_connection.execute(
        f"update {qualified} set fx_rate_to_usd = NULL, fx_rate_date = NULL, "
        f"fx_source = '', {reset_usd}"
    )
    duckdb_connection.execute(
        f"""
        update {qualified} as metrics
        set fx_rate_to_usd = fx.fx_rate,
            fx_rate_date = fx.fx_rate_date,
            fx_source = fx.fx_source,
            {set_usd}
        from _fr_financial_fx as fx
        where upper(metrics.currency) = fx.currency
          and metrics.period_end_date = fx.period_end
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
            "Applied France financial USD conversion: rate_pairs=%s "
            "rates_found=%s rows_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts


def _date_value(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
