from collections.abc import Callable
from typing import Any, Protocol

from duckdb import DuckDBPyConnection

from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
WIDE_STATEMENTS = tables.FINANCIAL_STATEMENTS_WIDE_TABLE
METRICS_TABLE = tables.FINANCIAL_METRICS_WIDE_TABLE
SOURCE_SLUG = "latvia_financial"


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def build_latvia_financial_metrics(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Distill native-currency headline metrics from the wide statements table."""
    factor_case = "\n".join(
        f"                    when '{unit}' then {factor}"
        for unit, factor in tables.ROUNDED_TO_NEAREST_FACTORS.items()
    )
    metric_select = ",\n            ".join(
        expr
        for name in tables.FINANCIAL_METRIC_NAMES
        for expr in (
            f"cast({tables.FINANCIAL_METRIC_SOURCE_COLUMNS[name]} * _factor as "
            f"decimal(38, 2)) as {name}_amount_original",
            f"cast(null as decimal(38, 2)) as {name}_amount_usd",
        )
    )
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    build_sql = f"""
        create or replace table {qualified} as
        with scaled as (
            select
                *,
                case upper(coalesce(rounded_to_nearest, ''))
{factor_case}
                    else 1
                end as _factor
            from {DLT_DATASET_NAME}.{WIDE_STATEMENTS}
        )
        select
            'LV' as country_iso2,
            '{SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            statement_id as source_record_id,
            source_payload_hash,
            statement_id,
            regcode,
            fiscal_year,
            period_start_date,
            period_end_date,
            employees,
            upper(coalesce(currency, '')) as currency,
            rounded_to_nearest,
            {metric_select},
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            cast(now() as timestamp) as resolved_at
        from scaled
    """
    known_units = ", ".join(f"'{unit}'" for unit in tables.ROUNDED_TO_NEAREST_FACTORS)
    duckdb_connection.execute(build_sql, [source_run_id])
    metrics = int(
        duckdb_connection.execute(f"select count(*) from {qualified}").fetchone()[0]
    )
    unknown_units = [
        row[0]
        for row in duckdb_connection.execute(
            f"""
            select distinct rounded_to_nearest
            from {qualified}
            where coalesce(rounded_to_nearest, '') <> ''
              and upper(rounded_to_nearest) not in ({known_units})
            """
        ).fetchall()
    ]

    if unknown_units and log is not None:
        log(
            "Latvia metrics: unknown rounded_to_nearest units defaulted to factor 1: %s",
            sorted(unknown_units),
        )
    counts = {"metrics": metrics}
    if log is not None:
        log("Built Latvia financial metrics (native): metrics=%s", counts["metrics"])
    return counts


_RATE_REQUEST_BATCH = 50


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


def apply_latvia_financial_usd_conversion(
    *,
    duckdb_connection: DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd and fx_* columns on the metrics table."""
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
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
        (currency, end, rate.rate, str(rate.rate_date), rate.source)
        for currency, end in pairs
        if (rate := rates.get((currency, end))) is not None
    ]

    reset_usd = ", ".join(
        f"{metric}_amount_usd = NULL" for metric in tables.FINANCIAL_METRIC_NAMES
    )
    set_usd = ", ".join(
        f"{metric}_amount_usd = cast({metric}_amount_original * fx.fx_rate as decimal(38, 2))"
        for metric in tables.FINANCIAL_METRIC_NAMES
    )
    duckdb_connection.execute(
        "create or replace temp table _lv_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        duckdb_connection.executemany(
            "insert into _lv_fx values "
            "(?, cast(? as date), cast(? as decimal(38, 12)), cast(? as date), ?)",
            fx_rows,
        )
    duckdb_connection.execute(
        f"update {qualified} set fx_rate_to_usd = NULL, fx_rate_date = NULL, "
        f"fx_source = '', {reset_usd}"
    )
    duckdb_connection.execute(
        f"""
        update {qualified} as mt
        set fx_rate_to_usd = fx.fx_rate,
            fx_rate_date = fx.fx_rate_date,
            fx_source = fx.fx_source,
            {set_usd}
        from _lv_fx as fx
        where upper(mt.currency) = fx.currency
          and mt.period_end_date = fx.period_end
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
            "Applied Latvia USD conversion: rate_pairs=%s rates_found=%s rows_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts
