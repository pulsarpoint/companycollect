from collections.abc import Callable
from typing import Any, Protocol

from duckdb import DuckDBPyConnection

from dagster_v3.defs.estonia_ar import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
WIDE_STATEMENTS = tables.FINANCIAL_STATEMENTS_WIDE_TABLE
METRICS_TABLE = tables.FINANCIAL_METRICS_WIDE_TABLE
SOURCE_SLUG = "estonia_ar_financials"


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    # Imported lazily so tests can inject a stub without the real client/env.
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def build_estonia_ar_financial_metrics(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Distill native-currency (EUR) headline metrics from the wide statements.

    Estonian reports are in full EUR (no rounded_to_nearest factor). USD conversion
    is a SEPARATE step (apply_estonia_ar_usd_conversion); *_usd and fx_* are left
    empty here. Output column order matches tables.EE_FINANCIAL_METRICS_COLUMNS.
    """
    metric_select = ",\n            ".join(
        expr
        for name in tables.FINANCIAL_METRIC_NAMES
        for expr in (
            f"cast({name} as decimal(38, 2)) as {name}_amount_original",
            f"cast(null as decimal(38, 2)) as {name}_amount_usd",
        )
    )
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    build_sql = f"""
        create or replace table {qualified} as
        select
            'EE' as country_iso2,
            '{SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            report_id as source_record_id,
            source_payload_hash,
            report_id,
            reg_code,
            fiscal_year,
            period_start_date,
            period_end_date,
            upper(coalesce(currency, '')) as currency,
            {metric_select},
            cast(null as decimal(38, 12)) as fx_rate_to_usd,
            cast(null as date) as fx_rate_date,
            '' as fx_source,
            cast(now() as timestamp) as resolved_at
        from {DLT_DATASET_NAME}.{WIDE_STATEMENTS}
    """
    duckdb_connection.execute(build_sql, [source_run_id])
    metrics = int(
        duckdb_connection.execute(f"select count(*) from {qualified}").fetchone()[0]
    )
    if log is not None:
        log("Built Estonia AR financial metrics (native EUR): metrics=%s", metrics)
    return {"metrics": metrics}


# The shared client builds one UNION ALL branch per requested (currency, date)
# pair; ClickHouse rejects very wide plans (code 572). Batch the request set.
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


def apply_estonia_ar_usd_conversion(
    *,
    duckdb_connection: DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd and fx_* columns on the metrics table (a separate, re-runnable step).

    Converts each native *_amount_original to USD by the report period_end_date via
    the shared exchange-rate client. Set-based: build a small (currency,
    period_end_date) -> rate table and UPDATE-join. No-op-safe when rates absent.
    """
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
        "create or replace temp table _ee_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        duckdb_connection.executemany(
            "insert into _ee_fx values "
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
        from _ee_fx as fx
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
            "Applied Estonia AR USD conversion: rate_pairs=%s rates_found=%s rows_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts
