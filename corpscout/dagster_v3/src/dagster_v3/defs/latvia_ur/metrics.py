from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import duckdb

from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
WIDE_STATEMENTS = tables.FINANCIAL_STATEMENTS_WIDE_TABLE
METRICS_TABLE = tables.FINANCIAL_METRICS_WIDE_TABLE
SOURCE_SLUG = "latvia_ur_financials"


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    # Imported lazily so tests can inject a stub without the real client/env.
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def build_latvia_ur_financial_metrics(
    *,
    database_path: str | Path,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Distill native-currency headline metrics from the wide statements.

    Values are scaled by rounded_to_nearest. USD conversion is a SEPARATE step
    (apply_latvia_ur_usd_conversion) so metrics can be built/exported before the
    exchange_rates table is populated; the *_usd and fx_* columns are left empty
    here and filled by that step.
    """
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
    # Native DuckDB set-based build: scale each native amount by rounded_to_nearest
    # in one CREATE TABLE AS. (Was a fetchall() + per-row Python Decimal loop over
    # ~2M rows — tens of minutes; this runs in seconds.) USD/fx columns are left
    # empty for the separate apply_latvia_ur_usd_conversion step. The output column
    # order matches tables.LV_FINANCIAL_METRICS_COLUMNS.
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
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(build_sql, [source_run_id])
        metrics = int(
            connection.execute(f"select count(*) from {qualified}").fetchone()[0]
        )
        unknown_units = [
            row[0]
            for row in connection.execute(
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
            "Latvia UR metrics: unknown rounded_to_nearest units defaulted to factor 1: %s",
            sorted(unknown_units),
        )
    counts = {"metrics": metrics}
    if log is not None:
        log("Built Latvia UR financial metrics (native): metrics=%s", counts["metrics"])
    return counts


# The shared client builds one `UNION ALL` branch per requested (currency, date)
# pair, and ClickHouse's query-plan optimizer rejects very wide plans (code 572
# TOO_MANY_QUERY_PLAN_OPTIMIZATIONS). Latvia spans ~1k distinct period_end dates,
# so the request set must be chunked. 50 keeps each plan small with large margin
# even if optimization count grows super-linearly in the union width.
_RATE_REQUEST_BATCH = 50


def _load_rates(
    exchange_rates: ExchangeRates, requests: list[Any]
) -> dict[tuple[str, str], Any]:
    # Batch the pairs (see _RATE_REQUEST_BATCH); within a batch, fall back to
    # per-request on a missing-rate LookupError so one absent rate doesn't drop
    # the rest (mirrors norway_brreg.financial_normalize._load_available_usd_rates).
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


def apply_latvia_ur_usd_conversion(
    *,
    database_path: str | Path,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill *_usd and fx_* columns on the metrics table (a separate, re-runnable step).

    Converts each native *_amount_original to USD by the report period_end_date
    using the shared exchange-rate client. Set-based: build a small (currency,
    period_end_date) -> rate table and UPDATE-join. Re-running first clears prior
    USD so rows that lost a rate are reset. No-op-safe when exchange_rates is
    empty (USD stays NULL).
    """
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    with duckdb.connect(str(database_path), read_only=True) as connection:
        pairs = connection.execute(
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
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            "create or replace temp table _lv_fx ("
            "currency varchar, period_end date, fx_rate decimal(38, 12), "
            "fx_rate_date date, fx_source varchar)"
        )
        if fx_rows:
            connection.executemany(
                "insert into _lv_fx values "
                "(?, cast(? as date), cast(? as decimal(38, 12)), cast(? as date), ?)",
                fx_rows,
            )
        connection.execute(
            f"update {qualified} set fx_rate_to_usd = NULL, fx_rate_date = NULL, "
            f"fx_source = '', {reset_usd}"
        )
        connection.execute(
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
            connection.execute(
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
            "Applied Latvia UR USD conversion: rate_pairs=%s rates_found=%s rows_converted=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts
