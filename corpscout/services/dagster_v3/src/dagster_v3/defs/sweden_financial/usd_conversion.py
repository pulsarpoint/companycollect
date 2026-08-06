from collections.abc import Callable
from datetime import date
from typing import Any

import pyarrow as pa
from exchange_rates import ExchangeRateRequest


SWEDEN_FINANCIAL_FACTS_TABLE = "sweden_financial.facts"
_RATE_REQUEST_BATCH = 50
_FX_BATCH_RELATION = "_sweden_financial_fx_batch"
_FX_TABLE = "_sweden_financial_fx"
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("currency", pa.string(), nullable=False),
        pa.field("report_period_end", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.string(), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
        pa.field("fx_source", pa.string(), nullable=False),
    ]
)


def apply_sweden_financial_facts_usd_conversion(
    *,
    duckdb_connection: Any,
    exchange_rates: Any,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    _require_facts_table(duckdb_connection)
    rate_pairs = _rate_pairs(duckdb_connection)
    rates = _load_rates(
        exchange_rates,
        [
            ExchangeRateRequest(currency=currency, rate_date=report_period_end)
            for currency, report_period_end in rate_pairs
        ],
    )
    fx_rows = [
        {
            "currency": currency,
            "report_period_end": date.fromisoformat(report_period_end),
            "fx_rate": str(rate.rate),
            "fx_rate_date": date.fromisoformat(str(rate.rate_date)),
            "fx_source": str(rate.source),
        }
        for currency, report_period_end in rate_pairs
        if (rate := rates.get((currency, report_period_end))) is not None
    ]

    monetary_rows, rows_converted = _replace_fact_usd_values(
        duckdb_connection,
        fx_rows,
    )
    counts = {
        "rate_pairs": len(rate_pairs),
        "rates_found": len(fx_rows),
        "monetary_rows": monetary_rows,
        "rows_converted": rows_converted,
        "rows_without_fx": monetary_rows - rows_converted,
    }
    if log is not None:
        log(
            "Applied Sweden financial fact USD conversion: "
            "rate_pairs=%s rates_found=%s monetary_rows=%s "
            "rows_converted=%s rows_without_fx=%s",
            counts["rate_pairs"],
            counts["rates_found"],
            counts["monetary_rows"],
            counts["rows_converted"],
            counts["rows_without_fx"],
        )
    return counts


def _require_facts_table(duckdb_connection: Any) -> None:
    exists = bool(
        duckdb_connection.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = 'sweden_financial'
              and table_name = 'facts'
            """
        ).fetchone()[0]
    )
    if exists:
        return
    raise RuntimeError(
        "Sweden financial facts table is missing. Materialize the matching "
        "sweden_financial_*_parsed_reports_duckdb asset before USD conversion."
    )


def _rate_pairs(duckdb_connection: Any) -> list[tuple[str, str]]:
    return [
        (str(currency), str(report_period_end))
        for currency, report_period_end in duckdb_connection.execute(
            f"""
            select distinct
                upper(trim(currency)) as currency,
                cast(report_period_end as varchar) as report_period_end
            from {SWEDEN_FINANCIAL_FACTS_TABLE}
            where amount_original is not null
              and coalesce(currency, '') <> ''
              and report_period_end is not null
            order by currency, report_period_end
            """
        ).fetchall()
    ]


def _replace_fx_table(
    duckdb_connection: Any,
    fx_rows: list[dict[str, Any]],
) -> None:
    duckdb_connection.execute(
        f"""
        create or replace temp table {_FX_TABLE} (
            currency varchar,
            report_period_end date,
            fx_rate decimal(38, 12),
            fx_rate_date date,
            fx_source varchar
        )
        """
    )
    if not fx_rows:
        return

    duckdb_connection.register(
        _FX_BATCH_RELATION,
        pa.Table.from_pylist(fx_rows, schema=_FX_ARROW_SCHEMA),
    )
    try:
        duckdb_connection.execute(
            f"""
            insert into {_FX_TABLE}
            select currency,
                   report_period_end,
                   cast(fx_rate as decimal(38, 12)),
                   fx_rate_date,
                   fx_source
            from {_FX_BATCH_RELATION}
            """
        )
    finally:
        duckdb_connection.unregister(_FX_BATCH_RELATION)


def _replace_fact_usd_values(
    duckdb_connection: Any,
    fx_rows: list[dict[str, Any]],
) -> tuple[int, int]:
    duckdb_connection.execute("begin transaction")
    try:
        _replace_fx_table(duckdb_connection, fx_rows)
        duckdb_connection.execute(
            f"""
            update {SWEDEN_FINANCIAL_FACTS_TABLE}
            set amount_usd = NULL,
                fx_rate_to_usd = NULL,
                fx_rate_date = NULL,
                fx_source = ''
            """
        )
        duckdb_connection.execute(
            f"""
            update {SWEDEN_FINANCIAL_FACTS_TABLE} as facts
            set amount_usd = try_cast(
                    cast(facts.amount_original as double)
                    * cast(fx.fx_rate as double)
                    as decimal(38, 10)
                ),
                fx_rate_to_usd = fx.fx_rate,
                fx_rate_date = fx.fx_rate_date,
                fx_source = fx.fx_source
            from {_FX_TABLE} as fx
            where upper(trim(coalesce(facts.currency, ''))) = fx.currency
              and facts.report_period_end = fx.report_period_end
              and facts.amount_original is not null
            """
        )
        monetary_rows = _count_monetary_rows(duckdb_connection)
        rows_converted = _count_converted_rows(duckdb_connection)
    except Exception:
        duckdb_connection.execute("rollback")
        raise
    duckdb_connection.execute("commit")
    return monetary_rows, rows_converted


def _count_monetary_rows(duckdb_connection: Any) -> int:
    return int(
        duckdb_connection.execute(
            f"""
            select count(*)
            from {SWEDEN_FINANCIAL_FACTS_TABLE}
            where amount_original is not null
              and coalesce(currency, '') <> ''
              and report_period_end is not null
            """
        ).fetchone()[0]
    )


def _count_converted_rows(duckdb_connection: Any) -> int:
    return int(
        duckdb_connection.execute(
            f"""
            select count(*)
            from {SWEDEN_FINANCIAL_FACTS_TABLE}
            where amount_original is not null
              and coalesce(currency, '') <> ''
              and report_period_end is not null
              and amount_usd is not null
              and fx_rate_to_usd is not null
            """
        ).fetchone()[0]
    )


def _load_rates(
    exchange_rates: Any,
    requests: list[ExchangeRateRequest],
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
