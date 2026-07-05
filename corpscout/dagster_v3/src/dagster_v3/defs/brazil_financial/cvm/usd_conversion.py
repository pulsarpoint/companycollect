from collections.abc import Callable
from typing import Any, Protocol

from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_STATEMENT_ROWS_TABLE,
)

_RATE_REQUEST_BATCH = 50


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def apply_brazil_cvm_statement_rows_usd_conversion(
    *,
    duckdb_connection: Any,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    return apply_brazil_cvm_statement_rows_usd_conversion_for_table(
        duckdb_connection=duckdb_connection,
        exchange_rates=exchange_rates,
        statement_rows_table=DFP_STATEMENT_ROWS_TABLE,
        log=log,
    )


def apply_brazil_cvm_statement_rows_usd_conversion_for_table(
    *,
    duckdb_connection: Any,
    exchange_rates: ExchangeRates,
    statement_rows_table: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    qualified_table = f"{BRAZIL_CVM_DUCKDB_SCHEMA}.{statement_rows_table}"
    _ensure_usd_columns(duckdb_connection, qualified_table)

    pairs = _rate_pairs(duckdb_connection, qualified_table)
    requests = [_request(currency, period_end) for currency, period_end in pairs]
    rates = _load_rates(exchange_rates, requests)
    fx_rows = [
        (currency, period_end, rate.rate, str(rate.rate_date), rate.source)
        for currency, period_end in pairs
        if (rate := rates.get((currency, period_end))) is not None
    ]

    duckdb_connection.execute(
        "create or replace temp table _br_cvm_fx ("
        "currency varchar, period_end date, fx_rate decimal(38, 12), "
        "fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        duckdb_connection.executemany(
            "insert into _br_cvm_fx values "
            "(?, cast(? as date), cast(? as decimal(38, 12)), cast(? as date), ?)",
            fx_rows,
        )

    duckdb_connection.execute(
        f"""
        update {qualified_table}
        set amount_usd = NULL,
            fx_rate_to_usd = NULL,
            fx_rate_date = NULL,
            fx_source = ''
        """
    )
    duckdb_connection.execute(
        f"""
        update {qualified_table} as statement_rows
        set amount_usd = try_cast(
                cast(statement_rows.amount_original as double)
                * {_scale_factor_expression("statement_rows.scale")}
                * cast(fx.fx_rate as double)
                as decimal(38, 6)
            ),
            fx_rate_to_usd = fx.fx_rate,
            fx_rate_date = fx.fx_rate_date,
            fx_source = fx.fx_source
        from _br_cvm_fx as fx
        where {_currency_expression("statement_rows.currency")} = fx.currency
          and statement_rows.period_end_date = fx.period_end
          and statement_rows.amount_original is not null
        """
    )
    converted = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified_table} where fx_rate_to_usd is not null"
        ).fetchone()[0]
    )

    counts = {
        "rate_pairs": len(pairs),
        "rates_found": len(fx_rows),
        "rows_converted": converted,
    }
    if log is not None:
        log(
            "Applied Brazil CVM statement row USD conversion: table=%s rate_pairs=%s rates_found=%s rows_converted=%s",
            qualified_table,
            counts["rate_pairs"],
            counts["rates_found"],
            counts["rows_converted"],
        )
    return counts


def _ensure_usd_columns(duckdb_connection: Any, qualified_table: str) -> None:
    duckdb_connection.execute(
        f"alter table {qualified_table} add column if not exists amount_usd decimal(38, 6)"
    )
    duckdb_connection.execute(
        f"alter table {qualified_table} add column if not exists fx_rate_to_usd decimal(38, 12)"
    )
    duckdb_connection.execute(
        f"alter table {qualified_table} add column if not exists fx_rate_date date"
    )
    duckdb_connection.execute(
        f"alter table {qualified_table} add column if not exists fx_source varchar"
    )


def _rate_pairs(duckdb_connection: Any, qualified_table: str) -> list[tuple[str, str]]:
    rows = duckdb_connection.execute(
        f"""
        select distinct currency, cast(period_end_date as varchar)
        from {qualified_table}
        where amount_original is not null
          and coalesce(currency, '') <> ''
          and period_end_date is not null
        """
    ).fetchall()
    return sorted(
        {
            (normalized_currency, period_end)
            for currency, period_end in rows
            if (normalized_currency := _normalize_currency(str(currency or ""))) != ""
        }
    )


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


def _normalize_currency(currency: str) -> str:
    normalized = currency.strip().upper()
    if normalized in {"REAL", "REAIS", "R$", "BRL"}:
        return "BRL"
    if normalized in {"DOLAR", "DÓLAR", "USD"}:
        return "USD"
    if normalized in {"EURO", "EUR"}:
        return "EUR"
    return normalized


def _currency_expression(column: str) -> str:
    return f"""
        case upper(trim(coalesce({column}, '')))
            when 'REAL' then 'BRL'
            when 'REAIS' then 'BRL'
            when 'R$' then 'BRL'
            when 'BRL' then 'BRL'
            when 'DOLAR' then 'USD'
            when 'DÓLAR' then 'USD'
            when 'USD' then 'USD'
            when 'EURO' then 'EUR'
            when 'EUR' then 'EUR'
            else upper(trim(coalesce({column}, '')))
        end
    """


def _scale_factor_expression(column: str) -> str:
    return f"""
        case upper(trim(coalesce({column}, '')))
            when 'MIL' then 1000
            when 'MILHAR' then 1000
            when 'MILHARES' then 1000
            when 'MILHÃO' then 1000000
            when 'MILHAO' then 1000000
            when 'MILHÕES' then 1000000
            when 'MILHOES' then 1000000
            else 1
        end
    """
