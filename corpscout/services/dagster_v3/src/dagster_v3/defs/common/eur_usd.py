from collections.abc import Callable
from typing import Any

_RATE_REQUEST_BATCH = 50
_FX_TABLE = "_national_procurement_eur_fx"


def apply_eur_usd_conversion(
    *,
    duckdb_connection: Any,
    exchange_rates: Any,
    qualified_table: str,
    rate_date_columns: tuple[str, ...],
    amount_columns: tuple[tuple[str, str], ...],
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    rate_date = _rate_date_expression("records", rate_date_columns)
    pairs = [
        ("EUR", str(row[0]))
        for row in duckdb_connection.execute(
            f"""
            SELECT DISTINCT {rate_date} AS rate_date
            FROM {qualified_table} AS records
            WHERE {rate_date} IS NOT NULL
              AND ({" OR ".join(f"records.{source} IS NOT NULL" for source, _ in amount_columns)})
            """
        ).fetchall()
    ]
    rates = _load_rates(
        exchange_rates,
        [_request(currency, rate_date_value) for currency, rate_date_value in pairs],
    )
    fx_rows = [
        (
            rate_date_value,
            str(rate.rate),
            str(rate.rate_date),
            str(rate.source),
        )
        for currency, rate_date_value in pairs
        if (rate := rates.get((currency, rate_date_value))) is not None
    ]
    duckdb_connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {_FX_TABLE}
        (
            rate_date DATE,
            fx_rate DECIMAL(38, 12),
            fx_rate_date DATE,
            fx_source VARCHAR
        )
        """
    )
    if fx_rows:
        duckdb_connection.executemany(
            f"""
            INSERT INTO {_FX_TABLE}
            VALUES (
                CAST(? AS DATE),
                CAST(? AS DECIMAL(38, 12)),
                CAST(? AS DATE),
                ?
            )
            """,
            fx_rows,
        )

    duckdb_connection.execute(
        f"""
        UPDATE {qualified_table}
        SET {", ".join(f"{target} = NULL" for _, target in amount_columns)}
        """
    )
    counts = {"rate_dates": len(pairs), "rates_found": len(fx_rows)}
    for source, target in amount_columns:
        duckdb_connection.execute(
            f"""
            UPDATE {qualified_table} AS records
            SET {target} = CAST(
                CAST(records.{source} AS DOUBLE) * CAST(fx.fx_rate AS DOUBLE)
                AS DECIMAL(38, 2)
            )
            FROM {_FX_TABLE} AS fx
            WHERE {rate_date} = fx.rate_date
              AND records.{source} IS NOT NULL
            """
        )
        counts[f"converted_{target}"] = int(
            duckdb_connection.execute(
                f"SELECT count(*) FROM {qualified_table} WHERE {target} IS NOT NULL"
            ).fetchone()[0]
        )
    if log is not None:
        log("Applied EUR/USD conversion to %s: %s", qualified_table, counts)
    return counts


def _rate_date_expression(alias: str, columns: tuple[str, ...]) -> str:
    if not columns:
        raise ValueError("rate_date_columns must not be empty")
    if len(columns) == 1:
        return f"{alias}.{columns[0]}"
    qualified = ", ".join(f"{alias}.{column}" for column in columns)
    return f"coalesce({qualified})"


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def _load_rates(exchange_rates: Any, requests: list[Any]) -> dict[tuple[str, str], Any]:
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
