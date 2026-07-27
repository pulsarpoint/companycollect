"""BRL -> USD for PNCP contracts, as a step of its own.

Separate from extraction on purpose, per the currency guidelines: the native
``valor_global`` is stored faithfully and the conversion is layered on top, so a
rate correction never means re-parsing contracts and a parsing fix never
disturbs a rate.

**Which date the rate is keyed on** is the one real decision here.
``data_assinatura`` -- when the contract was signed -- is the economically right
one, but PNCP leaves it null on a minority of records, and a contract with no
USD figure is invisible to every cross-country comparison. So it falls back to
``data_publicacao_pncp``, which is never null in practice because it is what the
register partitions on. ``fx_rate_date`` records the date the rate actually came
from, so a reader can always see which was used.

PNCP publishes no currency field at all -- there is no ``moeda`` in the API's 41
fields -- so BRL is implicit. It is written as a literal here rather than
inferred, because "the source said BRL" and "we assumed BRL" are different
claims and only the second is true.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any, Protocol

import pyarrow as pa

from dagster_v3.defs.brazil_pncp import tables

# The currency PNCP contracts are denominated in. Not read from the payload:
# the payload has no such field.
CONTRACT_CURRENCY = "BRL"

# usd_rates() takes an arbitrarily large request set in one call, so this is not
# about query-plan size. It bounds the *fallback*: usd_rates raises LookupError
# if any single pair is missing, so a batch keeps one absent date from
# discarding every rate fetched alongside it.
_RATE_REQUEST_BATCH = 50

_FX_BATCH_RELATION = "_br_pncp_fx_batch"
_FX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("rate_date", pa.date32(), nullable=False),
        pa.field("fx_rate", pa.string(), nullable=False),
        pa.field("fx_rate_date", pa.date32(), nullable=False),
        pa.field("fx_source", pa.string()),
    ]
)

# The date a contract's rate is looked up on, and the same expression the update
# joins on. Defined once so the two cannot drift apart.
RATE_DATE_EXPRESSION = "coalesce(data_assinatura, data_publicacao_pncp)"


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def apply_brazil_pncp_usd_conversion(
    *,
    duckdb_connection: Any,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Fill ``valor_global_usd`` and the three FX provenance columns."""
    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    _require_candidates_table(duckdb_connection)
    ensure_usd_columns(duckdb_connection)

    rate_dates = _rate_dates(duckdb_connection)
    rates = _load_rates(
        exchange_rates,
        [_request(CONTRACT_CURRENCY, rate_date) for rate_date in rate_dates],
    )
    fx_rows = [
        {
            "rate_date": date.fromisoformat(rate_date),
            "fx_rate": str(rate.rate),
            "fx_rate_date": date.fromisoformat(str(rate.rate_date)),
            "fx_source": rate.source,
        }
        for rate_date in rate_dates
        if (rate := rates.get((CONTRACT_CURRENCY, rate_date))) is not None
    ]

    duckdb_connection.execute(
        "create or replace temp table _br_pncp_fx ("
        "rate_date date, fx_rate decimal(38, 12), fx_rate_date date, fx_source varchar)"
    )
    if fx_rows:
        duckdb_connection.register(
            _FX_BATCH_RELATION, pa.Table.from_pylist(fx_rows, schema=_FX_ARROW_SCHEMA)
        )
        try:
            duckdb_connection.execute(
                "insert into _br_pncp_fx select rate_date, "
                "cast(fx_rate as decimal(38, 12)), fx_rate_date, fx_source "
                f"from {_FX_BATCH_RELATION}"
            )
        finally:
            duckdb_connection.unregister(_FX_BATCH_RELATION)

    # Cleared first so a re-run after a rate correction cannot leave a stale
    # conversion sitting beside a contract whose rate has since changed.
    duckdb_connection.execute(
        f"""
        update {qualified}
        set valor_global_usd = NULL,
            fx_rate_to_usd = NULL,
            fx_rate_date = NULL,
            fx_source = ''
        """
    )
    duckdb_connection.execute(
        f"""
        update {qualified} as contracts
        set valor_global_usd = try_cast(
                cast(contracts.valor_global as double) * cast(fx.fx_rate as double)
                as decimal(38, 2)
            ),
            fx_rate_to_usd = fx.fx_rate,
            fx_rate_date = fx.fx_rate_date,
            fx_source = fx.fx_source
        from _br_pncp_fx as fx
        where {RATE_DATE_EXPRESSION} = fx.rate_date
          and contracts.valor_global is not null
        """
    )

    counts = {
        "rate_dates": len(rate_dates),
        "rates_found": len(fx_rows),
        "rows_converted": _count(
            duckdb_connection, f"select count(*) from {qualified} where fx_rate_to_usd is not null"
        ),
        "rows_with_value": _count(
            duckdb_connection, f"select count(*) from {qualified} where valor_global is not null"
        ),
        "rows_missing_rate_date": _count(
            duckdb_connection,
            f"select count(*) from {qualified} "
            f"where valor_global is not null and {RATE_DATE_EXPRESSION} is null",
        ),
    }
    if log is not None:
        log(
            "PNCP USD conversion: %s distinct dates, %s rates found, "
            "%s/%s valued contracts converted (%s had no usable date)",
            counts["rate_dates"],
            counts["rates_found"],
            counts["rows_converted"],
            counts["rows_with_value"],
            counts["rows_missing_rate_date"],
        )
    return counts


def ensure_usd_columns(duckdb_connection: Any) -> None:
    """Add the FX columns to the candidates table if a prior build predates them."""
    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    for column, column_type in (
        ("valor_global_usd", "decimal(38, 2)"),
        ("fx_rate_to_usd", "decimal(38, 12)"),
        ("fx_rate_date", "date"),
        ("fx_source", "varchar"),
    ):
        duckdb_connection.execute(
            f"alter table {qualified} add column if not exists {column} {column_type}"
        )


def _require_candidates_table(duckdb_connection: Any) -> None:
    exists = bool(
        duckdb_connection.execute(
            """
            select count(*) from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [tables.DUCKDB_SCHEMA, tables.CANDIDATES_TABLE],
        ).fetchone()[0]
    )
    if not exists:
        raise RuntimeError(
            f"Brazil PNCP candidates table is missing: "
            f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}. Materialize "
            f"brazil_pncp_contracts_duckdb for this partition before converting."
        )


def _rate_dates(duckdb_connection: Any) -> list[str]:
    rows = duckdb_connection.execute(
        f"""
        select distinct cast({RATE_DATE_EXPRESSION} as varchar)
        from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}
        where valor_global is not null and {RATE_DATE_EXPRESSION} is not null
        """
    ).fetchall()
    return sorted(str(row[0]) for row in rows)


def _request(currency: str, rate_date: str) -> Any:
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def _load_rates(
    exchange_rates: ExchangeRates, requests: list[Any]
) -> dict[tuple[str, str], Any]:
    """Batched so one missing date degrades its batch, not the whole set.

    LookupError only -- a connection error is a real failure and must surface
    rather than be silently swallowed as "no rate available".
    """
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
