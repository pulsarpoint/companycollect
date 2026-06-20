from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import duckdb

from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
WIDE_STATEMENTS = tables.FINANCIAL_STATEMENTS_WIDE_TABLE
METRICS_TABLE = tables.FINANCIAL_METRICS_WIDE_TABLE
SOURCE_SLUG = "latvia_ur_financials"

_DECIMAL_AMOUNT_COLUMNS = {
    f"{metric}_amount_original" for metric in tables.FINANCIAL_METRIC_NAMES
} | {f"{metric}_amount_usd" for metric in tables.FINANCIAL_METRIC_NAMES}


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    # Imported lazily so tests can inject a stub without the real client/env.
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


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
    metric_cols = ", ".join(
        f"{src} as {name}"
        for name, src in tables.FINANCIAL_METRIC_SOURCE_COLUMNS.items()
    )
    select_sql = f"""
        select
            statement_id, regcode, fiscal_year, period_start_date, period_end_date,
            employees, currency, rounded_to_nearest, source_payload_hash,
            {metric_cols}
        from {DLT_DATASET_NAME}.{WIDE_STATEMENTS}
    """
    with duckdb.connect(str(database_path)) as connection:
        cursor = connection.execute(select_sql)
        names = [d[0] for d in cursor.description]
        records = [dict(zip(names, r, strict=True)) for r in cursor.fetchall()]

    unknown_units: set[str] = set()
    rows = [
        _metric_row(rec, source_run_id=source_run_id, unknown_units=unknown_units)
        for rec in records
    ]
    _write_metrics_table(database_path, rows)

    if unknown_units and log is not None:
        log(
            "Latvia UR metrics: unknown rounded_to_nearest units defaulted to factor 1: %s",
            sorted(unknown_units),
        )
    counts = {"metrics": len(rows)}
    if log is not None:
        log("Built Latvia UR financial metrics (native): metrics=%s", counts["metrics"])
    return counts


def _load_rates(
    exchange_rates: ExchangeRates, requests: list[Any]
) -> dict[tuple[str, str], Any]:
    # Mirrors norway_brreg.financial_normalize._load_available_usd_rates: try the
    # batch, and on a missing-rate LookupError fall back to per-request so one
    # absent rate doesn't drop the whole batch.
    if not requests:
        return {}
    try:
        return exchange_rates.usd_rates(requests)
    except LookupError:
        rates: dict[tuple[str, str], Any] = {}
        for request in requests:
            try:
                rates.update(exchange_rates.usd_rates([request]))
            except LookupError:
                continue
        return rates


def _metric_row(
    rec: dict[str, Any],
    *,
    source_run_id: str,
    unknown_units: set[str],
) -> dict[str, Any]:
    unit = str(rec["rounded_to_nearest"] or "").upper()
    if unit and unit not in tables.ROUNDED_TO_NEAREST_FACTORS:
        unknown_units.add(unit)
    factor = tables.ROUNDED_TO_NEAREST_FACTORS.get(unit, 1)

    row: dict[str, Any] = {
        "country_iso2": "LV",
        "source_slug": SOURCE_SLUG,
        "source_run_id": source_run_id,
        "source_record_id": rec["statement_id"],
        "source_payload_hash": rec["source_payload_hash"],
        "statement_id": rec["statement_id"],
        "regcode": rec["regcode"],
        "fiscal_year": rec["fiscal_year"],
        "period_start_date": rec["period_start_date"],
        "period_end_date": rec["period_end_date"],
        "employees": rec["employees"],
        "currency": str(rec["currency"] or "").upper(),
        "rounded_to_nearest": rec["rounded_to_nearest"],
        # FX columns are filled by the separate apply_latvia_ur_usd_conversion step.
        "fx_rate_to_usd": None,
        "fx_rate_date": None,
        "fx_source": "",
        "resolved_at": datetime.now(timezone.utc),
    }
    for metric in tables.FINANCIAL_METRIC_NAMES:
        raw = _decimal(rec[metric])
        row[f"{metric}_amount_original"] = None if raw is None else (raw * factor)
        row[f"{metric}_amount_usd"] = None
    return row


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


def _write_metrics_table(database_path: str | Path, rows: list[dict[str, Any]]) -> None:
    columns = tables.LV_FINANCIAL_METRICS_COLUMNS

    def ddl_type(col: str) -> str:
        if col in _DECIMAL_AMOUNT_COLUMNS:
            return "decimal(38, 2)"
        if col == "fx_rate_to_usd":
            return "decimal(38, 12)"
        if col in {"period_start_date", "period_end_date", "fx_rate_date"}:
            return "date"
        if col == "fiscal_year":
            return "integer"
        if col == "employees":
            return "bigint"
        if col == "resolved_at":
            return "timestamp"
        return "varchar"

    col_defs = ", ".join(f"{c} {ddl_type(c)}" for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        connection.execute(f"drop table if exists {qualified}")
        connection.execute(f"create table {qualified} ({col_defs})")
        if rows:
            connection.executemany(
                f"insert into {qualified} ({', '.join(columns)}) values ({placeholders})",
                [tuple(row.get(col) for col in columns) for row in rows],
            )
