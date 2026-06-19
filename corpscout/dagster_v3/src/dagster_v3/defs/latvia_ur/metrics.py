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
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Distill headline metrics from the wide statements, scale, and add USD.

    Values are scaled by rounded_to_nearest (before FX) and converted EUR->USD
    per report period_end_date via the shared exchange-rate client.
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

    requests: dict[tuple[str, str], Any] = {}
    for rec in records:
        currency = str(rec["currency"] or "").upper()
        end = "" if rec["period_end_date"] is None else str(rec["period_end_date"])
        if currency and end:
            requests[(currency, end)] = _request(currency, end)
    rates = _load_rates(exchange_rates, list(requests.values()))

    unknown_units: set[str] = set()
    rows = [
        _metric_row(rec, rates=rates, source_run_id=source_run_id, unknown_units=unknown_units)
        for rec in records
    ]
    _write_metrics_table(database_path, rows)

    if unknown_units and log is not None:
        log(
            "Latvia UR metrics: unknown rounded_to_nearest units defaulted to factor 1: %s",
            sorted(unknown_units),
        )
    counts = {"metrics": len(rows), "rate_pairs": len(requests)}
    if log is not None:
        log(
            "Built Latvia UR financial metrics: metrics=%s, rate_pairs=%s",
            counts["metrics"],
            counts["rate_pairs"],
        )
    return counts


def _load_rates(
    exchange_rates: ExchangeRates, requests: list[Any]
) -> dict[tuple[str, str], Any]:
    if not requests:
        return {}
    try:
        return exchange_rates.usd_rates(requests)
    except Exception:
        rates: dict[tuple[str, str], Any] = {}
        for request in requests:
            try:
                rates.update(exchange_rates.usd_rates([request]))
            except Exception:
                continue
        return rates


def _metric_row(
    rec: dict[str, Any],
    *,
    rates: dict[tuple[str, str], Any],
    source_run_id: str,
    unknown_units: set[str],
) -> dict[str, Any]:
    currency = str(rec["currency"] or "").upper()
    end = "" if rec["period_end_date"] is None else str(rec["period_end_date"])
    unit = str(rec["rounded_to_nearest"] or "").upper()
    if unit and unit not in tables.ROUNDED_TO_NEAREST_FACTORS:
        unknown_units.add(unit)
    factor = tables.ROUNDED_TO_NEAREST_FACTORS.get(unit, 1)
    fx_rate = rates.get((currency, end))

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
        "currency": currency,
        "rounded_to_nearest": rec["rounded_to_nearest"],
        "fx_rate_to_usd": None if fx_rate is None else fx_rate.rate,
        "fx_rate_date": None if fx_rate is None else fx_rate.rate_date,
        "fx_source": "" if fx_rate is None else fx_rate.source,
        "resolved_at": datetime.now(timezone.utc),
    }
    for metric in tables.FINANCIAL_METRIC_NAMES:
        raw = _decimal(rec[metric])
        scaled = None if raw is None else (raw * factor)
        row[f"{metric}_amount_original"] = scaled
        row[f"{metric}_amount_usd"] = (
            None if scaled is None or fx_rate is None else fx_rate.convert(scaled)
        )
    return row


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
