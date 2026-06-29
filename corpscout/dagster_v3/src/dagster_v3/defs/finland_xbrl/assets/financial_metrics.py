from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import dagster as dg
import polars as pl

from dagster_v3.defs.finland_xbrl import metric_mapping, tables
from dagster_v3.defs.finland_xbrl.clickhouse import (
    CLICKHOUSE_DATABASE,
    EUR_CURRENCY,
    FINANCIAL_METRICS_CLICKHOUSE_TABLE,
    MONEY_METRIC_TO_CLICKHOUSE_COLUMN,
    SOURCE_SYSTEM,
    export_finland_xbrl_financial_metrics_clickhouse,
)
from dagster_v3.defs.clickhouse.resources import ClickHouseConnectResource
from dagster_v3.defs.finland_xbrl.assets.parse import (
    finland_xbrl_parse_backfill,
    finland_xbrl_parse_incremental,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlParquetStorageResource

DECIMAL_SCALE = Decimal("0.000001")


def build_financial_metric_rows(
    *,
    statement_documents: list[dict[str, Any]],
    facts: list[dict[str, Any]],
    built_at: str,
) -> list[dict[str, Any]]:
    statements = _frame(
        statement_documents,
        columns=tables.STATEMENT_DOCUMENTS_COLUMNS,
        schema=tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA,
    )
    fact_frame = _frame(
        facts,
        columns=tables.FACTS_COLUMNS,
        schema=tables.FACTS_POLARS_SCHEMA,
    )
    if statements.is_empty():
        return []

    fact_counts = fact_frame.group_by("statement_key").agg(
        pl.len().alias("source_fact_count")
    )
    mapping = pl.DataFrame(metric_mapping.xbrl_metric_mapping_rows())
    current_numeric_facts = (
        fact_frame.filter(
            (pl.col("value_kind") == "numeric")
            & (pl.col("is_comparative").fill_null(False).not_())
        )
        .with_columns(
            pl.col("numeric_value")
            .str.strip_chars()
            .replace("", None)
            .cast(pl.Float64, strict=False)
            .alias("numeric_value")
        )
        .join(mapping, on=["concept_qname", "mcy_member_code"], how="left")
    )
    metric_pivot = _metric_pivot(current_numeric_facts)
    metrics = (
        statements.join(fact_counts, on="statement_key", how="left")
        .join(metric_pivot, on="statement_key", how="left")
        .with_columns(
            pl.col("reported_period_start").replace("", None).alias("period_start"),
            pl.coalesce(
                pl.col("reported_period_end").replace("", None),
                pl.col("financial_date").replace("", None),
            ).alias("period_end"),
            pl.col("source_fact_count").fill_null(0).cast(pl.Int64),
            pl.col("mapped_fact_count").fill_null(0).cast(pl.Int64),
            pl.col("unmapped_numeric_fact_count").fill_null(0).cast(pl.Int64),
            pl.col("employees").round(0).cast(pl.Int64, strict=False),
            pl.lit(metric_mapping.MAPPING_VERSION).alias("mapping_version"),
            pl.lit(built_at).alias("built_at"),
        )
        .with_columns(
            pl.when(pl.col("unmapped_numeric_fact_count") > 0)
            .then(
                pl.concat_str(
                    [
                        pl.lit('["unmapped numeric facts: '),
                        pl.col("unmapped_numeric_fact_count").cast(pl.Utf8),
                        pl.lit('"]'),
                    ]
                )
            )
            .when(pl.col("mapped_fact_count") == 0)
            .then(pl.lit('["no mapped metrics"]'))
            .otherwise(pl.lit("[]"))
            .alias("metric_warnings")
        )
        .select(tables.FINANCIAL_METRICS_COLUMNS)
        .sort(["business_id", "financial_date", "statement_key"])
    )
    return metrics.to_dicts()


def build_financial_metric_usd_rows(
    *,
    financial_metrics: list[dict[str, Any]],
    exchange_rates: Any,
    converted_at: str,
) -> list[dict[str, Any]]:
    if not financial_metrics:
        return []

    requests = _usd_rate_requests(financial_metrics)
    rates = _load_required_usd_rates(exchange_rates, requests)
    rows = []
    for row in financial_metrics:
        rate_date = _rate_date(row)
        rate = rates[(EUR_CURRENCY, rate_date)]
        usd_row = {
            "statement_key": str(row.get("statement_key") or ""),
            "business_id": str(row.get("business_id") or ""),
            "financial_date": _optional_string(row.get("financial_date")),
            "registration_date": _optional_string(row.get("registration_date")),
            "period_start": _optional_string(row.get("period_start")),
            "period_end": _optional_string(row.get("period_end")),
            "reported_company_name": _optional_string(row.get("reported_company_name")),
            "source_url": _optional_string(row.get("source_url")),
            "xml_object_key": _optional_string(row.get("xml_object_key")),
            "xml_sha256": _sha256_value(row.get("xml_sha256")),
            "xml_size_bytes": _int_value(row.get("xml_size_bytes")),
            "currency_original": EUR_CURRENCY,
            "employees": _int_value(row.get("employees")),
            "source_fact_count": _int_value(row.get("source_fact_count")) or 0,
            "mapped_fact_count": _int_value(row.get("mapped_fact_count")) or 0,
            "unmapped_numeric_fact_count": _int_value(
                row.get("unmapped_numeric_fact_count")
            )
            or 0,
            "metric_warnings": _optional_string(row.get("metric_warnings")) or "[]",
            "mapping_version": _optional_string(row.get("mapping_version")),
            "fx_rate_to_usd": float(rate.rate),
            "fx_rate_date": str(rate.rate_date),
            "fx_converted_at": converted_at,
            "source_system": SOURCE_SYSTEM,
            "source_run_id": _optional_string(row.get("source_run_id")),
            "source_record_id": str(row.get("statement_key") or ""),
            "source_payload_hash": _sha256_value(row.get("xml_sha256")),
            "resolved_at": _optional_string(row.get("built_at")) or converted_at,
        }
        for metric_name, original_column in MONEY_METRIC_TO_CLICKHOUSE_COLUMN.items():
            original_value = _money_value(row.get(metric_name))
            usd_row[original_column] = original_value
            usd_row[original_column.replace("_original", "_usd")] = (
                _converted_money_value(original_value, rate.rate)
            )
        rows.append(
            {
                column: usd_row.get(column)
                for column in tables.FINANCIAL_METRICS_USD_COLUMNS
            }
        )
    return rows


@dg.asset(
    name=tables.FINANCIAL_METRICS_TABLE,
    group_name="finland_xbrl",
    deps=[
        finland_xbrl_parse_backfill,
        finland_xbrl_parse_incremental,
    ],
    kinds={"python", "polars", "parquet"},
)
def finland_xbrl_financial_metrics(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    context.log.info("Building Finland XBRL financial metrics parquet from parsed rows")
    rows = build_financial_metric_rows(
        statement_documents=xbrl_parquet_storage.read_statement_documents(),
        facts=xbrl_parquet_storage.read_facts(),
        built_at=datetime.now(UTC).isoformat(),
    )
    parquet_path = xbrl_parquet_storage.write_financial_metrics(rows)
    context.log.info(
        "Finland XBRL financial metrics complete: row_count=%d parquet_path=%s",
        len(rows),
        parquet_path,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "parquet_path": str(parquet_path),
            "mapping_version": metric_mapping.MAPPING_VERSION,
        }
    )


@dg.asset(
    name=tables.FINANCIAL_METRICS_USD_TABLE,
    group_name="finland_xbrl",
    deps=[dg.AssetKey(tables.FINANCIAL_METRICS_TABLE)],
    kinds={"python", "fx", "parquet"},
)
def finland_xbrl_financial_metrics_usd(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    financial_metric_rows = xbrl_parquet_storage.read_financial_metrics()
    context.log.info(
        "Building Finland XBRL USD financial metrics parquet: source_rows=%d",
        len(financial_metric_rows),
    )
    rows = []
    if financial_metric_rows:
        rows = build_financial_metric_usd_rows(
            financial_metrics=financial_metric_rows,
            exchange_rates=ExchangeRateClient.from_env(),
            converted_at=datetime.now(UTC).isoformat(),
        )
    parquet_path = xbrl_parquet_storage.write_financial_metrics_usd(rows)
    rate_dates = sorted({row["fx_rate_date"] for row in rows if row.get("fx_rate_date")})
    context.log.info(
        "Finland XBRL USD financial metrics complete: row_count=%d rate_dates=%d parquet_path=%s",
        len(rows),
        len(rate_dates),
        parquet_path,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "rate_date_count": len(rate_dates),
            "currency_original": EUR_CURRENCY,
            "parquet_path": str(parquet_path),
        }
    )


@dg.asset(
    name="finland_xbrl_financial_metrics_clickhouse",
    group_name="finland_xbrl",
    deps=[dg.AssetKey(tables.FINANCIAL_METRICS_USD_TABLE)],
    kinds={"python", "parquet", "clickhouse"},
)
def finland_xbrl_financial_metrics_clickhouse(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    clickhouse: ClickHouseConnectResource,
) -> dg.MaterializeResult:
    row_count = export_finland_xbrl_financial_metrics_clickhouse(
        xbrl_parquet_storage=xbrl_parquet_storage,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "clickhouse_database": CLICKHOUSE_DATABASE,
            "clickhouse_table": FINANCIAL_METRICS_CLICKHOUSE_TABLE,
        }
    )


def _metric_pivot(current_numeric_facts: pl.DataFrame) -> pl.DataFrame:
    if current_numeric_facts.is_empty():
        return pl.DataFrame(
            schema={
                "statement_key": pl.Utf8,
                "mapped_fact_count": pl.Int64,
                "unmapped_numeric_fact_count": pl.Int64,
                **{
                    metric_code: _metric_column_dtype(metric_code)
                    for metric_code in metric_mapping.METRIC_CODES
                },
            }
        )
    return current_numeric_facts.group_by("statement_key").agg(
        pl.col("metric_code").is_not_null().sum().alias("mapped_fact_count"),
        pl.col("metric_code").is_null().sum().alias("unmapped_numeric_fact_count"),
        *[
            pl.when(pl.col("metric_code") == metric_code)
            .then(pl.col("numeric_value"))
            .otherwise(None)
            .max()
            .alias(metric_code)
            for metric_code in metric_mapping.METRIC_CODES
        ],
    )


def _metric_column_dtype(metric_code: str) -> pl.DataType:
    if metric_code == "employees":
        return pl.Int64
    return pl.Float64


def _frame(
    rows: list[dict[str, Any]],
    *,
    columns: list[str],
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(
        [{column: row.get(column) for column in columns} for row in rows],
        schema=schema,
    )


def _usd_rate_requests(financial_metrics: list[dict[str, Any]]) -> list[Any]:
    from exchange_rates import ExchangeRateRequest

    rate_dates = sorted({_rate_date(row) for row in financial_metrics})
    return [
        ExchangeRateRequest(currency=EUR_CURRENCY, rate_date=rate_date)
        for rate_date in rate_dates
    ]


def _load_required_usd_rates(
    exchange_rates: Any,
    requests: list[Any],
) -> dict[tuple[str, str], Any]:
    if not requests:
        return {}
    rates: dict[tuple[str, str], Any] = {}
    missing: list[str] = []
    try:
        rates.update(exchange_rates.usd_rates(requests))
    except LookupError:
        for request in requests:
            try:
                rates.update(exchange_rates.usd_rates([request]))
            except LookupError:
                missing.append(f"{request.currency}:{request.rate_date}")

    for request in requests:
        if (request.currency, request.rate_date) not in rates:
            missing.append(f"{request.currency}:{request.rate_date}")
    if missing:
        unique_missing = ", ".join(sorted(set(missing)))
        raise LookupError(f"Missing EUR/USD exchange rates for {unique_missing}")
    return rates


def _rate_date(row: dict[str, Any]) -> str:
    rate_date = _optional_string(row.get("period_end")) or _optional_string(
        row.get("financial_date")
    )
    if not rate_date:
        raise ValueError(
            "Finland XBRL financial metric row is missing period_end and financial_date"
        )
    return rate_date


def _optional_string(value: Any) -> str:
    return str(value or "")


def _sha256_value(value: Any) -> str:
    text = str(value or "")
    if len(text) == 64:
        return text
    return "0" * 64


def _int_value(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _money_value(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _converted_money_value(value: float | None, rate: Decimal) -> float | None:
    if value is None:
        return None
    amount = Decimal(str(value)) * rate
    return float(amount.quantize(DECIMAL_SCALE))
