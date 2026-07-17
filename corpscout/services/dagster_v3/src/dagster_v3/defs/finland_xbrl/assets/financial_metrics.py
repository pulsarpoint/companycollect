from decimal import Decimal
from typing import Any

import polars as pl

from dagster_v3.defs.finland_xbrl import metric_mapping, tables
from dagster_v3.defs.finland_xbrl.clickhouse import (
    EUR_CURRENCY,
    MONEY_METRIC_TO_CLICKHOUSE_COLUMN,
    SOURCE_SYSTEM,
)

DECIMAL_SCALE = Decimal("0.000001")


def build_finland_financial_metrics_insert_sql(target_table: str) -> str:
    """Build metrics directly from the complete ClickHouse XBRL model."""
    metric_expression = _metric_code_sql()
    money_columns = {
        "revenue": "revenue_amount_original",
        "operating_profit_loss": "operating_profit_loss_amount_original",
        "profit_loss": "profit_loss_amount_original",
        "total_assets": "total_assets_amount_original",
        "equity": "equity_amount_original",
        "liabilities": "liabilities_amount_original",
        "cash_and_bank": "cash_and_bank_amount_original",
        "current_assets": "current_assets_amount_original",
        "current_receivables": "current_receivables_amount_original",
        "current_liabilities": "current_liabilities_amount_original",
        "personnel_expenses": "personnel_expenses_amount_original",
        "wages_and_salaries": "wages_and_salaries_amount_original",
    }
    aggregations = ",\n".join(
        "        argMaxIf(facts.numeric_value, "
        "tuple(facts.precision_rank, facts.fact_ordinal), "
        f"facts.metric_code = '{metric_code}') AS {column}"
        for metric_code, column in money_columns.items()
    )
    original_columns = ",\n".join(
        f"    CAST(metrics.{column}, 'Nullable(Decimal(38, 6))') AS {column}"
        for column in money_columns.values()
    )
    usd_columns = ",\n".join(
        "    CAST(metrics."
        f"{column} * fx.rate, 'Nullable(Decimal(38, 6))') AS "
        f"{column.replace('_original', '_usd')}"
        for column in money_columns.values()
    )
    selected_fact_metrics = ",\n".join(
        f"        mapped_facts.{column} AS {column}"
        for column in (*money_columns.values(), "employees")
    )
    insert_columns = (
        "statement_key",
        "business_id",
        "financial_date",
        "registration_date",
        "period_start",
        "period_end",
        "fiscal_year",
        "reported_company_name",
        "source_url",
        "xml_object_key",
        "xml_source_uri",
        "xml_sha256",
        "xml_size_bytes",
        "taxonomy_entrypoint",
        "currency_original",
        *money_columns.values(),
        *(column.replace("_original", "_usd") for column in money_columns.values()),
        "employees",
        "source_fact_count",
        "mapped_fact_count",
        "unmapped_numeric_fact_count",
        "metric_warnings",
        "mapping_version",
        "fx_rate_to_usd",
        "fx_rate_date",
        "fx_source",
        "fx_converted_at",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    )
    insert_column_sql = ",\n    ".join(insert_columns)
    return f"""
INSERT INTO {target_table} (
    {insert_column_sql}
)
WITH reports AS (
    SELECT *
    FROM corpscout.fi_financial_statements FINAL
),
current_numeric_facts AS (
    SELECT
        facts.statement_key AS statement_key,
        facts.fact_ordinal,
        facts.numeric_value,
        {metric_expression} AS metric_code,
        multiIf(
            upper(ifNull(facts.precision, '')) = 'INF', 3000000,
            match(ifNull(facts.precision, ''), '^-?[0-9]+$'),
                2000000 + toInt32OrZero(facts.precision),
            upper(ifNull(facts.decimals, '')) = 'INF', 1500000,
            match(ifNull(facts.decimals, ''), '^-?[0-9]+$'),
                1000000 + toInt32OrZero(facts.decimals),
            0
        ) AS precision_rank
    FROM corpscout.fi_xbrl_facts_raw AS facts
    WHERE facts.value_kind = 'numeric'
      AND facts.numeric_value IS NOT NULL
      AND facts.is_nil = 0
      AND facts.is_comparative = 0
),
fact_counts AS (
    SELECT statement_key, count() AS source_fact_count
    FROM corpscout.fi_xbrl_facts_raw
    GROUP BY statement_key
),
fact_metrics AS (
    SELECT
        facts.statement_key AS statement_key,
        countIf(facts.metric_code != '') AS mapped_fact_count,
        countIf(facts.metric_code = '') AS unmapped_numeric_fact_count,
{aggregations},
        argMaxIf(
            toUInt64OrNull(toString(toInt256(facts.numeric_value))),
            tuple(facts.precision_rank, facts.fact_ordinal),
            facts.metric_code = 'employees'
                AND facts.numeric_value = trunc(facts.numeric_value)
        ) AS employees
    FROM current_numeric_facts AS facts
    GROUP BY facts.statement_key
),
metrics AS (
    SELECT
        reports.statement_key AS statement_key,
        reports.business_id AS business_id,
        reports.financial_date AS financial_date,
        reports.registration_date AS registration_date,
        reports.period_start AS period_start,
        reports.period_end AS period_end,
        reports.reported_company_name AS reported_company_name,
        reports.source_url AS source_url,
        reports.xml_object_key AS xml_object_key,
        reports.xml_sha256 AS xml_sha256,
        reports.xml_size_bytes AS xml_size_bytes,
        reports.taxonomy_entrypoint AS taxonomy_entrypoint,
        reports.source_run_id AS source_run_id,
        'EUR' AS currency_code,
        ifNull(fact_counts.source_fact_count, 0) AS source_fact_count,
        ifNull(mapped_facts.mapped_fact_count, 0) AS mapped_fact_count,
        ifNull(mapped_facts.unmapped_numeric_fact_count, 0) AS unmapped_numeric_fact_count,
{selected_fact_metrics}
    FROM reports
    LEFT JOIN fact_counts ON fact_counts.statement_key = reports.statement_key
    LEFT JOIN fact_metrics AS mapped_facts
        ON mapped_facts.statement_key = reports.statement_key
),
usd_rates AS (
    SELECT
        'EUR' AS base_currency,
        rate_date,
        argMax(rate, pulled_at) AS rate,
        argMax(source, pulled_at) AS source
    FROM corpscout.exchange_rates
    WHERE base_currency = 'EUR' AND quote_currency = 'USD'
    GROUP BY rate_date
    ORDER BY base_currency, rate_date
)
SELECT
    metrics.statement_key,
    metrics.business_id,
    metrics.financial_date,
    metrics.registration_date,
    metrics.period_start,
    metrics.period_end,
    toUInt16(toYear(metrics.period_end)) AS fiscal_year,
    metrics.reported_company_name,
    metrics.source_url,
    metrics.xml_object_key,
    concat('s3://source-finland-prh-xbrl/', metrics.xml_object_key) AS xml_source_uri,
    metrics.xml_sha256,
    metrics.xml_size_bytes,
    metrics.taxonomy_entrypoint,
    'EUR' AS currency_original,
{original_columns},
{usd_columns},
    metrics.employees,
    metrics.source_fact_count,
    metrics.mapped_fact_count,
    metrics.unmapped_numeric_fact_count,
    multiIf(
        metrics.mapped_fact_count = 0, '["no mapped metrics"]',
        metrics.unmapped_numeric_fact_count > 0,
            concat('["unmapped current-period numeric facts: ',
                   toString(metrics.unmapped_numeric_fact_count), '"]'),
        '[]'
    ) AS metric_warnings,
    '{metric_mapping.MAPPING_VERSION}' AS mapping_version,
    fx.rate AS fx_rate_to_usd,
    fx.rate_date AS fx_rate_date,
    fx.source AS fx_source,
    now64(3, 'UTC') AS fx_converted_at,
    '{SOURCE_SYSTEM}' AS source_system,
    metrics.source_run_id,
    metrics.statement_key AS source_record_id,
    metrics.xml_sha256 AS source_payload_hash,
    now64(3, 'UTC') AS resolved_at
FROM metrics
ASOF LEFT JOIN usd_rates AS fx
    ON fx.base_currency = metrics.currency_code
    AND fx.rate_date <= coalesce(metrics.period_end, metrics.financial_date)
""".strip()


def _metric_code_sql() -> str:
    conditions = []
    for concept_qname, member_code, metric_code in metric_mapping.XBRL_METRIC_MAPPINGS:
        conditions.extend(
            [
                "(facts.concept_qname = "
                f"'{concept_qname}' AND ifNull(facts.mcy_member_code, '') = "
                f"'{member_code}')",
                f"'{metric_code}'",
            ]
        )
    return f"multiIf({', '.join(conditions)}, '')"


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
