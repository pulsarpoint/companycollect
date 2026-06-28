from datetime import UTC, datetime
from typing import Any

import dagster as dg
import polars as pl

from dagster_v3.defs.finland_xbrl import metric_mapping, tables
from dagster_v3.defs.finland_xbrl.assets.parse import (
    finland_xbrl_parse_backfill,
    finland_xbrl_parse_incremental,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlParquetStorageResource


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


@dg.asset(
    name=tables.FINANCIAL_METRICS_TABLE,
    group_name="finland_xbrl",
    deps=[finland_xbrl_parse_backfill, finland_xbrl_parse_incremental],
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


def _metric_pivot(current_numeric_facts: pl.DataFrame) -> pl.DataFrame:
    if current_numeric_facts.is_empty():
        return pl.DataFrame(
            schema={
                "statement_key": pl.Utf8,
                "mapped_fact_count": pl.Int64,
                "unmapped_numeric_fact_count": pl.Int64,
                **{metric_code: pl.Float64 for metric_code in metric_mapping.METRIC_CODES},
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
