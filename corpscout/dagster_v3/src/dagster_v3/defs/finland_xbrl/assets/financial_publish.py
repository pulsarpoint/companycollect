from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.finland_xbrl import metric_mapping
from dagster_v3.defs.finland_xbrl.assets.data_daily_xml_duckdb import (
    data_daily_xml_duckdb,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    ParsedXmlDuckdbRows,
    data_snapshot_xml_duckdb,
    list_xml_parse_duckdb_paths,
    read_xml_parse_duckdb_rows,
)
from dagster_v3.defs.finland_xbrl.assets.financial_metrics import (
    build_financial_metric_rows,
    build_financial_metric_usd_rows,
)
from dagster_v3.defs.finland_xbrl.clickhouse import (
    CLICKHOUSE_DATABASE,
    FINANCIAL_METRICS_CLICKHOUSE_TABLE,
    FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
    export_finland_xbrl_financial_metrics_clickhouse,
    export_finland_xbrl_financial_statements_clickhouse,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlParquetStorageResource


@dg.asset(
    name="fi_financial_statements_ch",
    group_name="finland_xbrl",
    deps=[data_snapshot_xml_duckdb, data_daily_xml_duckdb],
    kinds={"python", "duckdb", "clickhouse"},
    description=(
        "Publishes parsed Finland XBRL statement document metadata from historical "
        "and daily parsed DuckDB partitions to ClickHouse."
    ),
)
def fi_financial_statements_ch(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    parsed_rows = _read_all_parsed_duckdb_rows(context)
    row_count = export_finland_xbrl_financial_statements_clickhouse(
        statement_documents=parsed_rows.statement_documents,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "parsed_duckdb_count": parsed_rows.duckdb_path_count,
            "clickhouse_database": CLICKHOUSE_DATABASE,
            "clickhouse_table": FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
        }
    )


@dg.asset(
    name="fi_financial_metrics_parquet",
    group_name="finland_xbrl",
    deps=[data_snapshot_xml_duckdb, data_daily_xml_duckdb],
    kinds={"python", "duckdb", "polars", "parquet"},
    description=(
        "Builds original-currency Finland XBRL financial metrics parquet from "
        "historical and daily parsed DuckDB partitions."
    ),
)
def fi_financial_metrics_parquet(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    parsed_rows = _read_all_parsed_duckdb_rows(context)
    rows = build_financial_metric_rows(
        statement_documents=parsed_rows.statement_documents,
        facts=parsed_rows.facts,
        built_at=datetime.now(UTC).isoformat(),
    )
    parquet_path = xbrl_parquet_storage.write_financial_metrics(rows)
    context.log.info(
        "Finland XBRL financial metrics parquet complete: rows=%d parsed_duckdb_count=%d path=%s",
        len(rows),
        parsed_rows.duckdb_path_count,
        parquet_path,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "parsed_duckdb_count": parsed_rows.duckdb_path_count,
            "statement_documents_count": parsed_rows.statement_documents_count,
            "facts_count": parsed_rows.facts_count,
            "parquet_path": str(parquet_path),
            "mapping_version": metric_mapping.MAPPING_VERSION,
        }
    )


@dg.asset(
    name="fi_financial_metrics_usd_parquet",
    group_name="finland_xbrl",
    deps=[fi_financial_metrics_parquet],
    kinds={"python", "fx", "parquet"},
    description="Converts Finland XBRL financial metrics parquet from EUR to USD.",
)
def fi_financial_metrics_usd_parquet(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    financial_metric_rows = xbrl_parquet_storage.read_financial_metrics()
    context.log.info(
        "Building Finland XBRL USD financial metrics parquet: source_rows=%d",
        len(financial_metric_rows),
    )
    rows: list[dict[str, Any]] = []
    if financial_metric_rows:
        rows = build_financial_metric_usd_rows(
            financial_metrics=financial_metric_rows,
            exchange_rates=ExchangeRateClient.from_env(),
            converted_at=datetime.now(UTC).isoformat(),
        )
    parquet_path = xbrl_parquet_storage.write_financial_metrics_usd(rows)
    rate_dates = sorted({row["fx_rate_date"] for row in rows if row.get("fx_rate_date")})
    context.log.info(
        "Finland XBRL USD financial metrics parquet complete: rows=%d rate_dates=%d path=%s",
        len(rows),
        len(rate_dates),
        parquet_path,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "rate_date_count": len(rate_dates),
            "currency_original": "EUR",
            "parquet_path": str(parquet_path),
        }
    )


@dg.asset(
    name="fi_financial_metrics_ch",
    group_name="finland_xbrl",
    deps=[fi_financial_metrics_usd_parquet],
    kinds={"python", "parquet", "clickhouse"},
    description="Publishes USD Finland XBRL financial metrics parquet to ClickHouse.",
)
def fi_financial_metrics_ch(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    clickhouse: ClickhouseResource,
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


def _read_all_parsed_duckdb_rows(
    context: dg.AssetExecutionContext,
) -> ParsedXmlDuckdbRows:
    duckdb_paths = list_xml_parse_duckdb_paths()
    if not duckdb_paths:
        raise FileNotFoundError(
            "No Finland XBRL parsed DuckDB files found under historical or daily parse directories"
        )
    context.log.info(
        "Reading Finland XBRL parsed DuckDB files: count=%d first=%s last=%s",
        len(duckdb_paths),
        duckdb_paths[0],
        duckdb_paths[-1],
    )
    parsed_rows = read_xml_parse_duckdb_rows(duckdb_paths=duckdb_paths)
    context.log.info(
        "Loaded Finland XBRL parsed rows: duckdb_count=%d statement_documents=%d facts=%d",
        parsed_rows.duckdb_path_count,
        parsed_rows.statement_documents_count,
        parsed_rows.facts_count,
    )
    return parsed_rows
