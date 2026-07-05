from typing import Any

import dagster as dg

from dagster_v3.defs.brazil_financial.cvm import tables
from dagster_v3.defs.brazil_financial.cvm.parsing import BRAZIL_CVM_DUCKDB_SCHEMA
from dagster_v3.defs.brazil_financial.cvm.storage import (
    brazil_fin_cvm_read_only_partitioned_connection,
)


def statement_rows_usd_coverage_check(
    *,
    clickhouse: Any,
    clickhouse_table: str,
    year_column: str,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        row = client.execute(
            f"""
            select
                count(),
                countIf(
                    amount_original is not null
                    and currency != ''
                    and period_end_date is not null
                ),
                countIf(
                    amount_original is not null
                    and currency != ''
                    and period_end_date is not null
                    and amount_usd is null
                ),
                countIf(
                    amount_original is not null
                    and currency != ''
                    and period_end_date is not null
                    and fx_rate_to_usd is null
                ),
                min({year_column}),
                max({year_column})
            from {tables.BRAZIL_CVM_DATABASE}.{clickhouse_table}
            """
        )[0]

    row_count = int(row[0] or 0)
    eligible_row_count = int(row[1] or 0)
    missing_usd_rows = int(row[2] or 0)
    missing_fx_rows = int(row[3] or 0)
    min_year = int(row[4]) if row[4] is not None else 0
    max_year = int(row[5]) if row[5] is not None else 0
    return dg.AssetCheckResult(
        passed=(
            row_count > 0
            and eligible_row_count > 0
            and missing_usd_rows == 0
            and missing_fx_rows == 0
        ),
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "eligible_row_count": dg.MetadataValue.int(eligible_row_count),
            "missing_usd_rows": dg.MetadataValue.int(missing_usd_rows),
            "missing_fx_rows": dg.MetadataValue.int(missing_fx_rows),
            "min_year": dg.MetadataValue.int(min_year),
            "max_year": dg.MetadataValue.int(max_year),
        },
    )


def financial_metrics_dataset_check(
    *,
    clickhouse: Any,
    source_dataset: str,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        row = client.execute(
            f"""
            select
                count(),
                uniqExact(cnpj),
                min(source_year),
                max(source_year),
                countIf(amount_usd is null)
            from {tables.QUALIFIED_BR_CVM_FINANCIAL_METRICS_TABLE}
            where source_dataset = %(source_dataset)s
            """,
            {"source_dataset": source_dataset},
        )[0]

    metric_row_count = int(row[0] or 0)
    issuer_count = int(row[1] or 0)
    min_year = int(row[2]) if row[2] is not None else 0
    max_year = int(row[3]) if row[3] is not None else 0
    missing_usd_metric_rows = int(row[4] or 0)
    return dg.AssetCheckResult(
        passed=metric_row_count > 0 and missing_usd_metric_rows == 0,
        metadata={
            "source_dataset": source_dataset,
            "metric_row_count": dg.MetadataValue.int(metric_row_count),
            "issuer_count": dg.MetadataValue.int(issuer_count),
            "min_year": dg.MetadataValue.int(min_year),
            "max_year": dg.MetadataValue.int(max_year),
            "missing_usd_metric_rows": dg.MetadataValue.int(missing_usd_metric_rows),
        },
    )


def statement_rows_row_count_matches_duckdb_check(
    *,
    clickhouse: Any,
    family: str,
    clickhouse_table: str,
    duckdb_table: str,
    years: tuple[str, ...],
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        clickhouse_row_count = int(
            client.execute(
                f"""
                select count()
                from {tables.BRAZIL_CVM_DATABASE}.{clickhouse_table} FINAL
                """
            )[0][0]
            or 0
        )

    with brazil_fin_cvm_read_only_partitioned_connection(
        family=family.lower(),
        years=years,
        table_names=(duckdb_table,),
    ) as connection:
        duckdb_row_count = int(
            connection.execute(
                f"""
                select count(*)
                from (
                    select distinct
                        cnpj,
                        reference_date,
                        version,
                        statement_code,
                        consolidation_type,
                        coalesce(period_end_date, date '1970-01-01') as period_end_date,
                        account_code,
                        equity_column
                    from {BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}
                )
                """
            ).fetchone()[0]
            or 0
        )

    return row_count_match_result(
        clickhouse_row_count=clickhouse_row_count,
        duckdb_row_count=duckdb_row_count,
        family=family,
    )


def row_count_match_result(
    *,
    clickhouse_row_count: int,
    duckdb_row_count: int,
    family: str,
) -> dg.AssetCheckResult:
    difference = clickhouse_row_count - duckdb_row_count
    return dg.AssetCheckResult(
        passed=clickhouse_row_count > 0 and difference == 0,
        metadata={
            "family": family,
            "clickhouse_row_count": dg.MetadataValue.int(clickhouse_row_count),
            "duckdb_row_count": dg.MetadataValue.int(duckdb_row_count),
            "row_count_difference": dg.MetadataValue.int(difference),
        },
    )
