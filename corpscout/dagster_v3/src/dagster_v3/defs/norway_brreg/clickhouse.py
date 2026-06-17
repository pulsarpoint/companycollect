from __future__ import annotations

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.norway_brreg import tables


def prepare_norway_brreg_clickhouse_companies_table(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NORWAY_BRREG_DATABASE}")
        client.execute(tables.COMPANIES_DDL.strip())


def prepare_norway_brreg_clickhouse_financial_statements_table(
    clickhouse: ClickhouseResource,
) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NORWAY_BRREG_DATABASE}")
        client.execute(tables.FINANCIAL_STATEMENTS_DDL.strip())
