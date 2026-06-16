from __future__ import annotations

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.norway_brreg import tables


def prepare_norway_brreg_clickhouse_tables(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NORWAY_BRREG_DATABASE}")
        client.execute(tables.COMPANIES_DDL.strip())
        client.execute(tables.FINANCIAL_STATEMENTS_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_COMPANIES_TABLE}")
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE}")
