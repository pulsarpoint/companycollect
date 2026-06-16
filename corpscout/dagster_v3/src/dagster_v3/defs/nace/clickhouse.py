from __future__ import annotations

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.nace import tables


def prepare_nace_categories_table(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NACE_DATABASE}")
        client.execute(tables.NACE_CATEGORIES_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_NACE_CATEGORIES_TABLE}")
