from __future__ import annotations

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.exchange_rates import tables


def prepare_exchange_rates_table(clickhouse: ClickhouseResource) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.EXCHANGE_RATES_DATABASE}")
        client.execute(tables.EXCHANGE_RATES_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_EXCHANGE_RATES_TABLE}")
