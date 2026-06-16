from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol

from dagster_v3.defs.nace import tables


class ClickhouseClient(Protocol):
    def execute(self, sql: str) -> object:
        ...


class ClickhouseResourceLike(Protocol):
    def get_connection(self) -> AbstractContextManager[ClickhouseClient]:
        ...


def prepare_nace_categories_table(clickhouse: ClickhouseResourceLike) -> None:
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {tables.NACE_DATABASE}")
        client.execute(tables.NACE_CATEGORIES_DDL.strip())
        client.execute(f"TRUNCATE TABLE {tables.QUALIFIED_NACE_CATEGORIES_TABLE}")
