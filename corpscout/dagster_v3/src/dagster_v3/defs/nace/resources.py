from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

import clickhouse_connect
import dagster as dg
from pydantic import PrivateAttr

from dagster_v3.defs.nace import tables


class ClickHouseClient(Protocol):
    def command(self, sql: str) -> Any:
        ...

    def insert(self, table: str, data: Sequence[Sequence[Any]], column_names: Sequence[str]) -> Any:
        ...


class ClickHouseResource(dg.ConfigurableResource):
    host: str
    port: str = "8123"
    username: str = "default"
    password: str
    database: str = "default"
    secure: str = "false"

    _client: ClickHouseClient | None = PrivateAttr(default=None)

    def __init__(self, clickhouse_client: ClickHouseClient | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._client = clickhouse_client

    def client(self) -> ClickHouseClient:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=int(self.port),
                username=self.username,
                password=self.password,
                database=self.database,
                secure=self.secure.lower() == "true",
            )
        return self._client


def prepare_nace_categories_table(clickhouse: ClickHouseResource) -> None:
    client = clickhouse.client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {tables.NACE_DATABASE}")
    client.command(tables.NACE_CATEGORIES_DDL.strip())
    client.command(f"TRUNCATE TABLE {tables.QUALIFIED_NACE_CATEGORIES_TABLE}")
