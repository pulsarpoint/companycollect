"""ClickHouse resource for source table imports."""

from collections.abc import Sequence
from typing import Any

import clickhouse_connect
from dagster import ConfigurableResource


class ClickHouseResource(ConfigurableResource):
    host: str
    port: str = "8123"
    username: str = "default"
    password: str
    database: str = "corpscout_sources"
    secure: str = "false"

    def client(self):
        secure = self.secure.lower() == "true"
        return clickhouse_connect.get_client(
            host=self.host,
            port=int(self.port),
            username=self.username,
            password=self.password,
            database=self.database,
            secure=secure,
        )

    def truncate_tables(self, client: Any, tables: Sequence[str]) -> None:
        for table in tables:
            client.command(f"TRUNCATE TABLE IF EXISTS {table}")

    def insert_rows(
        self,
        client: Any,
        table: str,
        columns: Sequence[str],
        rows: Sequence[dict[str, Any]],
    ) -> None:
        if not rows:
            return
        data = [[row.get(column) for column in columns] for row in rows]
        client.insert(table, data, column_names=list(columns))
