from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import dagster as dg
import clickhouse_connect
from pydantic import Field

DEFAULT_CLICKHOUSE_HTTP_PORT = 8123


class ClickHouseConnectClientAdapter:
    def __init__(self, client: Any) -> None:
        self._client = client

    def execute(self, sql: str, params: object | None = None) -> list[tuple[Any, ...]]:
        if sql.lstrip().lower().startswith("select"):
            return list(self._client.query(sql, parameters=params).result_rows)
        self._client.command(sql, parameters=params)
        return []

    def insert_arrow(
        self,
        table: str,
        arrow_table: Any,
        database: str | None = None,
    ) -> object:
        return self._client.insert_arrow(table, arrow_table, database=database)

    def insert_rows(
        self,
        table: str,
        rows: list[tuple[object, ...]],
        columns: tuple[str, ...] | list[str],
        database: str | None = None,
    ) -> object:
        return self._client.insert(
            table,
            rows,
            column_names=list(columns),
            database=database,
        )


class ClickHouseConnectResource(dg.ConfigurableResource):
    host: str = Field(description="ClickHouse server host.")
    port: int = Field(
        default=DEFAULT_CLICKHOUSE_HTTP_PORT,
        description="HTTP protocol port used by clickhouse-connect.",
    )
    user: str = Field(default="default", description="User name.")
    password: str = Field(default="", description="Password.")
    database: str = Field(default="default", description="Default ClickHouse database.")
    secure: bool = Field(default=False, description="Use TLS for the HTTP connection.")
    settings: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional ClickHouse server settings passed to clickhouse-connect.",
    )

    @contextmanager
    def get_connection(self) -> Iterator[ClickHouseConnectClientAdapter]:
        client = clickhouse_connect.get_client(
            host=_resolved_env_value(self.host),
            port=self.port,
            username=_resolved_env_value(self.user),
            password=_resolved_env_value(self.password),
            database=_resolved_env_value(self.database),
            secure=self.secure,
            settings=self.settings,
        )
        try:
            yield ClickHouseConnectClientAdapter(client)
        finally:
            client.close()


def clickhouse_resource_from_env() -> ClickHouseConnectResource:
    return ClickHouseConnectResource(
        host=dg.EnvVar("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_HTTP_PORT", DEFAULT_CLICKHOUSE_HTTP_PORT),
        user=dg.EnvVar("CLICKHOUSE_USER"),
        password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
        database=dg.EnvVar("CLICKHOUSE_DATABASE"),
        secure=_bool_env("CLICKHOUSE_SECURE", False),
    )


def _resolved_env_value(value: str | dg.EnvVar) -> str:
    if isinstance(value, dg.EnvVar):
        resolved = value.get_value()
        if resolved is None:
            raise RuntimeError(f"Missing required environment variable {value.env_var_name}")
        return resolved
    return value


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
