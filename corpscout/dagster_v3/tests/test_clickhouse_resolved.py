from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    REQUIRED_FINLAND_RESOLVED_TABLES,
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)


class FakeClickHouseClient:
    def __init__(self, existing_tables: set[str]) -> None:
        self.existing_tables = existing_tables
        self.statements: list[str] = []

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[tuple[str]]:
        self.statements.append(sql)
        if "system.tables" not in sql:
            raise AssertionError(sql)
        database = str(params["database"]) if params else ""
        requested = set(params["tables"]) if params else set()
        return [
            (table.removeprefix(f"{database}."),)
            for table in sorted(self.existing_tables)
            if table.startswith(f"{database}.") and table.removeprefix(f"{database}.") in requested
        ]


def test_required_finland_resolved_tables_are_explicit() -> None:
    assert REQUIRED_FINLAND_RESOLVED_TABLES == (
        "fi_companies",
        "fi_websites",
        "fi_industries",
        "fi_addresses",
        "fi_registered_entries",
        "fi_legal_forms",
        "fi_financial_statements",
        "fi_financial_metrics",
    )


def test_assert_clickhouse_tables_exist_uses_official_resource(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient(
        {"corpscout_resolved.fi_companies", "corpscout_resolved.fi_websites"}
    )

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    assert_clickhouse_tables_exist(
        resource,
        database="corpscout_resolved",
        tables=("fi_companies", "fi_websites"),
    )

    assert "system.tables" in client.statements[0]


def test_assert_clickhouse_tables_exist_reports_missing_tables(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient({"corpscout_resolved.fi_companies"})

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    try:
        assert_clickhouse_tables_exist(
            resource,
            database="corpscout_resolved",
            tables=("fi_companies", "fi_websites"),
        )
    except ValueError as exc:
        assert str(exc) == "Missing ClickHouse tables in corpscout_resolved: fi_websites"
    else:
        raise AssertionError("expected missing table error")


def test_export_duckdb_table_to_clickhouse_inserts_rows_in_column_order(tmp_path) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar,
                country_iso2 varchar,
                source_system varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8', 'FI', 'finland_prhytj')"
        )

    client = FakeInsertClickHouseClient()

    row_count = export_duckdb_table_to_clickhouse(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="finland_resolved",
        duckdb_table="fi_companies",
        clickhouse_database="corpscout_resolved",
        clickhouse_table="fi_companies",
        columns=("business_id", "country_iso2", "source_system"),
        truncate=True,
    )

    assert row_count == 1
    assert client.statements == ["TRUNCATE TABLE `corpscout_resolved`.`fi_companies`"]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout_resolved`.`fi_companies` (`business_id`, `country_iso2`, `source_system`) VALUES",
            [("1234567-8", "FI", "finland_prhytj")],
        )
    ]


class FakeInsertClickHouseClient(FakeClickHouseClient):
    def __init__(self) -> None:
        super().__init__(set())
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        if sql.startswith("INSERT INTO"):
            if not isinstance(params, list):
                raise AssertionError("insert params must be a list of row tuples")
            self.insert_calls.append((sql, params))
            return []
        self.statements.append(sql)
        return []
