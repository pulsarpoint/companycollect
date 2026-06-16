from collections.abc import Iterator
from contextlib import contextmanager

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    REQUIRED_FINLAND_RESOLVED_TABLES,
    assert_clickhouse_tables_exist,
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
