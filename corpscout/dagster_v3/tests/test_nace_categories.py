from collections.abc import Iterator
from contextlib import contextmanager
from typing import get_type_hints

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.nace import tables
from dagster_v3.defs.nace.clickhouse import prepare_nace_categories_table


def test_dlt_clickhouse_destination_dependencies_are_available() -> None:
    import clickhouse_connect
    import dlt

    assert clickhouse_connect
    assert hasattr(dlt.destinations, "clickhouse")


def test_dagster_clickhouse_resource_dependency_is_available() -> None:
    assert ClickhouseResource


def test_nace_categories_clickhouse_schema_contract() -> None:
    assert tables.NACE_DATABASE == "reference"
    assert tables.NACE_CATEGORIES_TABLE == "nace_categories"
    assert tables.QUALIFIED_NACE_CATEGORIES_TABLE == "reference.nace_categories"
    assert tables.NACE_CATEGORIES_COLUMNS == (
        "classification_version",
        "code",
        "normalized_code",
        "parent_code",
        "level",
        "section_code",
        "description_en",
        "concept_uri",
        "parent_concept_uri",
        "source_scheme_uri",
        "source_url",
        "source_payload_hash",
        "valid_from",
        "valid_to",
        "is_current",
        "source_run_id",
        "pulled_at",
    )
    assert "CREATE TABLE IF NOT EXISTS reference.nace_categories" in tables.NACE_CATEGORIES_DDL
    assert "ORDER BY (classification_version, normalized_code)" in tables.NACE_CATEGORIES_DDL


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)


def test_prepare_nace_categories_table_is_typed_for_official_resource() -> None:
    annotations = get_type_hints(prepare_nace_categories_table)

    assert annotations["clickhouse"] is ClickhouseResource


def test_prepare_nace_categories_table_uses_official_resource_connection(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()
    connection_calls: list[ClickhouseResource] = []

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        connection_calls.append(self)
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    prepare_nace_categories_table(resource)

    assert connection_calls == [resource]
    assert client.statements[0] == "CREATE DATABASE IF NOT EXISTS reference"
    assert client.statements[1].startswith("CREATE TABLE IF NOT EXISTS reference.nace_categories")
    assert client.statements[2] == "TRUNCATE TABLE reference.nace_categories"


def test_prepare_nace_categories_table_strips_ddl_whitespace(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    prepare_nace_categories_table(resource)

    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS reference",
        tables.NACE_CATEGORIES_DDL.strip(),
        "TRUNCATE TABLE reference.nace_categories",
    ]
    ddl = client.statements[1]
    assert ddl == ddl.strip()
