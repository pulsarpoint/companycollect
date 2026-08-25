from contextlib import contextmanager
from pathlib import Path

import dagster as dg
import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.serbia_apr_company_people import assets, tables
from dagster_v3.defs.serbia_apr_company_people.clickhouse import (
    replace_serbia_apr_beneficial_owners_clickhouse,
    replace_serbia_apr_representatives_clickhouse,
)


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
UP_MIGRATION = MIGRATIONS_DIR / "000319_corpscout_rs_apr_company_people.up.sql"
DOWN_MIGRATION = MIGRATIONS_DIR / "000319_corpscout_rs_apr_company_people.down.sql"


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.insert_calls: list[tuple[str, list[tuple[str, ...]]]] = []

    def execute(self, sql: str, params=None):
        if "system.tables" in sql:
            return [(table_name,) for table_name in params["tables"]]
        if isinstance(params, list):
            self.insert_calls.append((sql, params))
            return None
        self.statements.append(sql)
        return None


def test_clickhouse_assets_have_independent_future_duckdb_dependencies() -> None:
    repository = dg.Definitions.merge(
        assets.defs,
        dg.Definitions(resources={"clickhouse": ClickhouseResource(host="localhost")}),
    ).get_repository_def()
    graph = repository.asset_graph

    expected_dependencies = {
        tables.REPRESENTATIVE_OBSERVATIONS_ASSET: (
            tables.REPRESENTATIVE_OBSERVATIONS_DUCKDB_ASSET,
            tables.REPRESENTATIVES_SOURCE_SLUG,
        ),
        tables.REPRESENTATIVES_CURRENT_ASSET: (
            tables.REPRESENTATIVES_CURRENT_DUCKDB_ASSET,
            tables.REPRESENTATIVES_SOURCE_SLUG,
        ),
        tables.BENEFICIAL_OWNER_OBSERVATIONS_ASSET: (
            tables.BENEFICIAL_OWNER_OBSERVATIONS_DUCKDB_ASSET,
            tables.BENEFICIAL_OWNERS_SOURCE_SLUG,
        ),
        tables.BENEFICIAL_OWNERS_CURRENT_ASSET: (
            tables.BENEFICIAL_OWNERS_CURRENT_DUCKDB_ASSET,
            tables.BENEFICIAL_OWNERS_SOURCE_SLUG,
        ),
    }

    for asset_name, (upstream_name, source_slug) in expected_dependencies.items():
        node = graph.get(dg.AssetKey(asset_name))
        assert node.parent_keys == {dg.AssetKey(upstream_name)}
        assert node.group_name == tables.GROUP_NAME
        assert node.tags["country"] == "serbia"
        assert node.tags["source"] == source_slug
        assert node.tags["layer"] == "clickhouse"

    assert assets.serbia_apr_representatives_clickhouse.can_subset is False
    assert assets.serbia_apr_beneficial_owners_clickhouse.can_subset is False


def test_migration_column_order_matches_duckdb_export_contracts() -> None:
    up_sql = UP_MIGRATION.read_text()
    down_sql = DOWN_MIGRATION.read_text()

    for table_name, columns in tables.CLICKHOUSE_COLUMNS_BY_TABLE.items():
        block_start = up_sql.index(f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}")
        block_end = up_sql.index("\nENGINE =", block_start)
        table_block = up_sql[block_start:block_end]

        positions = [table_block.index(f"\n    {column} ") for column in columns]
        assert positions == sorted(positions)
        assert len(positions) == len(set(positions))
        assert f"DROP TABLE IF EXISTS corpscout.{table_name};" in down_sql

    assert "    jmbg " not in up_sql.lower()
    assert "    passport_number " not in up_sql.lower()
    assert "    personal_identifier_value " not in up_sql.lower()
    for columns in tables.CLICKHOUSE_COLUMNS_BY_TABLE.values():
        assert "source_payload_hash" not in columns


def test_representative_tables_are_replaced_as_one_atomic_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    connection = _duckdb_with_rows(
        tmp_path / "representatives.duckdb",
        schema=tables.REPRESENTATIVES_DUCKDB_SCHEMA,
        table_columns=tables.REPRESENTATIVE_COLUMNS_BY_TABLE,
    )
    fake = FakeClickHouseClient()
    _patch_clickhouse_connection(monkeypatch, fake)

    counts = replace_serbia_apr_representatives_clickhouse(
        duckdb_connection=connection,
        clickhouse=ClickhouseResource(host="localhost"),
    )

    assert counts == {
        tables.REPRESENTATIVE_OBSERVATIONS_TABLE: 1,
        tables.REPRESENTATIVES_CURRENT_TABLE: 1,
    }
    assert len(fake.insert_calls) == 2
    assert sum("EXCHANGE TABLES" in statement for statement in fake.statements) == 2
    assert (
        sum("DROP TABLE IF EXISTS" in statement for statement in fake.statements) == 2
    )


def test_beneficial_owner_tables_are_replaced_as_one_atomic_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    connection = _duckdb_with_rows(
        tmp_path / "beneficial_owners.duckdb",
        schema=tables.BENEFICIAL_OWNERS_DUCKDB_SCHEMA,
        table_columns=tables.BENEFICIAL_OWNER_COLUMNS_BY_TABLE,
    )
    fake = FakeClickHouseClient()
    _patch_clickhouse_connection(monkeypatch, fake)

    counts = replace_serbia_apr_beneficial_owners_clickhouse(
        duckdb_connection=connection,
        clickhouse=ClickhouseResource(host="localhost"),
    )

    assert counts == {
        tables.BENEFICIAL_OWNER_OBSERVATIONS_TABLE: 1,
        tables.BENEFICIAL_OWNERS_CURRENT_TABLE: 1,
    }
    assert len(fake.insert_calls) == 2
    assert sum("EXCHANGE TABLES" in statement for statement in fake.statements) == 2
    assert (
        sum("DROP TABLE IF EXISTS" in statement for statement in fake.statements) == 2
    )


def test_publish_refuses_an_empty_duckdb_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "representatives.duckdb"
    connection = duckdb.connect(str(database_path))
    _create_duckdb_tables(
        connection,
        schema=tables.REPRESENTATIVES_DUCKDB_SCHEMA,
        table_columns=tables.REPRESENTATIVE_COLUMNS_BY_TABLE,
        insert_rows=False,
    )
    fake = FakeClickHouseClient()
    _patch_clickhouse_connection(monkeypatch, fake)

    try:
        replace_serbia_apr_representatives_clickhouse(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
        )
    except ValueError as exc:
        assert "have 0 rows" in str(exc)
        assert tables.REPRESENTATIVE_OBSERVATIONS_TABLE in str(exc)
        assert tables.REPRESENTATIVES_CURRENT_TABLE in str(exc)
    else:
        raise AssertionError("expected empty-source protection")

    assert not fake.insert_calls
    assert not any("EXCHANGE TABLES" in statement for statement in fake.statements)


def _duckdb_with_rows(
    database_path: Path,
    *,
    schema: str,
    table_columns: dict[str, tuple[str, ...]],
):
    connection = duckdb.connect(str(database_path))
    _create_duckdb_tables(
        connection,
        schema=schema,
        table_columns=table_columns,
        insert_rows=True,
    )
    return connection


def _create_duckdb_tables(
    connection,
    *,
    schema: str,
    table_columns: dict[str, tuple[str, ...]],
    insert_rows: bool,
) -> None:
    connection.execute(f'CREATE SCHEMA "{schema}"')
    for table_name, columns in table_columns.items():
        column_sql = ", ".join(f'"{column}" VARCHAR' for column in columns)
        connection.execute(f'CREATE TABLE "{schema}"."{table_name}" ({column_sql})')
        if insert_rows:
            placeholders = ", ".join("?" for _ in columns)
            connection.execute(
                f'INSERT INTO "{schema}"."{table_name}" VALUES ({placeholders})',
                tuple(f"{table_name}:{column}" for column in columns),
            )


def _patch_clickhouse_connection(monkeypatch, fake: FakeClickHouseClient) -> None:
    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
