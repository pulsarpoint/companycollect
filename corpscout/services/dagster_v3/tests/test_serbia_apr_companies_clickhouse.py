from contextlib import contextmanager
from pathlib import Path

import dagster as dg
import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.serbia_apr_companies import assets, tables
from dagster_v3.defs.serbia_apr_companies.clickhouse import (
    replace_serbia_apr_companies_clickhouse,
)


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
UP_MIGRATION = MIGRATIONS_DIR / "000321_corpscout_rs_apr_company.up.sql"
DOWN_MIGRATION = MIGRATIONS_DIR / "000321_corpscout_rs_apr_company.down.sql"


class _FakeClickHouseClient:
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


def test_clickhouse_assets_publish_history_and_current_without_snapshot_runs() -> None:
    repository = dg.Definitions.merge(
        assets.defs,
        dg.Definitions(resources={"clickhouse": ClickhouseResource(host="localhost")}),
    ).get_repository_def()
    graph = repository.asset_graph

    expected_dependencies = {
        tables.COMPANY_HISTORY_ASSET: tables.COMPANY_OBSERVATIONS_ASSET,
        tables.COMPANY_ASSET: tables.COMPANIES_CURRENT_ASSET,
    }
    for asset_name, upstream_name in expected_dependencies.items():
        node = graph.get(dg.AssetKey(asset_name))
        assert node.parent_keys == {dg.AssetKey(upstream_name)}
        assert node.group_name == tables.GROUP_NAME
        assert node.pools == {tables.DUCKDB_POOL}
        assert {"python", "duckdb", "clickhouse", "apr"} <= node.kinds
        assert node.tags["country"] == "serbia"
        assert node.tags["layer"] == "clickhouse"

    assert assets.serbia_apr_companies_clickhouse_publish.can_subset is False
    assert not graph.has(dg.AssetKey("serbia_apr_company_snapshot_runs_clickhouse"))


def test_migration_matches_the_two_table_export_contract() -> None:
    up_sql = UP_MIGRATION.read_text(encoding="utf-8")
    down_sql = DOWN_MIGRATION.read_text(encoding="utf-8")

    assert "rs_apr_company_snapshot_runs" not in up_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_history" in up_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company\n" in up_sql
    assert "PARTITION BY toYear(snapshot_date)" in up_sql
    assert "ORDER BY (company_id, snapshot_date)" in up_sql
    assert "ORDER BY company_id" in up_sql

    for table_name, columns in tables.CLICKHOUSE_COLUMNS_BY_TABLE.items():
        block_start = up_sql.index(f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}")
        block_end = up_sql.index("\nENGINE =", block_start)
        table_block = up_sql[block_start:block_end]
        positions = [table_block.index(f"\n    {column} ") for column in columns]

        assert positions == sorted(positions)
        assert len(positions) == len(set(positions))
        assert f"DROP TABLE IF EXISTS corpscout.{table_name};" in down_sql

    for columns in tables.CLICKHOUSE_COLUMNS_BY_TABLE.values():
        assert "raw_entity" not in columns
        assert "source_payload_hash" not in columns


def test_history_and_current_are_replaced_as_one_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    connection = _duckdb_source_tables(tmp_path / "apr.duckdb", insert_rows=True)
    fake = _FakeClickHouseClient()
    _patch_clickhouse_connection(monkeypatch, fake)

    try:
        counts = replace_serbia_apr_companies_clickhouse(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
        )
    finally:
        connection.close()

    assert counts == {
        tables.COMPANY_HISTORY_TABLE: 1,
        tables.COMPANY_TABLE: 1,
    }
    assert len(fake.insert_calls) == 2
    assert sum("CREATE TABLE" in statement for statement in fake.statements) == 2
    assert sum("EXCHANGE TABLES" in statement for statement in fake.statements) == 2
    assert (
        sum("DROP TABLE IF EXISTS" in statement for statement in fake.statements) == 2
    )


def test_publish_refuses_empty_duckdb_company_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    connection = _duckdb_source_tables(tmp_path / "apr.duckdb", insert_rows=False)
    fake = _FakeClickHouseClient()
    _patch_clickhouse_connection(monkeypatch, fake)

    try:
        try:
            replace_serbia_apr_companies_clickhouse(
                duckdb_connection=connection,
                clickhouse=ClickhouseResource(host="localhost"),
            )
        except ValueError as exc:
            assert "have 0 rows" in str(exc)
            assert tables.COMPANY_HISTORY_TABLE in str(exc)
            assert tables.COMPANY_TABLE in str(exc)
        else:
            raise AssertionError("expected empty-source protection")
    finally:
        connection.close()

    assert not fake.insert_calls
    assert not any("EXCHANGE TABLES" in statement for statement in fake.statements)


def _duckdb_source_tables(
    database_path: Path,
    *,
    insert_rows: bool,
) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(database_path))
    connection.execute(f'CREATE SCHEMA "{tables.DUCKDB_SCHEMA}"')
    for table_name in (
        tables.COMPANY_OBSERVATIONS_TABLE,
        tables.COMPANIES_CURRENT_TABLE,
    ):
        column_sql = ", ".join(
            f'"{column}" VARCHAR' for column in tables.COMPANY_EXPORT_COLUMNS
        )
        connection.execute(
            f'CREATE TABLE "{tables.DUCKDB_SCHEMA}"."{table_name}" ({column_sql})'
        )
        if insert_rows:
            placeholders = ", ".join("?" for _ in tables.COMPANY_EXPORT_COLUMNS)
            connection.execute(
                f'INSERT INTO "{tables.DUCKDB_SCHEMA}"."{table_name}" '
                f"VALUES ({placeholders})",
                tuple(
                    f"{table_name}:{column}" for column in tables.COMPANY_EXPORT_COLUMNS
                ),
            )
    return connection


def _patch_clickhouse_connection(
    monkeypatch,
    fake: _FakeClickHouseClient,
) -> None:
    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
