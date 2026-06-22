"""Export the reference DuckDB tables to ClickHouse (schema owned by migrations)."""
from collections.abc import Callable
from pathlib import Path

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)
from dagster_v3.defs.commoncrawl_classify import tables


def _export(*, duckdb_table: str, clickhouse_table: str, columns, database_path: str | Path,
            clickhouse: ClickhouseResource, log: Callable[..., object] | None) -> int:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(clickhouse_table,))
    if log is not None:
        log("Exporting %s -> %s.%s", duckdb_table, tables.DATABASE, clickhouse_table)
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=duckdb_table,
            clickhouse_database=tables.DATABASE,
            clickhouse_table=clickhouse_table,
            columns=columns,
            truncate=True,
        )
    if log is not None:
        log("Exported %s.%s: rows=%s", tables.DATABASE, clickhouse_table, rows)
    return rows


def export_nace_category_embeddings(*, database_path: str | Path, clickhouse: ClickhouseResource,
                                    log: Callable[..., object] | None = None) -> int:
    return _export(
        duckdb_table=tables.NACE_EMBEDDINGS_TABLE,
        clickhouse_table=tables.NACE_EMBEDDINGS_TABLE,
        columns=tables.NACE_EMBEDDINGS_COLUMNS,
        database_path=database_path, clickhouse=clickhouse, log=log,
    )


def export_page_type_exemplars(*, database_path: str | Path, clickhouse: ClickhouseResource,
                               log: Callable[..., object] | None = None) -> int:
    return _export(
        duckdb_table=tables.PAGE_TYPE_EXEMPLARS_TABLE,
        clickhouse_table=tables.PAGE_TYPE_EXEMPLARS_TABLE,
        columns=tables.PAGE_TYPE_EXEMPLARS_COLUMNS,
        database_path=database_path, clickhouse=clickhouse, log=log,
    )
