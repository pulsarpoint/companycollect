from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_companies.cgu import parsing, tables
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)


def export_brazil_comp_cgu_table_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    table: tables.CguTable,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace one Brazil CGU ClickHouse raw table from the DuckDB stage."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_CGU_DATABASE,
        tables=(table.clickhouse_table,),
    )
    if log is not None:
        log(
            "Exporting Brazil CGU table to ClickHouse: table=%s.%s",
            tables.BRAZIL_COMP_CGU_DATABASE,
            table.clickhouse_table,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=parsing.BRAZIL_CGU_DUCKDB_SCHEMA,
            duckdb_table=table.duckdb_table,
            clickhouse_database=tables.BRAZIL_COMP_CGU_DATABASE,
            clickhouse_table=table.clickhouse_table,
            columns=table.columns,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil CGU ClickHouse export: rows=%s", rows)
    return rows
