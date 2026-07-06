from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_companies.cgu import parsing, tables
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)

_REQUIRED_DATE_COLUMNS = frozenset({"snapshot_date"})
_NULLABLE_DATE_COLUMNS = frozenset(
    {
        "sanction_start_date",
        "sanction_end_date",
        "publication_date",
        "final_judgment_date",
        "source_information_date",
        "agreement_start_date",
        "agreement_end_date",
        "information_date",
    }
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
            column_expressions=_date_column_expressions(table.columns),
        )
    if log is not None:
        log("Finished Brazil CGU ClickHouse export: rows=%s", rows)
    return rows


def _date_column_expressions(columns: tuple[str, ...]) -> dict[str, str]:
    expressions: dict[str, str] = {}
    for column in columns:
        if column in _REQUIRED_DATE_COLUMNS:
            expressions[column] = f"cast({column} as date)"
        elif column in _NULLABLE_DATE_COLUMNS:
            expressions[column] = (
                f"try_cast(nullif(cast({column} as varchar), '') as date)"
            )
    return expressions
