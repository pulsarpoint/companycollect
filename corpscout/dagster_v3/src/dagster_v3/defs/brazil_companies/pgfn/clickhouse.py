from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_companies.pgfn import parsing, tables
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)


def export_brazil_comp_pgfn_company_debts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_pgfn_company_debts from the PGFN DuckDB stage."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_PGFN_DATABASE,
        tables=(tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil PGFN company debts to ClickHouse: table=%s",
            tables.QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=parsing.BRAZIL_PGFN_DUCKDB_SCHEMA,
            duckdb_table=tables.COMPANY_DEBTS_TABLE,
            clickhouse_database=tables.BRAZIL_COMP_PGFN_DATABASE,
            clickhouse_table=tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH,
            columns=tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil PGFN company debts ClickHouse export: rows=%s", rows)
    return rows
