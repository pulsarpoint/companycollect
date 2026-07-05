from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_financial.cvm import tables
from dagster_v3.defs.brazil_financial.cvm.companies import CVM_COMPANIES_TABLE
from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_AUDITOR_REPORTS_TABLE,
    DFP_CAPITAL_COMPOSITION_TABLE,
    DFP_DOCUMENTS_TABLE,
    DFP_STATEMENT_ROWS_TABLE,
)
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)

EXPORT_TABLES = (
    (
        DFP_DOCUMENTS_TABLE,
        tables.BR_CVM_DFP_DOCUMENTS_TABLE,
        tables.BR_CVM_DFP_DOCUMENTS_EXPORT_COLUMNS,
    ),
    (
        DFP_STATEMENT_ROWS_TABLE,
        tables.BR_CVM_DFP_STATEMENT_ROWS_TABLE,
        tables.BR_CVM_DFP_STATEMENT_ROWS_EXPORT_COLUMNS,
    ),
    (
        DFP_CAPITAL_COMPOSITION_TABLE,
        tables.BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE,
        tables.BR_CVM_DFP_CAPITAL_COMPOSITION_EXPORT_COLUMNS,
    ),
    (
        DFP_AUDITOR_REPORTS_TABLE,
        tables.BR_CVM_DFP_AUDITOR_REPORTS_TABLE,
        tables.BR_CVM_DFP_AUDITOR_REPORTS_EXPORT_COLUMNS,
    ),
)


def export_brazil_fin_cvm_dfp_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Replace Brazil CVM DFP ClickHouse tables from converted DuckDB tables."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_CVM_DATABASE,
        tables=tables.BR_CVM_DFP_TABLES,
    )
    _assert_duckdb_tables_have_rows(duckdb_connection)

    row_counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for duckdb_table, clickhouse_table, columns in EXPORT_TABLES:
            if log is not None:
                log(
                    "Exporting Brazil CVM DFP table to ClickHouse: table=%s.%s",
                    tables.BRAZIL_CVM_DATABASE,
                    clickhouse_table,
                )
            row_counts[f"{clickhouse_table}_row_count"] = (
                export_duckdb_connection_table_to_clickhouse(
                    duckdb_connection=duckdb_connection,
                    clickhouse_client=client,
                    duckdb_schema=BRAZIL_CVM_DUCKDB_SCHEMA,
                    duckdb_table=duckdb_table,
                    clickhouse_database=tables.BRAZIL_CVM_DATABASE,
                    clickhouse_table=clickhouse_table,
                    columns=columns,
                    truncate=True,
                )
            )

    if log is not None:
        log("Finished Brazil CVM DFP ClickHouse export: row_counts=%s", row_counts)
    return row_counts


def export_brazil_fin_cvm_companies_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Replace the Brazil CVM company support table in ClickHouse from DuckDB."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_CVM_DATABASE,
        tables=(tables.BR_CVM_COMPANIES_TABLE,),
    )
    if _duckdb_row_count(duckdb_connection, CVM_COMPANIES_TABLE) == 0:
        raise ValueError(
            "Brazil CVM companies DuckDB table is empty; refusing to replace "
            f"ClickHouse table from {BRAZIL_CVM_DUCKDB_SCHEMA}.{CVM_COMPANIES_TABLE}"
        )

    if log is not None:
        log(
            "Exporting Brazil CVM companies table to ClickHouse: table=%s.%s",
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=BRAZIL_CVM_DUCKDB_SCHEMA,
            duckdb_table=CVM_COMPANIES_TABLE,
            clickhouse_database=tables.BRAZIL_CVM_DATABASE,
            clickhouse_table=tables.BR_CVM_COMPANIES_TABLE,
            columns=tables.BR_CVM_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    return {f"{tables.BR_CVM_COMPANIES_TABLE}_row_count": rows}


def _assert_duckdb_tables_have_rows(duckdb_connection: Any) -> None:
    for duckdb_table, _, _ in EXPORT_TABLES:
        if _duckdb_row_count(duckdb_connection, duckdb_table) == 0:
            raise ValueError(
                "Brazil CVM DFP DuckDB table is empty; refusing to replace "
                f"ClickHouse table from {BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}"
            )


def _duckdb_row_count(duckdb_connection: Any, duckdb_table: str) -> int:
    row = duckdb_connection.execute(
        f"select count(*) from {BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}"
    ).fetchone()
    if row is None:
        return 0
    return int(row[0])
