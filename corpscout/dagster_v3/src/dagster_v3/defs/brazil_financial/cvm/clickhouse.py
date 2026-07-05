from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_financial.cvm import tables
from dagster_v3.defs.brazil_financial.cvm.companies import CVM_COMPANIES_TABLE
from dagster_v3.defs.brazil_financial.cvm.itr_parsing import (
    ITR_AUDITOR_REPORTS_TABLE,
    ITR_CAPITAL_COMPOSITION_TABLE,
    ITR_DOCUMENTS_TABLE,
    ITR_STATEMENT_ROWS_TABLE,
)
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

ITR_EXPORT_TABLES = (
    (
        ITR_DOCUMENTS_TABLE,
        tables.BR_CVM_ITR_DOCUMENTS_TABLE,
        tables.BR_CVM_ITR_DOCUMENTS_EXPORT_COLUMNS,
    ),
    (
        ITR_STATEMENT_ROWS_TABLE,
        tables.BR_CVM_ITR_STATEMENT_ROWS_TABLE,
        tables.BR_CVM_ITR_STATEMENT_ROWS_EXPORT_COLUMNS,
    ),
    (
        ITR_CAPITAL_COMPOSITION_TABLE,
        tables.BR_CVM_ITR_CAPITAL_COMPOSITION_TABLE,
        tables.BR_CVM_ITR_CAPITAL_COMPOSITION_EXPORT_COLUMNS,
    ),
    (
        ITR_AUDITOR_REPORTS_TABLE,
        tables.BR_CVM_ITR_AUDITOR_REPORTS_TABLE,
        tables.BR_CVM_ITR_AUDITOR_REPORTS_EXPORT_COLUMNS,
    ),
)


def export_brazil_fin_cvm_clickhouse_table(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    duckdb_table: str,
    clickhouse_table: str,
    columns: tuple[str, ...],
    family: str,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace one Brazil CVM ClickHouse table from its DuckDB staging table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_CVM_DATABASE,
        tables=(clickhouse_table,),
    )
    _assert_duckdb_table_has_rows(
        duckdb_connection,
        duckdb_table=duckdb_table,
        family=family,
    )
    _assert_statement_rows_usd_complete(
        duckdb_connection,
        duckdb_table=duckdb_table,
        family=family,
    )

    if log is not None:
        log(
            "Exporting Brazil CVM %s table to ClickHouse: table=%s.%s",
            family,
            tables.BRAZIL_CVM_DATABASE,
            clickhouse_table,
        )
    with clickhouse.get_connection() as client:
        return export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=BRAZIL_CVM_DUCKDB_SCHEMA,
            duckdb_table=duckdb_table,
            clickhouse_database=tables.BRAZIL_CVM_DATABASE,
            clickhouse_table=clickhouse_table,
            columns=columns,
            truncate=True,
        )


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
            column_expressions={"auditor_cnpj": "coalesce(auditor_cnpj, '')"},
            truncate=True,
        )
    return {f"{tables.BR_CVM_COMPANIES_TABLE}_row_count": rows}


def _assert_duckdb_table_has_rows(
    duckdb_connection: Any,
    *,
    duckdb_table: str,
    family: str,
) -> None:
    if _duckdb_row_count(duckdb_connection, duckdb_table) == 0:
        raise ValueError(
            f"Brazil CVM {family} DuckDB table is empty; refusing to replace "
            f"ClickHouse table from {BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}"
        )


def _assert_statement_rows_usd_complete(
    duckdb_connection: Any,
    *,
    duckdb_table: str,
    family: str,
) -> None:
    if duckdb_table not in (DFP_STATEMENT_ROWS_TABLE, ITR_STATEMENT_ROWS_TABLE):
        return

    missing_usd_count = _eligible_statement_rows_missing_usd(
        duckdb_connection,
        duckdb_table=duckdb_table,
    )
    if missing_usd_count == 0:
        return

    raise ValueError(
        f"Brazil CVM {family} statement rows have {missing_usd_count} rows "
        "missing USD conversion; refusing to replace ClickHouse table from "
        f"{BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}"
    )


def _eligible_statement_rows_missing_usd(
    duckdb_connection: Any,
    *,
    duckdb_table: str,
) -> int:
    row = duckdb_connection.execute(
        f"""
        select count(*)
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}
        where amount_original is not null
          and nullif(trim(currency), '') is not null
          and period_end_date is not null
          and (
              amount_usd is null
              or fx_rate_to_usd is null
          )
        """
    ).fetchone()
    if row is None:
        return 0
    return int(row[0] or 0)


def _duckdb_row_count(duckdb_connection: Any, duckdb_table: str) -> int:
    row = duckdb_connection.execute(
        f"select count(*) from {BRAZIL_CVM_DUCKDB_SCHEMA}.{duckdb_table}"
    ).fetchone()
    if row is None:
        return 0
    return int(row[0])
