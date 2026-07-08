from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_companies.pgfn import parsing, tables
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)

PGFN_CLICKHOUSE_INSERT_BATCH_SIZE = 5_000
_REQUIRED_DATE_COLUMNS = frozenset({"snapshot_reference_date"})
_NULLABLE_DATE_COLUMNS = frozenset({"inscription_date"})
_NON_STRING_COLUMNS = (
    _REQUIRED_DATE_COLUMNS
    | _NULLABLE_DATE_COLUMNS
    | {
        "snapshot_year",
        "snapshot_quarter",
        "source_row_number",
        "is_lawsuit",
        "consolidated_amount_brl",
        "resolved_at",
    }
)


def export_brazil_comp_pgfn_company_debts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    snapshot_quarter: str | None = None,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_pgfn_company_debts from the PGFN DuckDB stage."""
    export_log = _snapshot_log(log, snapshot_quarter)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_PGFN_DATABASE,
        tables=(tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH,),
    )
    if export_log is not None:
        export_log(
            "Exporting Brazil PGFN company debts to ClickHouse: table=%s batch_size=%d",
            tables.QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE,
            PGFN_CLICKHOUSE_INSERT_BATCH_SIZE,
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
            batch_size=PGFN_CLICKHOUSE_INSERT_BATCH_SIZE,
            column_expressions=_clickhouse_export_expressions(
                tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS
            ),
            log=export_log,
        )
    if export_log is not None:
        export_log(
            "Finished Brazil PGFN company debts ClickHouse export: rows=%s", rows
        )
    return rows


def _snapshot_log(
    log: Callable[..., object] | None,
    snapshot_quarter: str | None,
) -> Callable[..., object] | None:
    if log is None or snapshot_quarter is None:
        return log

    def _log(message: str, *args: object) -> object:
        return log("snapshot_quarter=%s " + message, snapshot_quarter, *args)

    return _log


def _clickhouse_export_expressions(columns: tuple[str, ...]) -> dict[str, str]:
    expressions: dict[str, str] = {}
    for column in columns:
        if column in _REQUIRED_DATE_COLUMNS:
            expressions[column] = f"cast({column} as date)"
        elif column in _NULLABLE_DATE_COLUMNS:
            expressions[column] = (
                f"try_cast(nullif(cast({column} as varchar), '') as date)"
            )
        elif column not in _NON_STRING_COLUMNS:
            expressions[column] = f"coalesce(cast({column} as varchar), '')"
    return expressions
