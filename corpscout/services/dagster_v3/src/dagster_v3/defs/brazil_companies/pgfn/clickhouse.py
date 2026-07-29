from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_companies.pgfn import parsing, source, tables
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)

PGFN_CLICKHOUSE_INSERT_BATCH_SIZE = 5_000
PGFN_PARTITION_EXPORT_VIEW = "pgfn_company_debts_partition_export"
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
    snapshot_quarter: str,
    log: Callable[..., object] | None = None,
) -> int:
    """Append one PGFN quarterly snapshot from DuckDB to ClickHouse."""
    normalized_quarter = source.normalize_snapshot_quarter(snapshot_quarter)
    snapshot_year_text, snapshot_quarter_text = normalized_quarter.split("-Q", 1)
    snapshot_year = int(snapshot_year_text)
    snapshot_quarter_number = int(snapshot_quarter_text)
    export_log = _snapshot_log(log, snapshot_quarter)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_PGFN_DATABASE,
        tables=(tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH,),
    )
    if export_log is not None:
        export_log(
            "Exporting Brazil PGFN company debt partition to ClickHouse: "
            "table=%s batch_size=%d",
            tables.QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE,
            PGFN_CLICKHOUSE_INSERT_BATCH_SIZE,
        )
    duckdb_connection.execute(
        f"""
        create or replace temp view {PGFN_PARTITION_EXPORT_VIEW} as
        select *
        from {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
        where snapshot_year = {snapshot_year}
          and snapshot_quarter = {snapshot_quarter_number}
        """
    )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema="temp",
            duckdb_table=PGFN_PARTITION_EXPORT_VIEW,
            clickhouse_database=tables.BRAZIL_COMP_PGFN_DATABASE,
            clickhouse_table=tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH,
            columns=tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS,
            truncate=False,
            batch_size=PGFN_CLICKHOUSE_INSERT_BATCH_SIZE,
            column_expressions=_clickhouse_export_expressions(
                tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS
            ),
            log=export_log,
        )
    if rows == 0:
        raise ValueError(
            f"Brazil PGFN DuckDB partition {normalized_quarter} has 0 rows; "
            "refusing to materialize an empty ClickHouse partition"
        )
    if export_log is not None:
        export_log(
            "Finished Brazil PGFN company debt partition ClickHouse export: rows=%s",
            rows,
        )
    return rows


def _snapshot_log(
    log: Callable[..., object] | None,
    snapshot_quarter: str,
) -> Callable[..., object] | None:
    if log is None:
        return None

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
