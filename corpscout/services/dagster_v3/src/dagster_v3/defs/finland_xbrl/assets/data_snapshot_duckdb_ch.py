from collections.abc import Callable

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.finland_xbrl.assets.common import FINLAND_XBRL_DUCKDB_POOL
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb import (
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
)
from dagster_v3.defs.finland_xbrl.clickhouse import CLICKHOUSE_DATABASE

DATA_SNAPSHOT_CLICKHOUSE_TABLE = "fi_xbrl_financial_statement_listings"
DATA_SNAPSHOT_CLICKHOUSE_COLUMNS = (
    "business_id",
    "financial_date",
    "registration_date",
)
DATA_SNAPSHOT_DUCKDB_COLUMN_EXPRESSIONS = {
    "business_id": '"businessId"',
    "financial_date": 'cast("financialDate" as date)',
    "registration_date": 'cast("registrationDate" as date)',
}


def export_data_snapshot_duckdb_to_clickhouse(
    *,
    snapshot_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(DATA_SNAPSHOT_CLICKHOUSE_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Finland XBRL data snapshot DuckDB to ClickHouse: table=%s.%s",
            CLICKHOUSE_DATABASE,
            DATA_SNAPSHOT_CLICKHOUSE_TABLE,
        )

    with snapshot_duckdb.get_connection() as duckdb_connection:
        with clickhouse.get_connection() as clickhouse_client:
            row_count = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
                duckdb_table=FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
                clickhouse_database=CLICKHOUSE_DATABASE,
                clickhouse_table=DATA_SNAPSHOT_CLICKHOUSE_TABLE,
                columns=DATA_SNAPSHOT_CLICKHOUSE_COLUMNS,
                column_expressions=DATA_SNAPSHOT_DUCKDB_COLUMN_EXPRESSIONS,
                truncate=True,
            )

    if log is not None:
        log(
            "Finished Finland XBRL data snapshot ClickHouse export: rows=%d",
            row_count,
        )
    return row_count


@dg.asset(
    name="data_snapshot_duckdb_ch",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[dg.AssetKey("data_snapshot_duckdb")],
    kinds={"python", "duckdb", "clickhouse"},
    description=(
        "Replaces corpscout.fi_xbrl_financial_statement_listings from the "
        "Finland XBRL financial data snapshot DuckDB table."
    ),
)
def data_snapshot_duckdb_ch(
    context: dg.AssetExecutionContext,
    xbrl_financial_data_snapshot_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    row_count = export_data_snapshot_duckdb_to_clickhouse(
        snapshot_duckdb=xbrl_financial_data_snapshot_duckdb,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "clickhouse_database": CLICKHOUSE_DATABASE,
            "clickhouse_table": DATA_SNAPSHOT_CLICKHOUSE_TABLE,
            "duckdb_schema": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
            "duckdb_table": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
        }
    )
