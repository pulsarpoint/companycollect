from collections.abc import Callable

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.finland_xbrl.assets.common import (
    DAILY_PARTITIONS,
    FINLAND_XBRL_DUCKDB_POOL,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily_duckdb import (
    FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb import (
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb_ch import (
    DATA_SNAPSHOT_CLICKHOUSE_COLUMNS,
    DATA_SNAPSHOT_CLICKHOUSE_TABLE,
)
from dagster_v3.defs.finland_xbrl.clickhouse import CLICKHOUSE_DATABASE

DATA_DAILY_CLICKHOUSE_EXPORT_TABLE = "data_daily_clickhouse_export"


def export_data_daily_duckdb_to_clickhouse(
    *,
    partition_key: str,
    daily_duckdb: DuckDBResource,
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
            "Exporting Finland XBRL daily data DuckDB partition to ClickHouse: "
            "partition=%s table=%s.%s",
            partition_key,
            CLICKHOUSE_DATABASE,
            DATA_SNAPSHOT_CLICKHOUSE_TABLE,
        )

    with daily_duckdb.get_connection() as duckdb_connection:
        duckdb_connection.execute(
            f"""
            create or replace temp table {DATA_DAILY_CLICKHOUSE_EXPORT_TABLE} as
            select
                "businessId" as business_id,
                cast("financialDate" as date) as financial_date,
                cast("registrationDate" as date) as registration_date
            from {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE}
            where partition_key = ?
            """,
            [partition_key],
        )
        with clickhouse.get_connection() as clickhouse_client:
            row_count = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema="temp",
                duckdb_table=DATA_DAILY_CLICKHOUSE_EXPORT_TABLE,
                clickhouse_database=CLICKHOUSE_DATABASE,
                clickhouse_table=DATA_SNAPSHOT_CLICKHOUSE_TABLE,
                columns=DATA_SNAPSHOT_CLICKHOUSE_COLUMNS,
                truncate=False,
            )

    if log is not None:
        log(
            "Finished Finland XBRL daily data ClickHouse export: partition=%s rows=%d",
            partition_key,
            row_count,
        )
    return row_count


@dg.asset(
    name="data_daily_duckdb_ch",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[dg.AssetKey("data_daily_duckdb")],
    partitions_def=DAILY_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "duckdb", "clickhouse"},
    description=(
        "Inserts one daily Finland XBRL financial statement listing DuckDB "
        "partition into corpscout.fi_xbrl_financial_statement_listings."
    ),
)
def data_daily_duckdb_ch(
    context: dg.AssetExecutionContext,
    xbrl_financial_data_daily_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    row_count = export_data_daily_duckdb_to_clickhouse(
        partition_key=context.partition_key,
        daily_duckdb=xbrl_financial_data_daily_duckdb,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition": context.partition_key,
            "row_count": row_count,
            "clickhouse_database": CLICKHOUSE_DATABASE,
            "clickhouse_table": DATA_SNAPSHOT_CLICKHOUSE_TABLE,
            "duckdb_schema": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
            "duckdb_table": FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE,
        }
    )
