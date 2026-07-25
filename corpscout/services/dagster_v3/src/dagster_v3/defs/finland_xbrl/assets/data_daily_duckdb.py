import tempfile
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import (
    DAILY_PARTITIONS,
    FINLAND_XBRL_DUCKDB_POOL,
    XBRL_BUCKET,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily import (
    financial_data_daily_key,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb import (
    FINANCIAL_DATA_CSV_RAW_STAGE_TABLE,
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
    stage_financial_data_csv,
)

FINLAND_XBRL_FINANCIAL_DATA_DAILY_DUCKDB_PATH = (
    "data/finland_xbrl/financial_data_daily.duckdb"
)
FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE = "financial_data_daily"


def materialize_data_daily_duckdb(
    *,
    partition_key: str,
    object_store: ObjectStoreResource,
    daily_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    s3_key = financial_data_daily_key(partition_key)
    with tempfile.TemporaryDirectory(prefix="finland_xbrl_daily_") as temp_dir:
        csv_path = Path(temp_dir) / "financial_statements.csv"
        object_store.download_file(
            s3_key,
            csv_path,
            bucket=XBRL_BUCKET,
        )
        with daily_duckdb.get_connection() as connection:
            row_count = stage_financial_data_csv(
                connection=connection,
                csv_path=csv_path,
            )
            connection.execute("begin transaction")
            try:
                connection.execute(
                    f"create schema if not exists "
                    f"{FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}"
                )
                connection.execute(
                    f"""
                    create table if not exists
                      {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE} (
                        partition_key varchar,
                        "businessId" varchar,
                        "financialDate" varchar,
                        "registrationDate" varchar
                      )
                    """
                )
                connection.execute(
                    f"""
                    delete from {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE}
                    where partition_key = ?
                    """,
                    [partition_key],
                )
                connection.execute(
                    f"""
                    insert into
                      {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE}
                      (partition_key, "businessId", "financialDate", "registrationDate")
                    select
                        ?::varchar as partition_key,
                        trim(coalesce("businessId", ''))::varchar as "businessId",
                        trim(coalesce("financialDate", ''))::varchar as "financialDate",
                        trim(coalesce("registrationDate", ''))::varchar as "registrationDate"
                    from {FINANCIAL_DATA_CSV_RAW_STAGE_TABLE}
                    """,
                    [partition_key],
                )
            except Exception:
                connection.execute("rollback")
                raise
            connection.execute("commit")

    return dg.MaterializeResult(
        metadata={
            "partition": partition_key,
            "row_count": row_count,
            "duckdb_schema": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
            "duckdb_table": FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE,
            "s3_bucket": XBRL_BUCKET,
            "s3_key": s3_key,
        }
    )


@dg.asset(
    name="data_daily_duckdb",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[dg.AssetKey("data_daily")],
    partitions_def=DAILY_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "s3", "csv", "duckdb"},
    description=(
        "Reads one daily Finland XBRL financial statement listing CSV from S3 "
        "and stores the partition rows in DuckDB."
    ),
)
def data_daily_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    xbrl_financial_data_daily_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Loading Finland XBRL daily financial data CSV into DuckDB: partition=%s",
        context.partition_key,
    )
    result = materialize_data_daily_duckdb(
        partition_key=context.partition_key,
        object_store=object_store,
        daily_duckdb=xbrl_financial_data_daily_duckdb,
    )
    context.log.info(
        "Finland XBRL daily financial data DuckDB complete: partition=%s rows=%s",
        context.partition_key,
        result.metadata["row_count"],
    )
    return result
