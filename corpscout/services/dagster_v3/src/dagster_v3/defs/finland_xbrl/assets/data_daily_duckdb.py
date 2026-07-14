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
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
    financial_data_csv_rows,
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
    csv_body = object_store.read_bytes(s3_key, bucket=XBRL_BUCKET).decode("utf-8")
    rows = financial_data_csv_rows(csv_body)
    with daily_duckdb.get_connection() as connection:
        connection.execute(
            f"create schema if not exists {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}"
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
        if rows:
            connection.executemany(
                f"""
                insert into {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE}
                  (partition_key, "businessId", "financialDate", "registrationDate")
                values (?, ?, ?, ?)
                """,
                [
                    (
                        partition_key,
                        row["businessId"],
                        row["financialDate"],
                        row["registrationDate"],
                    )
                    for row in rows
                ],
            )

    return dg.MaterializeResult(
        metadata={
            "partition": partition_key,
            "row_count": len(rows),
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
