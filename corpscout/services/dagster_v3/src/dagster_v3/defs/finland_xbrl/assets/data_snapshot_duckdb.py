import csv
import tempfile
from pathlib import Path

import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import (
    FINLAND_XBRL_DUCKDB_POOL,
    XBRL_BUCKET,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot import (
    FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS,
    FINANCIAL_DATA_S3_SNAPSHOT_KEY,
)

FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH = (
    "data/finland_xbrl/financial_data_snapshot.duckdb"
)
FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA = "finland_prh_xbrl"
FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE = "financial_data_snapshot"
FINANCIAL_DATA_CSV_RAW_STAGE_TABLE = "_finland_xbrl_financial_data_csv_raw"


def materialize_data_snapshot_duckdb(
    *,
    object_store: ObjectStoreResource,
    snapshot_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with tempfile.TemporaryDirectory(prefix="finland_xbrl_snapshot_") as temp_dir:
        csv_path = Path(temp_dir) / "financial_statements.csv"
        object_store.download_file(
            FINANCIAL_DATA_S3_SNAPSHOT_KEY,
            csv_path,
            bucket=XBRL_BUCKET,
        )
        with snapshot_duckdb.get_connection() as connection:
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
                    create or replace table
                      {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE}
                    as
                    select
                        trim(coalesce("businessId", ''))::varchar as "businessId",
                        trim(coalesce("financialDate", ''))::varchar as "financialDate",
                        trim(coalesce("registrationDate", ''))::varchar as "registrationDate"
                    from {FINANCIAL_DATA_CSV_RAW_STAGE_TABLE}
                    """
                )
            except Exception:
                connection.execute("rollback")
                raise
            connection.execute("commit")

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "duckdb_schema": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
            "duckdb_table": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
            "s3_bucket": XBRL_BUCKET,
            "s3_key": FINANCIAL_DATA_S3_SNAPSHOT_KEY,
        }
    )


def stage_financial_data_csv(
    *,
    connection: duckdb.DuckDBPyConnection,
    csv_path: Path,
) -> int:
    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        reader = csv.reader(csv_file)
        fieldnames = next(reader, None)
    if fieldnames != list(FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS):
        raise ValueError(
            "Finland XBRL financial data snapshot CSV columns must be exactly "
            f"{list(FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS)}; got {fieldnames}"
        )

    connection.execute(
        f"""
        create or replace temp table {FINANCIAL_DATA_CSV_RAW_STAGE_TABLE}
        as
        select *
        from read_csv(?, header = true, all_varchar = true)
        """,
        [str(csv_path)],
    )
    return int(
        connection.execute(
            f"select count(*) from {FINANCIAL_DATA_CSV_RAW_STAGE_TABLE}"
        ).fetchone()[0]
    )


@dg.asset(
    name="data_snapshot_duckdb",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[dg.AssetKey("data_snapshot")],
    kinds={"python", "s3", "csv", "duckdb"},
    description=(
        "Reads the fixed Finland XBRL financial statement listing CSV snapshot "
        "from S3 and materializes the same three source columns into DuckDB."
    ),
)
def data_snapshot_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    xbrl_financial_data_snapshot_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Loading Finland XBRL financial data snapshot CSV from S3 into DuckDB: "
        "bucket=%s key=%s",
        XBRL_BUCKET,
        FINANCIAL_DATA_S3_SNAPSHOT_KEY,
    )
    result = materialize_data_snapshot_duckdb(
        object_store=object_store,
        snapshot_duckdb=xbrl_financial_data_snapshot_duckdb,
    )
    context.log.info(
        "Finland XBRL financial data snapshot DuckDB complete: rows=%s schema=%s table=%s",
        result.metadata["row_count"],
        FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
        FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
    )
    return result
