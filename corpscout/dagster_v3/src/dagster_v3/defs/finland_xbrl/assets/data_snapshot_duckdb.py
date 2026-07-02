import csv
from io import StringIO

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import XBRL_BUCKET
from dagster_v3.defs.finland_xbrl.assets.data_snapshot import (
    FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS,
    FINANCIAL_DATA_S3_SNAPSHOT_KEY,
)

FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH = (
    "data/finland_xbrl/financial_data_snapshot.duckdb"
)
FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA = "finland_prh_xbrl"
FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE = "financial_data_snapshot"


def materialize_data_snapshot_duckdb(
    *,
    object_store: ObjectStoreResource,
    snapshot_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    csv_body = object_store.read_bytes(
        FINANCIAL_DATA_S3_SNAPSHOT_KEY,
        bucket=XBRL_BUCKET,
    ).decode("utf-8")
    rows = _snapshot_csv_rows(csv_body)
    with snapshot_duckdb.get_connection() as connection:
        connection.execute(
            f"create schema if not exists {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}"
        )
        connection.execute(
            f"""
            create or replace table
              {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE} (
                "businessId" varchar,
                "financialDate" varchar,
                "registrationDate" varchar
              )
            """
        )
        if rows:
            connection.executemany(
                f"""
                insert into {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE}
                  ("businessId", "financialDate", "registrationDate")
                values (?, ?, ?)
                """,
                [
                    (
                        row["businessId"],
                        row["financialDate"],
                        row["registrationDate"],
                    )
                    for row in rows
                ],
            )

    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "duckdb_schema": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
            "duckdb_table": FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
            "s3_bucket": XBRL_BUCKET,
            "s3_key": FINANCIAL_DATA_S3_SNAPSHOT_KEY,
        }
    )


@dg.asset(
    name="data_snapshot_duckdb",
    group_name="finland_xbrl",
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


def _snapshot_csv_rows(csv_body: str) -> list[dict[str, str]]:
    reader = csv.DictReader(StringIO(csv_body))
    if reader.fieldnames != list(FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS):
        raise ValueError(
            "Finland XBRL financial data snapshot CSV columns must be exactly "
            f"{list(FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS)}; got {reader.fieldnames}"
        )
    return [
        {
            "businessId": _csv_value(row.get("businessId")),
            "financialDate": _csv_value(row.get("financialDate")),
            "registrationDate": _csv_value(row.get("registrationDate")),
        }
        for row in reader
    ]


def _csv_value(value: str | None) -> str:
    return str(value or "").strip()
