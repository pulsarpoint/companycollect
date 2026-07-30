"""CPV 2008 vocabulary: download the EU XML, land it in DuckDB, publish to ClickHouse.

Not partitioned and not scheduled. The vocabulary is an annex to Regulation
(EC) No 213/2008 and has not changed since the file was published in August
2008, so there is no cadence to match -- it is materialised when the table
needs (re)building. Partitioning it would add event-log churn and backfill
ceremony for a single 2.5 MB download.
"""

from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pyarrow as pa
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.cpv_vocabulary import tables
from dagster_v3.defs.cpv_vocabulary.source import (
    CPV_2008_URL,
    extract_cpv_xml,
    parse_cpv_vocabulary,
)

GROUP_NAME = "cpv_vocabulary"
CPV_VOCABULARY_DUCKDB_PATH = Path("data/cpv_vocabulary_source.duckdb")
# Single-writer pool covering every asset that opens the file, readers
# included: a DuckDB writer excludes readers across processes.
CPV_VOCABULARY_DUCKDB_POOL = "cpv_vocabulary_duckdb"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "cpv_vocabulary"},
    pool=CPV_VOCABULARY_DUCKDB_POOL,
    description="Downloads the EU CPV 2008 vocabulary and lands its 9,454 codes in DuckDB.",
)
def cpv_vocabulary_duckdb(
    context: dg.AssetExecutionContext,
    cpv_vocabulary_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    # dlt's session, not plain requests: it retries connection errors and
    # 429/5xx, which a 2.5 MB download off a public portal needs.
    response = requests.get(CPV_2008_URL, timeout=120)
    response.raise_for_status()

    rows = parse_cpv_vocabulary(extract_cpv_xml(response.content))
    if len(rows) < tables.MIN_CPV_VOCABULARY_ROWS:
        # Refuse to replace on a short read. A partial vocabulary would silently
        # unname whole branches of the tree rather than fail.
        raise ValueError(
            f"CPV vocabulary yielded {len(rows)} codes, "
            f"below the {tables.MIN_CPV_VOCABULARY_ROWS} floor"
        )

    retrieved_at = datetime.now(UTC).replace(tzinfo=None)
    # An Arrow table rather than row-wise executemany: DuckDB reads Arrow
    # directly in C++, and tests/test_duckdb_bulk_loading_contract.py forbids
    # the row-at-a-time path outside one explicitly tracked TED debt.
    batch = pa.table(
        {
            "code": pa.array([row.code for row in rows], pa.string()),
            "label_en": pa.array([row.label_en for row in rows], pa.string()),
            "significant_digits": pa.array(
                [row.significant_digits for row in rows], pa.uint8()
            ),
            "parent_code": pa.array([row.parent_code for row in rows], pa.string()),
            "source_url": pa.array([CPV_2008_URL] * len(rows), pa.string()),
            "source_run_id": pa.array([context.run_id] * len(rows), pa.string()),
            "retrieved_at": pa.array([retrieved_at] * len(rows), pa.timestamp("us")),
        }
    )

    with cpv_vocabulary_duckdb.get_connection() as connection:
        connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {tables.CPV_VOCABULARY_DUCKDB_SCHEMA}"
        )
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE
              {tables.CPV_VOCABULARY_DUCKDB_SCHEMA}.{tables.CPV_VOCABULARY_TABLE} (
                code VARCHAR NOT NULL,
                label_en VARCHAR NOT NULL,
                significant_digits UTINYINT NOT NULL,
                parent_code VARCHAR NOT NULL,
                source_url VARCHAR NOT NULL,
                source_run_id VARCHAR NOT NULL,
                retrieved_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.register("cpv_vocabulary_batch", batch)
        try:
            connection.execute(
                f"INSERT INTO {tables.CPV_VOCABULARY_DUCKDB_SCHEMA}."
                f"{tables.CPV_VOCABULARY_TABLE} SELECT * FROM cpv_vocabulary_batch"
            )
        finally:
            connection.unregister("cpv_vocabulary_batch")

    divisions = sum(1 for row in rows if not row.parent_code)
    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "divisions": divisions,
            "source_url": CPV_2008_URL,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("cpv_vocabulary_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "cpv_vocabulary"},
    pool=CPV_VOCABULARY_DUCKDB_POOL,
    description="Publishes the CPV 2008 vocabulary from DuckDB to ClickHouse.",
)
def cpv_vocabulary_clickhouse(
    clickhouse: ClickhouseResource,
    cpv_vocabulary_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    # The migration owns the schema; this asserts it exists and replaces the
    # contents rather than issuing DDL of its own.
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.CPV_VOCABULARY_TABLES,
    )
    with cpv_vocabulary_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as client:
            row_count = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=tables.CPV_VOCABULARY_DUCKDB_SCHEMA,
                duckdb_table=tables.CPV_VOCABULARY_TABLE,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=tables.CPV_VOCABULARY_TABLE,
                columns=tables.CPV_VOCABULARY_COLUMNS,
                truncate=True,
            )
    return dg.MaterializeResult(metadata={"row_count": row_count})


cpv_vocabulary_refresh_job = dg.define_asset_job(
    name="cpv_vocabulary_refresh_job",
    selection=dg.AssetSelection.assets(cpv_vocabulary_clickhouse).upstream(),
)

defs = dg.Definitions(
    assets=[cpv_vocabulary_duckdb, cpv_vocabulary_clickhouse],
    jobs=[cpv_vocabulary_refresh_job],
    resources={"cpv_vocabulary_duckdb": duckdb_resource(CPV_VOCABULARY_DUCKDB_PATH)},
)
