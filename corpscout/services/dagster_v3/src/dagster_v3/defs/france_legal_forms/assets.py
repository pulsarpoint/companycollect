"""INSEE catégorie juridique nomenclature: fetch, land in DuckDB, publish.

Reference data, not a register. It sits apart from `france_sirene` for the
same reason `cpv_vocabulary` sits apart from the procurement sources: its
cadence is the publisher's revision schedule, not Sirene's daily refresh. The
list last changed in September 2022, so this is scheduled yearly -- often
enough to pick up a revision, rarely enough that 309 rows are not re-fetched
for nothing.
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
from dagster_v3.defs.france_legal_forms import tables
from dagster_v3.defs.france_legal_forms.source import (
    CJ_QUERY,
    INSEE_SPARQL_URL,
    MIN_LEGAL_FORM_ROWS,
    parse_legal_forms,
)

GROUP_NAME = "france_legal_forms"
FR_LEGAL_FORMS_DUCKDB_PATH = Path("data/france_legal_forms_source.duckdb")
# Single-writer pool covering every asset that opens the file, readers
# included: a DuckDB writer excludes readers across processes.
FR_LEGAL_FORMS_DUCKDB_POOL = "france_legal_forms_duckdb"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "france_legal_forms"},
    pool=FR_LEGAL_FORMS_DUCKDB_POOL,
    description=(
        "Fetches INSEE's catégorie juridique nomenclature (309 codes across "
        "three levels) from its SPARQL endpoint and lands it in DuckDB."
    ),
)
def france_legal_forms_duckdb(
    context: dg.AssetExecutionContext,
    france_legal_forms_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    # dlt's session, not plain requests: it retries connection errors and
    # 429/5xx, which a public SPARQL endpoint needs.
    response = requests.get(
        INSEE_SPARQL_URL,
        params={"query": CJ_QUERY},
        headers={"Accept": "text/csv"},
        timeout=120,
    )
    response.raise_for_status()

    rows = parse_legal_forms(response.text)
    if len(rows) < MIN_LEGAL_FORM_ROWS:
        # Refuse to replace on a short read. A partial nomenclature would
        # silently unname whole families of legal form rather than fail, and
        # the failure would look like "those companies have no legal form".
        raise ValueError(
            f"INSEE nomenclature yielded {len(rows)} codes, "
            f"below the {MIN_LEGAL_FORM_ROWS} floor"
        )

    duplicates = len(rows) - len({row.code for row in rows})
    if duplicates:
        # The URI filter in CJ_QUERY exists to prevent exactly this: a renamed
        # form keeps its code and gains a second, historic concept. If both
        # come back, the label chosen would be arbitrary.
        raise ValueError(
            f"INSEE nomenclature returned {duplicates} duplicate codes; "
            f"the current-concept filter is no longer selecting one label each"
        )

    retrieved_at = datetime.now(UTC).replace(tzinfo=None)
    # An Arrow table rather than row-wise executemany: DuckDB reads Arrow
    # directly in C++, and tests/test_duckdb_bulk_loading_contract.py forbids
    # the row-at-a-time path outside one explicitly tracked TED debt.
    batch = pa.table(
        {
            "code": pa.array([row.code for row in rows], pa.string()),
            "level": pa.array([row.level for row in rows], pa.uint8()),
            "label_fr": pa.array([row.label_fr for row in rows], pa.string()),
            "parent_code": pa.array([row.parent_code for row in rows], pa.string()),
            "source_url": pa.array([INSEE_SPARQL_URL] * len(rows), pa.string()),
            "source_run_id": pa.array([context.run_id] * len(rows), pa.string()),
            "retrieved_at": pa.array([retrieved_at] * len(rows), pa.timestamp("us")),
        }
    )

    with france_legal_forms_duckdb.get_connection() as connection:
        connection.execute(
            f"CREATE SCHEMA IF NOT EXISTS {tables.FR_LEGAL_FORMS_DUCKDB_SCHEMA}"
        )
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE
              {tables.FR_LEGAL_FORMS_DUCKDB_SCHEMA}.{tables.FR_LEGAL_FORMS_TABLE} (
                code VARCHAR NOT NULL,
                level UTINYINT NOT NULL,
                label_fr VARCHAR NOT NULL,
                parent_code VARCHAR NOT NULL,
                source_url VARCHAR NOT NULL,
                source_run_id VARCHAR NOT NULL,
                retrieved_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.register("france_legal_forms_batch", batch)
        try:
            connection.execute(
                f"INSERT INTO {tables.FR_LEGAL_FORMS_DUCKDB_SCHEMA}."
                f"{tables.FR_LEGAL_FORMS_TABLE} SELECT * FROM france_legal_forms_batch"
            )
        finally:
            connection.unregister("france_legal_forms_batch")

    return dg.MaterializeResult(
        metadata={
            "row_count": len(rows),
            "level_1": sum(1 for row in rows if row.level == 1),
            "level_2": sum(1 for row in rows if row.level == 2),
            "level_3": sum(1 for row in rows if row.level == 3),
            "source_url": INSEE_SPARQL_URL,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("france_legal_forms_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "france_legal_forms"},
    pool=FR_LEGAL_FORMS_DUCKDB_POOL,
    description="Publishes the INSEE catégorie juridique nomenclature to ClickHouse.",
)
def france_legal_forms_clickhouse(
    clickhouse: ClickhouseResource,
    france_legal_forms_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    # The migration owns the schema; this asserts it exists and replaces the
    # contents rather than issuing DDL of its own.
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.FR_LEGAL_FORMS_TABLES,
    )
    with france_legal_forms_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as client:
            row_count = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=tables.FR_LEGAL_FORMS_DUCKDB_SCHEMA,
                duckdb_table=tables.FR_LEGAL_FORMS_TABLE,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=tables.FR_LEGAL_FORMS_TABLE,
                columns=tables.FR_LEGAL_FORMS_COLUMNS,
                truncate=True,
            )
    return dg.MaterializeResult(metadata={"row_count": row_count})


france_legal_forms_refresh_job = dg.define_asset_job(
    name="france_legal_forms_refresh_job",
    selection=dg.AssetSelection.assets(france_legal_forms_clickhouse).upstream(),
)

france_legal_forms_yearly_schedule = dg.ScheduleDefinition(
    name="france_legal_forms_yearly",
    job=france_legal_forms_refresh_job,
    # 03:40 on 1 February. The nomenclature last changed in September 2022, so
    # yearly picks up a revision without re-fetching 309 rows for nothing.
    cron_schedule="40 3 1 2 *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[france_legal_forms_duckdb, france_legal_forms_clickhouse],
    jobs=[france_legal_forms_refresh_job],
    schedules=[france_legal_forms_yearly_schedule],
    resources={
        "france_legal_forms_duckdb": duckdb_resource(FR_LEGAL_FORMS_DUCKDB_PATH)
    },
)
