import tempfile
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_uhm_procurement import tables
from dagster_v3.defs.sweden_uhm_procurement.clickhouse import (
    export_uhm_awards_clickhouse,
)
from dagster_v3.defs.sweden_uhm_procurement.normalize import (
    build_award_candidates,
    replace_raw_table,
)
from dagster_v3.defs.sweden_uhm_procurement.resources import (
    latest_snapshot_manifest,
    sync_uhm_snapshot,
)

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
DUCKDB_POOL = "sweden_uhm_procurement_duckdb"


@dg.asset(
    name="sweden_uhm_procurement_raw_snapshot_s3",
    group_name=tables.GROUP_NAME,
    kinds={"python", "csv", "s3"},
    description=(
        "Downloads the complete Upphandlingsmyndigheten supplier-award CSV, "
        "validates its size, and stores an immutable content-addressed snapshot."
    ),
)
def sweden_uhm_procurement_raw_snapshot_s3(
    context: dg.AssetExecutionContext,
    sweden_uhm_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_uhm_snapshot(
        object_store=sweden_uhm_procurement_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "source_url": tables.SOURCE_URL,
            "object_key": snapshot.object_key,
            "manifest_key": snapshot.manifest_key,
            "sha256": snapshot.sha256,
            "size_bytes": snapshot.size_bytes,
            "downloaded": snapshot.downloaded,
            "last_modified": snapshot.last_modified,
        }
    )


@dg.asset(
    name="sweden_uhm_procurement_raw_duckdb",
    deps=[dg.AssetKey("sweden_uhm_procurement_raw_snapshot_s3")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "csv", "s3", "duckdb"},
    pool=DUCKDB_POOL,
    description=(
        "Loads the latest immutable UHM CSV into DuckDB with all original "
        "Swedish columns preserved as VARCHAR."
    ),
)
def sweden_uhm_procurement_raw_duckdb(
    context: dg.AssetExecutionContext,
    sweden_uhm_procurement_duckdb: DuckDBResource,
    sweden_uhm_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_snapshot_manifest(sweden_uhm_procurement_object_store)
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sweden_uhm_raw_") as temp_dir:
        csv_path = Path(temp_dir) / "awards.csv"
        sweden_uhm_procurement_object_store.download_file(
            str(manifest["object_key"]),
            csv_path,
            bucket=tables.S3_BUCKET,
        )
        with sweden_uhm_procurement_duckdb.get_connection() as connection:
            rows = replace_raw_table(
                connection=connection,
                csv_path=csv_path,
                source_run_id=str(manifest["source_run_id"]),
                source_object_key=str(manifest["object_key"]),
                source_url=str(manifest["source_url"]),
                source_retrieved_at=datetime.fromisoformat(
                    str(manifest["retrieved_at"])
                ),
            )
    return dg.MaterializeResult(
        metadata={
            "raw_rows": rows,
            "object_key": str(manifest["object_key"]),
            "source_retrieved_at": str(manifest["retrieved_at"]),
        }
    )


@dg.asset(
    name="sweden_uhm_procurement_awards_duckdb",
    deps=[dg.AssetKey("sweden_uhm_procurement_raw_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description=(
        "Normalizes UHM award fields and Swedish supplier identifiers in "
        "set-based DuckDB SQL while classifying unsafe/unmatchable identities."
    ),
)
def sweden_uhm_procurement_awards_duckdb(
    context: dg.AssetExecutionContext,
    sweden_uhm_procurement_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with sweden_uhm_procurement_duckdb.get_connection() as connection:
        counts = build_award_candidates(
            connection=connection,
            source_run_id=context.run_id,
            resolved_at=datetime.now(UTC),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="sweden_uhm_procurement_awards_clickhouse",
    deps=[
        dg.AssetKey("sweden_uhm_procurement_awards_duckdb"),
        dg.AssetKey("sweden_company_companies_clickhouse"),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_AWARDS_TABLE},
    description=(
        "Publishes every normalized UHM supplier-award observation for market "
        "analysis and annotates exact ten-digit se_companies matches. Only exact "
        "matches can feed company-level government-contract evidence."
    ),
)
def sweden_uhm_procurement_awards_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_uhm_procurement_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_uhm_procurement_duckdb) as connection:
        counts = export_uhm_awards_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


sweden_uhm_procurement_job = dg.define_asset_job(
    "sweden_uhm_procurement_job",
    selection=dg.AssetSelection.assets(
        "sweden_uhm_procurement_awards_clickhouse"
    ).upstream(),
)

sweden_uhm_procurement_schedule = dg.ScheduleDefinition(
    name="sweden_uhm_procurement_schedule",
    job=sweden_uhm_procurement_job,
    cron_schedule="20 5 8 * *",
    execution_timezone="Europe/Stockholm",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        sweden_uhm_procurement_raw_snapshot_s3,
        sweden_uhm_procurement_raw_duckdb,
        sweden_uhm_procurement_awards_duckdb,
        sweden_uhm_procurement_awards_clickhouse,
    ],
    jobs=[sweden_uhm_procurement_job],
    schedules=[sweden_uhm_procurement_schedule],
    resources={
        "sweden_uhm_procurement_duckdb": duckdb_resource(DUCKDB_PATH),
        "sweden_uhm_procurement_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
