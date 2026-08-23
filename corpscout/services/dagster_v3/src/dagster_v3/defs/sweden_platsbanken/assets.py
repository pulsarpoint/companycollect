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
from dagster_v3.defs.sweden_platsbanken import tables
from dagster_v3.defs.sweden_platsbanken.clickhouse import (
    append_job_history_batch,
    publish_company_job_projections,
)
from dagster_v3.defs.sweden_platsbanken.normalize import (
    append_raw_jsonl_table,
    build_normalized_tables,
    replace_raw_jsonl_table,
)
from dagster_v3.defs.sweden_platsbanken.source import (
    extract_single_jsonl_archive,
    latest_historical_manifest,
    latest_jobstream_event_manifest,
    latest_jobstream_snapshot_manifest,
    parse_utc_datetime,
    resolve_jobstream_event_window,
    sync_historical_archives,
    sync_jobstream_events,
    sync_jobstream_snapshot,
)

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
DUCKDB_POOL = "sweden_platsbanken_duckdb"


class HistoricalArchivesConfig(dg.Config):
    refresh_existing: bool = False


class JobStreamEventsConfig(dg.Config):
    updated_after: str = ""
    updated_before: str = ""


@dg.asset(
    group_name=tables.GROUP_NAME,
    kinds={"python", "zip", "s3"},
    description=(
        "Stores every complete yearly/quarterly JobTech historical job-ad ZIP "
        "in content-addressed object storage with a replay manifest."
    ),
)
def sweden_platsbanken_historical_archives_s3(
    context: dg.AssetExecutionContext,
    config: HistoricalArchivesConfig,
    sweden_platsbanken_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = sync_historical_archives(
        object_store=sweden_platsbanken_object_store,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
        refresh_existing=config.refresh_existing,
    )
    archives = list(manifest["archives"])
    return dg.MaterializeResult(
        metadata={
            "archive_count": len(archives),
            "downloaded_count": sum(bool(item["downloaded"]) for item in archives),
            "total_size_bytes": sum(int(item["size_bytes"]) for item in archives),
            "manifest_key": str(manifest["manifest_key"]),
            "source_url": tables.HISTORICAL_CATALOG_URL,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_historical_archives_s3")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "zip", "jsonl", "s3", "duckdb"},
    pool=DUCKDB_POOL,
    description=(
        "Loads every complete historical archive into one auditable DuckDB JSON "
        "staging table using DuckDB's set-based JSON reader."
    ),
)
def sweden_platsbanken_historical_raw_duckdb(
    sweden_platsbanken_duckdb: DuckDBResource,
    sweden_platsbanken_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_historical_manifest(sweden_platsbanken_object_store)
    archives = list(manifest["archives"])
    if not archives:
        raise ValueError("Historical Platsbanken manifest contains no archives")

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    with tempfile.TemporaryDirectory(prefix="sweden_platsbanken_archives_") as temp:
        temp_path = Path(temp)
        with sweden_platsbanken_duckdb.get_connection() as connection:
            for index, archive in enumerate(archives):
                archive_path = temp_path / f"archive-{index}.zip"
                jsonl_path = temp_path / f"archive-{index}.jsonl"
                sweden_platsbanken_object_store.download_file(
                    str(archive["object_key"]),
                    archive_path,
                    bucket=tables.S3_BUCKET,
                )
                extract_single_jsonl_archive(archive_path, jsonl_path)
                parameters = {
                    "connection": connection,
                    "jsonl_path": jsonl_path,
                    "raw_table": tables.HISTORICAL_RAW_TABLE,
                    "record_kind": "archive_record",
                    "source_run_id": str(manifest["source_run_id"]),
                    "source_object_key": str(archive["object_key"]),
                    "source_url": str(archive["source_url"]),
                    "retrieved_at": parse_utc_datetime(str(manifest["retrieved_at"])),
                }
                if index == 0:
                    total_rows += replace_raw_jsonl_table(**parameters)
                else:
                    total_rows += append_raw_jsonl_table(**parameters)
    return dg.MaterializeResult(
        metadata={
            "archive_count": len(archives),
            "raw_rows": total_rows,
            "duckdb_table": (f"{tables.DUCKDB_SCHEMA}.{tables.HISTORICAL_RAW_TABLE}"),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_historical_raw_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description=(
        "Normalizes the 2016+ historical archive into full versions, lifecycle "
        "events, and versioned requirement facts."
    ),
)
def sweden_platsbanken_historical_normalized_duckdb(
    context: dg.AssetExecutionContext,
    sweden_platsbanken_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with sweden_platsbanken_duckdb.get_connection() as connection:
        counts = build_normalized_tables(
            connection=connection,
            raw_table=tables.HISTORICAL_RAW_TABLE,
            versions_table=tables.HISTORICAL_VERSIONS_TABLE,
            events_table=tables.HISTORICAL_EVENTS_TABLE,
            requirements_table=tables.HISTORICAL_REQUIREMENTS_TABLE,
            contacts_table=tables.HISTORICAL_CONTACTS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_historical_normalized_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    description=(
        "Idempotently appends normalized archive history to migration-owned "
        "ClickHouse version, event, and requirement tables."
    ),
)
def sweden_platsbanken_historical_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_platsbanken_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_platsbanken_duckdb) as connection:
        counts = append_job_history_batch(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            versions_table=tables.HISTORICAL_VERSIONS_TABLE,
            events_table=tables.HISTORICAL_EVENTS_TABLE,
            requirements_table=tables.HISTORICAL_REQUIREMENTS_TABLE,
            contacts_table=tables.HISTORICAL_CONTACTS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    group_name=tables.GROUP_NAME,
    kinds={"python", "jsonl", "s3"},
    description=(
        "Stores the complete JobStream snapshot for initial current-state bootstrap, "
        "including the application and employer contacts published with each ad."
    ),
)
def sweden_platsbanken_jobstream_snapshot_s3(
    context: dg.AssetExecutionContext,
    sweden_platsbanken_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_jobstream_snapshot(
        object_store=sweden_platsbanken_object_store,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "object_key": snapshot.object_key,
            "manifest_key": snapshot.manifest_key,
            "record_count": snapshot.record_count,
            "size_bytes": snapshot.size_bytes,
            "sha256": snapshot.sha256,
            "downloaded": snapshot.downloaded,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_jobstream_snapshot_s3")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "jsonl", "s3", "duckdb"},
    pool=DUCKDB_POOL,
    description="Loads the latest complete JobStream snapshot into DuckDB.",
)
def sweden_platsbanken_jobstream_snapshot_raw_duckdb(
    sweden_platsbanken_duckdb: DuckDBResource,
    sweden_platsbanken_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_jobstream_snapshot_manifest(sweden_platsbanken_object_store)
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sweden_platsbanken_snapshot_") as temp:
        jsonl_path = Path(temp) / "snapshot.jsonl"
        sweden_platsbanken_object_store.download_file(
            str(manifest["object_key"]),
            jsonl_path,
            bucket=tables.S3_BUCKET,
        )
        with sweden_platsbanken_duckdb.get_connection() as connection:
            rows = replace_raw_jsonl_table(
                connection=connection,
                jsonl_path=jsonl_path,
                raw_table=tables.JOBSTREAM_SNAPSHOT_RAW_TABLE,
                record_kind="snapshot",
                source_run_id=str(manifest["source_run_id"]),
                source_object_key=str(manifest["object_key"]),
                source_url=str(manifest["source_url"]),
                retrieved_at=parse_utc_datetime(str(manifest["retrieved_at"])),
            )
    return dg.MaterializeResult(metadata={"raw_rows": rows})


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_jobstream_snapshot_raw_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description="Normalizes the JobStream bootstrap snapshot as versioned history.",
)
def sweden_platsbanken_jobstream_snapshot_normalized_duckdb(
    context: dg.AssetExecutionContext,
    sweden_platsbanken_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with sweden_platsbanken_duckdb.get_connection() as connection:
        counts = build_normalized_tables(
            connection=connection,
            raw_table=tables.JOBSTREAM_SNAPSHOT_RAW_TABLE,
            versions_table=tables.JOBSTREAM_SNAPSHOT_VERSIONS_TABLE,
            events_table=tables.JOBSTREAM_SNAPSHOT_EVENTS_TABLE,
            requirements_table=tables.JOBSTREAM_SNAPSHOT_REQUIREMENTS_TABLE,
            contacts_table=tables.JOBSTREAM_SNAPSHOT_CONTACTS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_jobstream_snapshot_normalized_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    description="Appends the current JobStream bootstrap state to ClickHouse history.",
)
def sweden_platsbanken_jobstream_snapshot_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_platsbanken_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_platsbanken_duckdb) as connection:
        counts = append_job_history_batch(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            versions_table=tables.JOBSTREAM_SNAPSHOT_VERSIONS_TABLE,
            events_table=tables.JOBSTREAM_SNAPSHOT_EVENTS_TABLE,
            requirements_table=tables.JOBSTREAM_SNAPSHOT_REQUIREMENTS_TABLE,
            contacts_table=tables.JOBSTREAM_SNAPSHOT_CONTACTS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_jobstream_snapshot_s3")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "jsonl", "s3"},
    description=(
        "Stores the next complete JobStream event window using a durable "
        "manifest cursor with a five-minute replay overlap."
    ),
)
def sweden_platsbanken_jobstream_events_s3(
    context: dg.AssetExecutionContext,
    config: JobStreamEventsConfig,
    sweden_platsbanken_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    retrieved_at = datetime.now(UTC)
    updated_after, updated_before = resolve_jobstream_event_window(
        object_store=sweden_platsbanken_object_store,
        now=retrieved_at,
        configured_after=config.updated_after,
        configured_before=config.updated_before,
    )
    events = sync_jobstream_events(
        object_store=sweden_platsbanken_object_store,
        run_id=context.run.run_id,
        retrieved_at=retrieved_at,
        updated_after=updated_after,
        updated_before=updated_before,
    )
    return dg.MaterializeResult(
        metadata={
            "object_key": events.object_key,
            "manifest_key": events.manifest_key,
            "record_count": events.record_count,
            "size_bytes": events.size_bytes,
            "sha256": events.sha256,
            "updated_after": updated_after.isoformat(),
            "updated_before": updated_before.isoformat(),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_jobstream_events_s3")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "jsonl", "s3", "duckdb"},
    pool=DUCKDB_POOL,
    description="Loads the latest complete JobStream event batch into DuckDB.",
)
def sweden_platsbanken_jobstream_events_raw_duckdb(
    sweden_platsbanken_duckdb: DuckDBResource,
    sweden_platsbanken_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_jobstream_event_manifest(sweden_platsbanken_object_store)
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sweden_platsbanken_events_") as temp:
        jsonl_path = Path(temp) / "events.jsonl"
        sweden_platsbanken_object_store.download_file(
            str(manifest["object_key"]),
            jsonl_path,
            bucket=tables.S3_BUCKET,
        )
        with sweden_platsbanken_duckdb.get_connection() as connection:
            rows = replace_raw_jsonl_table(
                connection=connection,
                jsonl_path=jsonl_path,
                raw_table=tables.JOBSTREAM_EVENTS_RAW_TABLE,
                record_kind="stream_event",
                source_run_id=str(manifest["source_run_id"]),
                source_object_key=str(manifest["object_key"]),
                source_url=str(manifest["source_url"]),
                retrieved_at=parse_utc_datetime(str(manifest["retrieved_at"])),
                allow_empty=True,
            )
    return dg.MaterializeResult(metadata={"raw_rows": rows})


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_jobstream_events_raw_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description=(
        "Normalizes full JobStream upserts and sparse removals without deleting "
        "earlier complete job content."
    ),
)
def sweden_platsbanken_jobstream_events_normalized_duckdb(
    context: dg.AssetExecutionContext,
    sweden_platsbanken_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with sweden_platsbanken_duckdb.get_connection() as connection:
        counts = build_normalized_tables(
            connection=connection,
            raw_table=tables.JOBSTREAM_EVENTS_RAW_TABLE,
            versions_table=tables.JOBSTREAM_EVENTS_VERSIONS_TABLE,
            events_table=tables.JOBSTREAM_EVENTS_EVENTS_TABLE,
            requirements_table=tables.JOBSTREAM_EVENTS_REQUIREMENTS_TABLE,
            contacts_table=tables.JOBSTREAM_EVENTS_CONTACTS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("sweden_platsbanken_jobstream_events_normalized_duckdb")],
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    description="Idempotently appends the latest JobStream event batch to history.",
)
def sweden_platsbanken_jobstream_events_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    sweden_platsbanken_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_platsbanken_duckdb) as connection:
        counts = append_job_history_batch(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            versions_table=tables.JOBSTREAM_EVENTS_VERSIONS_TABLE,
            events_table=tables.JOBSTREAM_EVENTS_EVENTS_TABLE,
            requirements_table=tables.JOBSTREAM_EVENTS_REQUIREMENTS_TABLE,
            contacts_table=tables.JOBSTREAM_EVENTS_CONTACTS_TABLE,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[
        dg.AssetKey("sweden_platsbanken_historical_clickhouse"),
        dg.AssetKey("sweden_platsbanken_jobstream_snapshot_clickhouse"),
        dg.AssetKey("sweden_platsbanken_jobstream_events_clickhouse"),
        dg.AssetKey("sweden_company_companies_clickhouse"),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"python", "sql", "clickhouse"},
    description=(
        "Atomically publishes active intervals, exact-company job history, "
        "current jobs, and monthly advertised-hiring activity."
    ),
)
def sweden_platsbanken_company_jobs_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    counts = publish_company_job_projections(
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


sweden_platsbanken_historical_backfill_job = dg.define_asset_job(
    "sweden_platsbanken_historical_backfill_job",
    selection=dg.AssetSelection.assets(
        "sweden_platsbanken_historical_archives_s3",
        "sweden_platsbanken_historical_raw_duckdb",
        "sweden_platsbanken_historical_normalized_duckdb",
        "sweden_platsbanken_historical_clickhouse",
        "sweden_platsbanken_company_jobs_clickhouse",
    ),
)

sweden_platsbanken_jobstream_bootstrap_job = dg.define_asset_job(
    "sweden_platsbanken_jobstream_bootstrap_job",
    selection=dg.AssetSelection.assets(
        "sweden_platsbanken_jobstream_snapshot_s3",
        "sweden_platsbanken_jobstream_snapshot_raw_duckdb",
        "sweden_platsbanken_jobstream_snapshot_normalized_duckdb",
        "sweden_platsbanken_jobstream_snapshot_clickhouse",
        "sweden_platsbanken_company_jobs_clickhouse",
    ),
)

sweden_platsbanken_jobstream_incremental_job = dg.define_asset_job(
    "sweden_platsbanken_jobstream_incremental_job",
    selection=dg.AssetSelection.assets(
        "sweden_platsbanken_jobstream_events_s3",
        "sweden_platsbanken_jobstream_events_raw_duckdb",
        "sweden_platsbanken_jobstream_events_normalized_duckdb",
        "sweden_platsbanken_jobstream_events_clickhouse",
        "sweden_platsbanken_company_jobs_clickhouse",
    ),
)

defs = dg.Definitions(
    assets=[
        sweden_platsbanken_historical_archives_s3,
        sweden_platsbanken_historical_raw_duckdb,
        sweden_platsbanken_historical_normalized_duckdb,
        sweden_platsbanken_historical_clickhouse,
        sweden_platsbanken_jobstream_snapshot_s3,
        sweden_platsbanken_jobstream_snapshot_raw_duckdb,
        sweden_platsbanken_jobstream_snapshot_normalized_duckdb,
        sweden_platsbanken_jobstream_snapshot_clickhouse,
        sweden_platsbanken_jobstream_events_s3,
        sweden_platsbanken_jobstream_events_raw_duckdb,
        sweden_platsbanken_jobstream_events_normalized_duckdb,
        sweden_platsbanken_jobstream_events_clickhouse,
        sweden_platsbanken_company_jobs_clickhouse,
    ],
    jobs=[
        sweden_platsbanken_historical_backfill_job,
        sweden_platsbanken_jobstream_bootstrap_job,
        sweden_platsbanken_jobstream_incremental_job,
    ],
    resources={
        "sweden_platsbanken_duckdb": duckdb_resource(DUCKDB_PATH),
        "sweden_platsbanken_object_store": ObjectStoreResource(bucket=tables.S3_BUCKET),
    },
)
