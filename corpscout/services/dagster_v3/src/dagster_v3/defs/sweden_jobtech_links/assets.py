import tempfile
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.duckdb_runtime import apply_duckdb_runtime_settings
from dagster_v3.defs.common.partition_duckdb import (
    open_partition_duckdb,
    require_partition_duckdb,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_jobtech_links import tables
from dagster_v3.defs.sweden_jobtech_links.clickhouse import (
    append_normalized_partition_to_clickhouse,
    replace_job_ads_from_observations,
)
from dagster_v3.defs.sweden_jobtech_links.normalize import (
    LoadedSnapshot,
    SnapshotProvenance,
    append_snapshot_jsonl,
    build_normalized_tables,
    initialize_raw_tables,
    replace_snapshot_catalog,
)
from dagster_v3.defs.sweden_jobtech_links.partitions import (
    DAILY_PARTITIONS,
    HISTORICAL_PARTITIONS,
    MONTHLY_2026_PARTITIONS,
    PartitionKind,
    daily_partition_keys_from_catalog,
)
from dagster_v3.defs.sweden_jobtech_links.source import (
    extract_snapshot_jsonl_archive,
    fetch_snapshot_catalog,
    latest_snapshot_manifest,
    sync_snapshot_partition,
)

BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
DUCKDB_POOL = "sweden_jobtech_links_duckdb"
CLICKHOUSE_TABLES = [
    f"{tables.CLICKHOUSE_DATABASE}.{target_table}"
    for _, target_table, _, _ in tables.CLICKHOUSE_APPEND_TABLES
]
CLICKHOUSE_SERVING_TABLES = [
    f"{tables.CLICKHOUSE_DATABASE}.{tables.CLICKHOUSE_INTERVALS_TABLE}",
    f"{tables.CLICKHOUSE_DATABASE}.{tables.CLICKHOUSE_JOB_ADS_TABLE}",
]


class SnapshotPartitionConfig(dg.Config):
    refresh_existing: bool = False


def _materialize_snapshot_partition(
    *,
    context: dg.AssetExecutionContext,
    config: SnapshotPartitionConfig,
    object_store: ObjectStoreResource,
    partition_kind: PartitionKind,
) -> dg.MaterializeResult:
    partition = sync_snapshot_partition(
        object_store=object_store,
        partition_kind=partition_kind,
        partition_key=context.partition_key,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
        refresh_existing=config.refresh_existing,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_kind": partition_kind,
            "partition_key": partition.partition_key,
            "archive_count": partition.selected_count,
            "downloaded_count": partition.downloaded_count,
            "reused_count": partition.reused_count,
            "skipped_existing_partition": partition.skipped_existing,
            "total_archive_size_bytes": partition.total_archive_size_bytes,
            "total_raw_member_size_bytes": partition.total_raw_member_size_bytes,
            "manifest_key": partition.manifest_key,
            "source_url": tables.CATALOG_URL,
        }
    )


def _parse_timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a UTC offset: {value}")
    return parsed.astimezone(UTC)


def _parse_optional_http_timestamp(value: object) -> datetime | None:
    clean_value = str(value).strip()
    if clean_value == "":
        return None
    parsed = parsedate_to_datetime(clean_value)
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a UTC offset: {value}")
    return parsed.astimezone(UTC)


def _snapshot_provenance(
    *, manifest: dict[str, object], archive: dict[str, object]
) -> SnapshotProvenance:
    return SnapshotProvenance(
        snapshot_uid=str(archive["snapshot_uid"]),
        snapshot_date=datetime.fromisoformat(str(archive["snapshot_date"])).date(),
        catalog_url=str(manifest["catalog_url"]),
        source_url=str(archive["source_url"]),
        archive_object_key=str(archive["archive_object_key"]),
        archive_sha256=str(archive["archive_sha256"]),
        archive_etag=str(archive["source_etag"]),
        archive_size_bytes=int(archive["archive_size_bytes"]),
        raw_member_path=str(archive["raw_member_path"]),
        raw_member_size_bytes=int(archive["raw_member_size_bytes"]),
        source_last_modified_at=_parse_optional_http_timestamp(
            archive["source_last_modified"]
        ),
        source_run_id=str(manifest["source_run_id"]),
        retrieved_at=_parse_timestamp(manifest["retrieved_at"]),
    )


def _materialize_raw_duckdb(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    partition_kind: PartitionKind,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    manifest = latest_snapshot_manifest(
        object_store=object_store,
        partition_kind=partition_kind,
        partition_key=partition_key,
    )
    archives = list(manifest["archives"])
    partition_path = tables.partition_duckdb_path(partition_key)
    loaded_snapshots: list[LoadedSnapshot] = []
    with open_partition_duckdb(
        source=tables.SOURCE_SLUG, partition=partition_key
    ) as connection:
        apply_duckdb_runtime_settings(
            connection,
            default_temp_directory=partition_path.parent / "duckdb_tmp",
        )
        initialize_raw_tables(connection)
        for index, archive_value in enumerate(archives, start=1):
            archive = dict(archive_value)
            provenance = _snapshot_provenance(manifest=manifest, archive=archive)
            with tempfile.TemporaryDirectory(
                prefix="sweden_jobtech_links_duckdb_"
            ) as temp:
                temp_path = Path(temp)
                archive_path = temp_path / f"snapshot-{index}.tar.gz"
                jsonl_path = temp_path / f"snapshot-{index}.jsonl"
                object_store.download_file(
                    provenance.archive_object_key,
                    archive_path,
                    bucket=tables.S3_BUCKET,
                )
                extract_snapshot_jsonl_archive(
                    archive_path,
                    jsonl_path,
                    expected_member_path=provenance.raw_member_path,
                )
                loaded_snapshots.append(
                    append_snapshot_jsonl(
                        connection=connection,
                        jsonl_path=jsonl_path,
                        provenance=provenance,
                    )
                )
            context.log.info(
                "JobTech Links partition %s: loaded %s/%s S3 archives into DuckDB",
                partition_key,
                index,
                len(archives),
            )
        replace_snapshot_catalog(connection, loaded_snapshots)

    return dg.MaterializeResult(
        metadata={
            "partition_kind": partition_kind,
            "partition_key": partition_key,
            "manifest_key": str(manifest["manifest_key"]),
            "archive_count": len(loaded_snapshots),
            "raw_rows": sum(item.raw_row_count for item in loaded_snapshots),
            "platsbanken_rows": sum(
                item.platsbanken_row_count for item in loaded_snapshots
            ),
            "external_rows": sum(item.external_row_count for item in loaded_snapshots),
            "duckdb_path": str(partition_path),
            "duckdb_raw_table": (f"{tables.DUCKDB_SCHEMA}.{tables.RAW_EXTERNAL_TABLE}"),
        }
    )


def _materialize_normalized_duckdb(
    *, context: dg.AssetExecutionContext, partition_kind: PartitionKind
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    partition_path = tables.partition_duckdb_path(partition_key)
    with require_partition_duckdb(
        source=tables.SOURCE_SLUG, partition=partition_key
    ) as connection:
        apply_duckdb_runtime_settings(
            connection,
            default_temp_directory=partition_path.parent / "duckdb_tmp",
        )
        counts = build_normalized_tables(connection=connection)
    return dg.MaterializeResult(
        metadata={
            **counts,
            "partition_kind": partition_kind,
            "partition_key": partition_key,
            "duckdb_path": str(partition_path),
        }
    )


def _materialize_clickhouse(
    *,
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    partition_kind: PartitionKind,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    partition_path = tables.partition_duckdb_path(partition_key)
    with require_partition_duckdb(
        source=tables.SOURCE_SLUG, partition=partition_key
    ) as connection:
        apply_duckdb_runtime_settings(
            connection,
            default_temp_directory=partition_path.parent / "duckdb_tmp",
        )
        counts = append_normalized_partition_to_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "partition_kind": partition_kind,
            "partition_key": partition_key,
            "duckdb_path": str(partition_path),
            "clickhouse_database": tables.CLICKHOUSE_DATABASE,
            "clickhouse_tables": CLICKHOUSE_TABLES,
        }
    )


@dg.asset(
    partitions_def=HISTORICAL_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "http", "tar", "gzip", "jsonl", "s3"},
    description=(
        "Stores one fixed 2021-2025 JobTech Links year in content-addressed "
        "object storage for historical backfill."
    ),
)
def sweden_jobtech_links_historical_snapshot_s3(
    context: dg.AssetExecutionContext,
    config: SnapshotPartitionConfig,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_snapshot_partition(
        context=context,
        config=config,
        object_store=sweden_jobtech_links_object_store,
        partition_kind="year",
    )


@dg.asset(
    partitions_def=MONTHLY_2026_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "http", "tar", "gzip", "jsonl", "s3"},
    description=(
        "Stores one fixed January-August 2026 JobTech Links month and reuses a "
        "completed S3 manifest when a closed month is retried."
    ),
)
def sweden_jobtech_links_2026_month_snapshot_s3(
    context: dg.AssetExecutionContext,
    config: SnapshotPartitionConfig,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_snapshot_partition(
        context=context,
        config=config,
        object_store=sweden_jobtech_links_object_store,
        partition_kind="month",
    )


@dg.asset(
    partitions_def=DAILY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "http", "tar", "gzip", "jsonl", "s3"},
    description=(
        "Stores one JobTech Links daily archive from 2026-09-01 onward in "
        "content-addressed object storage."
    ),
)
def sweden_jobtech_links_daily_snapshot_s3(
    context: dg.AssetExecutionContext,
    config: SnapshotPartitionConfig,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_snapshot_partition(
        context=context,
        config=config,
        object_store=sweden_jobtech_links_object_store,
        partition_kind="day",
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_historical_snapshot_s3")],
    partitions_def=HISTORICAL_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "tar", "gzip", "jsonl", "duckdb"},
    pool=DUCKDB_POOL,
    description=(
        "Loads one historical year from its S3 manifest into an isolated DuckDB "
        "audit catalog and external-provider raw JSON table."
    ),
)
def sweden_jobtech_links_historical_raw_duckdb(
    context: dg.AssetExecutionContext,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_raw_duckdb(
        context=context,
        object_store=sweden_jobtech_links_object_store,
        partition_kind="year",
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_historical_raw_duckdb")],
    partitions_def=HISTORICAL_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description=(
        "Normalizes one historical year into external-provider versions, daily "
        "observations, locations, and accepted JobTech enrichments."
    ),
)
def sweden_jobtech_links_historical_normalized_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    return _materialize_normalized_duckdb(context=context, partition_kind="year")


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_historical_normalized_duckdb")],
    partitions_def=HISTORICAL_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    metadata={"tables": CLICKHOUSE_TABLES},
    description=(
        "Idempotently appends one historical JobTech Links year from its "
        "partition-local DuckDB file into migration-owned ClickHouse tables."
    ),
)
def sweden_jobtech_links_historical_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _materialize_clickhouse(
        context=context,
        clickhouse=clickhouse,
        partition_kind="year",
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_2026_month_snapshot_s3")],
    partitions_def=MONTHLY_2026_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "tar", "gzip", "jsonl", "duckdb"},
    pool=DUCKDB_POOL,
    description=(
        "Loads one fixed 2026 month from S3 into its partition-local DuckDB "
        "audit catalog and external-provider raw JSON table."
    ),
)
def sweden_jobtech_links_2026_month_raw_duckdb(
    context: dg.AssetExecutionContext,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_raw_duckdb(
        context=context,
        object_store=sweden_jobtech_links_object_store,
        partition_kind="month",
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_2026_month_raw_duckdb")],
    partitions_def=MONTHLY_2026_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description=(
        "Normalizes one fixed 2026 month into external-provider versions, daily "
        "observations, locations, and accepted JobTech enrichments."
    ),
)
def sweden_jobtech_links_2026_month_normalized_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    return _materialize_normalized_duckdb(context=context, partition_kind="month")


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_2026_month_normalized_duckdb")],
    partitions_def=MONTHLY_2026_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    metadata={"tables": CLICKHOUSE_TABLES},
    description=(
        "Idempotently appends one fixed 2026 month from partition-local DuckDB "
        "into migration-owned JobTech Links ClickHouse tables."
    ),
)
def sweden_jobtech_links_2026_month_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _materialize_clickhouse(
        context=context,
        clickhouse=clickhouse,
        partition_kind="month",
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_daily_snapshot_s3")],
    partitions_def=DAILY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "s3", "tar", "gzip", "jsonl", "duckdb"},
    pool=DUCKDB_POOL,
    description=(
        "Loads one daily S3 archive into its partition-local DuckDB audit "
        "catalog and external-provider raw JSON table."
    ),
)
def sweden_jobtech_links_daily_raw_duckdb(
    context: dg.AssetExecutionContext,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return _materialize_raw_duckdb(
        context=context,
        object_store=sweden_jobtech_links_object_store,
        partition_kind="day",
    )


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_daily_raw_duckdb")],
    partitions_def=DAILY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "sql"},
    pool=DUCKDB_POOL,
    description=(
        "Normalizes one daily partition into external-provider versions, its "
        "presence observation, locations, and accepted JobTech enrichments."
    ),
)
def sweden_jobtech_links_daily_normalized_duckdb(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    return _materialize_normalized_duckdb(context=context, partition_kind="day")


@dg.asset(
    deps=[dg.AssetKey("sweden_jobtech_links_daily_normalized_duckdb")],
    partitions_def=DAILY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=DUCKDB_POOL,
    metadata={"tables": CLICKHOUSE_TABLES},
    description=(
        "Idempotently appends one daily partition from DuckDB into the "
        "migration-owned JobTech Links ClickHouse tables."
    ),
)
def sweden_jobtech_links_daily_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _materialize_clickhouse(
        context=context,
        clickhouse=clickhouse,
        partition_kind="day",
    )


@dg.asset(
    deps=[
        dg.AssetKey("sweden_jobtech_links_historical_clickhouse"),
        dg.AssetKey("sweden_jobtech_links_2026_month_clickhouse"),
        dg.AssetKey("sweden_jobtech_links_daily_clickhouse"),
    ],
    group_name=tables.GROUP_NAME,
    kinds={"python", "sql", "clickhouse"},
    metadata={"tables": CLICKHOUSE_SERVING_TABLES},
    description=(
        "Globally resolves snapshot observations into active intervals and "
        "atomically publishes one JobTech Links row per job with an active or "
        "expired status. Source deadlines remain evidence, not lifecycle events."
    ),
)
def sweden_jobtech_links_job_ads_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    counts = replace_job_ads_from_observations(
        clickhouse=clickhouse,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            **counts,
            "clickhouse_database": tables.CLICKHOUSE_DATABASE,
            "clickhouse_tables": CLICKHOUSE_SERVING_TABLES,
        }
    )


sweden_jobtech_links_historical_snapshot_job = dg.define_asset_job(
    "sweden_jobtech_links_historical_snapshot_job",
    selection=dg.AssetSelection.assets(sweden_jobtech_links_historical_snapshot_s3),
)
sweden_jobtech_links_2026_month_snapshot_job = dg.define_asset_job(
    "sweden_jobtech_links_2026_month_snapshot_job",
    selection=dg.AssetSelection.assets(sweden_jobtech_links_2026_month_snapshot_s3),
)
sweden_jobtech_links_daily_snapshot_job = dg.define_asset_job(
    "sweden_jobtech_links_daily_snapshot_job",
    selection=dg.AssetSelection.assets(sweden_jobtech_links_daily_snapshot_s3),
)
sweden_jobtech_links_historical_duckdb_job = dg.define_asset_job(
    "sweden_jobtech_links_historical_duckdb_job",
    selection=dg.AssetSelection.assets(
        sweden_jobtech_links_historical_snapshot_s3,
        sweden_jobtech_links_historical_raw_duckdb,
        sweden_jobtech_links_historical_normalized_duckdb,
    ),
)
sweden_jobtech_links_2026_month_duckdb_job = dg.define_asset_job(
    "sweden_jobtech_links_2026_month_duckdb_job",
    selection=dg.AssetSelection.assets(
        sweden_jobtech_links_2026_month_snapshot_s3,
        sweden_jobtech_links_2026_month_raw_duckdb,
        sweden_jobtech_links_2026_month_normalized_duckdb,
    ),
)
sweden_jobtech_links_daily_duckdb_job = dg.define_asset_job(
    "sweden_jobtech_links_daily_duckdb_job",
    selection=dg.AssetSelection.assets(
        sweden_jobtech_links_daily_snapshot_s3,
        sweden_jobtech_links_daily_raw_duckdb,
        sweden_jobtech_links_daily_normalized_duckdb,
    ),
)
sweden_jobtech_links_historical_clickhouse_job = dg.define_asset_job(
    "sweden_jobtech_links_historical_clickhouse_job",
    selection=dg.AssetSelection.assets(
        sweden_jobtech_links_historical_snapshot_s3,
        sweden_jobtech_links_historical_raw_duckdb,
        sweden_jobtech_links_historical_normalized_duckdb,
        sweden_jobtech_links_historical_clickhouse,
    ),
)
sweden_jobtech_links_2026_month_clickhouse_job = dg.define_asset_job(
    "sweden_jobtech_links_2026_month_clickhouse_job",
    selection=dg.AssetSelection.assets(
        sweden_jobtech_links_2026_month_snapshot_s3,
        sweden_jobtech_links_2026_month_raw_duckdb,
        sweden_jobtech_links_2026_month_normalized_duckdb,
        sweden_jobtech_links_2026_month_clickhouse,
    ),
)
sweden_jobtech_links_daily_clickhouse_job = dg.define_asset_job(
    "sweden_jobtech_links_daily_clickhouse_job",
    selection=dg.AssetSelection.assets(
        sweden_jobtech_links_daily_snapshot_s3,
        sweden_jobtech_links_daily_raw_duckdb,
        sweden_jobtech_links_daily_normalized_duckdb,
        sweden_jobtech_links_daily_clickhouse,
    ),
)
sweden_jobtech_links_job_ads_job = dg.define_asset_job(
    "sweden_jobtech_links_job_ads_job",
    selection=dg.AssetSelection.assets(sweden_jobtech_links_job_ads_clickhouse),
)


@dg.sensor(
    job=sweden_jobtech_links_daily_snapshot_job,
    minimum_interval_seconds=3_600,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Launches catalog-backed fixed daily JobTech Links partitions from "
        "2026-09-01 onward."
    ),
)
def sweden_jobtech_links_daily_catalog_sensor(
    _context: dg.SensorEvaluationContext,
) -> dg.SensorResult | dg.SkipReason:
    archives = fetch_snapshot_catalog()
    partition_keys = daily_partition_keys_from_catalog(
        [archive.snapshot_date for archive in archives]
    )
    if not partition_keys:
        return dg.SkipReason(
            "JobTech Links catalog contains no daily archives from 2026-09-01"
        )
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=f"sweden-jobtech-links:day:{partition_key}",
                partition_key=partition_key,
            )
            for partition_key in partition_keys
        ]
    )


defs = dg.Definitions(
    assets=[
        sweden_jobtech_links_historical_snapshot_s3,
        sweden_jobtech_links_historical_raw_duckdb,
        sweden_jobtech_links_historical_normalized_duckdb,
        sweden_jobtech_links_historical_clickhouse,
        sweden_jobtech_links_2026_month_snapshot_s3,
        sweden_jobtech_links_2026_month_raw_duckdb,
        sweden_jobtech_links_2026_month_normalized_duckdb,
        sweden_jobtech_links_2026_month_clickhouse,
        sweden_jobtech_links_daily_snapshot_s3,
        sweden_jobtech_links_daily_raw_duckdb,
        sweden_jobtech_links_daily_normalized_duckdb,
        sweden_jobtech_links_daily_clickhouse,
        sweden_jobtech_links_job_ads_clickhouse,
    ],
    jobs=[
        sweden_jobtech_links_historical_snapshot_job,
        sweden_jobtech_links_2026_month_snapshot_job,
        sweden_jobtech_links_daily_snapshot_job,
        sweden_jobtech_links_historical_duckdb_job,
        sweden_jobtech_links_2026_month_duckdb_job,
        sweden_jobtech_links_daily_duckdb_job,
        sweden_jobtech_links_historical_clickhouse_job,
        sweden_jobtech_links_2026_month_clickhouse_job,
        sweden_jobtech_links_daily_clickhouse_job,
        sweden_jobtech_links_job_ads_job,
    ],
    sensors=[sweden_jobtech_links_daily_catalog_sensor],
    resources={
        "sweden_jobtech_links_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
