from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_jobtech_links import tables
from dagster_v3.defs.sweden_jobtech_links.partitions import (
    DAILY_PARTITIONS,
    HISTORICAL_PARTITIONS,
    MONTHLY_2026_PARTITIONS,
    PartitionKind,
    daily_partition_keys_from_catalog,
)
from dagster_v3.defs.sweden_jobtech_links.source import (
    fetch_snapshot_catalog,
    sync_snapshot_partition,
)

BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


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
        sweden_jobtech_links_2026_month_snapshot_s3,
        sweden_jobtech_links_daily_snapshot_s3,
    ],
    jobs=[
        sweden_jobtech_links_historical_snapshot_job,
        sweden_jobtech_links_2026_month_snapshot_job,
        sweden_jobtech_links_daily_snapshot_job,
    ],
    sensors=[sweden_jobtech_links_daily_catalog_sensor],
    resources={
        "sweden_jobtech_links_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
