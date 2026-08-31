from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_jobtech_links import tables
from dagster_v3.defs.sweden_jobtech_links.partitions import (
    PARTITIONS_NAME,
    SNAPSHOT_PARTITIONS,
    plan_catalog_partitions,
)
from dagster_v3.defs.sweden_jobtech_links.source import (
    fetch_snapshot_catalog,
    sync_snapshot_partition,
)


class SnapshotPartitionConfig(dg.Config):
    refresh_existing: bool = False


@dg.asset(
    partitions_def=SNAPSHOT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    group_name=tables.GROUP_NAME,
    kinds={"python", "http", "tar", "gzip", "jsonl", "s3"},
    description=(
        "Stores every dated JobTech Links archive in the selected yearly, monthly, "
        "or daily partition unchanged in content-addressed object storage."
    ),
)
def sweden_jobtech_links_snapshot_s3(
    context: dg.AssetExecutionContext,
    config: SnapshotPartitionConfig,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition = sync_snapshot_partition(
        object_store=sweden_jobtech_links_object_store,
        partition_key=context.partition_key,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
        refresh_existing=config.refresh_existing,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_key": partition.partition_key,
            "archive_count": partition.selected_count,
            "downloaded_count": partition.downloaded_count,
            "reused_count": partition.reused_count,
            "total_archive_size_bytes": partition.total_archive_size_bytes,
            "total_raw_member_size_bytes": partition.total_raw_member_size_bytes,
            "manifest_key": partition.manifest_key,
            "source_url": tables.CATALOG_URL,
        }
    )


sweden_jobtech_links_snapshot_job = dg.define_asset_job(
    "sweden_jobtech_links_snapshot_job",
    selection=dg.AssetSelection.assets("sweden_jobtech_links_snapshot_s3"),
)


@dg.sensor(
    job=sweden_jobtech_links_snapshot_job,
    minimum_interval_seconds=3_600,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Registers catalog-backed historical partitions and launches only newly "
        "published daily JobTech Links partitions from 2026-09-01 onward."
    ),
)
def sweden_jobtech_links_catalog_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult | dg.SkipReason:
    archives = fetch_snapshot_catalog()
    existing_partition_keys = set(
        context.instance.get_dynamic_partitions(PARTITIONS_NAME)
    )
    plan = plan_catalog_partitions(
        available_dates=tuple(archive.snapshot_date for archive in archives),
        existing_partition_keys=existing_partition_keys,
    )
    if not plan.partition_keys_to_add:
        return dg.SkipReason("JobTech Links catalog contains no new partitions")

    return dg.SensorResult(
        dynamic_partitions_requests=[
            SNAPSHOT_PARTITIONS.build_add_request(list(plan.partition_keys_to_add))
        ],
        run_requests=[
            dg.RunRequest(
                run_key=f"sweden-jobtech-links:{partition_key}",
                partition_key=partition_key,
            )
            for partition_key in plan.daily_partition_keys_to_run
        ],
    )


defs = dg.Definitions(
    assets=[sweden_jobtech_links_snapshot_s3],
    jobs=[sweden_jobtech_links_snapshot_job],
    sensors=[sweden_jobtech_links_catalog_sensor],
    resources={
        "sweden_jobtech_links_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
