from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_jobtech_links import tables
from dagster_v3.defs.sweden_jobtech_links.source import sync_latest_snapshot


@dg.asset(
    group_name=tables.GROUP_NAME,
    kinds={"python", "http", "tar", "gzip", "jsonl", "s3"},
    description=(
        "Discovers and stores the newest dated JobTech Links job-ad archive "
        "unchanged in content-addressed object storage."
    ),
)
def sweden_jobtech_links_snapshot_s3(
    context: dg.AssetExecutionContext,
    sweden_jobtech_links_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_latest_snapshot(
        object_store=sweden_jobtech_links_object_store,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "snapshot_uid": snapshot.snapshot_uid,
            "snapshot_date": snapshot.snapshot_date.isoformat(),
            "source_url": snapshot.source_url,
            "archive_object_key": snapshot.archive_object_key,
            "metadata_object_key": snapshot.metadata_object_key,
            "archive_sha256": snapshot.archive_sha256,
            "archive_size_bytes": snapshot.archive_size_bytes,
            "raw_member_path": snapshot.raw_member_path,
            "raw_member_size_bytes": snapshot.raw_member_size_bytes,
            "downloaded": snapshot.downloaded,
        }
    )


defs = dg.Definitions(
    assets=[sweden_jobtech_links_snapshot_s3],
    resources={
        "sweden_jobtech_links_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
