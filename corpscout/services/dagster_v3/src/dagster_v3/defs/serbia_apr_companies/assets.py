from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.serbia_apr_companies import tables
from dagster_v3.defs.serbia_apr_companies.resources import (
    sync_apr_companies_snapshot,
)


@dg.asset(
    name="serbia_apr_companies_raw_snapshot_s3",
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3", "apr"},
    tags=tables.ASSET_TAGS,
    metadata={
        "s3_bucket": tables.S3_BUCKET,
        "source_url": tables.SOURCE_URL,
    },
    description=(
        "Streams the complete Serbian APR companies open-data JSON response, "
        "validates its envelope and population, and stores an immutable "
        "content-addressed snapshot in S3-compatible object storage."
    ),
)
def serbia_apr_companies_raw_snapshot_s3(
    context: dg.AssetExecutionContext,
    serbia_apr_companies_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_apr_companies_snapshot(
        object_store=serbia_apr_companies_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "content_type": snapshot.content_type,
            "downloaded": snapshot.downloaded,
            "manifest_key": snapshot.manifest_key,
            "object_key": snapshot.object_key,
            "record_count": snapshot.record_count,
            "retrieved_at": snapshot.retrieved_at,
            "s3_bucket": tables.S3_BUCKET,
            "sha256": snapshot.sha256,
            "size_bytes": snapshot.size_bytes,
            "snapshot_date": snapshot.snapshot_date,
            "source_url": tables.SOURCE_URL,
        }
    )


defs = dg.Definitions(
    assets=[serbia_apr_companies_raw_snapshot_s3],
    resources={
        "serbia_apr_companies_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        )
    },
)
