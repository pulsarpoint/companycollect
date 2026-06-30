from __future__ import annotations

import hashlib
from datetime import date

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg.entity_parquet import entity_records_parquet_bytes
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    GROUP_NAME,
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource

NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
)


@dg.asset(
    name="norway_brreg_entity_updates_s3",
    group_name=GROUP_NAME,
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    kinds={"python", "s3", "parquet", "brreg"},
    description="Downloads Norway Brreg entity updates for one day and stores uniform entity records as parquet.",
)
def norway_brreg_entity_updates_s3(
    context,
    norway_brreg_api: NorwayBrregApiResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    updated_at_start, updated_at_end = entity_update_window(partition_date)
    context.log.info(
        "Loading Norway Brreg entity updates partition %s: updated_at=%s..%s",
        partition_date,
        updated_at_start,
        updated_at_end,
    )

    records = list(
        norway_brreg_api.iter_updated_entities(
            start=updated_at_start,
            end=updated_at_end,
        )
    )
    parquet_body = entity_records_parquet_bytes(records, allow_empty=True)
    parquet_sha256 = hashlib.sha256(parquet_body).hexdigest()
    s3_key = entity_updates_object_key(partition_date)

    context.log.info(
        "Writing Norway Brreg entity updates parquet: bucket=%s key=%s rows=%d",
        NORWAY_BRREG_ENTITY_BUCKET,
        s3_key,
        len(records),
    )
    object_store.ensure_bucket(NORWAY_BRREG_ENTITY_BUCKET)
    object_store.write_bytes(s3_key, parquet_body, bucket=NORWAY_BRREG_ENTITY_BUCKET)

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "updated_at_start": updated_at_start,
            "updated_at_end": updated_at_end,
            "row_count": len(records),
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
            "s3_key": s3_key,
            "parquet_sha256": parquet_sha256,
            "parquet_size_bytes": len(parquet_body),
        }
    )


def entity_update_window(partition_key: str) -> tuple[str, str]:
    partition_date = date.fromisoformat(partition_key)
    return (
        f"{partition_date.isoformat()}T00:00:00.000Z",
        f"{partition_date.isoformat()}T23:59:59.999Z",
    )


def entity_updates_object_key(partition_date: str) -> str:
    return f"norway_brreg/entities/updates/date={partition_date}/entities.parquet"
