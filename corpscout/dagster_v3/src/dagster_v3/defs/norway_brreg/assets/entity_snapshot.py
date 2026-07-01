from __future__ import annotations

import hashlib

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg.constants import (
    GROUP_NAME,
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg.entity_parquet import entity_records_parquet_bytes
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource

ENTITY_SNAPSHOT_OBJECT_KEY = "norway_brreg/entities/snapshot/entities.parquet"


@dg.asset(
    name="norway_brreg_entities_snapshot_s3",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
    description="Downloads the full Norway Brreg entity snapshot and stores uniform entity records as parquet.",
)
def norway_brreg_entities_snapshot_s3(
    context,
    norway_brreg_api: NorwayBrregApiResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    s3_key = entity_snapshot_object_key()
    if object_store.exists(s3_key, bucket=NORWAY_BRREG_ENTITY_BUCKET):
        context.log.info(
            "Reusing existing Norway Brreg full entity snapshot parquet: bucket=%s key=%s",
            NORWAY_BRREG_ENTITY_BUCKET,
            s3_key,
        )
        return dg.MaterializeResult(
            metadata={
                "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
                "s3_key": s3_key,
                "downloaded": False,
                "reused_existing_snapshot": True,
            }
        )

    context.log.info("Loading Norway Brreg full entity snapshot")
    records = list(norway_brreg_api.iter_all_entities(log=context.log.info))
    context.log.info("Loaded Norway Brreg full entity snapshot: rows=%d", len(records))
    parquet_body = entity_records_parquet_bytes(
        records,
        empty_error_message="Norway Brreg entity snapshot produced no rows",
    )
    parquet_sha256 = hashlib.sha256(parquet_body).hexdigest()

    context.log.info(
        "Writing Norway Brreg entity snapshot parquet: bucket=%s key=%s rows=%d",
        NORWAY_BRREG_ENTITY_BUCKET,
        s3_key,
        len(records),
    )
    object_store.ensure_bucket(NORWAY_BRREG_ENTITY_BUCKET)
    object_store.write_bytes(s3_key, parquet_body, bucket=NORWAY_BRREG_ENTITY_BUCKET)
    context.log.info(
        "Completed Norway Brreg entity snapshot parquet write: bucket=%s key=%s bytes=%d",
        NORWAY_BRREG_ENTITY_BUCKET,
        s3_key,
        len(parquet_body),
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": len(records),
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
            "s3_key": s3_key,
            "parquet_sha256": parquet_sha256,
            "parquet_size_bytes": len(parquet_body),
            "downloaded": True,
            "reused_existing_snapshot": False,
        }
    )


def entity_snapshot_object_key() -> str:
    return ENTITY_SNAPSHOT_OBJECT_KEY
