from __future__ import annotations

import hashlib

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg.entity_parquet import entity_records_parquet_bytes
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource

GROUP_NAME = "norway_brreg"
NORWAY_BRREG_ENTITY_BUCKET = "source-norway-brreg"


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
    records = list(norway_brreg_api.iter_all_entities())
    parquet_body = entity_records_parquet_bytes(
        records,
        empty_error_message="Norway Brreg entity snapshot produced no rows",
    )
    parquet_sha256 = hashlib.sha256(parquet_body).hexdigest()
    s3_key = entity_snapshot_object_key(context.op_execution_context.run_id)

    context.log.info(
        "Writing Norway Brreg entity snapshot parquet: bucket=%s key=%s rows=%d",
        NORWAY_BRREG_ENTITY_BUCKET,
        s3_key,
        len(records),
    )
    object_store.ensure_bucket(NORWAY_BRREG_ENTITY_BUCKET)
    object_store.write_bytes(s3_key, parquet_body, bucket=NORWAY_BRREG_ENTITY_BUCKET)

    return dg.MaterializeResult(
        metadata={
            "row_count": len(records),
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
            "s3_key": s3_key,
            "parquet_sha256": parquet_sha256,
            "parquet_size_bytes": len(parquet_body),
        }
    )


def entity_snapshot_object_key(run_id: str) -> str:
    return f"norway_brreg/entities/snapshot/run_id={run_id}/entities.parquet"
