from __future__ import annotations

import gzip
import hashlib
from typing import Any

import dagster as dg
import ijson
from dagster_aws.s3 import S3Resource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg.constants import (
    GROUP_NAME,
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg import entity_records
from dagster_v3.defs.norway_brreg.entity_parquet import entity_records_parquet_bytes
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource

ENTRIES_SNAPSHOT_RAW_OBJECT_KEY = "norway_brreg/entities/raw/snapshot/entities.json.gz"
ENTITY_SNAPSHOT_OBJECT_KEY = "norway_brreg/entities/snapshot/entities.parquet"


@dg.asset(
    name="norway_brreg_entries_snapshot_raw_s3",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gzip", "brreg"},
    description="Backs up the raw Norway Brreg full entity snapshot gzip archive to S3.",
)
def norway_brreg_entries_snapshot_raw_s3(
    context,
    norway_brreg_api: NorwayBrregApiResource,
    s3: S3Resource,
) -> dg.MaterializeResult:
    metadata = norway_brreg_api.entries_snapshot(
        s3=s3,
        bucket=NORWAY_BRREG_ENTITY_BUCKET,
        key=entries_snapshot_raw_object_key(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="norway_brreg_entities_snapshot_s3",
    deps=[dg.AssetKey("norway_brreg_entries_snapshot_raw_s3")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
    description="Parses the raw Norway Brreg entity snapshot archive and stores uniform entity records as parquet.",
)
def norway_brreg_entities_snapshot_s3(
    context,
    s3: S3Resource,
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

    context.log.info(
        "Loading Norway Brreg raw entity snapshot from S3: bucket=%s key=%s",
        NORWAY_BRREG_ENTITY_BUCKET,
        entries_snapshot_raw_object_key(),
    )
    raw_object = s3.get_client().get_object(
        Bucket=NORWAY_BRREG_ENTITY_BUCKET,
        Key=entries_snapshot_raw_object_key(),
    )
    records = list(
        _entity_snapshot_records_from_raw_body(
            raw_object["Body"],
            log=context.log.info,
        )
    )
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


def entries_snapshot_raw_object_key() -> str:
    return ENTRIES_SNAPSHOT_RAW_OBJECT_KEY


def entity_snapshot_object_key() -> str:
    return ENTITY_SNAPSHOT_OBJECT_KEY


def _entity_snapshot_records_from_raw_body(raw_body: Any, *, log: Any) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with gzip.GzipFile(fileobj=raw_body) as gzip_file:
        for row_count, entity in enumerate(ijson.items(gzip_file, "item"), start=1):
            if not isinstance(entity, dict):
                continue
            if row_count % 1000 == 0:
                log("Parsed Norway Brreg entity snapshot rows: rows=%s", row_count)
            records.append(entity_records.snapshot_entity_record(entity))
    return records
