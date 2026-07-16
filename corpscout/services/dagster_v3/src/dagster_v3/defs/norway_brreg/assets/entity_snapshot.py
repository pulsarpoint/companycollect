from __future__ import annotations

import gzip
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import dagster as dg
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_aws.s3 import S3Resource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg import entity_records
from dagster_v3.defs.norway_brreg.constants import (
    GROUP_NAME,
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg.entity_parquet import entity_record_parquet_row
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource

ENTRIES_SNAPSHOT_RAW_OBJECT_KEY = "norway_brreg/entities/raw/snapshot/entities.json.gz"
ENTRIES_SNAPSHOT_CSV_RAW_OBJECT_KEY = (
    "norway_brreg/entities/raw/snapshot/entities.csv.gz"
)
ENTITY_SNAPSHOT_OBJECT_KEY = "norway_brreg/entities/snapshot/entities.parquet"
EMPTY_SNAPSHOT_ERROR_MESSAGE = "Norway Brreg entity snapshot produced no rows"
PARQUET_BATCH_SIZE = 10_000

ENTITY_ARROW_SCHEMA = pa.schema(
    [
        ("org_number", pa.string()),
        ("change_type", pa.string()),
        ("source_change_type", pa.string()),
        ("updated_at", pa.string()),
        ("update_id", pa.int64()),
        ("entity_url", pa.string()),
        ("entity_json", pa.string()),
        ("raw_update_json", pa.string()),
    ]
)


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
    name="norway_brreg_entries_snapshot_csv_raw_s3",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "csv", "gzip", "brreg"},
    description=(
        "Backs up the BRREG entity CSV snapshot containing bulk-only contact fields, "
        "including epostadresse, to S3."
    ),
)
def norway_brreg_entries_snapshot_csv_raw_s3(
    context,
    norway_brreg_api: NorwayBrregApiResource,
    s3: S3Resource,
) -> dg.MaterializeResult:
    metadata = norway_brreg_api.entries_snapshot_csv(
        s3=s3,
        bucket=NORWAY_BRREG_ENTITY_BUCKET,
        key=entries_snapshot_csv_raw_object_key(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="norway_brreg_entities_snapshot_s3",
    deps=[dg.AssetKey("norway_brreg_entries_snapshot_raw_s3")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
    description=(
        "Converts the raw Norway Brreg full entity snapshot JSON gzip archive directly "
        "to normalized entity parquet on S3."
    ),
)
def norway_brreg_entities_snapshot_s3(
    context,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    parquet_key = entity_snapshot_object_key()
    context.log.info(
        "Converting Norway Brreg raw entity snapshot to parquet: bucket=%s raw_key=%s parquet_key=%s",
        NORWAY_BRREG_ENTITY_BUCKET,
        entries_snapshot_raw_object_key(),
        parquet_key,
    )
    with TemporaryDirectory() as tmp_dir:
        raw_path = Path(tmp_dir) / "entities.json.gz"
        parquet_path = Path(tmp_dir) / "entities.parquet"
        object_store.download_file(
            entries_snapshot_raw_object_key(),
            raw_path,
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )
        row_count = _write_entries_snapshot_parquet(
            raw_path=raw_path,
            parquet_path=parquet_path,
            log=context.log.info,
        )
        if row_count == 0:
            raise ValueError(EMPTY_SNAPSHOT_ERROR_MESSAGE)

        object_store.ensure_bucket(NORWAY_BRREG_ENTITY_BUCKET)
        object_store.upload_file(
            parquet_key, parquet_path, bucket=NORWAY_BRREG_ENTITY_BUCKET
        )
        parquet_size_bytes = parquet_path.stat().st_size

    context.log.info(
        "Completed Norway Brreg entity snapshot parquet write: bucket=%s key=%s rows=%d bytes=%d",
        NORWAY_BRREG_ENTITY_BUCKET,
        parquet_key,
        row_count,
        parquet_size_bytes,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
            "s3_key": parquet_key,
            "parquet_size_bytes": parquet_size_bytes,
            "converted": True,
        }
    )


def entries_snapshot_raw_object_key() -> str:
    return ENTRIES_SNAPSHOT_RAW_OBJECT_KEY


def entries_snapshot_csv_raw_object_key() -> str:
    return ENTRIES_SNAPSHOT_CSV_RAW_OBJECT_KEY


def entity_snapshot_object_key() -> str:
    return ENTITY_SNAPSHOT_OBJECT_KEY


def _write_entries_snapshot_parquet(
    *,
    raw_path: Path,
    parquet_path: Path,
    log: Callable[..., None] | None,
) -> int:
    row_count = 0
    row_batch: list[dict[str, Any]] = []
    writer: pq.ParquetWriter | None = None
    try:
        with gzip.open(raw_path, "rb") as source_file:
            for entity in ijson.items(source_file, "item"):
                if not isinstance(entity, dict):
                    continue
                row_count += 1
                row_batch.append(
                    entity_record_parquet_row(
                        entity_records.snapshot_entity_record(entity)
                    )
                )
                if log is not None and row_count % 1000 == 0:
                    log("Parsed Norway Brreg entity snapshot rows: rows=%s", row_count)
                if len(row_batch) >= PARQUET_BATCH_SIZE:
                    writer = _write_parquet_batch(
                        writer=writer,
                        parquet_path=parquet_path,
                        rows=row_batch,
                    )
                    row_batch = []
        if row_batch:
            writer = _write_parquet_batch(
                writer=writer,
                parquet_path=parquet_path,
                rows=row_batch,
            )
    finally:
        if writer is not None:
            writer.close()
    return row_count


def _write_parquet_batch(
    *,
    writer: pq.ParquetWriter | None,
    parquet_path: Path,
    rows: list[dict[str, Any]],
) -> pq.ParquetWriter:
    table = pa.Table.from_pylist(rows, schema=ENTITY_ARROW_SCHEMA)
    if writer is None:
        writer = pq.ParquetWriter(parquet_path, ENTITY_ARROW_SCHEMA)
    writer.write_table(table)
    return writer
