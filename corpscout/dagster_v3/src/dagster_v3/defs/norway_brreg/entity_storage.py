from __future__ import annotations

from io import BytesIO
from typing import Any

import dagster as dg
import polars as pl
from pydantic import PrivateAttr

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    NORWAY_BRREG_ENTITY_BUCKET,
    entity_snapshot_object_key,
)
from dagster_v3.defs.norway_brreg.assets.entity_updates import entity_updates_object_key

ENTITY_NORMALIZED_TABLE_NO_COMPANIES = "no_companies"
ENTITY_NORMALIZED_TABLE_NO_WEBSITES = "no_websites"
ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES = "no_industries"
ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS = "affected_orgs"
ENTITY_NORMALIZED_TABLE_REMOVED_ORGS = "removed_orgs"

ENTITY_NORMALIZED_TABLES = (
    ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
    ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
    ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
)


class NorwayBrregEntityParquetStorageResource(dg.ConfigurableResource):
    _object_store: Any = PrivateAttr()

    def __init__(self, object_store: object | None = None, **data: object) -> None:
        super().__init__(**data)
        self._object_store = object_store or ObjectStoreResource()

    @property
    def object_store(self) -> Any:
        return self._object_store

    def read_raw_snapshot_frame(self) -> pl.DataFrame:
        return _read_parquet_bytes(
            self.object_store.read_bytes(
                entity_snapshot_object_key(),
                bucket=NORWAY_BRREG_ENTITY_BUCKET,
            )
        )

    def read_raw_update_frame(self, partition_date: str) -> pl.DataFrame:
        return _read_parquet_bytes(
            self.object_store.read_bytes(
                entity_updates_object_key(partition_date),
                bucket=NORWAY_BRREG_ENTITY_BUCKET,
            )
        )

    def write_snapshot_table(self, run_id: str, table_name: str, frame: pl.DataFrame) -> str:
        key = normalized_snapshot_table_object_key(run_id, table_name)
        self.object_store.ensure_bucket(NORWAY_BRREG_ENTITY_BUCKET)
        self.object_store.write_bytes(
            key,
            _parquet_bytes(frame),
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )
        return key

    def write_update_table(
        self,
        partition_date: str,
        table_name: str,
        frame: pl.DataFrame,
    ) -> str:
        key = normalized_update_table_object_key(partition_date, table_name)
        self.object_store.ensure_bucket(NORWAY_BRREG_ENTITY_BUCKET)
        self.object_store.write_bytes(
            key,
            _parquet_bytes(frame),
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )
        return key

    def read_normalized_update_table(
        self,
        partition_date: str,
        table_name: str,
    ) -> pl.DataFrame:
        return _read_parquet_bytes(
            self.object_store.read_bytes(
                normalized_update_table_object_key(partition_date, table_name),
                bucket=NORWAY_BRREG_ENTITY_BUCKET,
            )
        )

    def read_normalized_snapshot_table(
        self,
        run_id: str,
        table_name: str,
    ) -> pl.DataFrame:
        return _read_parquet_bytes(
            self.object_store.read_bytes(
                normalized_snapshot_table_object_key(run_id, table_name),
                bucket=NORWAY_BRREG_ENTITY_BUCKET,
            )
        )


def normalized_snapshot_table_object_key(run_id: str, table_name: str) -> str:
    _validate_normalized_table_name(table_name)
    return f"norway_brreg/entities/normalized/snapshot/run_id={run_id}/{table_name}.parquet"


def normalized_update_table_object_key(partition_date: str, table_name: str) -> str:
    _validate_normalized_table_name(table_name)
    return f"norway_brreg/entities/normalized/updates/date={partition_date}/{table_name}.parquet"


def _validate_normalized_table_name(table_name: str) -> None:
    if table_name not in ENTITY_NORMALIZED_TABLES:
        raise ValueError(f"Unsupported Norway Brreg normalized entity table: {table_name}")


def _read_parquet_bytes(body: bytes) -> pl.DataFrame:
    return pl.read_parquet(BytesIO(body))


def _parquet_bytes(frame: pl.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()
