from __future__ import annotations

from io import BytesIO
from typing import Any

import dagster as dg
import polars as pl
from pydantic import PrivateAttr

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    NORWAY_BRREG_ENTITY_BUCKET,
)


class NorwayBrregFinancialParquetStorageResource(dg.ConfigurableResource):
    _object_store: Any = PrivateAttr()

    def __init__(self, object_store: object | None = None, **data: object) -> None:
        super().__init__(**data)
        self._object_store = object_store or ObjectStoreResource()

    @property
    def object_store(self) -> Any:
        return self._object_store

    def raw_fetch_exists(self, org_number: str, accounts_year: str) -> bool:
        return self.object_store.exists(
            financial_raw_fetch_object_key(org_number, accounts_year),
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )

    def write_raw_fetch(
        self,
        org_number: str,
        accounts_year: str,
        frame: pl.DataFrame,
        *,
        overwrite: bool,
    ) -> str:
        key = financial_raw_fetch_object_key(org_number, accounts_year)
        if not overwrite and self.object_store.exists(
            key, bucket=NORWAY_BRREG_ENTITY_BUCKET
        ):
            return key
        return self._write_frame(key, frame)

    def read_raw_fetch(self, org_number: str, accounts_year: str) -> pl.DataFrame:
        return self._read_frame(financial_raw_fetch_object_key(org_number, accounts_year))

    def write_snapshot_fetches(self, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_fetches_snapshot_object_key(), frame)

    def write_update_fetches(self, partition_date: str, frame: pl.DataFrame) -> str:
        return self._write_frame(
            financial_fetches_update_object_key(partition_date), frame
        )

    def read_snapshot_fetches(self) -> pl.DataFrame:
        return self._read_frame(financial_fetches_snapshot_object_key())

    def read_update_fetches(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(financial_fetches_update_object_key(partition_date))

    def write_update_financial_candidates(
        self,
        partition_date: str,
        frame: pl.DataFrame,
    ) -> str:
        return self._write_frame(
            financial_update_candidates_object_key(partition_date),
            frame,
        )

    def read_update_financial_candidates(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(financial_update_candidates_object_key(partition_date))

    def write_snapshot_statements(self, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_statements_snapshot_object_key(), frame)

    def write_update_statements(self, partition_date: str, frame: pl.DataFrame) -> str:
        return self._write_frame(
            financial_statements_update_object_key(partition_date), frame
        )

    def read_snapshot_statements(self) -> pl.DataFrame:
        return self._read_frame(financial_statements_snapshot_object_key())

    def read_update_statements(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(financial_statements_update_object_key(partition_date))

    def write_snapshot_usd_statements(self, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_statements_usd_snapshot_object_key(), frame)

    def write_update_usd_statements(
        self, partition_date: str, frame: pl.DataFrame
    ) -> str:
        return self._write_frame(
            financial_statements_usd_update_object_key(partition_date),
            frame,
        )

    def read_snapshot_usd_statements(self) -> pl.DataFrame:
        return self._read_frame(financial_statements_usd_snapshot_object_key())

    def read_update_usd_statements(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(
            financial_statements_usd_update_object_key(partition_date)
        )

    def _write_frame(self, key: str, frame: pl.DataFrame) -> str:
        self.object_store.ensure_bucket(NORWAY_BRREG_ENTITY_BUCKET)
        self.object_store.write_bytes(
            key,
            _parquet_bytes(frame),
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )
        return key

    def _read_frame(self, key: str) -> pl.DataFrame:
        return _read_parquet_bytes(
            self.object_store.read_bytes(
                key,
                bucket=NORWAY_BRREG_ENTITY_BUCKET,
            )
        )


def financial_fetches_snapshot_object_key() -> str:
    return "norway_brreg/financial/fetches/snapshot/financial_fetches.parquet"


def financial_fetches_update_object_key(partition_date: str) -> str:
    return f"norway_brreg/financial/fetches/updates/date={partition_date}/financial_fetches.parquet"


def financial_raw_fetch_object_key(org_number: str, accounts_year: str) -> str:
    return (
        f"norway_brreg/financial/raw_fetches/org={org_number}/"
        f"year={accounts_year}/financial_fetch.parquet"
    )


def financial_update_candidates_object_key(partition_date: str) -> str:
    return (
        "norway_brreg/financial/update_candidates/"
        f"date={partition_date}/financial_update_candidates.parquet"
    )


def financial_statements_snapshot_object_key() -> str:
    return "norway_brreg/financial/statements/snapshot/financial_statements.parquet"


def financial_statements_update_object_key(partition_date: str) -> str:
    return (
        "norway_brreg/financial/statements/updates/"
        f"date={partition_date}/financial_statements.parquet"
    )


def financial_statements_usd_snapshot_object_key() -> str:
    return "norway_brreg/financial/statements_usd/snapshot/financial_statements.parquet"


def financial_statements_usd_update_object_key(partition_date: str) -> str:
    return (
        "norway_brreg/financial/statements_usd/updates/"
        f"date={partition_date}/financial_statements.parquet"
    )


def _read_parquet_bytes(body: bytes) -> pl.DataFrame:
    return pl.read_parquet(BytesIO(body))


def _parquet_bytes(frame: pl.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()
