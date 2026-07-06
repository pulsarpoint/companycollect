from __future__ import annotations

import json
from io import BytesIO
from typing import Any
from urllib.parse import quote

import dagster as dg
import polars as pl
from pydantic import PrivateAttr

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg_financial.constants import (
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_fetches import (
    financial_fetches_parquet_schema,
)

RAW_FETCH_PREFIX = "norway_brreg/financial/raw_fetches/"
BOOTSTRAP_FETCH_PREFIX = "norway_brreg/financial/bootstrap/fetches/"
LEGACY_BOOTSTRAP_RAW_REPORT_PREFIX = "norway_brreg/finance/raw_reports/"


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
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
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
            key, bucket=NORWAY_BRREG_FINANCIAL_BUCKET
        ):
            return key
        return self._write_frame(key, frame)

    def read_raw_fetch(self, org_number: str, accounts_year: str) -> pl.DataFrame:
        return self._read_frame(financial_raw_fetch_object_key(org_number, accounts_year))

    def list_historical_raw_fetch_keys(self) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                RAW_FETCH_PREFIX,
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
            if key.endswith("/financial_fetch.parquet")
        )

    def read_historical_raw_fetches(self) -> pl.DataFrame:
        frames = [
            self._read_frame(key) for key in self.list_historical_raw_fetch_keys()
        ]
        if not frames:
            return pl.DataFrame(schema=financial_fetches_parquet_schema())
        return pl.concat(frames, how="vertical_relaxed")

    def read_consolidated_historical_fetches(self) -> pl.DataFrame:
        """Latest fetch row per org across bootstrap bucket chunks and the
        per-org raw_fetches layout written by the update assets."""
        frames = [self._read_frame(key) for key in self.list_all_bootstrap_chunk_keys()]
        frames.append(self.read_historical_raw_fetches())
        return latest_fetch_rows_per_org(pl.concat(frames, how="vertical_relaxed"))

    def list_all_bootstrap_chunk_keys(self) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                BOOTSTRAP_FETCH_PREFIX,
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
            if key.endswith(".parquet")
        )

    def list_bootstrap_chunk_keys(self, bucket_key: str) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                financial_bootstrap_bucket_prefix(bucket_key),
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
            if key.endswith(".parquet")
        )

    def read_bootstrap_bucket_fetches(self, bucket_key: str) -> pl.DataFrame:
        frames = [self._read_frame(key) for key in self.list_bootstrap_chunk_keys(bucket_key)]
        if not frames:
            return pl.DataFrame(schema=financial_fetches_parquet_schema())
        return pl.concat(frames, how="vertical_relaxed")

    def write_bootstrap_chunk(
        self,
        bucket_key: str,
        chunk_index: int,
        frame: pl.DataFrame,
    ) -> str:
        return self._write_frame(
            financial_bootstrap_chunk_object_key(bucket_key, chunk_index),
            frame,
        )

    def read_legacy_bootstrap_done_marker(self, org_number: str) -> dict[str, Any] | None:
        key = legacy_bootstrap_done_marker_key(org_number)
        if not self.object_store.exists(key, bucket=NORWAY_BRREG_FINANCIAL_BUCKET):
            return None
        marker = json.loads(
            self.object_store.read_bytes(key, bucket=NORWAY_BRREG_FINANCIAL_BUCKET)
        )
        if not isinstance(marker, dict):
            raise RuntimeError(
                f"Legacy Norway bootstrap done marker is not a JSON object: {key}"
            )
        return marker

    def read_legacy_bootstrap_raw_reports(
        self, raw_report_keys: list[str]
    ) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        for key in raw_report_keys:
            report = json.loads(
                self.object_store.read_bytes(key, bucket=NORWAY_BRREG_FINANCIAL_BUCKET)
            )
            if not isinstance(report, dict):
                raise RuntimeError(
                    f"Legacy Norway bootstrap raw report is not a JSON object: {key}"
                )
            reports.append(report)
        return reports

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
        self.object_store.ensure_bucket(NORWAY_BRREG_FINANCIAL_BUCKET)
        self.object_store.write_bytes(
            key,
            _parquet_bytes(frame),
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
        )
        return key

    def _read_frame(self, key: str) -> pl.DataFrame:
        return _read_parquet_bytes(
            self.object_store.read_bytes(
                key,
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
        )


def financial_bootstrap_bucket_prefix(bucket_key: str) -> str:
    return f"{BOOTSTRAP_FETCH_PREFIX}bucket={_safe_key_component(bucket_key)}/"


def financial_bootstrap_chunk_object_key(bucket_key: str, chunk_index: int) -> str:
    if chunk_index < 0:
        raise ValueError("Norway bootstrap chunk_index must not be negative")
    return f"{financial_bootstrap_bucket_prefix(bucket_key)}chunk={chunk_index:04d}.parquet"


def legacy_bootstrap_done_marker_key(org_number: str) -> str:
    return (
        f"{LEGACY_BOOTSTRAP_RAW_REPORT_PREFIX}"
        f"org={_safe_key_component(org_number)}/status/done.json"
    )


def latest_fetch_rows_per_org(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.sort("fetched_at").unique(subset=["org_number"], keep="last")


def _safe_key_component(value: str) -> str:
    component = str(value).strip()
    if component == "":
        raise RuntimeError("Norway financial S3 key component is empty")
    return quote(component, safe="")


def financial_fetches_snapshot_object_key() -> str:
    return "norway_brreg/financial/fetches/snapshot/financial_fetches.parquet"


def financial_fetches_update_object_key(partition_date: str) -> str:
    return f"norway_brreg/financial/fetches/updates/date={partition_date}/financial_fetches.parquet"


def financial_raw_fetch_object_key(org_number: str, accounts_year: str) -> str:
    return (
        f"{RAW_FETCH_PREFIX}org={org_number}/"
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
