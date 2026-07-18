from __future__ import annotations

import json
from collections.abc import Callable
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

FINANCIAL_RESPONSE_PREFIX = "norway_brreg/financial/responses/"
FINANCIAL_RESPONSE_INDEX_PREFIX = "norway_brreg/financial/response_index/"
ANNUAL_ACCOUNT_PDF_PREFIX = "norway_brreg/annual_accounts/pdfs/"
ANNUAL_ACCOUNT_DOCUMENT_PREFIX = "norway_brreg/annual_accounts/documents/"
READ_PROGRESS_INTERVAL = 100


class NorwayBrregFinancialParquetStorageResource(dg.ConfigurableResource):
    _object_store: Any = PrivateAttr()

    def __init__(self, object_store: object | None = None, **data: object) -> None:
        super().__init__(**data)
        self._object_store = object_store or ObjectStoreResource()

    @property
    def object_store(self) -> Any:
        return self._object_store

    def response_exists(self, key: str) -> bool:
        return self.object_store.exists(
            key,
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
        )

    def write_response(self, key: str, body: bytes) -> str:
        if self.response_exists(key):
            existing = self.read_response(key)
            if existing != body:
                raise RuntimeError(
                    "Refusing to overwrite an existing Norway Brreg financial "
                    f"response with different bytes: {key}"
                )
            return key
        self.object_store.ensure_bucket(NORWAY_BRREG_FINANCIAL_BUCKET)
        self.object_store.write_bytes(
            key,
            body,
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
        )
        return key

    def read_response(self, key: str) -> bytes:
        return self.object_store.read_bytes(
            key,
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
        )

    def write_json_object(self, key: str, value: dict[str, Any]) -> str:
        self.object_store.ensure_bucket(NORWAY_BRREG_FINANCIAL_BUCKET)
        body = _json_object_bytes(value)
        self.object_store.write_bytes(
            key,
            body,
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
        )
        return key

    def annual_account_document_exists(
        self,
        *,
        filing_year: int,
        chunk_key: str,
        org_number: str,
    ) -> bool:
        return self.response_exists(
            annual_account_document_object_key(
                filing_year,
                chunk_key,
                org_number,
            )
        )

    def annual_account_pdf_exists(
        self,
        *,
        filing_year: int,
        chunk_key: str,
        org_number: str,
    ) -> bool:
        return self.response_exists(
            annual_account_pdf_object_key(
                filing_year,
                chunk_key,
                org_number,
            )
        )

    def write_annual_account_pdf(
        self,
        *,
        filing_year: int,
        chunk_key: str,
        org_number: str,
        body: bytes,
    ) -> str:
        if not body.startswith(b"%PDF-"):
            raise RuntimeError(
                "Norway BRREG annual-account object is not a PDF: "
                f"org={org_number} year={filing_year}"
            )
        return self.write_response(
            annual_account_pdf_object_key(
                filing_year,
                chunk_key,
                org_number,
            ),
            body,
        )

    def read_annual_account_pdf(
        self,
        *,
        filing_year: int,
        chunk_key: str,
        org_number: str,
    ) -> bytes:
        return self.read_response(
            annual_account_pdf_object_key(
                filing_year,
                chunk_key,
                org_number,
            )
        )

    def delete_annual_account_pdfs(self, object_keys: list[str]) -> int:
        return self.object_store.delete_keys(
            object_keys,
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
        )

    def write_annual_account_pdf_catalog(
        self,
        *,
        filing_year: int,
        chunk_key: str,
        frame: pl.DataFrame,
    ) -> str:
        return self._write_frame(
            annual_account_pdf_catalog_object_key(filing_year, chunk_key),
            frame,
        )

    def read_annual_account_pdf_catalog(
        self,
        *,
        filing_year: int,
        chunk_key: str,
    ) -> pl.DataFrame:
        return self._read_frame(
            annual_account_pdf_catalog_object_key(filing_year, chunk_key)
        )

    def write_annual_account_document(
        self,
        *,
        filing_year: int,
        chunk_key: str,
        org_number: str,
        document: dict[str, Any],
    ) -> tuple[str, int]:
        key = annual_account_document_object_key(
            filing_year,
            chunk_key,
            org_number,
        )
        body = _json_object_bytes(document)
        self.write_response(key, body)
        return key, len(body)

    def read_annual_account_document(
        self,
        *,
        filing_year: int,
        chunk_key: str,
        org_number: str,
    ) -> dict[str, Any]:
        return self.read_json_object(
            annual_account_document_object_key(
                filing_year,
                chunk_key,
                org_number,
            )
        )

    def list_annual_account_document_keys(
        self,
        *,
        filing_year: int,
        chunk_key: str,
    ) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                annual_account_document_partition_prefix(filing_year, chunk_key),
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
            if key.endswith("/document.json")
        )

    def read_json_object(self, key: str) -> dict[str, Any]:
        value = json.loads(self.read_response(key))
        if not isinstance(value, dict):
            raise RuntimeError(
                f"Norway Brreg financial JSON object is not an object: {key}"
            )
        return value

    def list_response_checkpoint_keys(self, partition_prefix: str) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                financial_response_checkpoint_prefix(partition_prefix),
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
            if key.endswith(".json")
        )

    def read_response_records(self, partition_prefix: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for key in self.list_response_checkpoint_keys(partition_prefix):
            checkpoint = self.read_json_object(key)
            checkpoint_records = checkpoint.get("records")
            if not isinstance(checkpoint_records, list):
                raise RuntimeError(
                    "Norway Brreg financial response checkpoint is missing records: "
                    f"{key}"
                )
            for record in checkpoint_records:
                if not isinstance(record, dict):
                    raise RuntimeError(
                        "Norway Brreg financial response checkpoint contains a "
                        f"non-object record: {key}"
                    )
                records.append(record)
        return records

    def list_partition_response_keys(self, partition_prefix: str) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                partition_prefix,
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
            if key.endswith("/response.json")
        )

    def write_bootstrap_response_index(
        self,
        bucket_key: str,
        frame: pl.DataFrame,
    ) -> str:
        return self._write_frame(
            financial_bootstrap_response_index_object_key(bucket_key),
            frame,
        )

    def read_bootstrap_response_index(self, bucket_key: str) -> pl.DataFrame:
        return self._read_frame(
            financial_bootstrap_response_index_object_key(bucket_key)
        )

    def write_update_response_index(
        self,
        partition_date: str,
        frame: pl.DataFrame,
    ) -> str:
        return self._write_frame(
            financial_update_response_index_object_key(partition_date),
            frame,
        )

    def read_update_response_index(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(
            financial_update_response_index_object_key(partition_date)
        )

    def list_all_response_index_keys(self) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                FINANCIAL_RESPONSE_INDEX_PREFIX,
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
            if key.endswith("/responses.parquet")
        )

    def read_consolidated_historical_response_index(
        self,
        *,
        log: Callable[..., object] | None = None,
    ) -> pl.DataFrame:
        """All distinct response-index outcomes across bootstrap and updates."""
        response_index_keys = self.list_all_response_index_keys()
        _log(
            log,
            "Preparing Norway Brreg consolidated historical financial response "
            "index read: file_count=%d",
            len(response_index_keys),
        )
        frames = self._read_frames_with_progress(
            response_index_keys,
            label="financial response index parquet files",
            log=log,
        )
        if not frames:
            combined = pl.DataFrame(schema=financial_fetches_parquet_schema())
        else:
            combined = pl.concat(frames, how="vertical_relaxed")
        _log(
            log,
            "Deduplicating Norway Brreg historical financial response index rows: "
            "input_rows=%d",
            combined.height,
        )
        if combined.is_empty():
            consolidated = combined
        else:
            consolidated = combined.unique(
                subset=["org_number", "fetch_status", "source_object_key"],
                keep="last",
                maintain_order=True,
            )
        _log(
            log,
            "Completed Norway Brreg consolidated historical financial response "
            "index read: input_rows=%d output_rows=%d file_count=%d",
            combined.height,
            consolidated.height,
            len(response_index_keys),
        )
        return consolidated

    def write_snapshot_statements(
        self,
        frame: pl.DataFrame,
        *,
        log: Callable[..., object] | None = None,
    ) -> str:
        return self._write_frame(
            financial_statements_snapshot_object_key(),
            frame,
            log=log,
        )

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

    def _write_frame(
        self,
        key: str,
        frame: pl.DataFrame,
        *,
        log: Callable[..., object] | None = None,
    ) -> str:
        self.object_store.ensure_bucket(NORWAY_BRREG_FINANCIAL_BUCKET)
        _log(
            log,
            "Serializing Norway Brreg financial parquet frame: key=%s rows=%d columns=%d",
            key,
            frame.height,
            len(frame.columns),
        )
        body = _parquet_bytes(frame)
        _log(
            log,
            "Uploading Norway Brreg financial parquet frame: key=%s size_bytes=%d",
            key,
            len(body),
        )
        self.object_store.write_bytes(
            key,
            body,
            bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
        )
        _log(
            log,
            "Completed Norway Brreg financial parquet write: key=%s rows=%d columns=%d",
            key,
            frame.height,
            len(frame.columns),
        )
        return key

    def _read_frame(self, key: str) -> pl.DataFrame:
        return _read_parquet_bytes(
            self.object_store.read_bytes(
                key,
                bucket=NORWAY_BRREG_FINANCIAL_BUCKET,
            )
        )

    def _read_frames_with_progress(
        self,
        keys: list[str],
        *,
        label: str,
        log: Callable[..., object] | None,
    ) -> list[pl.DataFrame]:
        if not keys:
            _log(log, "No Norway Brreg %s found", label)
            return []

        total_files = len(keys)
        _log(
            log,
            "Reading Norway Brreg %s: total_files=%d",
            label,
            total_files,
        )
        frames: list[pl.DataFrame] = []
        for index, key in enumerate(keys, start=1):
            frames.append(self._read_frame(key))
            if _should_log_progress(index, total_files, READ_PROGRESS_INTERVAL):
                _log(
                    log,
                    "Read Norway Brreg %s: files_read=%d total_files=%d latest_key=%s",
                    label,
                    index,
                    total_files,
                    key,
                )
        _log(
            log,
            "Completed reading Norway Brreg %s: total_files=%d",
            label,
            total_files,
        )
        return frames


def financial_bootstrap_response_partition_prefix(bucket_key: str) -> str:
    return (
        f"{FINANCIAL_RESPONSE_PREFIX}bootstrap/"
        f"bucket={_safe_key_component(bucket_key)}/"
    )


def annual_account_document_object_key(
    filing_year: int,
    chunk_key: str,
    org_number: str,
) -> str:
    if filing_year < 1900 or filing_year > 9999:
        raise ValueError(f"Invalid Norway annual-account filing year: {filing_year}")
    return (
        f"{annual_account_document_partition_prefix(filing_year, chunk_key)}"
        f"org={_safe_key_component(org_number)}/document.json"
    )


def annual_account_document_partition_prefix(
    filing_year: int,
    chunk_key: str,
) -> str:
    if filing_year < 1900 or filing_year > 9999:
        raise ValueError(f"Invalid Norway annual-account filing year: {filing_year}")
    return (
        f"{ANNUAL_ACCOUNT_DOCUMENT_PREFIX}year={filing_year}/"
        f"chunk={_safe_key_component(chunk_key)}/"
    )


def annual_account_pdf_partition_prefix(
    filing_year: int,
    chunk_key: str,
) -> str:
    if filing_year < 1900 or filing_year > 9999:
        raise ValueError(f"Invalid Norway annual-account filing year: {filing_year}")
    return (
        f"{ANNUAL_ACCOUNT_PDF_PREFIX}year={filing_year}/"
        f"chunk={_safe_key_component(chunk_key)}/"
    )


def annual_account_pdf_object_key(
    filing_year: int,
    chunk_key: str,
    org_number: str,
) -> str:
    return (
        f"{annual_account_pdf_partition_prefix(filing_year, chunk_key)}"
        f"org={_safe_key_component(org_number)}/annual-account.pdf"
    )


def annual_account_pdf_catalog_object_key(
    filing_year: int,
    chunk_key: str,
) -> str:
    return (
        f"{annual_account_pdf_partition_prefix(filing_year, chunk_key)}catalog.parquet"
    )


def financial_update_response_partition_prefix(partition_date: str) -> str:
    return (
        f"{FINANCIAL_RESPONSE_PREFIX}updates/"
        f"date={_safe_key_component(partition_date)}/"
    )


def financial_response_object_key(
    partition_prefix: str,
    org_number: str,
) -> str:
    return f"{partition_prefix}org={_safe_key_component(org_number)}/response.json"


def financial_response_checkpoint_prefix(partition_prefix: str) -> str:
    return f"{partition_prefix}checkpoints/"


def financial_response_checkpoint_object_key(
    partition_prefix: str,
    source_run_id: str,
    batch_index: int,
) -> str:
    if batch_index < 0:
        raise ValueError("Norway response batch_index must not be negative")
    return (
        f"{financial_response_checkpoint_prefix(partition_prefix)}"
        f"run={_safe_key_component(source_run_id)}/batch={batch_index:06d}.json"
    )


def financial_response_success_object_key(partition_prefix: str) -> str:
    return f"{partition_prefix}_SUCCESS.json"


def financial_bootstrap_response_index_object_key(bucket_key: str) -> str:
    return (
        f"{FINANCIAL_RESPONSE_INDEX_PREFIX}bootstrap/"
        f"bucket={_safe_key_component(bucket_key)}/responses.parquet"
    )


def financial_update_response_index_object_key(partition_date: str) -> str:
    return (
        f"{FINANCIAL_RESPONSE_INDEX_PREFIX}updates/"
        f"date={_safe_key_component(partition_date)}/responses.parquet"
    )


def _safe_key_component(value: str) -> str:
    component = str(value).strip()
    if component == "":
        raise RuntimeError("Norway financial S3 key component is empty")
    return quote(component, safe="")


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


def _json_object_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _should_log_progress(index: int, total: int, interval: int) -> bool:
    return index == 1 or index == total or index % interval == 0


def _log(log: Callable[..., object] | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)
