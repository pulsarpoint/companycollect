from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import UTC, datetime
import hashlib
from typing import Any

import polars as pl

from dagster_v3.defs.norway_brreg.resources import (
    BRREG_ANNUAL_ACCOUNTS_BASE_URL,
    NorwayBrregApiResource,
)
from dagster_v3.defs.norway_brreg_financial.annual_account_pdf import (
    extract_annual_account_pdf,
    tesseract_ocr_image,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    annual_account_pdf_object_key,
)

ANNUAL_ACCOUNT_DOWNLOAD_WORKERS = 1
ANNUAL_ACCOUNT_DOWNLOAD_BATCH_SIZE = 250
ANNUAL_ACCOUNT_DOCUMENT_WORKERS = 6
ANNUAL_ACCOUNT_PROGRESS_INTERVAL = 100

ANNUAL_ACCOUNT_PDF_CATALOG_SCHEMA = {
    "source_run_id": pl.Utf8,
    "org_number": pl.Utf8,
    "legal_name": pl.Utf8,
    "filing_year": pl.Int64,
    "source_url": pl.Utf8,
    "source_object_key": pl.Utf8,
    "source_payload_hash": pl.Utf8,
    "pdf_size_bytes": pl.Int64,
    "fetch_status": pl.Utf8,
    "capture_method": pl.Utf8,
    "fetched_at": pl.Utf8,
}


def download_annual_account_pdfs(
    *,
    candidates: Sequence[Mapping[str, Any]],
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
    api: NorwayBrregApiResource,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., None] | None,
) -> dict[str, Any]:
    """Stage only unprocessed BRREG PDFs in S3 and write their partition catalog."""
    unique_candidates = _unique_candidates(candidates)
    records: list[dict[str, Any]] = []
    _log(
        log,
        "Starting Norway BRREG annual-account PDF downloads: year=%d chunk=%s "
        "candidates=%d workers=%d batch_size=%d",
        filing_year,
        chunk_key,
        len(unique_candidates),
        ANNUAL_ACCOUNT_DOWNLOAD_WORKERS,
        ANNUAL_ACCOUNT_DOWNLOAD_BATCH_SIZE,
    )
    catalog_key: str | None = None
    for batch_start in range(
        0, len(unique_candidates), ANNUAL_ACCOUNT_DOWNLOAD_BATCH_SIZE
    ):
        batch = unique_candidates[
            batch_start : batch_start + ANNUAL_ACCOUNT_DOWNLOAD_BATCH_SIZE
        ]
        for candidate in batch:
            org_number = _string(candidate.get("org_number"))
            try:
                records.append(
                    _stage_pdf(
                        candidate,
                        filing_year=filing_year,
                        chunk_key=chunk_key,
                        source_run_id=source_run_id,
                        api=api,
                        storage=storage,
                    )
                )
            except Exception as error:
                raise RuntimeError(
                    "Norway BRREG annual-account PDF download failed: "
                    f"org={org_number} year={filing_year} chunk={chunk_key}"
                ) from error

        catalog_key = storage.write_annual_account_pdf_catalog(
            filing_year=filing_year,
            chunk_key=chunk_key,
            frame=_catalog_frame(records),
        )
        _log_download_progress(
            log,
            records=records,
            candidate_count=len(unique_candidates),
            filing_year=filing_year,
            chunk_key=chunk_key,
        )

    if catalog_key is None:
        catalog_key = storage.write_annual_account_pdf_catalog(
            filing_year=filing_year,
            chunk_key=chunk_key,
            frame=_catalog_frame(records),
        )

    counts = _download_counts(records)
    metadata: dict[str, Any] = {
        "candidate_count": len(unique_candidates),
        **counts,
        "catalog_key": catalog_key,
    }
    _log(
        log,
        "Completed Norway BRREG annual-account PDF downloads: year=%d chunk=%s "
        "metadata=%s",
        filing_year,
        chunk_key,
        metadata,
    )
    return metadata


def materialize_annual_account_documents(
    *,
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., None] | None,
) -> dict[str, int]:
    """Process staged PDFs into immutable JSON without downloading source files."""
    records = storage.read_annual_account_pdf_catalog(
        filing_year=filing_year,
        chunk_key=chunk_key,
    ).to_dicts()
    available_records = [
        record for record in records if record["fetch_status"] == "success"
    ]
    already_parsed_records = [
        record for record in records if record["fetch_status"] == "already_parsed"
    ]
    for record in already_parsed_records:
        _verify_existing_document(storage, record, chunk_key=chunk_key)

    pending: list[dict[str, Any]] = []
    reused_count = len(already_parsed_records)
    for record in available_records:
        if storage.annual_account_document_exists(
            filing_year=filing_year,
            chunk_key=chunk_key,
            org_number=_string(record.get("org_number")),
        ):
            _verify_existing_document(storage, record, chunk_key=chunk_key)
            reused_count += 1
        else:
            pending.append(record)

    totals = {
        "candidate_count": len(records),
        "available_pdf_count": len(available_records),
        "not_found_count": sum(
            record["fetch_status"] == "not_found" for record in records
        ),
        "processed_count": 0,
        "reused_count": reused_count,
        "json_bytes": 0,
        "page_count": 0,
        "native_text_page_count": 0,
        "ocr_page_count": 0,
    }
    _log(
        log,
        "Starting Norway BRREG annual-account JSON processing: year=%d chunk=%s "
        "available_pdfs=%d already_parsed=%d pending=%d workers=%d",
        filing_year,
        chunk_key,
        len(available_records),
        reused_count,
        len(pending),
        ANNUAL_ACCOUNT_DOCUMENT_WORKERS,
    )
    completed_count = 0
    for batch_start in range(0, len(pending), ANNUAL_ACCOUNT_DOCUMENT_WORKERS):
        batch = pending[batch_start : batch_start + ANNUAL_ACCOUNT_DOCUMENT_WORKERS]
        with ThreadPoolExecutor(
            max_workers=ANNUAL_ACCOUNT_DOCUMENT_WORKERS
        ) as executor:
            futures = {
                executor.submit(
                    _process_staged_document,
                    record,
                    filing_year=filing_year,
                    chunk_key=chunk_key,
                    source_run_id=source_run_id,
                    storage=storage,
                ): _string(record.get("org_number"))
                for record in batch
            }
            for future in as_completed(futures):
                org_number = futures[future]
                try:
                    result = future.result()
                except Exception as error:
                    raise RuntimeError(
                        "Norway BRREG annual-account processing failed: "
                        f"org={org_number} year={filing_year} chunk={chunk_key}"
                    ) from error
                completed_count += 1
                totals["processed_count"] += 1
                totals["json_bytes"] += result["json_bytes"]
                totals["page_count"] += result["page_count"]
                totals["native_text_page_count"] += result["native_text_page_count"]
                totals["ocr_page_count"] += result["ocr_page_count"]
                if _should_log_progress(completed_count, len(pending)):
                    _log(
                        log,
                        "Norway BRREG annual-account JSON progress: year=%d "
                        "chunk=%s processed=%d pending_total=%d",
                        filing_year,
                        chunk_key,
                        completed_count,
                        len(pending),
                    )

    _log(
        log,
        "Completed Norway BRREG annual-account JSON processing: year=%d chunk=%s "
        "totals=%s",
        filing_year,
        chunk_key,
        totals,
    )
    return totals


def remove_processed_annual_account_pdfs(
    *,
    filing_year: int,
    chunk_key: str,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., None] | None,
) -> dict[str, int]:
    """Delete staged PDFs only after every catalogued PDF has verified JSON."""
    records = storage.read_annual_account_pdf_catalog(
        filing_year=filing_year,
        chunk_key=chunk_key,
    ).to_dicts()
    staged_records = [
        record for record in records if record["fetch_status"] == "success"
    ]

    for record in staged_records:
        org_number = _string(record.get("org_number"))
        if not storage.annual_account_document_exists(
            filing_year=filing_year,
            chunk_key=chunk_key,
            org_number=org_number,
        ):
            raise RuntimeError(
                "Norway BRREG staged annual-account PDF has no processed JSON: "
                f"org={org_number} year={filing_year} chunk={chunk_key}"
            )
        _verify_existing_document(storage, record, chunk_key=chunk_key)

    existing_keys = [
        _string(record.get("source_object_key"))
        for record in staged_records
        if storage.response_exists(_string(record.get("source_object_key")))
    ]
    deleted_count = storage.delete_annual_account_pdfs(existing_keys)
    metadata = {
        "catalog_pdf_count": len(staged_records),
        "verified_json_count": len(staged_records),
        "deleted_count": deleted_count,
        "already_removed_count": len(staged_records) - deleted_count,
    }
    _log(
        log,
        "Completed Norway BRREG processed PDF cleanup: year=%d chunk=%s metadata=%s",
        filing_year,
        chunk_key,
        metadata,
    )
    return metadata


def _stage_pdf(
    candidate: Mapping[str, Any],
    *,
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
    api: NorwayBrregApiResource,
    storage: NorwayBrregFinancialParquetStorageResource,
) -> dict[str, Any]:
    org_number = _string(candidate.get("org_number"))
    fetched_at = _utc_now_iso()
    pdf_key = annual_account_pdf_object_key(filing_year, chunk_key, org_number)
    if storage.response_exists(pdf_key):
        pdf_body = storage.read_response(pdf_key)
        _validate_pdf_body(pdf_body, org_number, filing_year)
        return _catalog_record(
            candidate=candidate,
            filing_year=filing_year,
            source_run_id=source_run_id,
            source_url=_source_pdf_url(org_number, filing_year),
            source_object_key=pdf_key,
            source_payload_hash=hashlib.sha256(pdf_body).hexdigest(),
            pdf_size_bytes=len(pdf_body),
            fetch_status="success",
            capture_method="staged_reuse",
            fetched_at=fetched_at,
        )

    if storage.annual_account_document_exists(
        filing_year=filing_year,
        chunk_key=chunk_key,
        org_number=org_number,
    ):
        document = storage.read_annual_account_document(
            filing_year=filing_year,
            chunk_key=chunk_key,
            org_number=org_number,
        )
        _validate_document_identity(document, org_number, filing_year)
        return _catalog_record(
            candidate=candidate,
            filing_year=filing_year,
            source_run_id=source_run_id,
            source_url=_string(document.get("source_pdf_url")),
            source_object_key=None,
            source_payload_hash=_string(document.get("source_pdf_sha256")),
            pdf_size_bytes=int(document.get("source_pdf_size_bytes") or 0),
            fetch_status="already_parsed",
            capture_method="existing_json",
            fetched_at=fetched_at,
        )

    pdf = api.annual_account_pdf(org_number=org_number, filing_year=filing_year)
    if pdf is None:
        return _catalog_record(
            candidate=candidate,
            filing_year=filing_year,
            source_run_id=source_run_id,
            source_url=_source_pdf_url(org_number, filing_year),
            source_object_key=None,
            source_payload_hash=None,
            pdf_size_bytes=None,
            fetch_status="not_found",
            capture_method="http_404",
            fetched_at=fetched_at,
        )

    storage.write_annual_account_pdf(
        filing_year=filing_year,
        chunk_key=chunk_key,
        org_number=org_number,
        body=pdf.body,
    )
    return _catalog_record(
        candidate=candidate,
        filing_year=filing_year,
        source_run_id=source_run_id,
        source_url=pdf.source_url,
        source_object_key=pdf_key,
        source_payload_hash=hashlib.sha256(pdf.body).hexdigest(),
        pdf_size_bytes=len(pdf.body),
        fetch_status="success",
        capture_method="http_download",
        fetched_at=fetched_at,
    )


def _process_staged_document(
    record: Mapping[str, Any],
    *,
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
    storage: NorwayBrregFinancialParquetStorageResource,
) -> dict[str, int]:
    org_number = _string(record.get("org_number"))
    object_key = _string(record.get("source_object_key"))
    pdf_body = storage.read_response(object_key)
    _validate_pdf_body(pdf_body, org_number, filing_year)
    actual_hash = hashlib.sha256(pdf_body).hexdigest()
    expected_hash = _string(record.get("source_payload_hash"))
    if actual_hash != expected_hash:
        raise RuntimeError(
            "Norway BRREG staged annual-account PDF hash mismatch: "
            f"key={object_key} expected={expected_hash} actual={actual_hash}"
        )

    document = extract_annual_account_pdf(
        pdf_body,
        org_number=org_number,
        legal_name=_string(record.get("legal_name")),
        filing_year=filing_year,
        source_pdf_url=_string(record.get("source_url")),
        source_run_id=source_run_id,
        retrieved_at=_string(record.get("fetched_at")),
        ocr_image=tesseract_ocr_image,
    )
    if _string(document.get("source_pdf_sha256")) != expected_hash:
        raise RuntimeError(
            "Norway BRREG processed annual-account JSON has the wrong PDF hash: "
            f"org={org_number} year={filing_year}"
        )
    _object_key, json_bytes = storage.write_annual_account_document(
        filing_year=filing_year,
        chunk_key=chunk_key,
        org_number=org_number,
        document=document,
    )
    return {
        "json_bytes": json_bytes,
        "page_count": int(document["pdf_page_count"]),
        "native_text_page_count": int(document["native_text_page_count"]),
        "ocr_page_count": int(document["ocr_page_count"]),
    }


def _verify_existing_document(
    storage: NorwayBrregFinancialParquetStorageResource,
    record: Mapping[str, Any],
    *,
    chunk_key: str,
) -> None:
    org_number = _string(record.get("org_number"))
    filing_year = int(record["filing_year"])
    document = storage.read_annual_account_document(
        filing_year=filing_year,
        chunk_key=chunk_key,
        org_number=org_number,
    )
    _validate_document_identity(document, org_number, filing_year)
    expected_hash = _string(record.get("source_payload_hash"))
    actual_hash = _string(document.get("source_pdf_sha256"))
    if expected_hash == "" or actual_hash != expected_hash:
        raise RuntimeError(
            "Norway BRREG processed annual-account JSON hash does not match its PDF "
            f"catalog: org={org_number} expected={expected_hash} actual={actual_hash}"
        )


def _validate_document_identity(
    document: Mapping[str, Any],
    org_number: str,
    filing_year: int,
) -> None:
    if (
        _string(document.get("org_number")) != org_number
        or int(document.get("filing_year") or 0) != filing_year
    ):
        raise RuntimeError(
            "Norway BRREG annual-account JSON identity mismatch: "
            f"expected_org={org_number} expected_year={filing_year}"
        )


def _catalog_record(
    *,
    candidate: Mapping[str, Any],
    filing_year: int,
    source_run_id: str,
    source_url: str,
    source_object_key: str | None,
    source_payload_hash: str | None,
    pdf_size_bytes: int | None,
    fetch_status: str,
    capture_method: str,
    fetched_at: str,
) -> dict[str, Any]:
    return {
        "source_run_id": source_run_id,
        "org_number": _string(candidate.get("org_number")),
        "legal_name": _string(candidate.get("legal_name")),
        "filing_year": filing_year,
        "source_url": source_url,
        "source_object_key": source_object_key,
        "source_payload_hash": source_payload_hash,
        "pdf_size_bytes": pdf_size_bytes,
        "fetch_status": fetch_status,
        "capture_method": capture_method,
        "fetched_at": fetched_at,
    }


def _catalog_frame(records: list[dict[str, Any]]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(schema=ANNUAL_ACCOUNT_PDF_CATALOG_SCHEMA)
    return pl.DataFrame(records, schema=ANNUAL_ACCOUNT_PDF_CATALOG_SCHEMA).sort(
        "org_number"
    )


def _download_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "downloaded_count": sum(
            record["capture_method"] == "http_download" for record in records
        ),
        "staged_reused_count": sum(
            record["capture_method"] == "staged_reuse" for record in records
        ),
        "already_parsed_count": sum(
            record["fetch_status"] == "already_parsed" for record in records
        ),
        "not_found_count": sum(
            record["fetch_status"] == "not_found" for record in records
        ),
        "pdf_bytes_downloaded": sum(
            int(record.get("pdf_size_bytes") or 0)
            for record in records
            if record["capture_method"] == "http_download"
        ),
    }


def _log_download_progress(
    log: Callable[..., None] | None,
    *,
    records: list[dict[str, Any]],
    candidate_count: int,
    filing_year: int,
    chunk_key: str,
) -> None:
    _log(
        log,
        "Norway BRREG annual-account PDF download progress: year=%d chunk=%s "
        "completed=%d total=%d counts=%s",
        filing_year,
        chunk_key,
        len(records),
        candidate_count,
        _download_counts(records),
    )


def _unique_candidates(
    candidates: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    by_org: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        org_number = _string(candidate.get("org_number"))
        if org_number == "":
            raise ValueError("Norway annual-account candidate org_number is empty")
        by_org[org_number] = candidate
    return [by_org[org_number] for org_number in sorted(by_org)]


def _validate_pdf_body(pdf_body: bytes, org_number: str, filing_year: int) -> None:
    if not pdf_body.startswith(b"%PDF-"):
        raise RuntimeError(
            "Norway BRREG staged annual-account object is not a PDF: "
            f"org={org_number} year={filing_year}"
        )


def _source_pdf_url(org_number: str, filing_year: int) -> str:
    return f"{BRREG_ANNUAL_ACCOUNTS_BASE_URL}/kopi/{org_number}/{filing_year}"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _should_log_progress(completed: int, total: int) -> bool:
    return (
        completed == 1
        or completed == total
        or completed % ANNUAL_ACCOUNT_PROGRESS_INTERVAL == 0
    )


def _string(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _log(log: Callable[..., None] | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)
