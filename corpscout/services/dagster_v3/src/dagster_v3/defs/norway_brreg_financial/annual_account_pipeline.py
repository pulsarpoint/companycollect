from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource
from dagster_v3.defs.norway_brreg_financial.annual_account_pdf import (
    extract_annual_account_pdf,
    tesseract_ocr_image,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)

ANNUAL_ACCOUNT_DOCUMENT_WORKERS = 6
ANNUAL_ACCOUNT_PROGRESS_INTERVAL = 100


def materialize_annual_account_documents(
    *,
    candidates: Sequence[Mapping[str, Any]],
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
    api: NorwayBrregApiResource,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., None] | None,
) -> dict[str, int]:
    """Download, OCR, persist JSON, and discard each source PDF from memory."""
    unique_candidates = _unique_candidates(candidates)
    totals = {
        "candidate_count": len(unique_candidates),
        "downloaded_count": 0,
        "reused_count": 0,
        "not_found_count": 0,
        "pdf_bytes": 0,
        "json_bytes": 0,
        "page_count": 0,
        "native_text_page_count": 0,
        "ocr_page_count": 0,
    }
    _log(
        log,
        "Starting Norway BRREG annual-account PDF extraction: year=%d chunk=%s "
        "candidates=%d workers=%d",
        filing_year,
        chunk_key,
        len(unique_candidates),
        ANNUAL_ACCOUNT_DOCUMENT_WORKERS,
    )
    with ThreadPoolExecutor(max_workers=ANNUAL_ACCOUNT_DOCUMENT_WORKERS) as executor:
        futures = {
            executor.submit(
                _process_document,
                candidate,
                filing_year=filing_year,
                chunk_key=chunk_key,
                source_run_id=source_run_id,
                api=api,
                storage=storage,
            ): _string(candidate.get("org_number"))
            for candidate in unique_candidates
        }
        for completed_count, future in enumerate(as_completed(futures), start=1):
            org_number = futures[future]
            try:
                result = future.result()
            except Exception as error:
                raise RuntimeError(
                    "Norway BRREG annual-account processing failed: "
                    f"org={org_number} year={filing_year} chunk={chunk_key}"
                ) from error
            totals[f"{result['status']}_count"] += 1
            totals["pdf_bytes"] += int(result.get("pdf_bytes", 0))
            totals["json_bytes"] += int(result.get("json_bytes", 0))
            totals["page_count"] += int(result.get("page_count", 0))
            totals["native_text_page_count"] += int(
                result.get("native_text_page_count", 0)
            )
            totals["ocr_page_count"] += int(result.get("ocr_page_count", 0))
            if _should_log_progress(completed_count, len(futures)):
                _log(
                    log,
                    "Norway BRREG annual-account progress: year=%d chunk=%s "
                    "completed=%d total=%d downloaded=%d reused=%d not_found=%d",
                    filing_year,
                    chunk_key,
                    completed_count,
                    len(futures),
                    totals["downloaded_count"],
                    totals["reused_count"],
                    totals["not_found_count"],
                )
    _log(
        log,
        "Completed Norway BRREG annual-account PDF extraction: year=%d chunk=%s "
        "totals=%s",
        filing_year,
        chunk_key,
        totals,
    )
    return totals


def _process_document(
    candidate: Mapping[str, Any],
    *,
    filing_year: int,
    chunk_key: str,
    source_run_id: str,
    api: NorwayBrregApiResource,
    storage: NorwayBrregFinancialParquetStorageResource,
) -> dict[str, Any]:
    org_number = _string(candidate.get("org_number"))
    if storage.annual_account_document_exists(
        filing_year=filing_year,
        chunk_key=chunk_key,
        org_number=org_number,
    ):
        return {"status": "reused"}

    pdf = api.annual_account_pdf(
        org_number=org_number,
        filing_year=filing_year,
    )
    if pdf is None:
        return {"status": "not_found"}

    document = extract_annual_account_pdf(
        pdf.body,
        org_number=org_number,
        legal_name=_string(candidate.get("legal_name")),
        filing_year=filing_year,
        source_pdf_url=pdf.source_url,
        source_run_id=source_run_id,
        retrieved_at=_utc_now_iso(),
        ocr_image=tesseract_ocr_image,
    )
    _object_key, json_bytes = storage.write_annual_account_document(
        filing_year=filing_year,
        chunk_key=chunk_key,
        org_number=org_number,
        document=document,
    )
    return {
        "status": "downloaded",
        "pdf_bytes": document["source_pdf_size_bytes"],
        "json_bytes": json_bytes,
        "page_count": document["pdf_page_count"],
        "native_text_page_count": document["native_text_page_count"],
        "ocr_page_count": document["ocr_page_count"],
    }


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
