from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import threading
import time
from typing import Any

import dagster as dg
import pymupdf
import pytest

from dagster_v3.defs.norway_brreg.resources import (
    BrregAnnualAccountPdf,
    BrregAnnualAccountPdfFailure,
)
from dagster_v3.defs.norway_brreg_financial import annual_account_pdf
from dagster_v3.defs.norway_brreg_financial import annual_account_pipeline
from dagster_v3.defs.norway_brreg_financial.annual_account_pdf import (
    extract_annual_account_pdf,
)
from dagster_v3.defs.norway_brreg_financial.annual_account_pipeline import (
    download_annual_account_pdfs,
    materialize_annual_account_documents,
    remove_processed_annual_account_pdfs,
)
from dagster_v3.defs.norway_brreg_financial.assets import annual_accounts
from dagster_v3.defs.norway_brreg_financial.assets.annual_accounts import (
    NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT,
    NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    norway_brreg_annual_account_documents_json,
    norway_brreg_annual_account_pdf_cleanup,
    norway_brreg_annual_account_pdfs,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    annual_account_document_object_key,
    annual_account_pdf_catalog_object_key,
    annual_account_pdf_failure_object_key,
    annual_account_pdf_object_key,
)


class FakeClickhouseClient:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, dict[str, int]]] = []

    def execute(
        self,
        sql: str,
        params: dict[str, int],
    ) -> list[tuple[str, str]]:
        self.executed.append((sql, params))
        return self.rows


class FakeClickhouseResource:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self.client = FakeClickhouseClient(rows)

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, _bucket: str | None = None) -> None:
        return None

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return (str(bucket), key) in self.objects

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        return self.objects[(str(bucket), key)]

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        self.objects[(str(bucket), key)] = body

    def delete_keys(
        self,
        keys: list[str] | tuple[str, ...],
        bucket: str | None = None,
    ) -> int:
        for key in keys:
            self.objects.pop((str(bucket), key), None)
        return len(keys)


class FakeAnnualAccountApi:
    def __init__(self, pdf: BrregAnnualAccountPdf | None) -> None:
        self.pdf = pdf
        self.calls: list[tuple[str, int]] = []

    def annual_account_pdf(
        self,
        *,
        org_number: str,
        filing_year: int,
    ) -> BrregAnnualAccountPdf | None:
        self.calls.append((org_number, filing_year))
        return self.pdf


class FailingSecondAnnualAccountApi:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def annual_account_pdf(
        self,
        *,
        org_number: str,
        filing_year: int,
    ) -> BrregAnnualAccountPdf | None:
        self.calls.append((org_number, filing_year))
        if len(self.calls) == 2:
            raise RuntimeError("rate limited")
        return BrregAnnualAccountPdf(
            source_url=f"https://example.test/{org_number}/{filing_year}",
            source_file_name=f"aarsregnskap-{filing_year}_{org_number}.pdf",
            body=f"%PDF-1.7 {org_number}".encode(),
        )


class FailedFirstAnnualAccountApi:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def annual_account_pdf(
        self,
        *,
        org_number: str,
        filing_year: int,
    ) -> BrregAnnualAccountPdf | BrregAnnualAccountPdfFailure:
        self.calls.append((org_number, filing_year))
        if org_number == "111111111":
            return BrregAnnualAccountPdfFailure(
                source_url=f"https://example.test/{org_number}/{filing_year}",
                http_status=504,
                attempt_count=3,
                error_type="http_error",
                error_message="HTTP 504",
                response_headers={"x-request-id": "failed-request"},
            )
        return BrregAnnualAccountPdf(
            source_url=f"https://example.test/{org_number}/{filing_year}",
            source_file_name=f"aarsregnskap-{filing_year}_{org_number}.pdf",
            body=f"%PDF-1.7 {org_number}".encode(),
        )


def test_annual_account_partitions_are_year_by_64_stable_chunks() -> None:
    keys = NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS.get_partition_keys(
        current_time=datetime(2026, 7, 18, tzinfo=UTC)
    )

    assert NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT == 64
    assert len(keys) == 15 * 64
    assert dg.MultiPartitionKey({"year": "2011", "chunk": "bucket_00"}) in keys
    assert dg.MultiPartitionKey({"year": "2025", "chunk": "bucket_63"}) in keys


def test_annual_account_asset_graph_separates_download_processing_and_cleanup() -> None:
    pdf_key = dg.AssetKey("norway_brreg_annual_account_pdfs")
    document_key = dg.AssetKey("norway_brreg_annual_account_documents_json")
    cleanup_key = dg.AssetKey("norway_brreg_annual_account_pdf_cleanup")

    assert norway_brreg_annual_account_pdfs.asset_deps[pdf_key] == set()
    assert norway_brreg_annual_account_documents_json.asset_deps[document_key] == {
        pdf_key
    }
    assert norway_brreg_annual_account_pdf_cleanup.asset_deps[cleanup_key] == {
        document_key
    }


@pytest.mark.parametrize(
    ("asset", "kwargs", "upstream_asset_name"),
    [
        (
            norway_brreg_annual_account_documents_json,
            {
                "config": annual_accounts.NorwayBrregAnnualAccountDocumentConfig(),
                "norway_brreg_financial_storage": object(),
            },
            "norway_brreg_annual_account_pdfs",
        ),
        (
            norway_brreg_annual_account_pdf_cleanup,
            {"norway_brreg_financial_storage": object()},
            "norway_brreg_annual_account_documents_json",
        ),
    ],
)
def test_annual_account_s3_stages_require_same_partition_parent(
    asset: Any,
    kwargs: dict[str, Any],
    upstream_asset_name: str,
) -> None:
    partition_key = dg.MultiPartitionKey(
        {"year": "2025", "chunk": "bucket_63"}
    )
    with (
        dg.DagsterInstance.ephemeral() as instance,
        dg.build_asset_context(
            instance=instance,
            partition_key=partition_key,
        ) as context,
        pytest.raises(dg.Failure, match=upstream_asset_name),
    ):
        asset(context=context, **kwargs)


def test_pdf_asset_queries_requested_year_and_chunk_without_processing(
    monkeypatch,
) -> None:
    clickhouse = FakeClickhouseResource([("923609016", "EQUINOR ASA")])
    captured: dict[str, Any] = {}

    def fake_download(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "candidate_count": 1,
            "downloaded_count": 1,
            "staged_reused_count": 0,
            "already_parsed_count": 0,
            "not_found_count": 0,
            "pdf_bytes_downloaded": 100,
            "catalog_key": "catalog.parquet",
        }

    monkeypatch.setattr(annual_accounts, "download_annual_account_pdfs", fake_download)

    result = norway_brreg_annual_account_pdfs(
        context=dg.build_asset_context(
            partition_key=dg.MultiPartitionKey({"year": "2025", "chunk": "bucket_07"})
        ),
        clickhouse=clickhouse,
        norway_brreg_api=object(),
        norway_brreg_financial_storage=object(),
    )

    [(sql, params)] = clickhouse.client.executed
    assert "cityHash64(toString(org_number))" in sql
    assert "last_submitted_accounts_year" in sql
    assert params == {
        "filing_year": 2025,
        "chunk_count": 64,
        "chunk_index": 7,
    }
    assert captured["candidates"] == [
        {"org_number": "923609016", "legal_name": "EQUINOR ASA"}
    ]
    assert captured["filing_year"] == 2025
    assert captured["chunk_key"] == "bucket_07"
    assert result.metadata["downloaded_count"] == 1
    assert "ocr_page_count" not in result.metadata


def test_pdf_download_stages_pdf_and_catalog_without_processing(monkeypatch) -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    pdf_body = b"%PDF-1.7 staged annual account"
    monkeypatch.setattr(
        annual_account_pipeline,
        "extract_annual_account_pdf",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("PDF download asset must not parse or OCR")
        ),
    )
    api = FakeAnnualAccountApi(
        BrregAnnualAccountPdf(
            source_url="https://example.test/923609016/2025",
            source_file_name="aarsregnskap-2025_923609016.pdf",
            body=pdf_body,
        )
    )

    metadata = download_annual_account_pdfs(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="run-1",
        api=api,
        storage=storage,
        log=None,
    )

    pdf_key = annual_account_pdf_object_key(2025, "bucket_07", "923609016")
    assert storage.read_response(pdf_key) == pdf_body
    assert not storage.annual_account_document_exists(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
    )
    catalog = storage.read_annual_account_pdf_catalog(
        filing_year=2025,
        chunk_key="bucket_07",
    )
    assert catalog.to_dicts() == [
        {
            "source_run_id": "run-1",
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "filing_year": 2025,
            "source_file_name": "aarsregnskap-2025_923609016.pdf",
            "source_url": "https://example.test/923609016/2025",
            "source_object_key": pdf_key,
            "source_payload_hash": hashlib.sha256(pdf_body).hexdigest(),
            "pdf_size_bytes": len(pdf_body),
            "fetch_status": "success",
            "capture_method": "http_download",
            "failure_object_key": None,
            "http_status": None,
            "request_attempt_count": None,
            "failure_type": None,
            "failure_message": None,
            "fetched_at": catalog["fetched_at"][0],
        }
    ]
    assert metadata["downloaded_count"] == 1
    assert metadata["already_parsed_count"] == 0
    assert metadata["catalog_key"] == annual_account_pdf_catalog_object_key(
        2025, "bucket_07"
    )


def test_pdf_download_skips_company_with_existing_parsed_json() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    storage.write_annual_account_document(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
        document={
            "org_number": "923609016",
            "filing_year": 2025,
            "source_pdf_url": "https://example.test/923609016/2025",
            "source_pdf_sha256": "parsed-hash",
            "source_pdf_size_bytes": 321,
        },
    )
    api = FakeAnnualAccountApi(None)

    metadata = download_annual_account_pdfs(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="run-2",
        api=api,
        storage=storage,
        log=None,
    )

    assert api.calls == []
    assert metadata["already_parsed_count"] == 1
    assert metadata["downloaded_count"] == 0
    assert storage.read_annual_account_pdf_catalog(
        filing_year=2025,
        chunk_key="bucket_07",
    )["fetch_status"].to_list() == ["already_parsed"]


def test_pdf_download_reuses_staged_pdf_without_http_request() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    pdf_body = b"%PDF-1.7 already staged"
    storage.write_annual_account_pdf(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
        body=pdf_body,
    )
    api = FakeAnnualAccountApi(None)

    metadata = download_annual_account_pdfs(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="run-2",
        api=api,
        storage=storage,
        log=None,
    )

    assert api.calls == []
    assert metadata["staged_reused_count"] == 1
    assert metadata["downloaded_count"] == 0


def test_pdf_download_records_exhausted_failure_and_continues_batch() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    candidates = [
        {"org_number": "111111111", "legal_name": "FAILED AS"},
        {"org_number": "222222222", "legal_name": "SUCCESS AS"},
    ]
    api = FailedFirstAnnualAccountApi()
    warnings: list[str] = []

    metadata = download_annual_account_pdfs(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="download-run",
        api=api,
        storage=storage,
        log=None,
        log_warning=lambda message, *args: warnings.append(message % args),
    )

    failure_key = annual_account_pdf_failure_object_key(
        2025,
        "bucket_00",
        "111111111",
    )
    assert api.calls == [("111111111", 2025), ("222222222", 2025)]
    assert metadata["failed_count"] == 1
    assert metadata["failed_request_attempt_count"] == 3
    assert metadata["downloaded_count"] == 1
    assert "HTTP 504" not in warnings[0]
    assert "http_status=504" in warnings[0]
    assert f"marker={failure_key}" in warnings[0]

    failure = storage.read_annual_account_pdf_failure(
        filing_year=2025,
        chunk_key="bucket_00",
        org_number="111111111",
    )
    assert failure == {
        "schema_version": 1,
        "org_number": "111111111",
        "legal_name": "FAILED AS",
        "filing_year": 2025,
        "source_url": "https://example.test/111111111/2025",
        "http_status": 504,
        "request_attempt_count": 3,
        "failure_type": "http_error",
        "failure_message": "HTTP 504",
        "response_headers": {"x-request-id": "failed-request"},
        "decision": "skip_document",
        "source_asset": "norway_brreg_annual_account_pdfs",
        "source_partition_key": "bucket_00|2025",
        "source_run_id": "download-run",
        "failure_object_key": failure_key,
        "first_failed_at": failure["first_failed_at"],
        "last_failed_at": failure["last_failed_at"],
        "failure_count": 1,
    }
    catalog = storage.read_annual_account_pdf_catalog(
        filing_year=2025,
        chunk_key="bucket_00",
    )
    failed_row = next(
        row for row in catalog.to_dicts() if row["org_number"] == "111111111"
    )
    assert failed_row["source_url"] == "https://example.test/111111111/2025"
    assert failed_row["fetch_status"] == "failed"
    assert failed_row["capture_method"] == "http_failure"
    assert failed_row["failure_object_key"] == failure_key
    assert failed_row["http_status"] == 504
    assert failed_row["request_attempt_count"] == 3


def test_pdf_download_reuses_failure_marker_without_another_request() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    candidates = [{"org_number": "111111111", "legal_name": "FAILED AS"}]
    first_api = FailedFirstAnnualAccountApi()
    download_annual_account_pdfs(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="first-run",
        api=first_api,
        storage=storage,
        log=None,
    )
    second_api = FakeAnnualAccountApi(None)

    metadata = download_annual_account_pdfs(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="second-run",
        api=second_api,
        storage=storage,
        log=None,
    )

    assert first_api.calls == [("111111111", 2025)]
    assert second_api.calls == []
    assert metadata["failed_count"] == 1
    assert metadata["failure_marker_reused_count"] == 1
    assert metadata["failed_request_attempt_count"] == 0
    [record] = storage.read_annual_account_pdf_catalog(
        filing_year=2025,
        chunk_key="bucket_00",
    ).to_dicts()
    assert record["fetch_status"] == "failed"
    assert record["capture_method"] == "failure_marker_reuse"
    assert record["source_run_id"] == "second-run"


def test_pdf_download_resumes_after_failure_without_redownloading_completed_pdf() -> (
    None
):
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    candidates = [
        {"org_number": "111111111", "legal_name": "FIRST AS"},
        {"org_number": "222222222", "legal_name": "SECOND AS"},
    ]
    first_api = FailingSecondAnnualAccountApi()

    with pytest.raises(RuntimeError, match="org=222222222"):
        download_annual_account_pdfs(
            candidates=candidates,
            filing_year=2025,
            chunk_key="bucket_00",
            source_run_id="failed-run",
            api=first_api,
            storage=storage,
            log=None,
        )

    assert storage.annual_account_pdf_exists(
        filing_year=2025,
        chunk_key="bucket_00",
        org_number="111111111",
    )

    resumed_api = FakeAnnualAccountApi(
        BrregAnnualAccountPdf(
            source_url="https://example.test/222222222/2025",
            source_file_name="aarsregnskap-2025_222222222.pdf",
            body=b"%PDF-1.7 second",
        )
    )
    metadata = download_annual_account_pdfs(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="resumed-run",
        api=resumed_api,
        storage=storage,
        log=None,
    )

    assert resumed_api.calls == [("222222222", 2025)]
    assert metadata["staged_reused_count"] == 1
    assert metadata["downloaded_count"] == 1


def test_document_processing_reads_staged_pdf_and_skips_existing_json(
    monkeypatch,
) -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    pdf_body = b"%PDF-1.7 staged annual account"
    download_annual_account_pdfs(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="download-run",
        api=FakeAnnualAccountApi(
            BrregAnnualAccountPdf(
                source_url="https://example.test/923609016/2025",
                source_file_name="aarsregnskap-2025_923609016.pdf",
                body=pdf_body,
            )
        ),
        storage=storage,
        log=None,
    )
    extract_calls: list[bytes] = []

    def fake_extract(body: bytes, **kwargs: Any) -> dict[str, Any]:
        extract_calls.append(body)
        return {
            "org_number": kwargs["org_number"],
            "filing_year": kwargs["filing_year"],
            "source_file_name": kwargs["source_file_name"],
            "source_pdf_url": kwargs["source_pdf_url"],
            "source_pdf_sha256": hashlib.sha256(body).hexdigest(),
            "source_pdf_size_bytes": len(body),
            "pdf_page_count": 2,
            "native_text_page_count": 0,
            "ocr_page_count": 2,
        }

    monkeypatch.setattr(
        annual_account_pipeline, "extract_annual_account_pdf", fake_extract
    )

    first = materialize_annual_account_documents(
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="process-run-1",
        max_documents=25,
        storage=storage,
        log=None,
    )
    second = materialize_annual_account_documents(
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="process-run-2",
        max_documents=25,
        storage=storage,
        log=None,
    )

    assert extract_calls == [pdf_body]
    assert first["processed_count"] == 1
    assert first["ocr_page_count"] == 2
    assert second["reused_count"] == 1
    assert second["processed_count"] == 0


def test_document_processing_uses_bounded_resumable_batches(monkeypatch) -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    candidates = [
        {"org_number": "111111111", "legal_name": "FIRST AS"},
        {"org_number": "222222222", "legal_name": "SECOND AS"},
        {"org_number": "333333333", "legal_name": "THIRD AS"},
    ]
    download_annual_account_pdfs(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="download-run",
        api=FakeAnnualAccountApi(
            BrregAnnualAccountPdf(
                source_url="https://example.test/annual-account.pdf",
                source_file_name="aarsregnskap-2025_111111111.pdf",
                body=b"%PDF-1.7 staged annual account",
            )
        ),
        storage=storage,
        log=None,
    )
    processed_org_numbers: list[str] = []

    def fake_extract(body: bytes, **kwargs: Any) -> dict[str, Any]:
        processed_org_numbers.append(kwargs["org_number"])
        return {
            "org_number": kwargs["org_number"],
            "filing_year": kwargs["filing_year"],
            "source_file_name": kwargs["source_file_name"],
            "source_pdf_url": kwargs["source_pdf_url"],
            "source_pdf_sha256": hashlib.sha256(body).hexdigest(),
            "source_pdf_size_bytes": len(body),
            "pdf_page_count": 1,
            "native_text_page_count": 0,
            "ocr_page_count": 1,
        }

    monkeypatch.setattr(
        annual_account_pipeline, "extract_annual_account_pdf", fake_extract
    )

    first = materialize_annual_account_documents(
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="process-run-1",
        max_documents=2,
        storage=storage,
        log=None,
    )

    assert processed_org_numbers == ["111111111", "222222222"]
    assert first["pending_before_count"] == 3
    assert first["max_documents_per_run"] == 2
    assert first["worker_count"] == 4
    assert first["selected_count"] == 2
    assert first["processed_count"] == 2
    assert first["remaining_count"] == 1

    first_cleanup = remove_processed_annual_account_pdfs(
        filing_year=2025,
        chunk_key="bucket_00",
        storage=storage,
        log=None,
    )

    assert first_cleanup["deleted_count"] == 2
    assert first_cleanup["pending_json_count"] == 1
    assert storage.annual_account_pdf_exists(
        filing_year=2025,
        chunk_key="bucket_00",
        org_number="333333333",
    )

    second = materialize_annual_account_documents(
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="process-run-2",
        max_documents=2,
        storage=storage,
        log=None,
    )

    assert processed_org_numbers == ["111111111", "222222222", "333333333"]
    assert second["reused_count"] == 2
    assert second["selected_count"] == 1
    assert second["processed_count"] == 1
    assert second["remaining_count"] == 0


def test_document_processing_runs_at_most_four_documents_in_parallel(
    monkeypatch,
) -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    candidates = [
        {"org_number": str(100_000_000 + index), "legal_name": f"COMPANY {index}"}
        for index in range(8)
    ]
    download_annual_account_pdfs(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="download-run",
        api=FakeAnnualAccountApi(
            BrregAnnualAccountPdf(
                source_url="https://example.test/annual-account.pdf",
                source_file_name="aarsregnskap-2025_100000000.pdf",
                body=b"%PDF-1.7 staged annual account",
            )
        ),
        storage=storage,
        log=None,
    )
    lock = threading.Lock()
    active_count = 0
    max_active_count = 0

    def fake_extract(body: bytes, **kwargs: Any) -> dict[str, Any]:
        nonlocal active_count, max_active_count
        with lock:
            active_count += 1
            max_active_count = max(max_active_count, active_count)
        time.sleep(0.02)
        with lock:
            active_count -= 1
        return {
            "org_number": kwargs["org_number"],
            "filing_year": kwargs["filing_year"],
            "source_file_name": kwargs["source_file_name"],
            "source_pdf_url": kwargs["source_pdf_url"],
            "source_pdf_sha256": hashlib.sha256(body).hexdigest(),
            "source_pdf_size_bytes": len(body),
            "pdf_page_count": 1,
            "native_text_page_count": 0,
            "ocr_page_count": 1,
        }

    monkeypatch.setattr(
        annual_account_pipeline, "extract_annual_account_pdf", fake_extract
    )

    metadata = materialize_annual_account_documents(
        filing_year=2025,
        chunk_key="bucket_00",
        source_run_id="process-run",
        max_documents=8,
        storage=storage,
        log=None,
    )

    assert metadata["processed_count"] == 8
    assert max_active_count == 4


def test_cleanup_deletes_pdf_only_after_matching_json_exists(monkeypatch) -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    pdf_body = b"%PDF-1.7 staged annual account"
    download_annual_account_pdfs(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="download-run",
        api=FakeAnnualAccountApi(
            BrregAnnualAccountPdf(
                source_url="https://example.test/923609016/2025",
                source_file_name="aarsregnskap-2025_923609016.pdf",
                body=pdf_body,
            )
        ),
        storage=storage,
        log=None,
    )
    monkeypatch.setattr(
        annual_account_pipeline,
        "extract_annual_account_pdf",
        lambda body, **kwargs: {
            "org_number": kwargs["org_number"],
            "filing_year": kwargs["filing_year"],
            "source_file_name": kwargs["source_file_name"],
            "source_pdf_url": kwargs["source_pdf_url"],
            "source_pdf_sha256": hashlib.sha256(body).hexdigest(),
            "source_pdf_size_bytes": len(body),
            "pdf_page_count": 1,
            "native_text_page_count": 1,
            "ocr_page_count": 0,
        },
    )
    materialize_annual_account_documents(
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="process-run",
        max_documents=25,
        storage=storage,
        log=None,
    )
    resumed_download = download_annual_account_pdfs(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="resumed-download-run",
        api=FakeAnnualAccountApi(None),
        storage=storage,
        log=None,
    )

    metadata = remove_processed_annual_account_pdfs(
        filing_year=2025,
        chunk_key="bucket_07",
        storage=storage,
        log=None,
    )

    assert resumed_download["staged_reused_count"] == 1
    assert resumed_download["already_parsed_count"] == 0
    assert metadata["deleted_count"] == 1
    assert not storage.annual_account_pdf_exists(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
    )
    assert storage.annual_account_document_exists(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
    )


def test_cleanup_keeps_pdf_when_its_json_is_missing() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    download_annual_account_pdfs(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="download-run",
        api=FakeAnnualAccountApi(
            BrregAnnualAccountPdf(
                source_url="https://example.test/923609016/2025",
                source_file_name="aarsregnskap-2025_923609016.pdf",
                body=b"%PDF-1.7 not processed",
            )
        ),
        storage=storage,
        log=None,
    )

    metadata = remove_processed_annual_account_pdfs(
        filing_year=2025,
        chunk_key="bucket_07",
        storage=storage,
        log=None,
    )

    assert metadata["deleted_count"] == 0
    assert metadata["pending_json_count"] == 1
    assert storage.annual_account_pdf_exists(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
    )


def test_annual_account_document_key_and_json_are_immutable() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    key = annual_account_document_object_key(2025, "bucket_07", "923609016")
    document = {"org_number": "923609016", "filing_year": 2025}

    first_key, first_size = storage.write_annual_account_document(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
        document=document,
    )
    second_key, second_size = storage.write_annual_account_document(
        filing_year=2025,
        chunk_key="bucket_07",
        org_number="923609016",
        document=document,
    )

    assert key == (
        "norway_brreg/annual_accounts/documents/year=2025/"
        "chunk=bucket_07/org=923609016/document.json"
    )
    assert first_key == second_key == key
    assert first_size == second_size
    assert json.loads(object_store.objects[("source-norway-brreg", key)]) == document


def test_native_text_pdf_does_not_invoke_ocr() -> None:
    pdf_bytes = _native_text_pdf()

    document = extract_annual_account_pdf(
        pdf_bytes,
        org_number="923609016",
        legal_name="EQUINOR ASA",
        filing_year=2025,
        source_file_name="aarsregnskap-2025_923609016.pdf",
        source_pdf_url="https://example.test/923609016/2025",
        source_run_id="run-1",
        retrieved_at="2026-07-17T12:00:00Z",
        ocr_image=lambda _body: (_ for _ in ()).throw(
            AssertionError("OCR must not run for native text")
        ),
    )

    assert document["pdf_page_count"] == 1
    assert document["native_text_page_count"] == 1
    assert document["ocr_page_count"] == 0
    assert document["pages"][0]["extraction_method"] == "native_text"
    assert "Annual account operating revenue 100" in document["pages"][0]["text"]
    assert document["source_file_name"] == "aarsregnskap-2025_923609016.pdf"
    assert document["source_pdf_url"].endswith("/923609016/2025")
    assert len(document["source_pdf_sha256"]) == 64


def test_scanned_pdf_preserves_ocr_words_coordinates_and_confidence() -> None:
    document = extract_annual_account_pdf(
        _scanned_image_pdf(),
        org_number="923609016",
        legal_name="EQUINOR ASA",
        filing_year=2025,
        source_file_name="aarsregnskap-2025_923609016.pdf",
        source_pdf_url="https://example.test/923609016/2025",
        source_run_id="run-1",
        retrieved_at="2026-07-17T12:00:00Z",
        ocr_image=lambda _body: _tesseract_tsv(),
    )

    page = document["pages"][0]
    assert document["native_text_page_count"] == 0
    assert document["ocr_page_count"] == 1
    assert page["extraction_method"] == "tesseract_ocr"
    assert page["text"] == "Sum inntekter 100"
    assert [word["text"] for word in page["words"]] == [
        "Sum",
        "inntekter",
        "100",
    ]
    assert page["words"][2]["confidence"] == 96.0
    assert page["words"][2]["bbox"] == [0.5, 0.1, 0.6, 0.15]


def test_tesseract_allows_twenty_minutes_per_page(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(command: list[str], **kwargs: Any):
        captured["command"] = command
        captured.update(kwargs)
        return annual_account_pdf.subprocess.CompletedProcess(
            command,
            returncode=0,
            stdout=b"",
            stderr=b"",
        )

    monkeypatch.setattr(annual_account_pdf.subprocess, "run", fake_run)

    assert annual_account_pdf.tesseract_ocr_image(b"image") == ""
    assert captured["timeout"] == 1_200
    assert captured["check"] is True


def _native_text_pdf() -> bytes:
    document = pymupdf.open()
    page = document.new_page(width=600, height=800)
    page.insert_text((72, 72), "Annual account operating revenue 100")
    body = document.tobytes()
    document.close()
    return body


def _scanned_image_pdf() -> bytes:
    pixmap = pymupdf.Pixmap(
        pymupdf.csGRAY,
        pymupdf.IRect(0, 0, 1000, 2000),
        False,
    )
    pixmap.clear_with(255)
    image = pixmap.tobytes("png")
    document = pymupdf.open()
    page = document.new_page(width=1000, height=2000)
    page.insert_image(page.rect, stream=image)
    body = document.tobytes()
    document.close()
    return body


def _tesseract_tsv() -> str:
    return "\n".join(
        [
            "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext",
            "5\t1\t1\t1\t1\t1\t100\t200\t100\t100\t95.0\tSum",
            "5\t1\t1\t1\t1\t2\t250\t200\t200\t100\t94.0\tinntekter",
            "5\t1\t1\t1\t1\t3\t500\t200\t100\t100\t96.0\t100",
        ]
    )
