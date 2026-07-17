from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import json
from typing import Any

import dagster as dg
import pymupdf

from dagster_v3.defs.norway_brreg_financial.annual_account_pdf import (
    extract_annual_account_pdf,
)
from dagster_v3.defs.norway_brreg_financial.annual_account_pipeline import (
    materialize_annual_account_documents,
)
from dagster_v3.defs.norway_brreg_financial.assets import annual_accounts
from dagster_v3.defs.norway_brreg_financial.assets.annual_accounts import (
    NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT,
    NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    norway_brreg_annual_account_documents_json,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    annual_account_document_object_key,
)
from dagster_v3.defs.norway_brreg.resources import BrregAnnualAccountPdf


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


def test_annual_account_partitions_are_year_by_64_stable_chunks() -> None:
    keys = NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS.get_partition_keys(
        current_time=datetime(2026, 7, 17, tzinfo=UTC)
    )

    assert NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT == 64
    assert len(keys) == 15 * 64
    assert dg.MultiPartitionKey(
        {"year": "2011", "chunk": "bucket_00"}
    ) in keys
    assert dg.MultiPartitionKey(
        {"year": "2025", "chunk": "bucket_63"}
    ) in keys


def test_annual_account_asset_queries_requested_year_and_chunk(
    monkeypatch,
) -> None:
    clickhouse = FakeClickhouseResource([("923609016", "EQUINOR ASA")])
    captured: dict[str, Any] = {}

    def fake_materialize(**kwargs: Any) -> dict[str, int]:
        captured.update(kwargs)
        return {
            "candidate_count": 1,
            "downloaded_count": 1,
            "reused_count": 0,
            "not_found_count": 0,
            "pdf_bytes": 100,
            "json_bytes": 200,
            "page_count": 3,
            "native_text_page_count": 0,
            "ocr_page_count": 3,
        }

    monkeypatch.setattr(
        annual_accounts,
        "materialize_annual_account_documents",
        fake_materialize,
    )

    result = norway_brreg_annual_account_documents_json(
        context=dg.build_asset_context(
            partition_key=dg.MultiPartitionKey(
                {"year": "2025", "chunk": "bucket_07"}
            )
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
    assert result.metadata["ocr_page_count"] == 3


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


def test_materialization_reuses_existing_document_without_redownloading() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    api = FakeAnnualAccountApi(
        BrregAnnualAccountPdf(
            source_url="https://example.test/923609016/2025",
            body=_native_text_pdf(),
        )
    )
    candidates = [{"org_number": "923609016", "legal_name": "EQUINOR ASA"}]

    first = materialize_annual_account_documents(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="run-1",
        api=api,
        storage=storage,
        log=None,
    )
    second = materialize_annual_account_documents(
        candidates=candidates,
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="run-2",
        api=api,
        storage=storage,
        log=None,
    )

    assert first["downloaded_count"] == 1
    assert first["page_count"] == 1
    assert second["reused_count"] == 1
    assert second["downloaded_count"] == 0
    assert api.calls == [("923609016", 2025)]


def test_materialization_counts_company_year_without_a_pdf() -> None:
    result = materialize_annual_account_documents(
        candidates=[{"org_number": "923609016", "legal_name": "EQUINOR ASA"}],
        filing_year=2025,
        chunk_key="bucket_07",
        source_run_id="run-1",
        api=FakeAnnualAccountApi(None),
        storage=NorwayBrregFinancialParquetStorageResource(
            object_store=FakeObjectStore()
        ),
        log=None,
    )

    assert result["candidate_count"] == 1
    assert result["not_found_count"] == 1
    assert result["downloaded_count"] == 0


def test_native_text_pdf_does_not_invoke_ocr() -> None:
    pdf_bytes = _native_text_pdf()

    document = extract_annual_account_pdf(
        pdf_bytes,
        org_number="923609016",
        legal_name="EQUINOR ASA",
        filing_year=2025,
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
    assert document["source_pdf_url"].endswith("/923609016/2025")
    assert len(document["source_pdf_sha256"]) == 64


def test_scanned_pdf_preserves_ocr_words_coordinates_and_confidence() -> None:
    document = extract_annual_account_pdf(
        _scanned_image_pdf(),
        org_number="923609016",
        legal_name="EQUINOR ASA",
        filing_year=2025,
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
