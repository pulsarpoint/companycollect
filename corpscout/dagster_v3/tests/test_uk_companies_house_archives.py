import json
from datetime import UTC, datetime
from pathlib import Path
import zipfile

import duckdb

from dagster_v3.defs.uk_companies_house import (
    incremental,
    raw_archives,
    resources,
    tables,
)


HEADER = "CompanyName,CompanyNumber,CompanyStatus"
ROW = "ACME LTD,08209948,Active"


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.upload_count = 0
        self.created_buckets: list[str] = []

    def ensure_bucket(self, bucket: str) -> None:
        self.created_buckets.append(bucket)

    def exists(self, key: str, *, bucket: str) -> bool:
        return (bucket, key) in self.objects

    def upload_file(self, key: str, source_path: str | Path, *, bucket: str) -> None:
        self.upload_count += 1
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def download_file(self, key: str, target_path: str | Path, *, bucket: str) -> None:
        Path(target_path).write_bytes(self.objects[(bucket, key)])

    def write_json(self, key: str, body: str, *, bucket: str) -> None:
        self.objects[(bucket, key)] = body.encode("utf-8")

    def write_bytes(self, key: str, body: bytes, *, bucket: str) -> None:
        self.objects[(bucket, key)] = body

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        return self.objects[(bucket, key)]

    def list_keys(self, prefix: str, *, bucket: str) -> list[str]:
        return sorted(
            key
            for stored_bucket, key in self.objects
            if stored_bucket == bucket and key.startswith(prefix)
        )


class FakeResponse:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.text = body.decode("utf-8", errors="replace")
        self.headers = {"Content-Length": str(len(body))}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [self.body]


class FakeSession:
    def __init__(self, body: bytes | dict[str, bytes]) -> None:
        self.body = body
        self.calls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.calls.append(url)
        if isinstance(self.body, dict):
            return FakeResponse(self.body[url])
        return FakeResponse(self.body)


def _zip_bytes(filename: str, body: str | bytes) -> bytes:
    from io import BytesIO

    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(filename, body)
    return output.getvalue()


def _store_archive(
    object_store: FakeObjectStore,
    *,
    kind: str,
    published_date: str,
    source_url: str,
    body: bytes,
) -> raw_archives.StoredArchive:
    result = raw_archives.sync_archive(
        object_store=object_store,
        kind=kind,
        published_date=published_date,
        source_url=source_url,
        session=FakeSession(body),
        synced_at=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
    )
    return result.archive


def test_sync_archive_is_content_addressed_and_reuses_existing_object() -> None:
    object_store = FakeObjectStore()
    source_url = tables.DOWNLOAD_BASE_URL + "BasicCompanyDataAsOneFile-2026-07-01.zip"
    body = _zip_bytes("BasicCompanyData.csv", f"{HEADER}\n{ROW}\n")

    first = raw_archives.sync_archive(
        object_store=object_store,
        kind=raw_archives.REGISTER_KIND,
        published_date="2026-07-01",
        source_url=source_url,
        session=FakeSession(body),
        synced_at=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
    )
    second = raw_archives.sync_archive(
        object_store=object_store,
        kind=raw_archives.REGISTER_KIND,
        published_date="2026-07-01",
        source_url=source_url,
        session=FakeSession(body),
        synced_at=datetime(2026, 7, 10, 9, 0, tzinfo=UTC),
    )

    assert first.archive.object_key == second.archive.object_key
    assert "/published_date=2026-07-01/sha256=" in first.archive.object_key
    assert first.archive.object_key.endswith(
        "/BasicCompanyDataAsOneFile-2026-07-01.zip"
    )
    assert first.reused_existing is False
    assert second.reused_existing is True
    assert object_store.upload_count == 1

    metadata = json.loads(
        object_store.read_bytes(
            first.archive.metadata_key,
            bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
        )
    )
    assert metadata["source_url"] == source_url
    assert metadata["object_key"] == first.archive.object_key
    assert metadata["sha256"] == first.archive.sha256


def test_register_raw_duckdb_loads_the_latest_archive_from_object_storage(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    old_body = _zip_bytes("old.csv", f"{HEADER}\nOLD LTD,00000001,Dissolved\n")
    new_body = _zip_bytes("new.csv", f"{HEADER}\n{ROW}\n")
    _store_archive(
        object_store,
        kind=raw_archives.REGISTER_KIND,
        published_date="2026-06-01",
        source_url=tables.DOWNLOAD_BASE_URL
        + "BasicCompanyDataAsOneFile-2026-06-01.zip",
        body=old_body,
    )
    latest = _store_archive(
        object_store,
        kind=raw_archives.REGISTER_KIND,
        published_date="2026-07-01",
        source_url=tables.DOWNLOAD_BASE_URL
        + "BasicCompanyDataAsOneFile-2026-07-01.zip",
        body=new_body,
    )

    with duckdb.connect(str(tmp_path / "uk.duckdb")) as connection:
        result = resources.load_uk_companies_house_raw_from_object_store(
            connection=connection,
            object_store=object_store,
        )
        rows = connection.execute(
            f"select companynumber from {tables.DLT_DATASET_NAME}."
            f"{tables.COMPANIES_RAW_TABLE}"
        ).fetchall()

    assert result == {
        "rows": 1,
        "source_url": latest.source_url,
        "source_object_key": latest.object_key,
        "source_sha256": latest.sha256,
    }
    assert rows == [("08209948",)]


def test_incremental_metrics_processes_accounts_archives_from_object_storage(
    tmp_path: Path,
) -> None:
    from tests.test_xbrl_common import SAMPLE

    object_store = FakeObjectStore()
    archive = _store_archive(
        object_store,
        kind=raw_archives.ACCOUNTS_KIND,
        published_date="2026-07-09",
        source_url=tables.DOWNLOAD_BASE_URL + "Accounts_Bulk_Data-2026-07-09.zip",
        body=_zip_bytes("Prod_synth_01234567.html", SAMPLE),
    )

    with duckdb.connect(str(tmp_path / "accounts.duckdb")) as connection:
        counts = incremental.build_incremental_metrics(
            connection=connection,
            object_store=object_store,
            source_run_id="run-1",
        )
        cursor = incremental.read_cursor(connection)

    assert counts["processed_archives"] == [archive.published_date]
    assert counts["source_object_keys"] == [archive.object_key]
    assert counts["companies"] == 1
    assert counts["filings_parsed"] == 1
    assert cursor is None


def test_accounts_archive_selection_bootstraps_latest_then_fills_forward() -> None:
    published = [
        ("2026-07-07", "u/07.zip"),
        ("2026-07-08", "u/08.zip"),
        ("2026-07-09", "u/09.zip"),
    ]

    assert raw_archives.select_accounts_archives_to_sync(
        published,
        stored_dates=set(),
        max_archives=10,
    ) == [published[-1]]
    assert raw_archives.select_accounts_archives_to_sync(
        published,
        stored_dates={"2026-07-07"},
        max_archives=1,
    ) == [published[1]]


def test_accounts_sync_bootstraps_only_the_latest_published_archive() -> None:
    object_store = FakeObjectStore()
    old_url = tables.DOWNLOAD_BASE_URL + "Accounts_Bulk_Data-2026-07-08.zip"
    latest_url = tables.DOWNLOAD_BASE_URL + "Accounts_Bulk_Data-2026-07-09.zip"
    session = FakeSession(
        {
            tables.ACCOUNTS_INDEX_URL: (
                '<a href="Accounts_Bulk_Data-2026-07-08.zip">old</a>'
                '<a href="Accounts_Bulk_Data-2026-07-09.zip">latest</a>'
            ).encode(),
            old_url: _zip_bytes("old.html", "old"),
            latest_url: _zip_bytes("latest.html", "latest"),
        }
    )

    result = raw_archives.sync_accounts_archives(
        object_store=object_store,
        max_archives=10,
        session=session,
        synced_at=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
    )

    assert [item.archive.published_date for item in result.archives] == ["2026-07-09"]
    assert latest_url in session.calls
    assert old_url not in session.calls


def test_api_accounts_documents_are_persisted_with_provenance() -> None:
    from dagster_v3.defs.uk_companies_house import documents_api
    from tests.test_xbrl_common import SAMPLE

    class ApiResponse:
        def __init__(self, *, payload: dict | None = None, content: bytes = b"") -> None:
            self.payload = payload
            self.content = content

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict | None:
            return self.payload

    class ApiSession:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get(
            self,
            url: str,
            *,
            auth: tuple[str, str] | None = None,
            timeout: int | None = None,
            headers: dict[str, str] | None = None,
        ) -> ApiResponse:
            self.calls.append(url)
            if "filing-history" in url:
                return ApiResponse(
                    payload={
                        "items": [
                            {
                                "date": "2024-12-31",
                                "links": {
                                    "document_metadata": "https://doc-api/document/OLD"
                                },
                            },
                            {
                                "date": "2025-12-31",
                                "links": {
                                    "document_metadata": "https://doc-api/document/NEW"
                                },
                            },
                        ]
                    }
                )
            if url.endswith("/content"):
                return ApiResponse(content=SAMPLE.encode("utf-8"))
            return ApiResponse(
                payload={
                    "resources": {"application/xhtml+xml": {}},
                    "links": {"document": url + "/content"},
                }
            )

    object_store = FakeObjectStore()
    session = ApiSession()
    result = documents_api.sync_api_accounts_documents(
        object_store=object_store,
        company_numbers=["01234567"],
        run_id="run-1",
        client=documents_api.CompaniesHouseClient("KEY", session=session),
        request_delay_seconds=0,
        retrieved_at=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
    )
    catalog = documents_api.read_api_accounts_batch_catalog(
        object_store=object_store,
        run_id="run-1",
    )
    second = documents_api.sync_api_accounts_documents(
        object_store=object_store,
        company_numbers=["01234567"],
        run_id="run-2",
        client=documents_api.CompaniesHouseClient("KEY", session=session),
        request_delay_seconds=0,
        retrieved_at=datetime(2026, 7, 10, 9, 0, tzinfo=UTC),
    )
    second_catalog = documents_api.read_api_accounts_batch_catalog(
        object_store=object_store,
        run_id="run-2",
    )

    assert result.requested == 1
    assert result.stored == 1
    assert result.missing == 0
    assert len(catalog.documents) == 1
    document = catalog.documents[0]
    assert document.company_number == "01234567"
    assert document.filing_date == "2025-12-31"
    assert document.metadata_url == "https://doc-api/document/NEW"
    assert document.document_url == "https://doc-api/document/NEW/content"
    assert "/company_number=01234567/filing_date=2025-12-31/sha256=" in document.object_key
    assert object_store.read_bytes(
        document.object_key,
        bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
    ) == SAMPLE.encode("utf-8")
    assert second.reused == 1
    assert second_catalog.documents[0].object_key == document.object_key
    assert any(url.endswith("/document/NEW/content") for url in session.calls)
    assert not any(url.endswith("/document/OLD/content") for url in session.calls)


def test_api_financial_metrics_are_built_from_the_s3_batch_catalog(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.uk_companies_house import documents_api
    from tests.test_xbrl_common import SAMPLE

    object_store = FakeObjectStore()
    document = documents_api.StoredApiAccountsDocument.from_content(
        company_number="01234567",
        filing_date="2025-12-31",
        metadata_url="https://doc-api/document/NEW",
        document_url="https://doc-api/document/NEW/content",
        content_type="application/xhtml+xml",
        content=SAMPLE.encode("utf-8"),
        retrieved_at=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
    )
    documents_api.write_api_accounts_batch_catalog(
        object_store=object_store,
        run_id="run-1",
        requested_company_numbers=("01234567",),
        documents=(document,),
        missing_company_numbers=(),
        created_at=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
    )
    object_store.write_bytes(
        document.object_key,
        SAMPLE.encode("utf-8"),
        bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
    )

    with duckdb.connect(str(tmp_path / "api.duckdb")) as connection:
        counts = documents_api.load_api_financial_metrics_from_object_store(
            connection=connection,
            object_store=object_store,
            run_id="run-1",
            source_run_id="run-1",
        )
        row = connection.execute(
            f"select company_number, source_slug, revenue_amount_original "
            f"from {tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_TABLE}"
        ).fetchone()

    assert counts == {
        "companies": 1,
        "with_revenue": 1,
        "requested": 1,
        "stored_documents": 1,
        "parsed_documents": 1,
        "missing": 0,
        "parse_failed": 0,
    }
    assert row == ("01234567", "uk_companies_house_accounts_api", 1234000)


def test_api_accounts_catalog_records_missing_ixbrl_documents() -> None:
    from dagster_v3.defs.uk_companies_house import documents_api

    class MissingClient:
        def latest_accounts_ixbrl_document(
            self,
            company_number: str,
        ) -> None:
            return None

    object_store = FakeObjectStore()
    result = documents_api.sync_api_accounts_documents(
        object_store=object_store,
        company_numbers=["99999999"],
        run_id="run-missing",
        client=MissingClient(),
        request_delay_seconds=0,
        retrieved_at=datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
    )
    catalog = documents_api.read_api_accounts_batch_catalog(
        object_store=object_store,
        run_id="run-missing",
    )

    assert result.stored == 0
    assert result.missing == 1
    assert catalog.documents == ()
    assert catalog.missing_company_numbers == ("99999999",)
