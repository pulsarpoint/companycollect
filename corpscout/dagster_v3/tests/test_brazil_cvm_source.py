from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_cvm.source import (
    BRAZIL_CVM_RAW_BUCKET,
    BrazilCvmDfpResource,
    dfp_archive_object_key,
    dfp_metadata_object_key,
    dfp_source_url,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []
        self.written_json: list[tuple[str, str]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.uploaded_files.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        assert bucket is not None
        self.written_json.append((bucket, key))
        self.objects[(bucket, key)] = body.encode("utf-8")


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        content_type: str = "application/zip",
        last_modified: str = "Sun, 28 Jun 2026 07:13:00 GMT",
    ) -> None:
        self._body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
            "Last-Modified": last_modified,
        }

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0) -> list[bytes]:
        return [self._body[:3], self._body[3:]]


class FakeSession:
    def __init__(self, responses_by_url: dict[str, bytes]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requested_urls.append(url)
        assert kwargs["stream"] is True
        assert isinstance(kwargs["timeout"], int)
        return FakeResponse(self.responses_by_url[url])


class NoDownloadSession:
    def __init__(self) -> None:
        self.requested_urls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requested_urls.append(url)
        raise AssertionError(f"unexpected HTTP request: {url}")


def test_dfp_source_url_and_object_keys_are_deterministic() -> None:
    assert (
        dfp_source_url("2026")
        == "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2026.zip"
    )
    assert (
        dfp_archive_object_key("2026")
        == "brazil_cvm/dfp/raw_archives/year=2026/archive.zip"
    )
    assert (
        dfp_metadata_object_key("2026")
        == "brazil_cvm/dfp/raw_archives/year=2026/metadata.json"
    )


def test_brazil_cvm_dfp_resource_downloads_missing_year_archive() -> None:
    resource = BrazilCvmDfpResource()
    object_store = FakeObjectStore()
    url = dfp_source_url("2026")
    session = FakeSession({url: b"zip-body"})

    result = resource.sync_year_archive(
        year="2026",
        object_store=object_store,
        session=session,
    )

    archive_key = dfp_archive_object_key("2026")
    metadata_key = dfp_metadata_object_key("2026")
    assert object_store.created_buckets == [BRAZIL_CVM_RAW_BUCKET]
    assert session.requested_urls == [url]
    assert object_store.uploaded_files == [(BRAZIL_CVM_RAW_BUCKET, archive_key)]
    assert object_store.objects[(BRAZIL_CVM_RAW_BUCKET, archive_key)] == b"zip-body"
    assert object_store.written_json == [(BRAZIL_CVM_RAW_BUCKET, metadata_key)]
    assert result.downloaded is True
    assert result.reused_existing_archive is False
    assert result.year == "2026"
    assert result.source_url == url
    assert result.archive_key == archive_key
    assert result.metadata_key == metadata_key
    assert result.size_bytes == len(b"zip-body")
    assert result.sha256


def test_brazil_cvm_dfp_resource_skips_existing_year_archive_without_http() -> None:
    resource = BrazilCvmDfpResource()
    object_store = FakeObjectStore()
    archive_key = dfp_archive_object_key("2026")
    object_store.objects[(BRAZIL_CVM_RAW_BUCKET, archive_key)] = b"already-there"
    session = NoDownloadSession()

    result = resource.sync_year_archive(
        year="2026",
        object_store=object_store,
        session=session,
    )

    assert session.requested_urls == []
    assert object_store.uploaded_files == []
    assert object_store.written_json == []
    assert result.downloaded is False
    assert result.reused_existing_archive is True
    assert result.year == "2026"
    assert result.archive_key == archive_key
    assert result.size_bytes is None
    assert result.sha256 is None
