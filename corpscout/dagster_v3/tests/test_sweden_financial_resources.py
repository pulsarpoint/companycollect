from pathlib import Path

from dagster_v3.defs.sweden_financial.resources import (
    SWEDEN_FINANCIAL_RAW_BUCKET,
    SwedenFinancialReportsResource,
    archive_object_key,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []

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


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> None:
        self._body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
        }

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0) -> list[bytes]:
        return [self._body[:2], self._body[2:]]

    @property
    def text(self) -> str:
        return self._body.decode("utf-8")


class FakeSession:
    def __init__(self, responses_by_url: dict[str, bytes]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.requested_urls.append(url)
        return FakeResponse(self.responses_by_url[url])


class ListingOnlySession:
    def __init__(self, responses_by_url: dict[str, bytes]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.requested_urls.append(url)
        if stream:
            raise AssertionError(f"unexpected archive download for {url}")
        return FakeResponse(self.responses_by_url[url])


def test_sweden_financial_reports_downloads_missing_archives_to_s3() -> None:
    resource = SwedenFinancialReportsResource()
    listing_url = resource.listing_url()
    archive_url = (
        f"{resource.archive_base_url}/arsredovisningar/2020/08_2.zip"
    )
    object_store = FakeObjectStore()
    session = FakeSession(
        {
            listing_url: _listing_xml(
                key="arsredovisningar/2020/08_2.zip",
                last_modified="2025-02-07T09:13:53.713Z",
                etag='"1bb9cb50"',
                size=6,
            ),
            archive_url: b"zip-bytes",
        }
    )

    result = resource.download_raw_archives(
        object_store=object_store,
        session=session,
    )

    expected_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    assert object_store.created_buckets == [SWEDEN_FINANCIAL_RAW_BUCKET]
    assert object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, expected_key)] == b"zip-bytes"
    assert object_store.uploaded_files == [(SWEDEN_FINANCIAL_RAW_BUCKET, expected_key)]
    assert session.requested_urls == [listing_url, archive_url]
    assert result.metadata["archive_count"] == 1
    assert result.metadata["downloaded_archive_count"] == 1
    assert result.metadata["reused_archive_count"] == 0
    assert result.metadata["downloaded_size_bytes"] == len(b"zip-bytes")
    assert result.metadata["sample_s3_keys"] == [expected_key]


def test_sweden_financial_reports_skips_existing_last_modified_archives() -> None:
    resource = SwedenFinancialReportsResource()
    listing_url = resource.listing_url()
    existing_key = archive_object_key(
        upstream_key="arsredovisningar/2020/08_2.zip",
        source_last_modified="2025-02-07T09:13:53.713Z",
    )
    object_store = FakeObjectStore()
    object_store.objects[(SWEDEN_FINANCIAL_RAW_BUCKET, existing_key)] = b"already-there"
    session = ListingOnlySession(
        {
            listing_url: _listing_xml(
                key="arsredovisningar/2020/08_2.zip",
                last_modified="2025-02-07T09:13:53.713Z",
                etag='"1bb9cb50"',
                size=6,
            ),
        }
    )

    result = resource.download_raw_archives(
        object_store=object_store,
        session=session,
    )

    assert object_store.uploaded_files == []
    assert session.requested_urls == [listing_url]
    assert result.metadata["archive_count"] == 1
    assert result.metadata["downloaded_archive_count"] == 0
    assert result.metadata["reused_archive_count"] == 1


def test_sweden_financial_reports_follows_listing_pagination() -> None:
    resource = SwedenFinancialReportsResource()
    first_url = resource.listing_url()
    second_url = resource.listing_url(marker="arsredovisningar/2020/08_2.zip")
    first_archive_url = (
        f"{resource.archive_base_url}/arsredovisningar/2020/08_2.zip"
    )
    second_archive_url = (
        f"{resource.archive_base_url}/arsredovisningar/2021/01_1.zip"
    )
    object_store = FakeObjectStore()
    session = FakeSession(
        {
            first_url: _listing_xml(
                key="arsredovisningar/2020/08_2.zip",
                last_modified="2025-02-07T09:13:53.713Z",
                etag='"first"',
                size=6,
                next_marker="arsredovisningar/2020/08_2.zip",
            ),
            second_url: _listing_xml(
                key="arsredovisningar/2021/01_1.zip",
                last_modified="2025-02-10T10:00:00.000Z",
                etag='"second"',
                size=9,
            ),
            first_archive_url: b"first",
            second_archive_url: b"second",
        }
    )

    result = resource.download_raw_archives(
        object_store=object_store,
        session=session,
    )

    assert session.requested_urls == [
        first_url,
        second_url,
        first_archive_url,
        second_archive_url,
    ]
    assert result.metadata["archive_count"] == 2
    assert result.metadata["downloaded_archive_count"] == 2


def _listing_xml(
    *,
    key: str,
    last_modified: str,
    etag: str,
    size: int,
    next_marker: str | None = None,
) -> bytes:
    next_marker_xml = (
        f"<NextMarker>{next_marker}</NextMarker>" if next_marker is not None else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>bulkfil-paketering-zipfiler-prod</Name>
  <Prefix>arsredovisningar/</Prefix>
  <Contents>
    <Key>{key}</Key>
    <LastModified>{last_modified}</LastModified>
    <ETag>{etag}</ETag>
    <Size>{size}</Size>
  </Contents>
  {next_marker_xml}
</ListBucketResult>
""".encode("utf-8")
