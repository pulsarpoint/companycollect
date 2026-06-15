import json
from io import BytesIO

from finland_raw_ingest import (
    FINLAND_PRH_XBRL_BUCKET,
    FINLAND_PRH_YTJ_BUCKET,
    download_xbrl_window_to_s3,
    download_ytj_snapshot_to_s3,
)


class FakeClientError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: set[str] = set()

    def create_bucket(self, Bucket: str) -> None:
        self.created_buckets.add(Bucket)

    def head_object(self, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            raise FakeClientError("404")

    def put_object(self, Bucket: str, Key: str, Body: bytes | str) -> None:
        body = Body.encode("utf-8") if isinstance(Body, str) else Body
        self.objects[(Bucket, Key)] = body

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}


class FakeResponse:
    def __init__(self, payload: dict | None = None, content: bytes = b"", status_code: int = 200) -> None:
        self._payload = payload or {}
        self.content = content
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: int) -> FakeResponse:
        self.calls.append((url, params))
        if url.endswith("/companies"):
            return FakeResponse(
                {
                    "totalResults": 2,
                    "companies": [
                        {"businessId": {"value": "1234567-8"}, "names": []},
                        {"businessId": {"value": "8765432-1"}, "names": []},
                    ],
                }
            )
        if url.endswith("/all_financial_statements"):
            return FakeResponse(
                {
                    "totalResults": 1,
                    "financials": [
                        {
                            "businessId": "1234567-8",
                            "financialDate": "2024-12-31",
                            "registrationDate": "2025-01-02",
                        }
                    ],
                }
            )
        if url.endswith("/financial"):
            return FakeResponse(content=b"<xbrl/>")
        raise AssertionError(f"unexpected URL {url}")


def test_ytj_snapshot_skips_existing_s3_object() -> None:
    s3 = FakeS3()
    key = "snapshots/2026-06-15/source.ndjson"
    s3.objects[(FINLAND_PRH_YTJ_BUCKET, key)] = b'{"existing":true}\n'
    session = FakeSession()

    result = download_ytj_snapshot_to_s3(
        s3=s3,
        session=session,
        snapshot_date="2026-06-15",
        max_companies=2,
        refresh=False,
    )

    assert result["source_key"] == key
    assert result["skipped"] is True
    assert result["downloaded"] is False
    assert result["company_count"] == 1
    assert session.calls == []


def test_ytj_snapshot_downloads_and_writes_manifest() -> None:
    s3 = FakeS3()

    result = download_ytj_snapshot_to_s3(
        s3=s3,
        session=FakeSession(),
        snapshot_date="2026-06-15",
        max_companies=1,
        refresh=False,
    )

    source = s3.objects[(FINLAND_PRH_YTJ_BUCKET, "snapshots/2026-06-15/source.ndjson")]
    manifest = json.loads(s3.objects[(FINLAND_PRH_YTJ_BUCKET, "snapshots/2026-06-15/manifest.json")])
    assert json.loads(source.splitlines()[0])["businessId"]["value"] == "1234567-8"
    assert result["company_count"] == 1
    assert result["downloaded"] is True
    assert manifest["company_count"] == 1


def test_xbrl_window_skips_existing_xml_and_downloads_missing_listing() -> None:
    s3 = FakeS3()
    xml_key = "companies/1234567-8/2024-12-31.xml"
    s3.objects[(FINLAND_PRH_XBRL_BUCKET, xml_key)] = b"<existing/>"
    session = FakeSession()

    result = download_xbrl_window_to_s3(
        s3=s3,
        session=session,
        registered_start="2025-01-01",
        registered_end="2025-01-03",
        refresh=False,
    )

    listing = json.loads(s3.objects[(FINLAND_PRH_XBRL_BUCKET, "windows/2025-01-01_2025-01-03/listing.json")])
    manifest = json.loads(s3.objects[(FINLAND_PRH_XBRL_BUCKET, "windows/2025-01-01_2025-01-03/manifest.json")])
    assert listing["documents"][0]["object_key"] == xml_key
    assert s3.objects[(FINLAND_PRH_XBRL_BUCKET, xml_key)] == b"<existing/>"
    assert result["document_count"] == 1
    assert result["downloaded_count"] == 0
    assert result["skipped_count"] == 1
    assert manifest["skipped_count"] == 1
