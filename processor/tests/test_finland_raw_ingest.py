import json
from io import BytesIO
from zipfile import ZipFile

import polars as pl

from finland_raw_ingest import (
    FINLAND_PRH_YTJ_BUCKET,
    download_ytj_full_and_base_to_s3,
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
    def __init__(self, payload: dict | None = None, content: bytes | None = None, status_code: int = 200) -> None:
        self._payload = payload
        self.content = content if content is not None else json.dumps(payload or {}).encode("utf-8")
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: int) -> FakeResponse:
        self.calls.append((url, params))
        if url.endswith("/all_companies"):
            payload = {
                "companies": [
                    {"businessId": {"value": "old"}, "registrationDate": "2023-12-31"},
                    {"businessId": {"value": "base-1"}, "registrationDate": "2024-01-01"},
                    {"businessId": {"value": "base-2"}, "registrationDate": "2024-06-15"},
                    {"businessId": {"value": "today"}, "registrationDate": "2026-06-15"},
                ]
            }
            return FakeResponse(payload={}, content=_zip_json("companies.json", payload))
        raise AssertionError(f"unexpected URL {url}")


def _zip_json(name: str, payload: dict) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(name, json.dumps(payload))
    return buffer.getvalue()


def test_download_ytj_full_and_base_to_s3_downloads_once_and_filters_base() -> None:
    s3 = FakeS3()
    session = FakeSession()

    result = download_ytj_full_and_base_to_s3(
        s3=s3,
        session=session,
        start_date="2024-01-01",
        today="2026-06-15",
        refresh=False,
    )

    full_key = "full/date=2026-06-15/companies.json"
    base_key = "base/start_date=2024-01-01/end_date=2026-06-15/base.json"
    parquet_key = "base/start_date=2024-01-01/end_date=2026-06-15/base.parquet"
    manifest_key = "base/start_date=2024-01-01/end_date=2026-06-15/manifest.json"

    assert result["full_key"] == full_key
    assert result["base_key"] == base_key
    assert result["parquet_key"] == parquet_key
    assert result["full_downloaded"] is True
    assert result["full_skipped"] is False
    assert result["base_skipped"] is False
    assert result["parquet_downloaded"] is True
    assert result["full_count"] == 4
    assert result["base_count"] == 2
    assert session.calls == [("https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies", {})]

    full_payload = json.loads(s3.objects[(FINLAND_PRH_YTJ_BUCKET, full_key)])
    base_payload = json.loads(s3.objects[(FINLAND_PRH_YTJ_BUCKET, base_key)])
    manifest = json.loads(s3.objects[(FINLAND_PRH_YTJ_BUCKET, manifest_key)])
    parquet = pl.read_parquet(BytesIO(s3.objects[(FINLAND_PRH_YTJ_BUCKET, parquet_key)]))

    assert len(full_payload["companies"]) == 4
    assert [company["businessId"]["value"] for company in base_payload["companies"]] == ["base-1", "base-2"]
    assert manifest["base_count"] == 2
    assert parquet["business_id"].to_list() == ["base-1", "base-2"]
    assert parquet["lifecycle_status"].to_list() == ["active", "active"]


def test_download_ytj_full_and_base_to_s3_skips_when_base_json_exists() -> None:
    s3 = FakeS3()
    base_key = "base/start_date=2024-01-01/end_date=2026-06-15/base.json"
    parquet_key = "base/start_date=2024-01-01/end_date=2026-06-15/base.parquet"
    s3.objects[(FINLAND_PRH_YTJ_BUCKET, base_key)] = json.dumps(
        {
            "country": "FI",
            "source": "finland_prhytj",
            "start_date": "2024-01-01",
            "end_date": "2026-06-15",
            "companies": [
                {
                    "businessId": {"value": "base-1"},
                    "registrationDate": "2024-01-01",
                    "tradeRegisterStatus": "2",
                    "status": "2",
                    "endDate": None,
                    "names": [{"name": "Base One Oy", "type": "1", "endDate": None}],
                }
            ]
        }
    ).encode("utf-8")
    session = FakeSession()

    result = download_ytj_full_and_base_to_s3(
        s3=s3,
        session=session,
        start_date="2024-01-01",
        today="2026-06-15",
        refresh=False,
    )

    assert result["full_downloaded"] is False
    assert result["full_skipped"] is True
    assert result["base_skipped"] is True
    assert result["parquet_downloaded"] is True
    assert result["base_count"] == 1
    assert session.calls == []
    parquet = pl.read_parquet(BytesIO(s3.objects[(FINLAND_PRH_YTJ_BUCKET, parquet_key)]))
    assert parquet.row(0, named=True) == {
        "business_id": "base-1",
        "registration_date": "2024-01-01",
        "end_date": None,
        "trade_register_status": "2",
        "status": "2",
        "lifecycle_status": "active",
        "is_active": True,
        "legal_name": "Base One Oy",
    }
