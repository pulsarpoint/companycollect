import json
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import dagster as dg
import pytest
import requests


class _FakeResponse:
    def __init__(
        self,
        *,
        payload: dict[str, object] | None = None,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self._body = body
        self.headers = headers or {}
        self.status_code = status_code

    def json(self) -> dict[str, object]:
        if self._payload is None:
            raise ValueError("response has no JSON payload")
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int):
        for offset in range(0, len(self._body), chunk_size):
            yield self._body[offset : offset + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _FakeSession:
    def __init__(self, catalog: dict[str, object], downloads: dict[str, bytes]) -> None:
        self.catalog = catalog
        self.downloads = downloads
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if "collections/belagenhetsadresser/items" in url:
            return _FakeResponse(payload=self.catalog)
        body = self.downloads[url]
        return _FakeResponse(
            body=body,
            headers={"Content-Length": str(len(body)), "Content-Type": "application/zip"},
        )


class _DeniedDownloadSession(_FakeSession):
    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        if "collections/belagenhetsadresser/items" in url:
            return super().get(url, **kwargs)
        self.calls.append({"url": url, **kwargs})
        response = requests.Response()
        response.status_code = 403
        response.url = url
        raise requests.HTTPError("403 Client Error", response=response)


class _FakeObjectStore:
    def __init__(self) -> None:
        self.bucket: str | None = None
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self, bucket: str) -> None:
        self.bucket = bucket

    def exists(self, key: str, bucket: str) -> bool:
        assert bucket == self.bucket
        return key in self.objects

    def upload_file(self, key: str, source_path: str | Path, bucket: str) -> None:
        assert bucket == self.bucket
        self.objects[key] = Path(source_path).read_bytes()

    def write_json(self, key: str, body: str, bucket: str) -> None:
        assert bucket == self.bucket
        self.objects[key] = body.encode("utf-8")

    def read_bytes(self, key: str, bucket: str) -> bytes:
        assert bucket == self.bucket
        return self.objects[key]


def _zip_body(name: str) -> bytes:
    output = BytesIO()
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(f"{name}.gpkg", b"geopackage-fixture")
    return output.getvalue()


def _stac_item(code: str, updated: str, body: bytes) -> dict[str, object]:
    url = (
        "https://dl1.lantmateriet.se/adress/belagenhetsadresser/"
        f"belagenhetsadresser_kn{code}.zip"
    )
    return {
        "id": code,
        "type": "Feature",
        "collection": "belagenhetsadresser",
        "properties": {
            "title": f"Belägenhetsadresser för kommun {code}",
            "created": "2025-02-03T00:00:00Z",
            "updated": updated,
            "datetime": updated,
            "lanskod": code[:2],
            "proj:epsg": 3006,
        },
        "assets": {
            "data": {
                "href": url,
                "type": "application/zip",
                "title": f"belagenhetsadresser_kn{code}",
                "roles": ["data"],
                "file:size": len(body),
            }
        },
    }


def test_lantmateriet_stac_catalog_parses_municipality_archives() -> None:
    from dagster_v3.defs.sweden_address_geocoding.source import parse_stac_items

    items = parse_stac_items(
        {
            "type": "FeatureCollection",
            "features": [
                _stac_item(
                    "2580",
                    "2026-08-07T22:59:21.070000Z",
                    _zip_body("2580"),
                ),
                _stac_item(
                    "0140",
                    "2026-08-07T22:01:46.583000Z",
                    _zip_body("0140"),
                ),
            ],
            "links": [],
        }
    )

    assert [item.municipality_code for item in items] == ["0140", "2580"]
    assert items[0].county_code == "01"
    assert items[0].source_epsg == 3006
    assert items[0].file_name == "belagenhetsadresser_kn0140.zip"
    assert items[1].source_updated_at.isoformat() == "2026-08-07T22:59:21.070000+00:00"


def test_lantmateriet_snapshot_archives_files_and_run_manifest() -> None:
    from dagster_v3.defs.sweden_address_geocoding.source import (
        LantmaterietAddressResource,
        SWEDEN_LANTMATERIET_ADDRESS_BUCKET,
    )

    bodies = {
        "2580": _zip_body("2580"),
        "0140": _zip_body("0140"),
    }
    catalog = {
        "type": "FeatureCollection",
        "features": [
            _stac_item("2580", "2026-08-07T22:59:21.070000Z", bodies["2580"]),
            _stac_item("0140", "2026-08-07T22:01:46.583000Z", bodies["0140"]),
        ],
        "links": [],
    }
    downloads = {
        str(item["assets"]["data"]["href"]): bodies[str(item["id"])]
        for item in catalog["features"]
    }
    session = _FakeSession(catalog, downloads)
    object_store = _FakeObjectStore()
    resource = LantmaterietAddressResource(
        username="api-user",
        password="api-password",
        expected_municipality_count=2,
        download_max_attempts=1,
    )

    result = resource.download_snapshot(
        object_store=object_store,
        run_id="test-run",
        session=session,
    )

    assert result.metadata["municipality_count"] == 2
    assert result.metadata["downloaded_file_count"] == 2
    assert object_store.bucket == SWEDEN_LANTMATERIET_ADDRESS_BUCKET
    manifest_key = str(result.metadata["manifest_key"])
    manifest = json.loads(object_store.objects[manifest_key])
    assert manifest["collection"] == "belagenhetsadresser"
    assert manifest["municipality_count"] == 2
    assert {row["municipality_code"] for row in manifest["files"]} == {
        "0140",
        "2580",
    }
    download_calls = [
        call
        for call in session.calls
        if "dl1.lantmateriet.se" in str(call["url"])
    ]
    assert len(download_calls) == 2
    assert all(call["auth"] == ("api-user", "api-password") for call in download_calls)

    second_result = resource.download_snapshot(
        object_store=object_store,
        run_id="test-run-2",
        session=session,
    )

    assert second_result.metadata["downloaded_file_count"] == 0
    assert second_result.metadata["reused_file_count"] == 2
    assert len(
        [call for call in session.calls if "dl1.lantmateriet.se" in str(call["url"])]
    ) == 2


def test_lantmateriet_asset_and_weekly_schedule_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    node = repo.asset_graph.get(dg.AssetKey("sweden_lantmateriet_address_archives_s3"))
    schedule = repo.get_schedule_def("sweden_lantmateriet_addresses_weekly")

    assert node.group_name == "sweden_address_geocoding"
    assert node.partitions_def is None
    assert schedule.job.name == "sweden_lantmateriet_addresses_job"
    assert schedule.cron_schedule == "40 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_lantmateriet_permission_denial_fails_without_retrying() -> None:
    from dagster_v3.defs.sweden_address_geocoding.source import (
        LantmaterietAddressResource,
    )

    body = _zip_body("0114")
    catalog = {
        "type": "FeatureCollection",
        "features": [
            _stac_item("0114", "2026-08-07T22:01:46.583000Z", body),
        ],
        "links": [],
    }
    source_url = str(catalog["features"][0]["assets"]["data"]["href"])
    session = _DeniedDownloadSession(catalog, {source_url: body})
    resource = LantmaterietAddressResource(
        username="api-user",
        password="api-password",
        expected_municipality_count=1,
        download_max_attempts=4,
        download_retry_base_seconds=0,
    )

    with pytest.raises(PermissionError, match="needs approved access"):
        resource.download_snapshot(
            object_store=_FakeObjectStore(),
            run_id="denied-run",
            session=session,
        )

    download_calls = [call for call in session.calls if call["url"] == source_url]
    assert len(download_calls) == 1


def test_lantmateriet_credentials_are_documented_without_values() -> None:
    env_example = (
        Path(__file__).resolve().parents[1] / ".env.example"
    ).read_text(encoding="utf-8")

    assert "LANTMATERIET_USERNAME=" in env_example
    assert "LANTMATERIET_PASSWORD=" in env_example
    assert "goran.raovic" not in env_example


def test_sweden_address_design_starts_with_official_raw_snapshot() -> None:
    design = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "sweden_address_geocoding"
        / "docs"
        / "sweden_address_geocoding-design.md"
    ).read_text(encoding="utf-8")

    assert "290 municipality" in design
    assert "STAC" in design
    assert "RustFS" in design
    assert "GeoNames" in design
    assert "fallback" in design
