import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster_v3.defs.sweden_company.resources import (
    SWEDEN_COMPANY_RAW_BUCKET,
    SwedenCompanyBulkResource,
    manifest_for_run,
    manifest_object_key,
    raw_file_object_key,
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
        self, key: str, source_path: str | Path, bucket: str | None = None
    ) -> None:
        assert bucket is not None
        self.uploaded_files.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body.encode("utf-8")

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return [
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        ]

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]


def _latest_manifest_object_key(*, retrieved_date: str) -> str:
    return f"sweden_company/raw/retrieved_date={retrieved_date}/manifest.json"


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        last_modified: str = "Mon, 29 Jun 2026 01:27:14 GMT",
    ) -> None:
        self._body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": "application/zip",
            "Last-Modified": last_modified,
        }
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0) -> list[bytes]:
        return [self._body[:2], self._body[2:]]


class FakeSession:
    def __init__(self, bodies_by_url: dict[str, bytes]) -> None:
        self.bodies_by_url = bodies_by_url
        self.requested_urls: list[str] = []
        self.requested_head_urls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.requested_urls.append(url)
        return FakeResponse(self.bodies_by_url[url])

    def head(self, url: str, *, timeout: int) -> FakeResponse:
        self.requested_head_urls.append(url)
        return FakeResponse(self.bodies_by_url[url])


class HeadOnlySession:
    def __init__(self, bodies_by_url: dict[str, bytes]) -> None:
        self.bodies_by_url = bodies_by_url
        self.requested_head_urls: list[str] = []

    def head(self, url: str, *, timeout: int) -> FakeResponse:
        self.requested_head_urls.append(url)
        return FakeResponse(self.bodies_by_url[url])

    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any:
        raise AssertionError(f"unexpected download for {url}")


def test_sweden_company_snapshot_downloads_missing_files_to_s3() -> None:
    retrieved_at = datetime(2026, 7, 3, 10, 30, tzinfo=UTC)
    object_store = FakeObjectStore()
    resource = SwedenCompanyBulkResource()
    session = FakeSession(
        {
            resource.scb_bulk_url: b"scb-zip",
            resource.bolagsverket_bulk_url: b"bolag-zip",
        }
    )

    result = resource.download_snapshot(
        object_store=object_store,
        run_id="run-1",
        retrieved_at=retrieved_at,
        session=session,
    )

    scb_key = raw_file_object_key(
        source_slug="scb_bulkfil",
        source_last_modified="2026-06-29T01-27-14Z",
    )
    bolagsverket_key = raw_file_object_key(
        source_slug="bolagsverket_bulkfil",
        source_last_modified="2026-06-29T01-27-14Z",
    )
    manifest_key = manifest_object_key(retrieved_date="2026-07-03", run_id="run-1")
    latest_manifest_key = _latest_manifest_object_key(retrieved_date="2026-07-03")

    assert object_store.created_buckets == [SWEDEN_COMPANY_RAW_BUCKET]
    assert object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, scb_key)] == b"scb-zip"
    assert object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, bolagsverket_key)] == b"bolag-zip"
    assert (SWEDEN_COMPANY_RAW_BUCKET, manifest_key) in object_store.objects
    assert (SWEDEN_COMPANY_RAW_BUCKET, latest_manifest_key) in object_store.objects
    assert session.requested_urls == [
        resource.scb_bulk_url,
        resource.bolagsverket_bulk_url,
    ]
    assert session.requested_head_urls == [
        resource.scb_bulk_url,
        resource.bolagsverket_bulk_url,
    ]
    assert result.metadata["downloaded_file_count"] == 2
    assert result.metadata["reused_file_count"] == 0
    assert result.metadata["total_size_bytes"] == len(b"scb-zip") + len(b"bolag-zip")
    assert result.metadata["manifest_key"] == manifest_key

    manifest = json.loads(object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, manifest_key)])
    assert manifest["source"] == "sweden_company"
    assert manifest["run_id"] == "run-1"
    assert {item["source_slug"] for item in manifest["files"]} == {
        "scb_bulkfil",
        "bolagsverket_bulkfil",
    }
    assert all(
        item["source_last_modified"] == "2026-06-29T01-27-14Z"
        for item in manifest["files"]
    )
    assert all(item["downloaded"] is True for item in manifest["files"])


def test_sweden_company_snapshot_skips_existing_source_date_keys() -> None:
    retrieved_at = datetime(2026, 7, 3, 11, 0, tzinfo=UTC)
    object_store = FakeObjectStore()
    for source_slug in ("scb_bulkfil", "bolagsverket_bulkfil"):
        object_store.objects[
            (
                SWEDEN_COMPANY_RAW_BUCKET,
                raw_file_object_key(
                    source_slug=source_slug,
                    source_last_modified="2026-06-29T01-27-14Z",
                ),
            )
        ] = b"already-there"
    resource = SwedenCompanyBulkResource()
    session = HeadOnlySession(
        {
            resource.scb_bulk_url: b"scb-zip",
            resource.bolagsverket_bulk_url: b"bolag-zip",
        }
    )

    result = resource.download_snapshot(
        object_store=object_store,
        run_id="run-2",
        retrieved_at=retrieved_at,
        session=session,
    )

    assert object_store.uploaded_files == []
    assert session.requested_head_urls == [
        resource.scb_bulk_url,
        resource.bolagsverket_bulk_url,
    ]
    assert result.metadata["downloaded_file_count"] == 0
    assert result.metadata["reused_file_count"] == 2
    manifest = json.loads(
        object_store.objects[
            (
                SWEDEN_COMPANY_RAW_BUCKET,
                manifest_object_key(retrieved_date="2026-07-03", run_id="run-2"),
            )
        ]
    )
    assert all(item["downloaded"] is False for item in manifest["files"])


def test_sweden_company_snapshot_preserves_same_day_run_manifests() -> None:
    object_store = FakeObjectStore()
    resource = SwedenCompanyBulkResource()

    for run_id, retrieved_at in (
        ("morning-run", datetime(2026, 7, 3, 8, 0, tzinfo=UTC)),
        ("afternoon-run", datetime(2026, 7, 3, 15, 0, tzinfo=UTC)),
    ):
        session = FakeSession(
            {
                resource.scb_bulk_url: f"scb-zip-{run_id}".encode(),
                resource.bolagsverket_bulk_url: f"bolag-zip-{run_id}".encode(),
            }
        )
        resource.download_snapshot(
            object_store=object_store,
            run_id=run_id,
            retrieved_at=retrieved_at,
            session=session,
        )

    morning_key = manifest_object_key(
        retrieved_date="2026-07-03",
        run_id="morning-run",
    )
    afternoon_key = manifest_object_key(
        retrieved_date="2026-07-03",
        run_id="afternoon-run",
    )

    assert (SWEDEN_COMPANY_RAW_BUCKET, morning_key) in object_store.objects
    assert (SWEDEN_COMPANY_RAW_BUCKET, afternoon_key) in object_store.objects
    assert manifest_for_run(object_store, "morning-run")["run_id"] == "morning-run"
    assert manifest_for_run(object_store, "afternoon-run")["run_id"] == "afternoon-run"


def test_sweden_company_manifest_for_run_prefers_current_run() -> None:
    object_store = FakeObjectStore()
    object_store.write_json(
        manifest_object_key(retrieved_date="2026-07-02", run_id="other-run"),
        json.dumps(
            {
                "run_id": "other-run",
                "retrieved_at": "2026-07-02T10:00:00+00:00",
                "retrieved_date": "2026-07-02",
                "files": [],
            }
        ),
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
    )
    object_store.write_json(
        manifest_object_key(retrieved_date="2026-07-01", run_id="current-run"),
        json.dumps(
            {
                "run_id": "current-run",
                "retrieved_at": "2026-07-01T10:00:00+00:00",
                "retrieved_date": "2026-07-01",
                "files": [],
            }
        ),
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
    )

    assert manifest_for_run(object_store, "current-run")["run_id"] == "current-run"
    assert manifest_for_run(object_store, "missing-run")["run_id"] == "other-run"


def test_sweden_company_manifest_for_run_can_fallback_to_latest_pointer() -> None:
    object_store = FakeObjectStore()
    object_store.write_json(
        _latest_manifest_object_key(retrieved_date="2026-07-03"),
        json.dumps(
            {
                "run_id": "manual-upstream-run",
                "retrieved_at": "2026-07-03T10:00:00+00:00",
                "retrieved_date": "2026-07-03",
                "files": [],
            }
        ),
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
    )

    assert manifest_for_run(object_store, "downstream-only-run")["run_id"] == (
        "manual-upstream-run"
    )
