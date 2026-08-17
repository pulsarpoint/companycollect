import json
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dagster_v3.defs.common.object_catalog import (
    OBJECT_CATALOG_REQUIRED_COLUMNS,
    ObjectCatalogCommit,
)
from dagster_v3.defs.sweden_company.resources import (
    SWEDEN_COMPANY_RAW_BUCKET,
    SwedenCompanyBulkResource,
    SwedenCompanyRawSnapshotReference,
    catalog_location,
    integrity_object_key,
    load_catalog_manifest,
    manifest_object_key,
    raw_file_object_key,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []
        self.written_keys: list[tuple[str, str]] = []
        self.downloaded_object_keys: list[tuple[str, str]] = []
        self.read_object_keys: list[tuple[str, str]] = []
        self.corrupt_catalog_reads = False

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def object_size(self, key: str, bucket: str | None = None) -> int:
        assert bucket is not None
        return len(self.objects[(bucket, key)])

    def upload_file(
        self, key: str, source_path: str | Path, bucket: str | None = None
    ) -> None:
        assert bucket is not None
        self.uploaded_files.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()
        self.written_keys.append((bucket, key))

    def download_file(
        self, key: str, target_path: str | Path, bucket: str | None = None
    ) -> None:
        assert bucket is not None
        self.downloaded_object_keys.append((bucket, key))
        Path(target_path).write_bytes(self.objects[(bucket, key)])

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body
        self.written_keys.append((bucket, key))

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body.encode("utf-8")
        self.written_keys.append((bucket, key))

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        raise AssertionError(
            f"Sweden company must not list object-store prefixes: {bucket}/{prefix}"
        )

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        self.read_object_keys.append((bucket, key))
        body = self.objects[(bucket, key)]
        if self.corrupt_catalog_reads and key.endswith("/catalog.parquet"):
            return body + b"corrupt"
        return body


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
    assert (
        object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, bolagsverket_key)]
        == b"bolag-zip"
    )
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

    manifest = json.loads(
        object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, manifest_key)]
    )
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

    location = catalog_location(retrieved_date="2026-07-03")
    commit_key = location.commit_object_key()
    commit = ObjectCatalogCommit.from_json_bytes(
        object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, commit_key)]
    )
    catalog_body = object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, commit.catalog.key)]
    catalog = pq.read_table(source=BytesIO(catalog_body))

    assert catalog.column_names[: len(OBJECT_CATALOG_REQUIRED_COLUMNS)] == list(
        OBJECT_CATALOG_REQUIRED_COLUMNS
    )
    assert catalog.to_pylist() == [
        {
            "schema_version": 2,
            "source": "sweden_company",
            "dataset": "raw_archives",
            "partition_json": '{"snapshot_date":"2026-07-03"}',
            "source_run_id": "run-1",
            "created_at": retrieved_at,
            "object_key": bolagsverket_key,
            "object_format": "zip",
            "size_bytes": len(b"bolag-zip"),
            "sha256": sha256(b"bolag-zip").hexdigest(),
            "row_count": None,
            "source_slug": "bolagsverket_bulkfil",
            "source_name": "Bolagsverket legal-register bulk file",
            "source_url": resource.bolagsverket_bulk_url,
            "source_last_modified": "2026-06-29T01-27-14Z",
            "content_type": "application/zip",
            "last_modified": "Mon, 29 Jun 2026 01:27:14 GMT",
        },
        {
            "schema_version": 2,
            "source": "sweden_company",
            "dataset": "raw_archives",
            "partition_json": '{"snapshot_date":"2026-07-03"}',
            "source_run_id": "run-1",
            "created_at": retrieved_at,
            "object_key": scb_key,
            "object_format": "zip",
            "size_bytes": len(b"scb-zip"),
            "sha256": sha256(b"scb-zip").hexdigest(),
            "row_count": None,
            "source_slug": "scb_bulkfil",
            "source_name": "SCB/FDB company bulk file",
            "source_url": resource.scb_bulk_url,
            "source_last_modified": "2026-06-29T01-27-14Z",
            "content_type": "application/zip",
            "last_modified": "Mon, 29 Jun 2026 01:27:14 GMT",
        },
    ]
    assert commit.data_object_count == 2
    assert commit.data_size_bytes == len(b"scb-zip") + len(b"bolag-zip")
    assert commit.catalog.size_bytes == len(catalog_body)
    assert commit.catalog.sha256 == sha256(catalog_body).hexdigest()
    assert object_store.written_keys[-1] == (SWEDEN_COMPANY_RAW_BUCKET, commit_key)
    assert result.metadata["object_catalog_commit_key"] == commit_key
    assert result.metadata["object_catalog_key"] == commit.catalog.key
    assert result.metadata["object_catalog_sha256"] == commit.catalog.sha256
    assert result.value == SwedenCompanyRawSnapshotReference(
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
        snapshot_date="2026-07-03",
        commit_key=commit_key,
        source_run_id="run-1",
    )


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
    assert all(item["sha256"] is not None for item in manifest["files"])
    assert all(
        item["size_bytes"] == len(b"already-there") for item in manifest["files"]
    )
    for source_slug in ("scb_bulkfil", "bolagsverket_bulkfil"):
        raw_key = raw_file_object_key(
            source_slug=source_slug,
            source_last_modified="2026-06-29T01-27-14Z",
        )
        assert (
            SWEDEN_COMPANY_RAW_BUCKET,
            integrity_object_key(raw_key),
        ) in object_store.objects


def test_sweden_company_snapshot_bootstraps_integrity_without_listing() -> None:
    retrieved_at = datetime(2026, 7, 3, 11, 0, tzinfo=UTC)
    object_store = FakeObjectStore()
    resource = SwedenCompanyBulkResource()
    source_bodies = {
        "scb_bulkfil": b"existing-scb",
        "bolagsverket_bulkfil": b"existing-bolag",
    }
    for source_slug, body in source_bodies.items():
        raw_key = raw_file_object_key(
            source_slug=source_slug,
            source_last_modified="2026-06-29T01-27-14Z",
        )
        object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, raw_key)] = body
    session = HeadOnlySession(
        {
            resource.scb_bulk_url: b"ignored-scb-body",
            resource.bolagsverket_bulk_url: b"ignored-bolag-body",
        }
    )

    resource.download_snapshot(
        object_store=object_store,
        run_id="run-2",
        retrieved_at=retrieved_at,
        session=session,
    )

    assert object_store.uploaded_files == []
    assert object_store.downloaded_object_keys == [
        (
            SWEDEN_COMPANY_RAW_BUCKET,
            raw_file_object_key(
                source_slug="bolagsverket_bulkfil",
                source_last_modified="2026-06-29T01-27-14Z",
            ),
        ),
        (
            SWEDEN_COMPANY_RAW_BUCKET,
            raw_file_object_key(
                source_slug="scb_bulkfil",
                source_last_modified="2026-06-29T01-27-14Z",
            ),
        ),
    ]
    for source_slug, body in source_bodies.items():
        raw_key = raw_file_object_key(
            source_slug=source_slug,
            source_last_modified="2026-06-29T01-27-14Z",
        )
        integrity = json.loads(
            object_store.objects[
                (SWEDEN_COMPANY_RAW_BUCKET, integrity_object_key(raw_key))
            ]
        )
        assert integrity == {
            "object_key": raw_key,
            "sha256": sha256(body).hexdigest(),
            "size_bytes": len(body),
        }


def test_sweden_company_catalog_verification_failure_preserves_previous_commit() -> (
    None
):
    retrieved_at = datetime(2026, 7, 3, 10, 30, tzinfo=UTC)
    object_store = FakeObjectStore()
    location = catalog_location(retrieved_date="2026-07-03")
    previous_commit = b"previous-valid-commit\n"
    object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, location.commit_object_key())] = (
        previous_commit
    )
    object_store.corrupt_catalog_reads = True
    resource = SwedenCompanyBulkResource()
    session = FakeSession(
        {
            resource.scb_bulk_url: b"scb-zip",
            resource.bolagsverket_bulk_url: b"bolag-zip",
        }
    )

    with pytest.raises(ValueError, match="catalog verification failed"):
        resource.download_snapshot(
            object_store=object_store,
            run_id="run-1",
            retrieved_at=retrieved_at,
            session=session,
        )

    assert (
        object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, location.commit_object_key())]
        == previous_commit
    )


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


def test_sweden_company_catalog_reader_uses_exact_commit_and_catalog_keys() -> None:
    object_store = FakeObjectStore()
    resource = SwedenCompanyBulkResource()
    result = resource.download_snapshot(
        object_store=object_store,
        run_id="catalog-run",
        retrieved_at=datetime(2026, 7, 3, 10, 30, tzinfo=UTC),
        session=FakeSession(
            {
                resource.scb_bulk_url: b"scb-zip",
                resource.bolagsverket_bulk_url: b"bolag-zip",
            }
        ),
    )
    reference = result.value
    object_store.read_object_keys.clear()

    commit, manifest = load_catalog_manifest(
        object_store=object_store,
        snapshot=reference,
    )

    assert object_store.read_object_keys == [
        (SWEDEN_COMPANY_RAW_BUCKET, reference.commit_key),
        (SWEDEN_COMPANY_RAW_BUCKET, commit.catalog.key),
    ]
    assert manifest["run_id"] == "catalog-run"
    assert manifest["retrieved_date"] == "2026-07-03"
    assert [file["source_slug"] for file in manifest["files"]] == [
        "bolagsverket_bulkfil",
        "scb_bulkfil",
    ]
    assert [file["s3_key"] for file in manifest["files"]] == [
        row["object_key"]
        for row in pq.read_table(
            BytesIO(
                object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, commit.catalog.key)]
            )
        ).to_pylist()
    ]


def test_sweden_company_catalog_reader_rejects_source_run_mismatch() -> None:
    object_store = FakeObjectStore()
    resource = SwedenCompanyBulkResource()
    result = resource.download_snapshot(
        object_store=object_store,
        run_id="catalog-run",
        retrieved_at=datetime(2026, 7, 3, 10, 30, tzinfo=UTC),
        session=FakeSession(
            {
                resource.scb_bulk_url: b"scb-zip",
                resource.bolagsverket_bulk_url: b"bolag-zip",
            }
        ),
    )

    with pytest.raises(ValueError, match="source run ID mismatch"):
        load_catalog_manifest(
            object_store=object_store,
            snapshot=replace(result.value, source_run_id="replaced-run"),
        )


def test_sweden_company_catalog_reader_rejects_corrupt_catalog() -> None:
    object_store = FakeObjectStore()
    resource = SwedenCompanyBulkResource()
    result = resource.download_snapshot(
        object_store=object_store,
        run_id="catalog-run",
        retrieved_at=datetime(2026, 7, 3, 10, 30, tzinfo=UTC),
        session=FakeSession(
            {
                resource.scb_bulk_url: b"scb-zip",
                resource.bolagsverket_bulk_url: b"bolag-zip",
            }
        ),
    )
    object_store.corrupt_catalog_reads = True

    with pytest.raises(ValueError, match="catalog size mismatch"):
        load_catalog_manifest(
            object_store=object_store,
            snapshot=result.value,
        )


def test_sweden_company_catalog_reader_rejects_wrong_schema() -> None:
    object_store = FakeObjectStore()
    resource = SwedenCompanyBulkResource()
    result = resource.download_snapshot(
        object_store=object_store,
        run_id="catalog-run",
        retrieved_at=datetime(2026, 7, 3, 10, 30, tzinfo=UTC),
        session=FakeSession(
            {
                resource.scb_bulk_url: b"scb-zip",
                resource.bolagsverket_bulk_url: b"bolag-zip",
            }
        ),
    )
    reference = result.value
    commit = ObjectCatalogCommit.from_json_bytes(
        object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, reference.commit_key)]
    )
    catalog = pq.read_table(
        BytesIO(object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, commit.catalog.key)])
    ).drop_columns(["last_modified"])
    sink = pa.BufferOutputStream()
    pq.write_table(catalog, sink, compression="zstd")
    catalog_body = sink.getvalue().to_pybytes()
    changed_commit = commit.model_copy(
        update={
            "catalog": commit.catalog.model_copy(
                update={
                    "sha256": sha256(catalog_body).hexdigest(),
                    "size_bytes": len(catalog_body),
                }
            )
        }
    )
    object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, commit.catalog.key)] = catalog_body
    object_store.objects[(SWEDEN_COMPANY_RAW_BUCKET, reference.commit_key)] = (
        changed_commit.to_json_bytes()
    )

    with pytest.raises(ValueError, match="catalog schema mismatch"):
        load_catalog_manifest(
            object_store=object_store,
            snapshot=reference,
        )


def test_sweden_company_catalog_reader_rejects_missing_commit() -> None:
    object_store = FakeObjectStore()
    reference = SwedenCompanyRawSnapshotReference(
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
        snapshot_date="2026-07-03",
        commit_key=catalog_location(retrieved_date="2026-07-03").commit_object_key(),
        source_run_id="missing-run",
    )

    with pytest.raises(ValueError, match="commit does not exist"):
        load_catalog_manifest(
            object_store=object_store,
            snapshot=reference,
        )
