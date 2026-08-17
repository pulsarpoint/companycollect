from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
import json
from pathlib import Path
from typing import Any

from boto3.s3.transfer import TransferConfig

from dagster_v3.defs.common.resources import ObjectStoreResource


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def create_bucket(self, Bucket: str) -> None:
        return None

    def upload_file(
        self,
        Filename: str,
        Bucket: str,
        Key: str,
        Config: TransferConfig | None = None,
    ) -> None:
        self.objects[(Bucket, Key)] = Path(Filename).read_bytes()

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        Path(Filename).write_bytes(self.objects[(Bucket, Key)])

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            error = Exception("missing")
            setattr(error, "response", {"Error": {"Code": "404"}})
            raise error
        return {}

    def put_object(self, Bucket: str, Key: str, Body: Any) -> None:
        if isinstance(Body, bytes):
            body = Body
        elif isinstance(Body, str):
            body = Body.encode()
        else:
            body = Body.read()
        self.objects[(Bucket, Key)] = body

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def get_paginator(self, operation_name: str) -> Any:
        raise NotImplementedError(operation_name)

    def delete_objects(self, Bucket: str, Delete: dict[str, Any]) -> None:
        for item in Delete["Objects"]:
            self.objects.pop((Bucket, item["Key"]), None)


def test_object_store_uploads_and_downloads_files(tmp_path: Path) -> None:
    source = tmp_path / "source.zip"
    target = tmp_path / "target.zip"
    source.write_bytes(b"large-file-bytes")

    fake = FakeS3Client()
    object_store = ObjectStoreResource(s3_client=fake)
    object_store.upload_file(
        "raw/source.zip",
        source,
        bucket="source-open-page-rank-domains",
    )
    object_store.download_file(
        "raw/source.zip",
        target,
        bucket="source-open-page-rank-domains",
    )

    assert target.read_bytes() == b"large-file-bytes"


def test_open_page_rank_object_keys_are_run_scoped() -> None:
    from dagster_v3.defs.open_page_rank.source import (
        manifest_object_key,
        raw_file_object_key,
    )

    assert raw_file_object_key(run_id="run-1", retrieved_date="2026-06-21") == (
        "raw/run_id=run-1/retrieved_date=2026-06-21/source.csv.zip"
    )
    assert manifest_object_key(run_id="run-1", retrieved_date="2026-06-21") == (
        "raw/run_id=run-1/retrieved_date=2026-06-21/manifest.json"
    )


def test_open_page_rank_manifest_records_source_file() -> None:
    from dagster_v3.defs.open_page_rank.source import (
        OpenPageRankRawFile,
        build_manifest,
    )

    manifest = build_manifest(
        run_id="run-1",
        retrieved_at=datetime(2026, 6, 21, 10, 30, tzinfo=UTC),
        file=OpenPageRankRawFile(
            source_url="https://www.domcop.com/files/top/top10milliondomains.csv.zip",
            s3_key="raw/run_id=run-1/retrieved_date=2026-06-21/source.csv.zip",
            size_bytes=123,
            sha256="a" * 64,
        ),
    )

    assert manifest["source"] == "open_page_rank"
    assert manifest["run_id"] == "run-1"
    assert manifest["retrieved_date"] == "2026-06-21"
    assert manifest["file"]["s3_key"].endswith("/source.csv.zip")


def test_open_page_rank_retention_keeps_newest_raw_file_and_manifests() -> None:
    from dagster_v3.defs.open_page_rank.source import (
        select_open_page_rank_raw_keys_for_deletion,
    )

    keys = [
        "raw/run_id=old/retrieved_date=2026-06-14/source.csv.zip",
        "raw/run_id=old/retrieved_date=2026-06-14/manifest.json",
        "raw/run_id=new/retrieved_date=2026-06-21/source.csv.zip",
        "raw/run_id=new/retrieved_date=2026-06-21/manifest.json",
    ]

    assert select_open_page_rank_raw_keys_for_deletion(keys) == [
        "raw/run_id=old/retrieved_date=2026-06-14/source.csv.zip"
    ]


def test_open_page_rank_manifest_for_run_falls_back_to_latest_manifest() -> None:
    from dagster_v3.defs.open_page_rank.source import manifest_for_run

    object_store = FakeManifestObjectStore(
        {
            "raw/run_id=old/retrieved_date=2026-06-14/manifest.json": {
                "run_id": "old",
                "retrieved_at": "2026-06-14T10:30:00+00:00",
            },
            "raw/run_id=new/retrieved_date=2026-06-21/manifest.json": {
                "run_id": "new",
                "retrieved_at": "2026-06-21T10:30:00+00:00",
            },
        }
    )

    assert manifest_for_run(object_store, "downstream-only-run")["run_id"] == "new"


class FakeManifestObjectStore:
    def __init__(self, manifests: dict[str, dict[str, Any]]) -> None:
        self.manifests = manifests

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        del bucket
        return [key for key in self.manifests if key.startswith(prefix)]

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        del bucket
        return json.dumps(self.manifests[key]).encode()
