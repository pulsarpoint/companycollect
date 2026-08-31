import io
import json
import tarfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dagster import AssetKey

from dagster_v3.defs.sweden_jobtech_links.assets import defs
from dagster_v3.defs.sweden_jobtech_links import source, tables
from dagster_v3.defs.sweden_jobtech_links.source import (
    discover_snapshot_archives,
    sync_latest_snapshot,
)


class _Response:
    def __init__(
        self,
        body: bytes,
        *,
        content_length: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.body = body
        self.text = body.decode(errors="replace")
        self.headers = {
            "Content-Length": str(
                len(body) if content_length is None else content_length
            ),
            **(headers or {}),
        }

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [
            self.body[offset : offset + chunk_size]
            for offset in range(0, len(self.body), chunk_size)
        ]


class _Session:
    def __init__(self, responses: dict[str, list[_Response]]) -> None:
        self.responses = responses
        self.requested_urls: list[str] = []

    def get(
        self,
        url: str,
        *,
        timeout: int,
        stream: bool = False,
        headers: dict[str, str] | None = None,
    ) -> _Response:
        self.requested_urls.append(url)
        return self.responses[url].pop(0)


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.upload_count = 0
        self.write_count = 0

    def ensure_bucket(self, bucket: str | None = None) -> None:
        pass

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return key in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        self.upload_count += 1
        self.objects[key] = Path(source_path).read_bytes()

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        self.write_count += 1
        self.objects[key] = body.encode()


def _archive(
    payload: bytes, *, member_name: str = "jobtechdev/minio/arkiv/output.json"
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        member = tarfile.TarInfo(member_name)
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))
    return buffer.getvalue()


def _catalog(*hrefs: str) -> bytes:
    return "\n".join(f'<a href="{href}">{href}</a>' for href in hrefs).encode()


def test_catalog_discovers_only_strictly_dated_archives_in_date_order() -> None:
    html = _catalog(
        "/annonser/jobtechlinks/2026-08-29.tar.gz.dcat.xml",
        "/annonser/jobtechlinks/2026-08-30.tar.gz",
        "2026-08-29.tar.gz",
        "/annonser/jobtechlinks/latest.tar.gz",
        "/annonser/jobtechlinks/2026-02-30.tar.gz",
    ).decode()

    snapshots = discover_snapshot_archives(html, catalog_url=tables.CATALOG_URL)

    assert [snapshot.snapshot_date.isoformat() for snapshot in snapshots] == [
        "2026-08-29",
        "2026-08-30",
    ]
    assert snapshots[-1].source_url == (
        "https://data.jobtechdev.se/annonser/jobtechlinks/2026-08-30.tar.gz"
    )


def test_snapshot_asset_is_non_partitioned_and_has_its_object_store() -> None:
    asset_key = AssetKey("sweden_jobtech_links_snapshot_s3")
    asset_node = defs.resolve_asset_graph().get(asset_key)

    assert asset_node.partitions_def is None
    assert asset_node.group_name == tables.GROUP_NAME
    assert "sweden_jobtech_links_object_store" in defs.resources


def test_sync_stores_latest_archive_and_immutable_metadata() -> None:
    older_url = "https://data.jobtechdev.se/annonser/jobtechlinks/2026-08-29.tar.gz"
    latest_url = "https://data.jobtechdev.se/annonser/jobtechlinks/2026-08-30.tar.gz"
    archive = _archive(b'{"id":"job-1"}\n{"id":"job-2"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [
                _Response(_catalog("2026-08-29.tar.gz", "2026-08-30.tar.gz"))
            ],
            latest_url: [
                _Response(
                    archive,
                    headers={
                        "ETag": '"source-etag"',
                        "Last-Modified": "Sun, 30 Aug 2026 03:00:00 GMT",
                    },
                )
            ],
        }
    )
    store = _ObjectStore()

    snapshot = sync_latest_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="snapshot-run",
        retrieved_at=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
        session=session,  # type: ignore[arg-type]
    )

    assert session.requested_urls == [tables.CATALOG_URL, latest_url]
    assert older_url not in session.requested_urls
    assert snapshot.snapshot_date.isoformat() == "2026-08-30"
    assert snapshot.archive_object_key == (
        f"snapshots/snapshot_date=2026-08-30/sha256={snapshot.archive_sha256}/"
        "2026-08-30.tar.gz"
    )
    assert snapshot.metadata_object_key == (
        f"snapshots/snapshot_date=2026-08-30/sha256={snapshot.archive_sha256}/"
        "metadata.json"
    )
    assert store.objects[snapshot.archive_object_key] == archive
    metadata = json.loads(store.objects[snapshot.metadata_object_key])
    assert metadata["snapshot_uid"] == snapshot.snapshot_uid
    assert metadata["source_url"] == latest_url
    assert metadata["archive_sha256"] == snapshot.archive_sha256
    assert metadata["raw_member_path"] == "jobtechdev/minio/arkiv/output.json"
    assert metadata["raw_member_size_bytes"] == 30
    assert metadata["source_etag"] == '"source-etag"'
    assert metadata["source_last_modified"] == "Sun, 30 Aug 2026 03:00:00 GMT"
    assert snapshot.downloaded is True
    assert store.upload_count == 1
    assert store.write_count == 1


def test_sync_retries_the_whole_archive_after_content_length_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest_url = "https://data.jobtechdev.se/annonser/jobtechlinks/2026-08-30.tar.gz"
    archive = _archive(b'{"id":"job-1"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-08-30.tar.gz"))],
            latest_url: [
                _Response(archive[:-8], content_length=len(archive)),
                _Response(archive),
            ],
        }
    )
    store = _ObjectStore()
    monkeypatch.setattr(source.time, "sleep", lambda _: None)

    snapshot = sync_latest_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="retry-run",
        retrieved_at=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
        session=session,  # type: ignore[arg-type]
    )

    assert session.requested_urls.count(latest_url) == 2
    assert store.objects[snapshot.archive_object_key] == archive


def test_sync_does_not_rewrite_an_existing_content_addressed_snapshot() -> None:
    latest_url = "https://data.jobtechdev.se/annonser/jobtechlinks/2026-08-30.tar.gz"
    archive = _archive(b'{"id":"job-1"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-08-30.tar.gz"))],
            latest_url: [_Response(archive)],
        }
    )
    store = _ObjectStore()

    first = sync_latest_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="first-run",
        retrieved_at=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
        session=session,  # type: ignore[arg-type]
    )
    original_metadata = store.objects[first.metadata_object_key]
    session.responses[tables.CATALOG_URL] = [_Response(_catalog("2026-08-30.tar.gz"))]
    session.responses[latest_url] = [_Response(archive)]

    second = sync_latest_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="second-run",
        retrieved_at=datetime(2026, 8, 31, 11, 0, tzinfo=UTC),
        session=session,  # type: ignore[arg-type]
    )

    assert second.downloaded is False
    assert first.snapshot_uid == second.snapshot_uid
    assert store.objects[first.metadata_object_key] == original_metadata
    assert store.upload_count == 1
    assert store.write_count == 1


def test_sync_rejects_an_archive_without_the_expected_raw_member() -> None:
    latest_url = "https://data.jobtechdev.se/annonser/jobtechlinks/2026-08-30.tar.gz"
    session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-08-30.tar.gz"))],
            latest_url: [_Response(_archive(b"{}\n", member_name="unexpected.json"))],
        }
    )
    store = _ObjectStore()

    with pytest.raises(ValueError, match="exactly one output.json member"):
        sync_latest_snapshot(
            object_store=store,  # type: ignore[arg-type]
            run_id="invalid-run",
            retrieved_at=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
            session=session,  # type: ignore[arg-type]
        )

    assert store.objects == {}
