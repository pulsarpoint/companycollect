import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_platsbanken.source import (
    discover_historical_archive_urls,
    historical_archive_year,
    resolve_jobstream_event_window,
    sync_historical_archives,
    sync_jobstream_snapshot,
)


class _Response:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.text = body.decode()
        self.headers = {"Content-Length": str(len(body))}

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [
            self.body[offset : offset + chunk_size]
            for offset in range(0, len(self.body), chunk_size)
        ]


class _Session:
    def __init__(self, response: _Response) -> None:
        self.response = response
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
        return self.response


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

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
        self.objects[key] = Path(source_path).read_bytes()

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        self.objects[key] = body.encode()

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        return self.objects[key]


def test_historical_catalog_keeps_complete_zip_archives_only() -> None:
    html = """
    <a href="/annonser/historiska/berikade/kompletta/2025_beta1_jsonl.zip">2025</a>
    <a href="/annonser/historiska/berikade/kompletta/2026-Q1_beta1_jsonl.zst">Q1 zstd</a>
    <a href="/annonser/historiska/berikade/kompletta/2026-Q1_beta1_jsonl.zip">Q1 zip</a>
    <a href="/annonser/historiska/berikade/exempel/2025_beta1_1_percent_jsonl.zip">sample</a>
    """

    assert discover_historical_archive_urls(
        html,
        catalog_url=(
            "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
        ),
    ) == (
        "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
        "2025_beta1_jsonl.zip",
        "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
        "2026-Q1_beta1_jsonl.zip",
    )


def test_historical_archive_year_accepts_annual_and_quarterly_files() -> None:
    assert historical_archive_year("https://example.test/2016.zip") == "2016"
    assert (
        historical_archive_year(
            "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
            "2025_beta1_jsonl.zip"
        )
        == "2025"
    )
    assert (
        historical_archive_year(
            "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
            "2026-Q1_beta1_jsonl.zip"
        )
        == "2026"
    )

    with pytest.raises(ValueError, match="archive year"):
        historical_archive_year("https://example.test/historical-latest.zip")


def test_historical_sync_downloads_and_manifests_only_the_partition_year() -> None:
    catalog = b"""
    <a href="/annonser/historiska/berikade/kompletta/2025_beta1_jsonl.zip">2025</a>
    <a href="/annonser/historiska/berikade/kompletta/2026-Q1_beta1_jsonl.zip">2026 Q1</a>
    """
    session = _Session(_Response(catalog))
    store = _ObjectStore()

    manifest = sync_historical_archives(
        object_store=store,  # type: ignore[arg-type]
        run_id="historical-2025",
        retrieved_at=datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
        refresh_existing=False,
        archive_year="2025",
        session=session,  # type: ignore[arg-type]
    )

    assert manifest["partition_year"] == "2025"
    assert [archive["source_file"] for archive in manifest["archives"]] == [
        "2025_beta1_jsonl.zip"
    ]
    assert "historical/year=2025" in str(manifest["manifest_key"])
    assert session.requested_urls == [
        "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/",
        "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
        "2025_beta1_jsonl.zip",
    ]


def test_historical_sync_reuses_objects_from_the_legacy_global_manifest() -> None:
    archive_url = (
        "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
        "2025_beta1_jsonl.zip"
    )
    object_key = (
        "historical/source_file=2025_beta1_jsonl.zip/"
        "sha256=previous/2025_beta1_jsonl.zip"
    )
    catalog = f'<a href="{archive_url}">2025</a>'.encode()
    session = _Session(_Response(catalog))
    store = _ObjectStore()
    store.objects[object_key] = b"existing archive"
    store.objects["manifests/historical/retrieved_at=2026-08-01/run_id=legacy.json"] = (
        json.dumps(
            {
                "retrieved_at": "2026-08-01T00:00:00+00:00",
                "archives": [
                    {
                        "source_url": archive_url,
                        "source_file": "2025_beta1_jsonl.zip",
                        "object_key": object_key,
                        "sha256": "previous",
                        "size_bytes": len(store.objects[object_key]),
                        "etag": "",
                        "last_modified": "",
                        "downloaded": True,
                    }
                ],
            }
        ).encode()
    )

    manifest = sync_historical_archives(
        object_store=store,  # type: ignore[arg-type]
        run_id="historical-2025",
        retrieved_at=datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
        refresh_existing=False,
        archive_year="2025",
        session=session,  # type: ignore[arg-type]
    )

    assert manifest["archives"][0]["downloaded"] is False
    assert session.requested_urls == [
        "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
    ]


def test_snapshot_sync_preserves_job_and_employer_contacts() -> None:
    body = (
        json.dumps(
            {
                "id": "31380149",
                "application_contacts": [
                    {
                        "name": "Recruiter",
                        "description": "Hiring manager",
                        "email": "recruiter@example.se",
                        "telephone": "0101234567",
                        "contact_type": "contact",
                    }
                ],
                "application_details": {
                    "email": "jobs@example.se",
                    "url": "https://example.se/apply",
                    "other": "Apply through the portal",
                    "reference": "JOB-42",
                    "information": "Applications are reviewed continuously",
                    "via_af": False,
                },
                "employer": {
                    "organization_number": "5563519437",
                    "email": "company@example.se",
                    "phone_number": "0107654321",
                },
            }
        )
        + "\n"
    ).encode()
    store = _ObjectStore()

    snapshot = sync_jobstream_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="snapshot-run",
        retrieved_at=datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
        session=_Session(_Response(body)),  # type: ignore[arg-type]
    )

    assert store.objects[snapshot.object_key] == body
    persisted = json.loads(store.objects[snapshot.object_key])
    assert persisted["id"] == "31380149"
    assert persisted["application_contacts"][0]["name"] == "Recruiter"
    assert persisted["application_contacts"][0]["email"] == "recruiter@example.se"
    assert persisted["application_details"]["url"] == "https://example.se/apply"
    assert persisted["application_details"]["reference"] == "JOB-42"
    assert persisted["employer"]["email"] == "company@example.se"
    assert persisted["employer"]["phone_number"] == "0107654321"
    manifest = json.loads(store.objects[snapshot.manifest_key])
    assert manifest["record_count"] == 1
    assert manifest["object_key"] == snapshot.object_key


def test_event_window_replays_five_minutes_from_durable_cursor() -> None:
    store = _ObjectStore()
    store.objects[
        "manifests/jobstream/events/retrieved_at=2026/run_id=previous.json"
    ] = json.dumps(
        {
            "retrieved_at": "2026-08-23T09:00:00+00:00",
            "updated_before": "2026-08-23T08:58:00Z",
        }
    ).encode()

    updated_after, updated_before = resolve_jobstream_event_window(
        object_store=store,  # type: ignore[arg-type]
        now=datetime(2026, 8, 23, 10, 0, tzinfo=UTC),
        configured_after="",
        configured_before="",
    )

    assert updated_after == datetime(2026, 8, 23, 8, 53, tzinfo=UTC)
    assert updated_before == datetime(2026, 8, 23, 9, 58, tzinfo=UTC)
