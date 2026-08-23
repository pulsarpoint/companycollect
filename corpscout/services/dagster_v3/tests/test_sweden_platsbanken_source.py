import json
from datetime import UTC, datetime
from pathlib import Path

from dagster_v3.defs.sweden_platsbanken.source import (
    discover_historical_archive_urls,
    resolve_jobstream_event_window,
    sanitize_jobstream_jsonl,
    sync_jobstream_snapshot,
)


class _Response:
    def __init__(self, body: bytes) -> None:
        self.body = body
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

    def get(
        self,
        url: str,
        *,
        timeout: int,
        stream: bool,
        headers: dict[str, str],
    ) -> _Response:
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


def test_jobstream_snapshot_is_redacted_before_durable_storage(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.jsonl"
    target_path = tmp_path / "sanitized.jsonl"
    source_path.write_text(
        json.dumps(
            {
                "id": "31380149",
                "headline": "Maskinoperatör",
                "application_contacts": [
                    {
                        "name": "Personal Contact",
                        "email": "person@example.se",
                        "telephone": "0700000000",
                    }
                ],
                "application_details": {
                    "email": "person@example.se",
                    "url": "https://example.se/apply",
                },
                "employer": {
                    "name": "Example AB",
                    "organization_number": "5563519437",
                    "email": "person@example.se",
                    "phone_number": "0700000000",
                    "url": "https://example.se",
                },
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    row_count = sanitize_jobstream_jsonl(source_path, target_path)

    assert row_count == 1
    [record] = [
        json.loads(line)
        for line in target_path.read_text(encoding="utf-8").splitlines()
    ]
    assert "application_contacts" not in record
    assert "application_details" not in record
    assert "email" not in record["employer"]
    assert "phone_number" not in record["employer"]
    assert record["employer"]["organization_number"] == "5563519437"


def test_snapshot_sync_persists_only_sanitized_jobstream_jsonl() -> None:
    body = (
        json.dumps(
            {
                "id": "31380149",
                "application_contacts": [{"name": "Personal Contact"}],
                "application_details": {"email": "person@example.se"},
                "employer": {
                    "organization_number": "5563519437",
                    "email": "person@example.se",
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

    persisted = json.loads(store.objects[snapshot.object_key])
    assert persisted["id"] == "31380149"
    assert "application_contacts" not in persisted
    assert "application_details" not in persisted
    assert "email" not in persisted["employer"]
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
