import json
from pathlib import Path

import duckdb

from dagster_v3.defs.gleif import duckdb_state
from dagster_v3.defs.gleif import source
from tests.test_gleif_csv_transforms import seed_raw_tables


def test_latest_manifest_uses_publish_date_not_lexical_load_mode() -> None:
    object_store = _FakeObjectStore(
        {
            (
                "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
                "run_id=run-full/manifest.json"
            ): {
                "load_mode": "full",
                "publish_date": "2026-06-20T16:00:00+00:00",
                "run_id": "run-full",
            },
            (
                "gleif/raw/load_mode=delta/delta=LastDay/"
                "publish_date=2026-06-21T16-00-00Z/run_id=run-delta/manifest.json"
            ): {
                "load_mode": "delta",
                "publish_date": "2026-06-21T16:00:00+00:00",
                "run_id": "run-delta",
            },
        }
    )

    assert source.latest_manifest(object_store)["run_id"] == "run-delta"


def test_manifest_for_run_prefers_current_run_over_existing_newer_snapshot() -> None:
    object_store = _FakeObjectStore(
        {
            (
                "gleif/raw/load_mode=delta/delta=LastDay/"
                "publish_date=2026-06-21T16-00-00Z/run_id=run-current/manifest.json"
            ): {
                "load_mode": "delta",
                "publish_date": "2026-06-21T16:00:00+00:00",
                "run_id": "run-current",
            },
            (
                "gleif/raw/load_mode=delta/delta=LastDay/"
                "publish_date=2026-06-22T16-00-00Z/run_id=run-other/manifest.json"
            ): {
                "load_mode": "delta",
                "publish_date": "2026-06-22T16:00:00+00:00",
                "run_id": "run-other",
            },
        }
    )

    assert source.manifest_for_run(object_store, "run-current")["run_id"] == "run-current"


def test_refresh_duckdb_state_normalizes_from_dlt_raw_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "gleif_reference.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        seed_raw_tables(connection)

    source_key = (
        "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
        "run_id=run-full/file_kind=lei_records/source.csv.zip"
    )
    manifest_key = (
        "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
        "run_id=run-full/manifest.json"
    )
    object_store = _FakeObjectStore(
        {
            manifest_key: {
                "load_mode": "full",
                "publish_date": "2026-06-20T16:00:00+00:00",
                "pulled_at": "2026-06-20T17:00:00+00:00",
                "run_id": "run-full",
                "files": [
                    {
                        "file_kind": "lei_records",
                        "file_format": "csv",
                        "source_url": "https://example.test/lei2/latest.csv",
                        "s3_key": source_key,
                        "sha256": "a" * 64,
                    }
                ],
            }
        },
    )

    result = duckdb_state.refresh_gleif_duckdb_state(
        context=_FakeContext("run-full"),
        object_store=object_store,
        database_path=database_path,
    )

    assert result.metadata["gleif_lei_records_row_count"] == 1
    assert source_key not in object_store.read_bytes_keys
    assert object_store.state["last_full_publish_date"] == "2026-06-20T16:00:00+00:00"


class _FakeObjectStore:
    def __init__(self, objects: dict[str, dict]) -> None:
        self.objects = objects
        self.read_bytes_keys: list[str] = []
        self.state: dict | None = None

    def list_keys(self, prefix: str, *, bucket: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        self.read_bytes_keys.append(key)
        return json.dumps(self.objects[key]).encode()

    def write_json(self, key: str, body: str, *, bucket: str) -> None:
        self.state = json.loads(body)


class _FakeContext:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
