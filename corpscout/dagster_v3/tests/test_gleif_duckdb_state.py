import json
import zipfile
from io import BytesIO
from pathlib import Path

import duckdb

from dagster_v3.defs.gleif import duckdb_state
from dagster_v3.defs.gleif import source
from tests.test_gleif_csv_transforms import seed_raw_tables


def test_full_refresh_replaces_current_state(tmp_path: Path) -> None:
    db_path = tmp_path / "gleif.duckdb"
    rows = duckdb_state.GleifStateRows(
        lei_records=[_minimal_lei_record("LEI0000000000000001", "Alpha Inc", "run-full")]
    )

    counts = duckdb_state.replace_current_state(db_path, rows)

    assert counts["gleif_lei_records"] == 1
    with duckdb.connect(str(db_path), read_only=True) as connection:
        assert (
            connection.execute(
                'select legal_name from "gleif"."gleif"."gleif_lei_records"'
            ).fetchone()[0]
            == "Alpha Inc"
        )


def test_full_refresh_replaces_old_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "gleif.duckdb"
    first = duckdb_state.GleifStateRows(
        lei_records=[_minimal_lei_record("LEI0000000000000001", "Alpha Inc", "run-full")]
    )
    second = duckdb_state.GleifStateRows(
        lei_records=[_minimal_lei_record("LEI0000000000000002", "Beta Inc", "run-full-2")]
    )

    duckdb_state.replace_current_state(db_path, first)
    counts = duckdb_state.replace_current_state(db_path, second)

    assert counts["gleif_lei_records"] == 1
    with duckdb.connect(str(db_path), read_only=True) as connection:
        rows = connection.execute(
            'select lei, legal_name from "gleif"."gleif"."gleif_lei_records"'
        ).fetchall()

    assert rows == [("LEI0000000000000002", "Beta Inc")]


def test_delta_upserts_existing_lei_record(tmp_path: Path) -> None:
    db_path = tmp_path / "gleif.duckdb"
    base = duckdb_state.GleifStateRows(
        lei_records=[_minimal_lei_record("LEI0000000000000001", "Alpha Inc", "run-full")]
    )
    delta = duckdb_state.GleifStateRows(
        lei_records=[
            _minimal_lei_record("LEI0000000000000001", "Alpha Group Inc", "run-delta")
        ]
    )

    duckdb_state.replace_current_state(db_path, base)
    counts = duckdb_state.apply_delta_state(db_path, delta)

    assert counts["gleif_lei_records"] == 1
    with duckdb.connect(str(db_path), read_only=True) as connection:
        rows = connection.execute(
            'select lei, legal_name from "gleif"."gleif"."gleif_lei_records"'
        ).fetchall()

    assert rows == [("LEI0000000000000001", "Alpha Group Inc")]


def test_delta_appends_new_identifier_without_replacing_other_identifiers(tmp_path: Path) -> None:
    db_path = tmp_path / "gleif.duckdb"
    base = duckdb_state.GleifStateRows(
        lei_records=[_minimal_lei_record("LEI0000000000000001", "Alpha Inc", "run-full")],
        lei_identifiers=[
            _minimal_identifier("LEI0000000000000001", "BIC", "ALPHAUS1XXX", "run-full")
        ],
    )
    delta = duckdb_state.GleifStateRows(
        lei_identifiers=[
            _minimal_identifier("LEI0000000000000001", "ISIN", "US0000000001", "run-delta")
        ]
    )

    duckdb_state.replace_current_state(db_path, base)
    counts = duckdb_state.apply_delta_state(db_path, delta)

    assert counts["gleif_lei_identifiers"] == 2
    with duckdb.connect(str(db_path), read_only=True) as connection:
        rows = connection.execute(
            'select identifier_type, identifier_value from "gleif"."gleif"."gleif_lei_identifiers" order by 1'
        ).fetchall()

    assert rows == [("BIC", "ALPHAUS1XXX"), ("ISIN", "US0000000001")]


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
    def __init__(self, objects: dict[str, dict], blobs: dict[str, bytes] | None = None) -> None:
        self.objects = objects
        self.blobs = blobs or {}
        self.read_bytes_keys: list[str] = []
        self.state: dict | None = None

    def list_keys(self, prefix: str, *, bucket: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        self.read_bytes_keys.append(key)
        if key in self.blobs:
            raise AssertionError("raw ZIP blobs must be streamed through the S3 client")
        return json.dumps(self.objects[key]).encode()

    def write_json(self, key: str, body: str, *, bucket: str) -> None:
        self.state = json.loads(body)

    def client(self) -> "_FakeS3Client":
        return _FakeS3Client(self.blobs)


class _FakeS3Client:
    def __init__(self, blobs: dict[str, bytes]) -> None:
        self.blobs = blobs

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        return {"Body": BytesIO(self.blobs[Key])}


class _FakeContext:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id


def _zip_json(member_name: str, payload: dict) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, json.dumps(payload))
    return buffer.getvalue()


def _minimal_lei_record(lei: str, legal_name: str, source_run_id: str) -> dict:
    return {
        "lei": lei,
        "legal_name": legal_name,
        "legal_name_language": "en",
        "entity_status": "ACTIVE",
        "registration_status": "ISSUED",
        "jurisdiction": "US",
        "category": "GENERAL",
        "subcategory": None,
        "legal_form_id": None,
        "legal_form_other": None,
        "registered_at_id": None,
        "registered_at_other": None,
        "registered_as": None,
        "associated_entity_lei": None,
        "associated_entity_name": None,
        "successor_entity_lei": None,
        "successor_entity_name": None,
        "creation_date": None,
        "expiration_date": None,
        "expiration_reason": None,
        "initial_registration_date": None,
        "last_update_date": None,
        "next_renewal_date": None,
        "managing_lou": None,
        "corroboration_level": None,
        "validated_at_id": None,
        "validated_at_other": None,
        "validated_as": None,
        "conformity_flag": None,
        "legal_address_country": "US",
        "headquarters_address_country": "US",
        "primary_country_iso2": "US",
        "golden_copy_publish_date": None,
        "source_system": "gleif",
        "source_run_id": source_run_id,
        "retrieved_at": "2026-06-20T17:00:00+00:00",
        "resolved_at": "2026-06-20T17:01:00+00:00",
    }


def _minimal_identifier(
    lei: str,
    identifier_type: str,
    identifier_value: str,
    source_run_id: str,
) -> dict:
    return {
        "lei": lei,
        "identifier_type": identifier_type,
        "identifier_value": identifier_value,
        "identifier_scope": None,
        "mapping_source": "gleif",
        "is_primary": 1,
        "source_system": "gleif",
        "source_run_id": source_run_id,
        "retrieved_at": "2026-06-20T17:00:00+00:00",
        "resolved_at": "2026-06-20T17:01:00+00:00",
    }
