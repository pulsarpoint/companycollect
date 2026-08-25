import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import dagster as dg
import duckdb
import pytest

from dagster_v3.defs.serbia_apr_companies import assets, tables
from dagster_v3.defs.serbia_apr_companies.duckdb import (
    replace_serbia_apr_companies_duckdb,
)
from dagster_v3.defs.serbia_apr_companies.resources import latest_snapshot_manifest


class _ObjectStore:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket == tables.S3_BUCKET
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket == tables.S3_BUCKET
        return self.objects[key]

    def download_file(
        self,
        key: str,
        target_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket == tables.S3_BUCKET
        Path(target_path).write_bytes(self.objects[key])


def _company(
    *,
    name: str,
    status: str = "Активан",
    municipality_code: str = "70017",
    municipality_name: str = "ГРАД БЕОГРАД",
) -> dict[str, str]:
    return {
        "PoslovnoIme": name,
        "SifraOpstine": municipality_code,
        "NazivOpstine": municipality_name,
        "NazivStatus": status,
        "DatumOsnivanja": "1990-07-01",
        "NazivPravneForme": "Друштво са ограниченом одговорношћу",
        "SifraDelatnosti": "6201",
    }


def _snapshot(
    records: dict[str, dict[str, str]],
    *,
    snapshot_date: str = "2026-07-31",
) -> bytes:
    return json.dumps(
        {"DatumPreseka": snapshot_date, "Podaci": records},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _manifest(
    body: bytes,
    *,
    object_key: str = "serbia_apr_companies/raw/companies.json",
    record_count: int = 2,
    snapshot_date: str = "2026-07-31",
    retrieved_at: str = "2026-08-25T08:30:00+00:00",
    run_id: str = "raw-run",
) -> dict[str, object]:
    return {
        "bucket": tables.S3_BUCKET,
        "content_type": "application/json",
        "downloaded": True,
        "object_key": object_key,
        "record_count": record_count,
        "retrieved_at": retrieved_at,
        "sha256": sha256(body).hexdigest(),
        "size_bytes": len(body),
        "snapshot_date": snapshot_date,
        "source_license": tables.SOURCE_LICENSE,
        "source_run_id": run_id,
        "source_slug": tables.SOURCE_SLUG,
        "source_url": tables.SOURCE_URL,
    }


def _valid_snapshot() -> tuple[_ObjectStore, dict[str, object]]:
    body = _snapshot(
        {
            "00003506": _company(name="ПРВО ДРУШТВО"),
            "21141666": _company(
                name="DRUGO DRUŠTVO",
                status="У стечају",
                municipality_code="70670",
                municipality_name="НОВИ САД",
            ),
        }
    )
    manifest = _manifest(body)
    return _ObjectStore({str(manifest["object_key"]): body}), manifest


def _table_columns(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> tuple[str, ...]:
    return tuple(
        row[1]
        for row in connection.execute(
            f"pragma table_info('{tables.DUCKDB_SCHEMA}.{table_name}')"
        ).fetchall()
    )


def test_latest_manifest_prefers_snapshot_date_then_retrieval_time() -> None:
    older_body = _snapshot({}, snapshot_date="2026-06-30")
    current_body = _snapshot({}, snapshot_date="2026-07-31")
    corrected_body = _snapshot({}, snapshot_date="2026-07-31") + b" "
    manifests = {
        f"{tables.S3_MANIFEST_PREFIX}/older.json": _manifest(
            older_body,
            record_count=0,
            snapshot_date="2026-06-30",
            retrieved_at="2026-08-25T10:00:00+00:00",
            run_id="older-snapshot",
        ),
        f"{tables.S3_MANIFEST_PREFIX}/current.json": _manifest(
            current_body,
            record_count=0,
            retrieved_at="2026-08-25T08:00:00+00:00",
            run_id="current",
        ),
        f"{tables.S3_MANIFEST_PREFIX}/corrected.json": _manifest(
            corrected_body,
            record_count=0,
            retrieved_at="2026-08-25T09:00:00+00:00",
            run_id="corrected",
        ),
    }
    store = _ObjectStore(
        {key: json.dumps(value).encode("utf-8") for key, value in manifests.items()}
    )

    selected = latest_snapshot_manifest(store)  # type: ignore[arg-type]

    assert selected["source_run_id"] == "corrected"


def test_latest_manifest_requires_the_raw_s3_asset_first() -> None:
    with pytest.raises(ValueError, match="raw_snapshot_s3"):
        latest_snapshot_manifest(_ObjectStore({}))  # type: ignore[arg-type]


def test_duckdb_loader_creates_typed_catalog_history_and_current_tables(
    tmp_path: Path,
) -> None:
    store, manifest = _valid_snapshot()
    connection = duckdb.connect(str(tmp_path / "apr.duckdb"))
    try:
        counts = replace_serbia_apr_companies_duckdb(
            connection=connection,
            object_store=store,  # type: ignore[arg-type]
            manifest=manifest,
            loaded_at=datetime(2026, 8, 25, 9, 0, tzinfo=UTC),
            minimum_record_count=1,
            batch_size=1,
        )

        assert counts == {
            tables.SNAPSHOT_RUNS_TABLE: 1,
            tables.COMPANY_OBSERVATIONS_TABLE: 2,
            tables.COMPANIES_CURRENT_TABLE: 2,
        }
        assert _table_columns(connection, tables.SNAPSHOT_RUNS_TABLE) == (
            tables.SNAPSHOT_RUN_COLUMNS
        )
        assert _table_columns(connection, tables.COMPANY_OBSERVATIONS_TABLE) == (
            tables.COMPANY_COLUMNS
        )
        assert _table_columns(connection, tables.COMPANIES_CURRENT_TABLE) == (
            tables.COMPANY_COLUMNS
        )

        rows = connection.execute(
            f"""
            select registration_number, legal_name, status, is_active,
                   length(source_payload_hash), length(source_record_uid),
                   length(state_fingerprint), raw_entity
            from {tables.DUCKDB_SCHEMA}.{tables.COMPANIES_CURRENT_TABLE}
            order by registration_number
            """
        ).fetchall()
        assert rows[0][:7] == (
            "00003506",
            "ПРВО ДРУШТВО",
            "active",
            True,
            64,
            64,
            64,
        )
        assert json.loads(rows[0][7])["PoslovnoIme"] == "ПРВО ДРУШТВО"
        assert rows[1][2:4] == ("bankruptcy", False)
    finally:
        connection.close()


def test_duckdb_loader_is_idempotent_for_the_same_snapshot(tmp_path: Path) -> None:
    store, manifest = _valid_snapshot()
    connection = duckdb.connect(str(tmp_path / "apr.duckdb"))
    try:
        for loaded_at in (
            datetime(2026, 8, 25, 9, 0, tzinfo=UTC),
            datetime(2026, 8, 25, 9, 5, tzinfo=UTC),
        ):
            replace_serbia_apr_companies_duckdb(
                connection=connection,
                object_store=store,  # type: ignore[arg-type]
                manifest=manifest,
                loaded_at=loaded_at,
                minimum_record_count=1,
            )

        assert (
            connection.execute(
                f"select count(*) from {tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_RUNS_TABLE}"
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                f"select count(*) from {tables.DUCKDB_SCHEMA}.{tables.COMPANY_OBSERVATIONS_TABLE}"
            ).fetchone()[0]
            == 2
        )
        assert (
            connection.execute(
                f"select count(*) from {tables.DUCKDB_SCHEMA}.{tables.COMPANIES_CURRENT_TABLE}"
            ).fetchone()[0]
            == 2
        )
    finally:
        connection.close()


def test_duckdb_loader_refuses_an_s3_object_that_differs_from_its_manifest(
    tmp_path: Path,
) -> None:
    store, manifest = _valid_snapshot()
    object_key = str(manifest["object_key"])
    store.objects[object_key] = store.objects[object_key].replace(b"6201", b"6202", 1)
    connection = duckdb.connect(str(tmp_path / "apr.duckdb"))
    try:
        with pytest.raises(ValueError, match="SHA-256"):
            replace_serbia_apr_companies_duckdb(
                connection=connection,
                object_store=store,  # type: ignore[arg-type]
                manifest=manifest,
                loaded_at=datetime(2026, 8, 25, 9, 0, tzinfo=UTC),
                minimum_record_count=1,
            )
    finally:
        connection.close()


def test_invalid_new_snapshot_leaves_all_durable_tables_unchanged(
    tmp_path: Path,
) -> None:
    store, manifest = _valid_snapshot()
    connection = duckdb.connect(str(tmp_path / "apr.duckdb"))
    try:
        replace_serbia_apr_companies_duckdb(
            connection=connection,
            object_store=store,  # type: ignore[arg-type]
            manifest=manifest,
            loaded_at=datetime(2026, 8, 25, 9, 0, tzinfo=UTC),
            minimum_record_count=1,
        )
        before = {
            table_name: connection.execute(
                f"select count(*) from {tables.DUCKDB_SCHEMA}.{table_name}"
            ).fetchone()[0]
            for table_name in (
                tables.SNAPSHOT_RUNS_TABLE,
                tables.COMPANY_OBSERVATIONS_TABLE,
                tables.COMPANIES_CURRENT_TABLE,
            )
        }

        invalid_body = _snapshot(
            {"12345678": _company(name="INVALID", status="Нови статус")},
            snapshot_date="2026-08-31",
        )
        invalid_manifest = _manifest(
            invalid_body,
            object_key="serbia_apr_companies/raw/invalid.json",
            record_count=1,
            snapshot_date="2026-08-31",
            run_id="invalid-run",
        )
        store.objects[str(invalid_manifest["object_key"])] = invalid_body

        with pytest.raises(ValueError, match="unrecognized APR company status"):
            replace_serbia_apr_companies_duckdb(
                connection=connection,
                object_store=store,  # type: ignore[arg-type]
                manifest=invalid_manifest,
                loaded_at=datetime(2026, 9, 1, tzinfo=UTC),
                minimum_record_count=1,
            )

        after = {
            table_name: connection.execute(
                f"select count(*) from {tables.DUCKDB_SCHEMA}.{table_name}"
            ).fetchone()[0]
            for table_name in before
        }
        assert after == before
        assert (
            connection.execute(
                f"select legal_name from {tables.DUCKDB_SCHEMA}.{tables.COMPANIES_CURRENT_TABLE} "
                "where registration_number = '00003506'"
            ).fetchone()[0]
            == "ПРВО ДРУШТВО"
        )
    finally:
        connection.close()


def test_duckdb_assets_share_one_atomic_op_and_the_raw_s3_parent() -> None:
    from dagster_v3.definitions import defs as load_defs

    graph = load_defs().get_repository_def().asset_graph
    for asset_name in tables.DUCKDB_ASSET_NAMES:
        node = graph.get(dg.AssetKey(asset_name))
        assert node.group_name == tables.GROUP_NAME
        assert node.parent_keys == {dg.AssetKey("serbia_apr_companies_raw_snapshot_s3")}
        assert node.pools == {tables.DUCKDB_POOL}
        assert {"python", "duckdb", "s3", "json", "apr"} <= node.kinds
        assert node.tags["layer"] == "duckdb"

    assert assets.serbia_apr_companies_duckdb_load.can_subset is False


def test_loader_uses_arrow_batches_instead_of_executemany() -> None:
    source_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "serbia_apr_companies"
        / "duckdb.py"
    )

    assert "executemany" not in source_path.read_text(encoding="utf-8")
