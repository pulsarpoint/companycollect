import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.denmark_cvr.company_details import (
    DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    DenmarkCvrCompanyDetailDownload,
    company_detail_bucket_key,
)
from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_DUCKDB_SCHEMA,
    DENMARK_CVR_PRODUCTION_UNITS_TABLE,
)
from dagster_v3.defs.denmark_cvr.partitions import DENMARK_CVR_ACTIVE_PARTITIONS
from dagster_v3.defs.denmark_cvr.production_units import (
    DenmarkCvrProductionUnitCaptureError,
    denmark_cvr_production_unit_updates_duckdb,
    denmark_cvr_production_unit_updates_s3,
    denmark_cvr_production_units_duckdb,
    denmark_cvr_production_units_s3,
    production_unit_object_key,
    production_unit_update_object_key,
    replace_production_units_from_captures,
    write_production_unit_partition,
)

DENMARK_CVR_BUCKET = "source-denmark-cvr"


class FakeObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = objects or {}
        self.list_prefixes: list[str] = []
        self.read_keys: list[str] = []
        self.written_keys: list[str] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket == DENMARK_CVR_BUCKET

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket == DENMARK_CVR_BUCKET
        self.list_prefixes.append(prefix)
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket == DENMARK_CVR_BUCKET
        self.read_keys.append(key)
        return self.objects[key]

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket == DENMARK_CVR_BUCKET
        self.objects[key] = body
        self.written_keys.append(key)


class FakeDetailResource:
    def __init__(self, downloads: dict[str, DenmarkCvrCompanyDetailDownload]) -> None:
        self.downloads = downloads
        self.calls: list[tuple[str, ...]] = []

    def iter_company_details(self, cvrs: tuple[str, ...], **_: Any):
        self.calls.append(cvrs)
        for cvr in cvrs:
            yield self.downloads[cvr]


def _duckdb_resource(path: Path) -> DuckDBResource:
    return DuckDBResource(database=str(path))


def _unit(
    cvr: str,
    p_number: str,
    *,
    name: str,
    cessation_date: str | None = None,
) -> dict[str, object]:
    return {
        "stamdata": {
            "cvrnummer": cvr,
            "pnummer": p_number,
            "navn": name,
            "adresse": "Produktionsvej 1",
            "postnummerOgBy": "1000 København K",
            "email": "unit@example.test",
            "telefon": "+45 11111111",
            "hovedbranche": {"branchekode": "620100", "titel": "Programming"},
            "bibrancher": [{"branchekode": "620200", "titel": "Consulting"}],
            "startdato": "2020-01-02",
            "ophoersdato": cessation_date,
            "reklamebeskyttet": False,
            "virksomhedsnavn": "Example ApS",
        },
        "antalAnsatte": {"maanedsbeskaeftigelse": []},
        "historiskStamdata": {"navn": []},
        "revisionsvirksomhed": None,
    }


def _production_units(
    cvr: str,
    *,
    active_units: list[dict[str, object]],
    ceased_units: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "aktiveProduktionsenheder": active_units,
        "ophoerteProduktionsenheder": ceased_units,
    }


def _download(
    cvr: str,
    *,
    active_units: list[dict[str, object]],
    ceased_units: list[dict[str, object]],
) -> DenmarkCvrCompanyDetailDownload:
    payload = {
        "stamdata": {"cvrnummer": cvr},
        "produktionsenheder": _production_units(
            cvr,
            active_units=active_units,
            ceased_units=ceased_units,
        ),
    }
    return DenmarkCvrCompanyDetailDownload(
        cvr=cvr,
        source_url=(
            "https://datacvr.virk.dk/gateway/virksomhed/"
            f"hentVirksomhed?cvrnummer={cvr}&locale=en"
        ),
        payload=payload,
        raw_body=json.dumps(payload, ensure_ascii=False),
        status=200,
        response_headers={"content-type": "application/json"},
    )


def _capture(
    cvr: str,
    *,
    production_units: dict[str, object],
    capture_type: str = "production_unit_snapshot",
    partition_key: str = "bucket_000",
    run_id: str = "capture-run",
) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "source": "denmark_cvr",
            "source_url": (
                "https://datacvr.virk.dk/gateway/virksomhed/"
                f"hentVirksomhed?cvrnummer={cvr}&locale=en"
            ),
            "source_capture_type": capture_type,
            "source_partition_key": partition_key,
            "retrieved_at": "2026-07-18T12:00:00+00:00",
            "run_id": run_id,
            "cvrnummer": cvr,
            "produktionsenheder": production_units,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def test_raw_partition_uses_company_bucket_and_one_detail_session() -> None:
    first_cvr = "45448037"
    partition_key = company_detail_bucket_key(first_cvr)
    second_cvr = next(
        str(value).zfill(8)
        for value in range(1, 100_000)
        if company_detail_bucket_key(str(value).zfill(8)) == partition_key
        and str(value).zfill(8) != first_cvr
    )
    details = FakeDetailResource(
        {
            first_cvr: _download(
                first_cvr,
                active_units=[_unit(first_cvr, "1000000001", name="First")],
                ceased_units=[],
            ),
            second_cvr: _download(
                second_cvr,
                active_units=[],
                ceased_units=[],
            ),
        }
    )
    store = FakeObjectStore()

    summary = write_production_unit_partition(
        object_store=store,
        details=details,
        partition_key=partition_key,
        cvrs=(first_cvr, second_cvr),
        run_id="raw-run",
        retrieved_at=datetime(2026, 7, 18, 12, 0, tzinfo=UTC),
    )

    assert details.calls == [(first_cvr, second_cvr)]
    assert summary.selected_company_count == 2
    assert summary.downloaded_company_count == 2
    assert summary.written_object_count == 2
    stored = json.loads(store.objects[production_unit_object_key(partition_key, first_cvr)])
    assert stored["cvrnummer"] == first_cvr
    assert stored["source_capture_type"] == "production_unit_snapshot"
    assert set(stored["produktionsenheder"]) == {
        "aktiveProduktionsenheder",
        "ophoerteProduktionsenheder",
    }
    assert "stamdata" not in stored


def test_capture_load_extracts_active_and_ceased_units_into_duckdb(
    tmp_path: Path,
) -> None:
    cvr = "45448037"
    partition_key = company_detail_bucket_key(cvr)
    store = FakeObjectStore(
        {
            production_unit_object_key(partition_key, cvr): _capture(
                cvr,
                partition_key=partition_key,
                production_units=_production_units(
                    cvr,
                    active_units=[_unit(cvr, "1000000001", name="Active unit")],
                    ceased_units=[
                        _unit(
                            cvr,
                            "1000000002",
                            name="Ceased unit",
                            cessation_date="2025-05-01",
                        )
                    ],
                ),
            )
        }
    )
    resource = _duckdb_resource(tmp_path / "denmark.duckdb")

    summary = replace_production_units_from_captures(
        object_store=store,
        denmark_cvr_duckdb=resource,
        source_prefix=f"denmark_cvr/production_units/{partition_key}/",
        expected_capture_type="production_unit_snapshot",
        expected_partition_key=partition_key,
        ingestion_run_id="normalize-run",
        processed_at=datetime(2026, 7, 18, 13, 0, tzinfo=UTC),
    )

    assert summary.company_count == 1
    assert summary.production_unit_count == 2
    with resource.get_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT p_number, company_cvr, is_active, name, source_run_id,
                   ingestion_run_id
            FROM {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PRODUCTION_UNITS_TABLE}
            ORDER BY p_number
            """
        ).fetchall()
    assert rows == [
        ("1000000001", cvr, True, "Active unit", "capture-run", "normalize-run"),
        ("1000000002", cvr, False, "Ceased unit", "capture-run", "normalize-run"),
    ]


def test_daily_capture_replaces_only_the_selected_company(tmp_path: Path) -> None:
    first_cvr = "45448037"
    second_cvr = "22756214"
    resource = _duckdb_resource(tmp_path / "denmark.duckdb")
    initial_store = FakeObjectStore(
        {
            f"denmark_cvr/production_units/initial/cvr={first_cvr}/production_units.json": _capture(
                first_cvr,
                production_units=_production_units(
                    first_cvr,
                    active_units=[_unit(first_cvr, "1000000001", name="Old first")],
                    ceased_units=[],
                ),
                partition_key="initial",
            ),
            f"denmark_cvr/production_units/initial/cvr={second_cvr}/production_units.json": _capture(
                second_cvr,
                production_units=_production_units(
                    second_cvr,
                    active_units=[_unit(second_cvr, "2000000001", name="Second")],
                    ceased_units=[],
                ),
                partition_key="initial",
            ),
        }
    )
    replace_production_units_from_captures(
        object_store=initial_store,
        denmark_cvr_duckdb=resource,
        source_prefix="denmark_cvr/production_units/initial/",
        expected_capture_type="production_unit_snapshot",
        expected_partition_key="initial",
        ingestion_run_id="initial-normalize",
        processed_at=datetime(2026, 7, 18, 13, 0, tzinfo=UTC),
    )
    update_date = "2026-07-19"
    update_store = FakeObjectStore(
        {
            production_unit_update_object_key(update_date, first_cvr): _capture(
                first_cvr,
                capture_type="production_unit_update",
                partition_key=update_date,
                run_id="update-capture",
                production_units=_production_units(
                    first_cvr,
                    active_units=[_unit(first_cvr, "1000000002", name="New first")],
                    ceased_units=[],
                ),
            )
        }
    )

    replace_production_units_from_captures(
        object_store=update_store,
        denmark_cvr_duckdb=resource,
        source_prefix=f"denmark_cvr/production_units/updates/date={update_date}/",
        expected_capture_type="production_unit_update",
        expected_partition_key=update_date,
        ingestion_run_id="update-normalize",
        processed_at=datetime(2026, 7, 19, 13, 0, tzinfo=UTC),
    )

    with resource.get_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT p_number, company_cvr, name
            FROM {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PRODUCTION_UNITS_TABLE}
            ORDER BY p_number
            """
        ).fetchall()
    assert rows == [
        ("1000000002", first_cvr, "New first"),
        ("2000000001", second_cvr, "Second"),
    ]


def test_invalid_capture_leaves_existing_rows_unchanged(tmp_path: Path) -> None:
    cvr = "45448037"
    resource = _duckdb_resource(tmp_path / "denmark.duckdb")
    valid_store = FakeObjectStore(
        {
            f"denmark_cvr/production_units/valid/cvr={cvr}/production_units.json": _capture(
                cvr,
                partition_key="valid",
                production_units=_production_units(
                    cvr,
                    active_units=[_unit(cvr, "1000000001", name="Existing")],
                    ceased_units=[],
                ),
            )
        }
    )
    replace_production_units_from_captures(
        object_store=valid_store,
        denmark_cvr_duckdb=resource,
        source_prefix="denmark_cvr/production_units/valid/",
        expected_capture_type="production_unit_snapshot",
        expected_partition_key="valid",
        ingestion_run_id="valid-normalize",
        processed_at=datetime(2026, 7, 18, 13, 0, tzinfo=UTC),
    )

    with pytest.raises(DenmarkCvrProductionUnitCaptureError):
        replace_production_units_from_captures(
            object_store=FakeObjectStore(
                {"denmark_cvr/production_units/invalid/production_units.json": b"{}"}
            ),
            denmark_cvr_duckdb=resource,
            source_prefix="denmark_cvr/production_units/invalid/",
            expected_capture_type="production_unit_update",
            expected_partition_key="2026-07-19",
            ingestion_run_id="invalid-normalize",
            processed_at=datetime(2026, 7, 19, 13, 0, tzinfo=UTC),
        )

    with resource.get_connection() as connection:
        assert connection.execute(
            f"SELECT p_number, name FROM {DENMARK_CVR_DUCKDB_SCHEMA}."
            f"{DENMARK_CVR_PRODUCTION_UNITS_TABLE}"
        ).fetchall() == [("1000000001", "Existing")]


def test_production_unit_s3_assets_are_company_duckdb_peers_of_detail_assets() -> None:
    for asset, partitions in (
        (denmark_cvr_production_units_s3, DENMARK_CVR_COMPANY_DETAIL_PARTITIONS),
        (denmark_cvr_production_unit_updates_s3, DENMARK_CVR_ACTIVE_PARTITIONS),
    ):
        assert asset.partitions_def is partitions
        assert {dependency.asset_key for dependency in asset.get_asset_spec().deps} == {
            dg.AssetKey("denmark_cvr_companies_duckdb")
        }
        assert asset.get_asset_spec().group_name == "denmark_cvr_production_units"
        assert asset.op.pool == "denmark_cvr_production_units"

    assert {
        dependency.asset_key
        for dependency in denmark_cvr_production_units_duckdb.get_asset_spec().deps
    } == {dg.AssetKey("denmark_cvr_production_units_s3")}
    assert {
        dependency.asset_key
        for dependency in denmark_cvr_production_unit_updates_duckdb.get_asset_spec().deps
    } == {dg.AssetKey("denmark_cvr_production_unit_updates_s3")}
    for asset in (
        denmark_cvr_production_units_duckdb,
        denmark_cvr_production_unit_updates_duckdb,
    ):
        assert asset.get_asset_spec().group_name == "denmark_cvr_production_units"
        assert asset.op.pool == "denmark_cvr_duckdb"


def test_production_unit_branch_is_registered_without_search_assets() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset_keys = load_defs().get_repository_def().asset_graph.get_all_asset_keys()

    for asset_key in (
        "denmark_cvr_production_units_s3",
        "denmark_cvr_production_unit_updates_s3",
        "denmark_cvr_production_units_duckdb",
        "denmark_cvr_production_unit_updates_duckdb",
    ):
        assert dg.AssetKey(asset_key) in asset_keys
    assert dg.AssetKey("denmark_cvr_production_units_backfill_s3") not in asset_keys
    assert dg.AssetKey("denmark_cvr_production_units_active_s3") not in asset_keys
