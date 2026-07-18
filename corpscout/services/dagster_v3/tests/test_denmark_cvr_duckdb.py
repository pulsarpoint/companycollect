import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_COMPANIES_TABLE,
    DENMARK_CVR_DUCKDB_SCHEMA,
    DENMARK_CVR_INGESTED_OBJECTS_TABLE,
    DENMARK_CVR_PERSONS_TABLE,
    DENMARK_CVR_PRODUCTION_UNITS_TABLE,
    DenmarkCvrStoredObjectError,
    denmark_cvr_companies_duckdb,
    denmark_cvr_persons_duckdb,
    denmark_cvr_production_units_duckdb,
    defs,
    source_result_object_keys,
    update_denmark_cvr_companies_duckdb,
    update_denmark_cvr_persons_duckdb,
    update_denmark_cvr_production_units_duckdb,
)
from dagster_v3.defs.denmark_cvr.resources import (
    DATACVR_COMPANY_ENTITY_TYPE,
    DATACVR_PERSON_ENTITY_TYPE,
    DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
    DenmarkCvrEntityType,
)

DENMARK_CVR_BUCKET = "source-denmark-cvr"


class FakeObjectStore:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.read_keys: list[str] = []
        self.list_prefixes: list[str] = []

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket == DENMARK_CVR_BUCKET
        self.list_prefixes.append(prefix)
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket == DENMARK_CVR_BUCKET
        self.read_keys.append(key)
        return self.objects[key]


def _company(
    cvr: str,
    *,
    name: str | None,
    start_date: str,
) -> dict[str, Any]:
    return {
        "beliggenhedsadresse": "Testvej 1",
        "by": "Testby",
        "coNavn": None,
        "cvr": cvr,
        "email": "company@example.test",
        "enhedsnummer": f"4{cvr}",
        "enhedstype": "virksomhed",
        "harPseudoCvr": False,
        "highlightBinavn": False,
        "highlightHistoriskBinavn": False,
        "highlightHistoriskHovednavn": False,
        "hovedbranche": "Test industry",
        "ophoersDato": "",
        "postnummer": "1000",
        "reg": None,
        "reklameBeskyttet": False,
        "senesteNavn": name,
        "startDato": start_date,
        "status": "NORMAL",
        "telefonnummer": "+45 00000000",
        "virksomhedsform": "Anpartsselskab",
        "visNavnPostfix": False,
    }


def _production_unit(
    production_unit_number: str,
    *,
    name: str,
    start_date: str,
) -> dict[str, Any]:
    return {
        "beliggenhedsadresse": "Produktionsvej 3",
        "by": "Testby",
        "coNavn": None,
        "email": "unit@example.test",
        "enhedstype": "produktionsenhed",
        "hovedbranche": "Test production",
        "ophoersDato": "",
        "pNummer": production_unit_number,
        "postnummer": "1000",
        "reklameBeskyttet": False,
        "senesteNavn": name,
        "startDato": start_date,
        "status": "NORMAL",
        "telefonnummer": "+45 11111111",
    }


def _person(
    entity_number: str,
    *,
    name: str,
    company_cvr: str,
) -> dict[str, Any]:
    return {
        "aktiveTilknytninger": [{"rolle": "DIREKTION"}],
        "beliggenhedsadresse": "Personvej 2",
        "by": None,
        "coNavn": None,
        "enhedsnummer": entity_number,
        "enhedstype": "person",
        "harAktiveRelationer": True,
        "personType": "PERSON",
        "postnummer": None,
        "senesteNavn": name,
        "tilknytning": [{"cvr": company_cvr}],
    }


def _entity_capture(
    partition_key: str,
    entity_type: DenmarkCvrEntityType,
    entities: list[dict[str, Any]],
    *,
    is_complete: bool = True,
) -> bytes:
    if len(partition_key) == 7:
        start_date = f"{partition_key}-01"
        end_date = f"{partition_key}-28"
    else:
        start_date = partition_key
        end_date = partition_key
    return json.dumps(
        {
            "schema_version": 1,
            "source": "denmark_cvr",
            "source_url": "https://datacvr.virk.dk",
            "entity_type": entity_type,
            "partition_key": partition_key,
            "start_date": start_date,
            "end_date": end_date,
            "retrieved_at": "2026-07-16T12:00:00+00:00",
            "run_id": f"run-{entity_type}-{partition_key}",
            "is_complete": is_complete,
            "generic_advertised_count": len(entities),
            "filtered_advertised_count": len(entities),
            "downloaded_entity_count": len(entities),
            "enheder": entities,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _capture(
    partition_key: str,
    companies: list[dict[str, Any]],
    *,
    is_complete: bool = True,
) -> bytes:
    return _entity_capture(
        partition_key,
        DATACVR_COMPANY_ENTITY_TYPE,
        companies,
        is_complete=is_complete,
    )


def _duckdb_resource(path: Path) -> DuckDBResource:
    return DuckDBResource(database=str(path))


def test_source_result_keys_include_backfill_and_active_results_only() -> None:
    object_store = FakeObjectStore(
        {
            "denmark_cvr/backfill/month=2025-01/companies.json": b"{}",
            "denmark_cvr/backfill/month=2025-02/companies_incomplete.json": b"{}",
            "denmark_cvr/backfill/month=2025-02/invalid/filter=x/page=0.invalid.json": b"{}",
            "denmark_cvr/active/date=2026-07-01/companies.json": b"{}",
            "denmark_cvr/active/date=2026-07-01/other.json": b"{}",
        }
    )

    keys = source_result_object_keys(
        object_store,
        entity_type=DATACVR_COMPANY_ENTITY_TYPE,
    )

    assert keys == (
        "denmark_cvr/backfill/month=2025-01/companies.json",
        "denmark_cvr/backfill/month=2025-02/companies_incomplete.json",
        "denmark_cvr/active/date=2026-07-01/companies.json",
    )


def test_source_result_keys_are_isolated_by_entity_type() -> None:
    object_store = FakeObjectStore(
        {
            "denmark_cvr/backfill/month=2025-01/companies.json": b"{}",
            "denmark_cvr/backfill/month=2025-01/production_units.json": b"{}",
            "denmark_cvr/active/date=2026-07-01/persons_incomplete.json": b"{}",
        }
    )

    production_unit_keys = source_result_object_keys(
        object_store,
        entity_type=DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
    )
    person_keys = source_result_object_keys(
        object_store,
        entity_type=DATACVR_PERSON_ENTITY_TYPE,
    )

    assert production_unit_keys == (
        "denmark_cvr/backfill/month=2025-01/production_units.json",
    )
    assert person_keys == (
        "denmark_cvr/active/date=2026-07-01/persons_incomplete.json",
    )


def test_duckdb_initial_load_normalizes_deduplicates_and_records_state(
    tmp_path: Path,
) -> None:
    backfill_key = "denmark_cvr/backfill/month=2026-06/companies.json"
    active_key = "denmark_cvr/active/date=2026-07-01/companies.json"
    object_store = FakeObjectStore(
        {
            backfill_key: _capture(
                "2026-06",
                [_company("12345678", name="Older name", start_date="2026-06-15")],
            ),
            active_key: _capture(
                "2026-07-01",
                [
                    _company("12345678", name="Latest name", start_date="2026-06-15"),
                    _company("87654321", name=None, start_date="2026-07-01"),
                ],
            ),
        }
    )
    database_path = tmp_path / "denmark.duckdb"
    duckdb_resource = _duckdb_resource(database_path)

    summary = update_denmark_cvr_companies_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="ingestion-run",
        processed_at=datetime(2026, 7, 16, 13, 0, tzinfo=UTC),
    )

    assert summary.discovered_object_count == 2
    assert summary.processed_object_count == 2
    assert summary.processed_row_count == 3
    assert summary.entity_count == 2
    assert summary.incomplete_object_count == 0
    assert object_store.read_keys == [backfill_key, active_key]
    with duckdb_resource.get_connection() as connection:
        companies = connection.execute(
            f"""
            select cvr, name, source_capture_type, source_object_key, raw_record
            from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE}
            order by cvr
            """
        ).fetchall()
        ingested_objects = connection.execute(
            f"""
            select object_key, source_row_count
            from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_INGESTED_OBJECTS_TABLE}
            order by object_key
            """
        ).fetchall()

    assert companies[0][:4] == (
        "12345678",
        "Latest name",
        "active",
        active_key,
    )
    assert json.loads(companies[1][4])["senesteNavn"] is None
    assert ingested_objects == [(active_key, 2), (backfill_key, 1)]


def test_duckdb_repeated_run_reads_only_new_source_objects(tmp_path: Path) -> None:
    first_key = "denmark_cvr/active/date=2026-07-01/companies.json"
    second_key = "denmark_cvr/active/date=2026-07-02/companies_incomplete.json"
    object_store = FakeObjectStore(
        {
            first_key: _capture(
                "2026-07-01",
                [_company("12345678", name="First", start_date="2026-07-01")],
            )
        }
    )
    duckdb_resource = _duckdb_resource(tmp_path / "denmark.duckdb")

    update_denmark_cvr_companies_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="first-run",
        processed_at=datetime(2026, 7, 16, 13, 0, tzinfo=UTC),
    )
    repeated = update_denmark_cvr_companies_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="repeated-run",
        processed_at=datetime(2026, 7, 16, 13, 5, tzinfo=UTC),
    )
    object_store.objects[second_key] = _capture(
        "2026-07-02",
        [_company("87654321", name="Second", start_date="2026-07-02")],
        is_complete=False,
    )
    incremental = update_denmark_cvr_companies_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="incremental-run",
        processed_at=datetime(2026, 7, 16, 13, 10, tzinfo=UTC),
    )

    assert repeated.processed_object_count == 0
    assert repeated.processed_row_count == 0
    assert incremental.processed_object_count == 1
    assert incremental.processed_row_count == 1
    assert incremental.entity_count == 2
    assert incremental.incomplete_object_count == 1
    assert object_store.read_keys == [first_key, second_key]


def test_production_units_duckdb_normalizes_and_deduplicates_rows(
    tmp_path: Path,
) -> None:
    backfill_key = "denmark_cvr/backfill/month=2026-06/production_units.json"
    active_key = "denmark_cvr/active/date=2026-07-01/production_units.json"
    object_store = FakeObjectStore(
        {
            backfill_key: _entity_capture(
                "2026-06",
                DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
                [
                    _production_unit(
                        "1000000001",
                        name="Older unit name",
                        start_date="2026-06-15",
                    )
                ],
            ),
            active_key: _entity_capture(
                "2026-07-01",
                DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
                [
                    _production_unit(
                        "1000000001",
                        name="Latest unit name",
                        start_date="2026-06-15",
                    ),
                    _production_unit(
                        "1000000002",
                        name="Second unit",
                        start_date="2026-07-01",
                    ),
                ],
            ),
        }
    )
    duckdb_resource = _duckdb_resource(tmp_path / "denmark.duckdb")

    summary = update_denmark_cvr_production_units_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="production-unit-run",
        processed_at=datetime(2026, 7, 16, 13, 0, tzinfo=UTC),
    )

    assert summary.entity_count == 2
    assert summary.processed_object_count == 2
    assert summary.processed_row_count == 3
    assert summary.min_start_date.isoformat() == "2026-06-15"
    assert summary.max_start_date.isoformat() == "2026-07-01"
    with duckdb_resource.get_connection() as connection:
        rows = connection.execute(
            f"""
            select p_number, name, email, source_capture_type, raw_record
            from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PRODUCTION_UNITS_TABLE}
            order by p_number
            """
        ).fetchall()

    assert rows[0][:4] == (
        "1000000001",
        "Latest unit name",
        "unit@example.test",
        "active",
    )
    assert json.loads(rows[1][4])["pNummer"] == "1000000002"


def test_persons_duckdb_preserves_affiliations_as_json(tmp_path: Path) -> None:
    person_key = "denmark_cvr/active/date=2026-07-01/persons.json"
    object_store = FakeObjectStore(
        {
            person_key: _entity_capture(
                "2026-07-01",
                DATACVR_PERSON_ENTITY_TYPE,
                [
                    _person(
                        "4000000002",
                        name="Example Person",
                        company_cvr="12345678",
                    )
                ],
            )
        }
    )
    duckdb_resource = _duckdb_resource(tmp_path / "denmark.duckdb")

    summary = update_denmark_cvr_persons_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="person-run",
        processed_at=datetime(2026, 7, 16, 13, 0, tzinfo=UTC),
    )

    assert summary.entity_count == 1
    assert summary.processed_object_count == 1
    assert summary.processed_row_count == 1
    with duckdb_resource.get_connection() as connection:
        row = connection.execute(
            f"""
            select entity_number, name, has_active_relations,
                   active_affiliations, affiliations, raw_record
            from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PERSONS_TABLE}
            """
        ).fetchone()

    assert row[:3] == ("4000000002", "Example Person", True)
    assert json.loads(row[3]) == [{"rolle": "DIREKTION"}]
    assert json.loads(row[4]) == [{"cvr": "12345678"}]
    assert json.loads(row[5])["personType"] == "PERSON"


def test_entity_loaders_share_ingestion_state_without_consuming_other_types(
    tmp_path: Path,
) -> None:
    production_unit_key = "denmark_cvr/active/date=2026-07-01/production_units.json"
    person_key = "denmark_cvr/active/date=2026-07-01/persons.json"
    object_store = FakeObjectStore(
        {
            production_unit_key: _entity_capture(
                "2026-07-01",
                DATACVR_PRODUCTION_UNIT_ENTITY_TYPE,
                [
                    _production_unit(
                        "1000000001",
                        name="Production unit",
                        start_date="2026-07-01",
                    )
                ],
            ),
            person_key: _entity_capture(
                "2026-07-01",
                DATACVR_PERSON_ENTITY_TYPE,
                [
                    _person(
                        "4000000002",
                        name="Person",
                        company_cvr="12345678",
                    )
                ],
            ),
        }
    )
    duckdb_resource = _duckdb_resource(tmp_path / "denmark.duckdb")

    update_denmark_cvr_production_units_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="production-unit-run",
        processed_at=datetime(2026, 7, 16, 13, 0, tzinfo=UTC),
    )
    person_summary = update_denmark_cvr_persons_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=duckdb_resource,
        ingestion_run_id="person-run",
        processed_at=datetime(2026, 7, 16, 13, 5, tzinfo=UTC),
    )

    assert person_summary.discovered_object_count == 1
    assert person_summary.already_ingested_object_count == 0
    assert object_store.read_keys == [production_unit_key, person_key]
    with duckdb_resource.get_connection() as connection:
        ingested_keys = connection.execute(
            f"""
            select object_key
            from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_INGESTED_OBJECTS_TABLE}
            order by object_key
            """
        ).fetchall()
    assert ingested_keys == [(person_key,), (production_unit_key,)]


def test_invalid_stored_object_rolls_back_the_run_without_logging_payload(
    tmp_path: Path,
) -> None:
    valid_key = "denmark_cvr/backfill/month=2026-06/companies.json"
    invalid_key = "denmark_cvr/active/date=2026-07-01/companies.json"
    private_value = "private company value"
    object_store = FakeObjectStore(
        {
            valid_key: _capture(
                "2026-06",
                [_company("12345678", name="Valid", start_date="2026-06-01")],
            ),
            invalid_key: json.dumps({"private": private_value}).encode("utf-8"),
        }
    )
    duckdb_resource = _duckdb_resource(tmp_path / "denmark.duckdb")

    with pytest.raises(DenmarkCvrStoredObjectError) as exc_info:
        update_denmark_cvr_companies_duckdb(
            object_store=object_store,
            denmark_cvr_duckdb=duckdb_resource,
            ingestion_run_id="failed-run",
            processed_at=datetime(2026, 7, 16, 13, 0, tzinfo=UTC),
        )

    assert invalid_key in str(exc_info.value)
    assert private_value not in str(exc_info.value)
    with duckdb_resource.get_connection() as connection:
        assert (
            connection.execute(
                f"select count(*) from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE}"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                f"select count(*) from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_INGESTED_OBJECTS_TABLE}"
            ).fetchone()[0]
            == 0
        )


def test_denmark_cvr_duckdb_asset_has_both_raw_dependencies() -> None:
    spec = denmark_cvr_companies_duckdb.get_asset_spec()

    assert {dependency.asset_key for dependency in spec.deps} == {
        dg.AssetKey("denmark_cvr_backfill_s3"),
        dg.AssetKey("denmark_cvr_active_s3"),
    }
    assert denmark_cvr_companies_duckdb.partitions_def is None
    assert denmark_cvr_companies_duckdb.op.pool == "denmark_cvr_duckdb"
    assert spec.tags["layer"] == "normalized"


@pytest.mark.parametrize(
    ("asset", "expected_dependencies", "expected_table"),
    [
        (
            denmark_cvr_production_units_duckdb,
            {
                dg.AssetKey("denmark_cvr_production_units_backfill_s3"),
                dg.AssetKey("denmark_cvr_production_units_active_s3"),
            },
            DENMARK_CVR_PRODUCTION_UNITS_TABLE,
        ),
        (
            denmark_cvr_persons_duckdb,
            {
                dg.AssetKey("denmark_cvr_persons_backfill_s3"),
                dg.AssetKey("denmark_cvr_persons_active_s3"),
            },
            DENMARK_CVR_PERSONS_TABLE,
        ),
    ],
)
def test_non_company_duckdb_assets_have_matching_raw_dependencies(
    asset: Any,
    expected_dependencies: set[dg.AssetKey],
    expected_table: str,
) -> None:
    spec = asset.get_asset_spec()

    assert {dependency.asset_key for dependency in spec.deps} == expected_dependencies
    assert asset.partitions_def is None
    assert asset.op.pool == "denmark_cvr_duckdb"
    assert spec.metadata["duckdb_table"] == expected_table
    assert spec.tags["layer"] == "normalized"


def test_denmark_cvr_duckdb_definitions_register_assets_and_resource() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()

    assert dg.AssetKey("denmark_cvr_companies_duckdb") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_production_units_duckdb") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert dg.AssetKey("denmark_cvr_persons_duckdb") in (
        repository.asset_graph.get_all_asset_keys()
    )
    assert len(defs.assets) == 3
    assert set(defs.resources) == {"denmark_cvr_duckdb"}
    assert defs.schedules is None
