import json
from collections.abc import Iterator
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
import requests
from dagster import AssetKey
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.wikidata import tables
from dagster_v3.definitions import defs as load_project_defs

WIKIDATA_TEST_BUCKET = "source-wikidata-weekly"


def test_wikidata_table_assets_are_registered_one_to_one() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    resource_keys = repository.get_top_level_resources().keys()

    assert {
        "wikidata_exchanges_raw",
        "wikidata_company_source_units",
        "wikidata_company_pages_raw",
        "wikidata_company_profiles_raw",
        "wikidata_company_identifiers_raw",
        "wikidata_company_relationships_raw",
        "wikidata_company_people_raw",
        "wikidata_persons_raw",
        "wikidata_company_source_snapshot",
        "wikidata_raw_snapshot",
    }.issubset(asset_keys)
    assert "wikidata_company_seed_units" not in asset_keys
    assert "wikidata_company_seed_raw_objects" not in asset_keys
    assert "wikidata_company_seed_raw_snapshot" not in asset_keys
    for table_name in tables.WIKIDATA_TABLES:
        assert f"{table_name}_duckdb" in asset_keys
        assert table_name in asset_keys
    assert "wikidata_snapshot_complete" in asset_keys
    assert "wikidata_company_seed_clickhouse" not in asset_keys
    assert "wikidata_listed_companies_duckdb" not in asset_keys
    assert "clickhouse" in resource_keys
    assert "object_store" in resource_keys
    assert (
        repository.get_top_level_resources()["clickhouse"].configurable_resource_cls
        is ClickhouseResource
    )


def test_wikidata_clickhouse_tables_depend_on_matching_duckdb_assets() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph

    for table_name in tables.WIKIDATA_TABLES:
        clickhouse_asset = asset_graph.get(AssetKey([table_name]))
        assert clickhouse_asset.parent_keys == {AssetKey([f"{table_name}_duckdb"])}


def test_wikidata_duckdb_tables_depend_on_raw_s3_snapshot() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph

    for table_name in tables.WIKIDATA_TABLES:
        asset = asset_graph.get(AssetKey([f"{table_name}_duckdb"]))
        assert asset.is_executable
        assert asset.parent_keys == {
            AssetKey(["wikidata_raw_snapshot"]),
        }


def test_wikidata_completion_depends_on_all_clickhouse_tables() -> None:
    repository = load_project_defs().get_repository_def()

    completion_asset = repository.asset_graph.get(
        AssetKey(["wikidata_snapshot_complete"])
    )

    assert completion_asset.parent_keys == {
        AssetKey([table_name]) for table_name in tables.WIKIDATA_TABLES
    }


def test_wikidata_tables_use_distinct_duckdb_files(monkeypatch, tmp_path: Path) -> None:
    from dagster_v3.defs.wikidata import assets

    monkeypatch.setattr(assets, "WIKIDATA_DUCKDB_DIRECTORY", tmp_path)

    paths = {
        assets.wikidata_duckdb_path(table_name) for table_name in tables.WIKIDATA_TABLES
    }

    assert len(paths) == len(tables.WIKIDATA_TABLES)
    assert paths == {
        tmp_path / f"{table_name}.duckdb" for table_name in tables.WIKIDATA_TABLES
    }


def test_wikidata_completion_verifies_one_source_run(monkeypatch) -> None:
    from dagster_v3.defs.wikidata import assets

    class Client:
        def execute(self, query: str) -> list[tuple[Any, ...]]:
            if "listings.exchange_wikidata_id" in query:
                return []
            table_name = query.split("from corpscout.", 1)[1].split(" ", 1)[0]
            if table_name in assets.WIKIDATA_EMPTY_ALLOWED_TABLES:
                return []
            return [("run-1", 3)]

    class ConnectionContext:
        def __enter__(self) -> Client:
            return Client()

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        ClickhouseResource,
        "get_connection",
        lambda _self: ConnectionContext(),
    )

    result = assets._validate_wikidata_clickhouse_snapshot(
        ClickhouseResource(host="localhost")
    )

    assert result.metadata["source_run_id"] == "run-1"
    assert result.metadata["wikidata_companies_row_count"] == 3
    assert result.metadata["wikidata_company_people_row_count"] == 0


def test_wikidata_completion_rejects_mixed_source_runs(monkeypatch) -> None:
    from dagster_v3.defs.wikidata import assets

    class Client:
        def execute(self, query: str) -> list[tuple[Any, ...]]:
            if "listings.exchange_wikidata_id" in query:
                return []
            table_name = query.split("from corpscout.", 1)[1].split(" ", 1)[0]
            source_run_id = (
                "run-2"
                if table_name == tables.WIKIDATA_COMPANY_WEBSITES_TABLE
                else "run-1"
            )
            return [(source_run_id, 3)]

    class ConnectionContext:
        def __enter__(self) -> Client:
            return Client()

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        ClickhouseResource,
        "get_connection",
        lambda _self: ConnectionContext(),
    )

    with pytest.raises(ValueError, match="different source_run_id"):
        assets._validate_wikidata_clickhouse_snapshot(
            ClickhouseResource(host="localhost")
        )


def test_wikidata_domain_jobs_and_weekly_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    job_names = set(repository.job_names)
    schedule_names = {schedule.name for schedule in repository.schedule_defs}
    weekly_schedule = next(
        schedule
        for schedule in repository.schedule_defs
        if schedule.name == "wikidata_weekly_schedule"
    )

    assert {
        "wikidata_publish_job",
        "wikidata_exchange_discovery_job",
        "wikidata_company_source_discovery_job",
        "wikidata_company_pages_job",
        "wikidata_company_profiles_job",
        "wikidata_company_identifiers_job",
        "wikidata_company_relationships_job",
        "wikidata_company_people_job",
        "wikidata_persons_job",
        "wikidata_company_source_snapshot_job",
        "wikidata_raw_snapshot_job",
    }.issubset(job_names)
    assert "wikidata_weekly_schedule" in schedule_names
    assert weekly_schedule.job_name == "wikidata_exchange_discovery_job"
    assert weekly_schedule.cron_schedule == "30 3 * * 1"
    assert weekly_schedule.execution_timezone == "Europe/Belgrade"
    assert weekly_schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_wikidata_canonical_contacts_asset_is_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "wikidata_clickhouse_canonical_contacts" in asset_keys


def test_wikidata_canonical_contacts_depends_on_completed_snapshot() -> None:
    repository = load_project_defs().get_repository_def()

    deps = repository.asset_graph.get(
        AssetKey(["wikidata_clickhouse_canonical_contacts"])
    ).parent_keys

    assert deps == {AssetKey(["wikidata_snapshot_complete"])}


def test_wikidata_canonical_contacts_asset_in_publish_job() -> None:
    repository = load_project_defs().get_repository_def()
    job_asset_keys = {
        key.path[-1]
        for key in repository.get_job(
            "wikidata_publish_job"
        ).asset_layer.executable_asset_keys
    }

    assert "wikidata_clickhouse_canonical_contacts" in job_asset_keys
    assert "wikidata_company_source_records_clickhouse" in job_asset_keys


def test_wikidata_raw_domain_asset_dependencies_are_split() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph

    assert asset_graph.get(AssetKey(["wikidata_company_pages_raw"])).parent_keys == {
        AssetKey(["wikidata_company_source_units"]),
    }
    for asset_name in (
        "wikidata_company_profiles_raw",
        "wikidata_company_identifiers_raw",
        "wikidata_company_relationships_raw",
        "wikidata_company_people_raw",
    ):
        assert asset_graph.get(AssetKey([asset_name])).parent_keys == {
            AssetKey(["wikidata_company_pages_raw"]),
        }
    assert asset_graph.get(AssetKey(["wikidata_persons_raw"])).parent_keys == {
        AssetKey(["wikidata_company_people_raw"]),
    }
    assert asset_graph.get(
        AssetKey(["wikidata_company_source_snapshot"])
    ).parent_keys == {
        AssetKey(["wikidata_company_pages_raw"]),
        AssetKey(["wikidata_company_profiles_raw"]),
        AssetKey(["wikidata_company_identifiers_raw"]),
        AssetKey(["wikidata_company_relationships_raw"]),
        AssetKey(["wikidata_company_people_raw"]),
        AssetKey(["wikidata_persons_raw"]),
    }
    assert asset_graph.get(AssetKey(["wikidata_raw_snapshot"])).parent_keys == {
        AssetKey(["wikidata_company_source_snapshot"]),
    }


def test_wikidata_company_source_asset_depends_on_exchanges_and_registry_spines() -> (
    None
):
    from dagster_v3.defs.wikidata.registry_seed import (
        WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS,
    )

    repository = load_project_defs().get_repository_def()

    discovery_asset = repository.asset_graph.get(
        AssetKey(["wikidata_company_source_units"])
    )

    assert discovery_asset.parent_keys == {
        AssetKey(["wikidata_exchanges_raw"]),
        *(
            AssetKey([spine_asset_key])
            for spine_asset_key in WIKIDATA_REGISTRY_SEED_SPINE_ASSET_KEYS
        ),
    }


def test_wikidata_raw_domain_assets_are_partitioned_by_week_and_company_source() -> (
    None
):
    from dagster_v3.defs.wikidata import assets

    repository = load_project_defs().get_repository_def()
    for asset_name in (
        "wikidata_company_pages_raw",
        "wikidata_company_profiles_raw",
        "wikidata_company_identifiers_raw",
        "wikidata_company_relationships_raw",
        "wikidata_company_people_raw",
        "wikidata_persons_raw",
        "wikidata_company_source_snapshot",
    ):
        raw_asset = repository.asset_graph.get(AssetKey([asset_name]))
        assert raw_asset.partitions_def == assets.WIKIDATA_RAW_PARTITIONS
        assert raw_asset.partitions_def.get_partitions_def_for_dimension("date") == (
            assets.WIKIDATA_WEEKLY_PARTITIONS
        )
        assert (
            raw_asset.partitions_def.get_partitions_def_for_dimension("company_source")
            == assets.WIKIDATA_COMPANY_SOURCE_PARTITIONS
        )


def test_wikidata_registry_seed_specs_are_wired_into_seed_asset() -> None:
    # Every country module with a Wikidata registry-number property owns ONE
    # WikidataRegistrySeedSpec (declared next to that module's own tables, not in a
    # central list under defs/wikidata/ -- see defs/common/wikidata_registry_seed.py).
    # This test is what fails loudly when someone adds a spec without wiring it, or
    # wires an edge without a real spec backing it.
    from dagster_v3.defs.wikidata.registry_seed import WIKIDATA_REGISTRY_SEED_SPECS

    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    all_asset_keys = asset_graph.get_all_asset_keys()
    discovery_asset = asset_graph.get(AssetKey(["wikidata_company_source_units"]))

    expected_property_ids = {
        "P6460",  # SE
        "P2333",  # NO
        "P1059",  # DK
        "P12980",  # FI
        "P2622",  # GB (UK Companies House)
        "P1616",  # FR
        "P4156",  # CZ
        "P8053",  # LV
        "P6204",  # BR
    }
    assert {spec.property_id for spec in WIKIDATA_REGISTRY_SEED_SPECS} == (
        expected_property_ids
    )
    assert len(WIKIDATA_REGISTRY_SEED_SPECS) == 9

    country_iso2_values = [spec.country_iso2 for spec in WIKIDATA_REGISTRY_SEED_SPECS]
    assert len(country_iso2_values) == len(set(country_iso2_values))

    for spec in WIKIDATA_REGISTRY_SEED_SPECS:
        spine_key = AssetKey([spec.spine_asset_key])
        assert spine_key in all_asset_keys, (
            f"{spec.spine_asset_key} (country={spec.country_iso2}, "
            f"property={spec.property_id}) is declared in a WikidataRegistrySeedSpec "
            "but is not a registered Dagster asset"
        )
        assert spine_key in discovery_asset.parent_keys, (
            f"{spec.spine_asset_key} (country={spec.country_iso2}) is a registered "
            "asset but wikidata_company_source_units has no deps edge onto it"
        )


def test_wikidata_publish_job_excludes_raw_and_registry_source_pipelines() -> None:
    repository = load_project_defs().get_repository_def()
    job_asset_keys = {
        key.path[-1]
        for key in repository.get_job(
            "wikidata_publish_job"
        ).asset_layer.executable_asset_keys
    }

    assert "wikidata_exchanges_raw" not in job_asset_keys
    assert "wikidata_company_source_units" not in job_asset_keys
    assert "wikidata_company_pages_raw" not in job_asset_keys
    assert "wikidata_persons_raw" not in job_asset_keys
    assert "wikidata_raw_snapshot" not in job_asset_keys
    for table_name in tables.WIKIDATA_TABLES:
        assert f"{table_name}_duckdb" in job_asset_keys
        assert table_name in job_asset_keys
    assert "wikidata_snapshot_complete" in job_asset_keys
    assert "wikidata_clickhouse_canonical_contacts" in job_asset_keys

    assert "sweden_company_raw_snapshot_s3" not in job_asset_keys
    assert "sweden_company_companies_clickhouse" not in job_asset_keys
    assert "norway_brreg_entities_snapshot_clickhouse" not in job_asset_keys
    assert "denmark_cvr_companies_duckdb" not in job_asset_keys
    assert "finland_ytj_resolved_clickhouse" not in job_asset_keys
    assert "uk_companies_house_clickhouse_companies" not in job_asset_keys
    assert "france_sirene_clickhouse_companies" not in job_asset_keys
    assert "czech_ares_clickhouse_companies" not in job_asset_keys
    assert "latvia_ur_clickhouse_companies" not in job_asset_keys
    assert "brazil_comp_rfb_clickhouse_companies" not in job_asset_keys


def test_wikidata_companies_duckdb_materialization_writes_its_own_database(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, _s3_client = _object_store()
    source_row = {
        column_name: (
            0
            if schema["data_type"] == "bigint"
            else "2026-06-19T10:00:00+00:00"
            if column_name == "retrieved_at"
            else ""
        )
        for column_name, schema in wikidata_source.WIKIDATA_LISTED_COMPANIES_COLUMNS.items()
    }
    source_row.update(
        {
            "source_run_id": "run-1",
            "exchange_wikidata_id": "QEX1",
            "exchange_name": "Test Exchange",
            "company_wikidata_id": "Q1",
            "company_url": "http://www.wikidata.org/entity/Q1",
            "company_label": "Alpha Inc",
            "source_record_id": "QEX1:1:1:Q1",
            "source_payload_hash": "hash-1",
        }
    )
    monkeypatch.setattr(assets, "WIKIDATA_DUCKDB_DIRECTORY", tmp_path)
    monkeypatch.setattr(
        assets,
        "iter_wikidata_listed_company_rows",
        lambda **_kwargs: iter([source_row]),
    )
    monkeypatch.setattr(
        assets,
        "resolve_wikidata_snapshot_partition_date",
        lambda **_kwargs: "2026-06-16",
    )

    result = dg.materialize(
        [assets.wikidata_companies_duckdb],
        resources={"object_store": object_store},
        run_config={
            "ops": {
                "wikidata_companies_duckdb": {
                    "config": {"partition_date": "2026-06-16"}
                }
            }
        },
    )

    assert result.success
    database_path = tmp_path / "wikidata_companies.duckdb"
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            "select wikidata_id, name, source_run_id from wikidata.wikidata_companies"
        ).fetchall()
    assert rows == [("Q1", "Alpha Inc", "run-1")]


def test_wikidata_listed_company_rows_read_raw_s3_pages() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, s3_client = _object_store()
    page_key = "partition_date=2026-06-16/exchange_id=QEX1/page=000001.json"
    manifest_key = "partition_date=2026-06-16/exchange_id=QEX1/manifest.json"
    s3_client.objects[(WIKIDATA_TEST_BUCKET, page_key)] = json.dumps(
        _wikidata_response(
            [
                _wikidata_company_binding(
                    company_id="Q1",
                    company_label="Alpha Inc",
                    listing_id="Q1-L1",
                    ticker="AAA",
                    website="https://alpha.example",
                )
            ]
        )
    ).encode("utf-8")
    s3_client.objects[(WIKIDATA_TEST_BUCKET, manifest_key)] = json.dumps(
        {
            "source": "wikidata",
            "query_mode": "exchange",
            "partition_date": "2026-06-16",
            "source_run_id": "2026-06-16",
            "exchange_id": "QEX1",
            "exchange_name": "Test Exchange One",
            "listed_company_count_on_exchange": 1,
            "page_size": 100,
            "row_count": 1,
            "page_count": 1,
            "started_at": "2026-06-19T10:00:00+00:00",
            "completed_at": "2026-06-19T10:00:01+00:00",
            "objects": [page_key],
        }
    ).encode("utf-8")
    _write_snapshot_manifest(
        s3_client,
        partition_date="2026-06-16",
        manifest_keys=[manifest_key],
    )

    rows = list(
        wikidata_source.iter_wikidata_listed_company_rows(
            object_store=object_store,
            partition_date="2026-06-16",
            source_run_id="downstream-only-run",
        )
    )

    assert [row["exchange_wikidata_id"] for row in rows] == ["QEX1"]
    assert [row["exchange_name"] for row in rows] == ["Test Exchange One"]
    assert [row["company_wikidata_id"] for row in rows] == ["Q1"]
    assert [row["ticker"] for row in rows] == ["AAA"]
    assert [row["company_description"] for row in rows] == ["test listed company"]
    assert [row["headquarters_wikidata_id"] for row in rows] == ["Q60"]
    assert [row["headquarters_country_wikidata_id"] for row in rows] == ["Q30"]
    assert [row["headquarters_country_label"] for row in rows] == ["United States"]
    assert [row["headquarters_country_iso2"] for row in rows] == ["US"]
    assert [row["inception_date"] for row in rows] == ["2001-02-03T00:00:00Z"]
    assert [row["legal_form_wikidata_id"] for row in rows] == ["Q4830453"]
    assert [row["legal_form_label"] for row in rows] == ["business"]
    assert [row["employee_count"] for row in rows] == ["1234"]
    assert [row["employee_count_point_in_time"] for row in rows] == [
        "2025-12-31T00:00:00Z"
    ]
    assert [row["logo_image"] for row in rows] == ["Alpha logo.svg"]
    assert [row["logo_image_url"] for row in rows] == [
        "https://commons.wikimedia.org/wiki/Special:FilePath/Alpha%20logo.svg"
    ]
    assert [row["industry_wikidata_id"] for row in rows] == ["Q7397"]
    assert [row["opencorporates_company_id"] for row in rows] == ["us_de/2923466"]
    assert [row["eu_vat_number"] for row in rows] == ["FI12345678"]
    assert [row["duns_number"] for row in rows] == ["123456789"]
    assert [row["permid"] for row in rows] == ["4295907168"]
    assert [row["bloomberg_company_id"] for row in rows] == ["ALPHA:US"]
    assert [row["linkedin_company_id"] for row in rows] == ["alpha-inc"]
    assert [row["parent_organization_wikidata_id"] for row in rows] == ["Q100"]
    assert [row["parent_organization_label"] for row in rows] == ["Alpha Holdings"]
    assert [row["child_organization_wikidata_id"] for row in rows] == ["Q101"]
    assert [row["child_organization_label"] for row in rows] == ["Alpha Subsidiary"]
    assert [row["owned_by_wikidata_id"] for row in rows] == ["Q102"]
    assert [row["owned_by_label"] for row in rows] == ["Alpha Owner"]
    assert [row["owner_of_wikidata_id"] for row in rows] == ["Q103"]
    assert [row["owner_of_label"] for row in rows] == ["Alpha Owned Business"]
    assert rows[0]["source_run_id"] == "2026-06-16"
    assert rows[0]["retrieved_at"] == "2026-06-19T10:00:00+00:00"
    assert rows[0]["source_record_id"] == "QEX1:000001:000001:Q1:Q1-L1"
    assert len(rows[0]["source_payload_hash"]) == 64
    assert len(rows[0]["query_hash"]) == 64


def test_wikidata_listed_company_rows_keep_partition_date_strict() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, s3_client = _object_store()
    manifest_key = "partition_date=2026-06-16/exchange_id=QEX1/manifest.json"
    s3_client.objects[(WIKIDATA_TEST_BUCKET, manifest_key)] = json.dumps(
        {
            "partition_date": "2026-06-16",
            "source_run_id": "2026-06-16",
            "started_at": "2026-06-19T10:00:00+00:00",
            "exchange_id": "QEX1",
            "objects": [],
        }
    ).encode("utf-8")

    with pytest.raises(ValueError, match="partition_date=2026-06-23"):
        list(
            wikidata_source.iter_wikidata_listed_company_rows(
                object_store=object_store,
                partition_date="2026-06-23",
                source_run_id="downstream-only-run",
            )
        )


def test_wikidata_completed_snapshot_manifest_is_strictly_partition_scoped() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, s3_client = _object_store()
    old_manifest_key = "partition_date=2026-06-16/exchange_id=QEX1/manifest.json"
    new_manifest_key = "partition_date=2026-06-23/exchange_id=QEX1/manifest.json"
    _write_snapshot_manifest(
        s3_client,
        partition_date="2026-06-16",
        manifest_keys=[old_manifest_key],
    )
    _write_snapshot_manifest(
        s3_client,
        partition_date="2026-06-23",
        manifest_keys=[new_manifest_key],
    )

    assert wikidata_source.completed_wikidata_raw_manifest_keys(
        object_store=object_store,
        partition_date="2026-06-16",
    ) == [old_manifest_key]
    with pytest.raises(ValueError, match="completed Wikidata raw snapshot"):
        wikidata_source.completed_wikidata_raw_manifest_keys(
            object_store=object_store,
            partition_date="2026-06-30",
        )


def test_wikidata_active_exchange_query_and_collapse_preserve_multiple_mics() -> None:
    from dagster_v3.defs.wikidata.source import (
        active_listed_exchange_row_from_binding,
        build_active_listed_exchanges_query,
        collapse_active_listed_exchange_rows,
    )

    query = build_active_listed_exchanges_query()
    assert "wdt:P7534 ?mic" in query
    assert "wdt:P17 ?country" in query
    assert "wdt:P297 ?countryIso2" in query

    bindings = [
        {
            "exchange": {"value": "http://www.wikidata.org/entity/QEX1"},
            "exchangeLabel": {"value": "Test Exchange"},
            "mic": {"value": "XONE"},
            "country": {"value": "http://www.wikidata.org/entity/Q30"},
            "countryLabel": {"value": "United States"},
            "countryIso2": {"value": "US"},
            "listedCompanyCount": {"value": "12"},
        },
        {
            "exchange": {"value": "http://www.wikidata.org/entity/QEX1"},
            "exchangeLabel": {"value": "Test Exchange"},
            "mic": {"value": "XTWO"},
            "country": {"value": "http://www.wikidata.org/entity/Q30"},
            "countryLabel": {"value": "United States"},
            "countryIso2": {"value": "US"},
            "listedCompanyCount": {"value": "12"},
        },
    ]
    rows = [
        active_listed_exchange_row_from_binding(
            binding,
            source_run_id="run-1",
            retrieved_at="2026-07-21T10:00:00+00:00",
            source_row_number=index,
        )
        for index, binding in enumerate(bindings, start=1)
    ]

    assert collapse_active_listed_exchange_rows(rows) == [
        {
            "exchange_wikidata_id": "QEX1",
            "exchange_name": "Test Exchange",
            "mics": ["XONE", "XTWO"],
            "country_wikidata_id": "Q30",
            "country_name": "United States",
            "country_iso2": "US",
            "listed_company_count_on_exchange": 12,
            "source_run_id": "run-1",
            "retrieved_at": "2026-07-21T10:00:00+00:00",
            "source_row_number": 1,
        }
    ]


def test_wikidata_listed_company_rows_merge_manifest_augmentation_objects() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, s3_client = _object_store()
    page_key = "partition_date=2026-06-16/exchange_id=QEX1/page=000001.json"
    augmentation_key = (
        "partition_date=2026-06-16/"
        "exchange_id=QEX1/augmentation_kind=profile/page=000001_batch=000001.json"
    )
    manifest_key = "partition_date=2026-06-16/exchange_id=QEX1/manifest.json"
    s3_client.objects[(WIKIDATA_TEST_BUCKET, page_key)] = json.dumps(
        _wikidata_response(
            [
                {
                    "company": {"value": "http://www.wikidata.org/entity/Q1"},
                    "companyLabel": {"value": "Alpha Inc"},
                    "listing": {
                        "value": "http://www.wikidata.org/entity/statement/Q1-L1"
                    },
                    "ticker": {"value": "AAA"},
                }
            ]
        )
    ).encode("utf-8")
    s3_client.objects[(WIKIDATA_TEST_BUCKET, augmentation_key)] = json.dumps(
        _wikidata_response(
            [
                _wikidata_company_binding(
                    company_id="Q1",
                    company_label="Alpha Inc",
                    listing_id="Q1-L1",
                    ticker="AAA",
                    website="https://alpha.example",
                )
            ]
        )
    ).encode("utf-8")
    s3_client.objects[(WIKIDATA_TEST_BUCKET, manifest_key)] = json.dumps(
        {
            "source": "wikidata",
            "query_mode": "exchange",
            "partition_date": "2026-06-16",
            "source_run_id": "2026-06-16",
            "exchange_id": "QEX1",
            "exchange_name": "Test Exchange One",
            "listed_company_count_on_exchange": 1,
            "page_size": 100,
            "row_count": 1,
            "augmentation_row_count": 1,
            "page_count": 1,
            "started_at": "2026-06-19T10:00:00+00:00",
            "completed_at": "2026-06-19T10:00:01+00:00",
            "objects": [page_key],
            "augmentation_objects": [augmentation_key],
        }
    ).encode("utf-8")
    _write_snapshot_manifest(
        s3_client,
        partition_date="2026-06-16",
        manifest_keys=[manifest_key],
    )

    rows = list(
        wikidata_source.iter_wikidata_listed_company_rows(
            object_store=object_store,
            partition_date="2026-06-16",
            source_run_id="2026-06-16",
        )
    )

    assert len(rows) == 1
    assert rows[0]["source_record_id"] == "QEX1:000001:000001:Q1:Q1-L1:aug:000001"
    assert rows[0]["legal_form_wikidata_id"] == "Q4830453"
    assert rows[0]["linkedin_company_id"] == "alpha-inc"
    assert rows[0]["parent_organization_wikidata_id"] == "Q100"


def test_wikidata_listed_companies_stage_declares_explicit_schema() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    schema = wikidata_source.WIKIDATA_LISTED_COMPANIES_COLUMNS

    assert schema["exchange_wikidata_id"]["data_type"] == "text"
    assert schema["listed_company_count_on_exchange"]["data_type"] == "bigint"
    assert schema["page_number"]["data_type"] == "bigint"
    assert schema["source_payload_hash"]["data_type"] == "text"
    assert schema["company_description"]["data_type"] == "text"
    assert schema["headquarters_wikidata_id"]["data_type"] == "text"
    assert schema["headquarters_country_wikidata_id"]["data_type"] == "text"
    assert schema["headquarters_country_label"]["data_type"] == "text"
    assert schema["headquarters_country_iso2"]["data_type"] == "text"
    for column_name in (
        "inception_date",
        "legal_form_wikidata_id",
        "legal_form_label",
        "employee_count",
        "employee_count_point_in_time",
        "logo_image",
        "logo_image_url",
        "industry_wikidata_id",
        "opencorporates_company_id",
        "eu_vat_number",
        "duns_number",
        "permid",
        "bloomberg_company_id",
        "linkedin_company_id",
        "parent_organization_statement_id",
        "parent_organization_wikidata_id",
        "parent_organization_label",
        "parent_organization_start_date",
        "parent_organization_end_date",
        "child_organization_statement_id",
        "child_organization_wikidata_id",
        "child_organization_label",
        "child_organization_start_date",
        "child_organization_end_date",
        "owned_by_statement_id",
        "owned_by_wikidata_id",
        "owned_by_label",
        "owned_by_start_date",
        "owned_by_end_date",
        "owner_of_statement_id",
        "owner_of_wikidata_id",
        "owner_of_label",
        "owner_of_start_date",
        "owner_of_end_date",
    ):
        assert schema[column_name]["data_type"] == "text"


def test_wikidata_stage_streams_arrow_batches() -> None:
    from dagster_v3.defs.wikidata import assets

    columns = {
        "source_id": {"data_type": "text"},
        "retrieved_at": {"data_type": "timestamp"},
        "source_count": {"data_type": "bigint"},
    }
    consumed = 0

    def rows() -> Iterator[dict[str, Any]]:
        nonlocal consumed
        for index in range(5):
            consumed += 1
            yield {
                "source_id": f"Q{index}",
                "retrieved_at": "2026-07-25T12:00:00",
                "source_count": index,
            }

    with duckdb.connect(":memory:") as connection:
        row_count = assets._replace_wikidata_stage_table(
            connection,
            table_name="stream_test",
            columns=columns,
            rows=rows(),
            log=lambda *_args: None,
            batch_rows=2,
        )
        stored = connection.execute(
            f"select source_id, source_count from "
            f"{assets.WIKIDATA_DUCKDB_DATASET_NAME}.stream_test order by source_count"
        ).fetchall()

    assert consumed == 5
    assert row_count == 5
    assert stored == [(f"Q{index}", index) for index in range(5)]


def test_wikidata_exchange_rows_read_completed_real_exchange_manifests() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, s3_client = _object_store()
    exchange_manifest_key = "partition_date=2026-07-20/exchange_id=QEX1/manifest.json"
    registry_manifest_key = (
        "partition_date=2026-07-20/exchange_id=registry_P6460/manifest.json"
    )
    s3_client.objects[(WIKIDATA_TEST_BUCKET, exchange_manifest_key)] = json.dumps(
        {
            "source": "wikidata",
            "query_mode": "exchange",
            "partition_date": "2026-07-20",
            "source_run_id": "2026-07-20",
            "exchange_id": "QEX1",
            "exchange_name": "Test Exchange",
            "mics": ["XONE", "XTWO"],
            "country_wikidata_id": "Q30",
            "country_name": "United States",
            "country_iso2": "US",
            "listed_company_count_on_exchange": 12,
            "started_at": "2026-07-21T10:00:00+00:00",
            "objects": [],
        }
    ).encode("utf-8")
    s3_client.objects[(WIKIDATA_TEST_BUCKET, registry_manifest_key)] = json.dumps(
        {
            "source": "wikidata",
            "query_mode": "registry_number",
            "partition_date": "2026-07-20",
            "source_run_id": "2026-07-20",
            "exchange_id": "registry_P6460",
            "started_at": "2026-07-21T10:00:00+00:00",
            "objects": [],
        }
    ).encode("utf-8")
    _write_snapshot_manifest(
        s3_client,
        partition_date="2026-07-20",
        manifest_keys=[exchange_manifest_key, registry_manifest_key],
    )

    rows = list(
        wikidata_source.iter_wikidata_exchange_rows(
            object_store=object_store,
            partition_date="2026-07-20",
            source_run_id="2026-07-20",
        )
    )

    assert [row["mic"] for row in rows] == ["XONE", "XTWO"]
    assert {row["exchange_wikidata_id"] for row in rows} == {"QEX1"}
    assert {row["country_iso2"] for row in rows} == {"US"}
    assert all(len(row["source_payload_hash"]) == 64 for row in rows)


def test_wikidata_exchanges_stage_declares_explicit_schema() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    schema = wikidata_source.WIKIDATA_EXCHANGES_COLUMNS

    assert schema["exchange_wikidata_id"]["data_type"] == "text"
    assert schema["listed_company_count"]["data_type"] == "bigint"
    assert schema["source_payload_hash"]["data_type"] == "text"


def test_wikidata_company_pages_and_augmentations_write_separate_manifests() -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    object_store, s3_client = _object_store()
    pages_client = FakeWikidataClient(
        pages=[
            _wikidata_response(
                [{"company": {"value": "http://www.wikidata.org/entity/Q1"}}]
            ),
            _wikidata_response(
                [{"company": {"value": "http://www.wikidata.org/entity/Q2"}}]
            ),
            _wikidata_response([]),
        ]
    )
    progress_messages: list[str] = []

    def capture_progress(message: str, *args: object) -> None:
        progress_messages.append(message % args)

    pages_result = assets.pull_wikidata_company_pages_for_seed_unit(
        client=pages_client,
        object_store=object_store,
        config=WikidataRawPullConfig(
            page_size=1,
            max_pages=None,
            request_delay_seconds=0.25,
            user_agent="test-agent",
        ),
        partition_date="2026-06-16",
        seed_unit=_wikidata_seed_unit("Q13677"),
        source_run_id="2026-06-16",
        retrieved_at="2026-06-19T10:00:00+00:00",
        sleep=lambda _seconds: None,
        log=capture_progress,
    )

    page_keys = [
        "partition_date=2026-06-16/exchange_id=Q13677/page=000001.json",
        "partition_date=2026-06-16/exchange_id=Q13677/page=000002.json",
    ]
    pages_manifest_key = (
        "partition_date=2026-06-16/exchange_id=Q13677/"
        "data_kind=company_pages/manifest.json"
    )
    profile_keys = [
        "partition_date=2026-06-16/"
        "exchange_id=Q13677/augmentation_kind=profile/"
        "page=000001_batch=000001.json",
        "partition_date=2026-06-16/"
        "exchange_id=Q13677/augmentation_kind=profile/"
        "page=000002_batch=000001.json",
    ]
    profile_manifest_key = (
        "partition_date=2026-06-16/"
        "exchange_id=Q13677/data_kind=company_profiles/manifest.json"
    )

    assert all(
        (WIKIDATA_TEST_BUCKET, page_key) in s3_client.objects for page_key in page_keys
    )
    pages_manifest = json.loads(
        s3_client.objects[(WIKIDATA_TEST_BUCKET, pages_manifest_key)].decode("utf-8")
    )
    assert pages_manifest["data_kind"] == "company_pages"
    assert pages_manifest["objects"] == page_keys
    assert pages_manifest["row_count"] == 2
    assert pages_result.metadata["data_kind"] == "company_pages"
    assert pages_result.metadata["reused"] is False
    assert not any(
        "augmentation_kind=" in key
        for bucket, key in s3_client.objects
        if bucket == WIKIDATA_TEST_BUCKET
    )

    profile_client = FakeWikidataClient(
        pages=[],
        augmentation_payloads=[
            _wikidata_response([]),
            _wikidata_response([]),
        ],
    )
    profile_result = assets.pull_wikidata_augmentation_for_seed_unit(
        client=profile_client,
        object_store=object_store,
        config=WikidataRawPullConfig(
            page_size=1,
            request_delay_seconds=0,
            user_agent="test-agent",
        ),
        partition_date="2026-06-16",
        seed_unit_id="Q13677",
        data_kind=assets.WIKIDATA_COMPANY_PROFILE_KIND,
        source_run_id="2026-06-16",
        retrieved_at="2026-06-19T10:05:00+00:00",
        sleep=lambda _seconds: None,
        log=capture_progress,
    )

    assert all(
        (WIKIDATA_TEST_BUCKET, profile_key) in s3_client.objects
        for profile_key in profile_keys
    )
    profile_manifest = json.loads(
        s3_client.objects[(WIKIDATA_TEST_BUCKET, profile_manifest_key)].decode("utf-8")
    )
    assert profile_manifest["data_kind"] == "company_profiles"
    assert profile_manifest["objects"] == profile_keys
    assert profile_result.metadata["data_kind"] == "company_profiles"
    assert not any(
        "augmentation_kind=identifiers" in key
        or "augmentation_kind=relationships" in key
        or "augmentation_kind=people" in key
        for bucket, key in s3_client.objects
        if bucket == WIKIDATA_TEST_BUCKET
    )
    assert any(
        "Wikidata company page: partition_date=2026-06-16 seed_unit=Q13677" in message
        for message in progress_messages
    )
    assert pages_client.offsets == [0, 1, 2]


def test_wikidata_raw_pull_resumes_from_checkpoint_after_transient_failure() -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import (
        WikidataRawPullConfig,
        WikidataTransientRequestError,
    )

    class FailingAfterFirstPageClient:
        def __init__(self) -> None:
            self.offsets: list[int] = []

        def fetch(self, query: str, *, user_agent: str) -> dict[str, Any]:
            assert user_agent == "test-agent"
            if "VALUES ?company" in query:
                return _wikidata_response([])
            offset = int(query.split("OFFSET ", 1)[1].splitlines()[0])
            self.offsets.append(offset)
            if offset == 0:
                return _wikidata_response(
                    [{"company": {"value": "http://www.wikidata.org/entity/Q1"}}]
                )
            raise WikidataTransientRequestError("Wikidata unavailable")

    object_store, _s3_client = _object_store()
    config = WikidataRawPullConfig(
        exchange_ids_csv="QEX1",
        page_size=1,
        include_registry_seed=False,
        request_delay_seconds=0,
        user_agent="test-agent",
    )
    first_client = FailingAfterFirstPageClient()

    with pytest.raises(WikidataTransientRequestError, match="unavailable"):
        assets.pull_wikidata_company_pages_for_seed_unit(
            client=first_client,
            object_store=object_store,
            config=config,
            partition_date="2026-07-20",
            seed_unit=_wikidata_seed_unit("QEX1"),
            source_run_id="first-dagster-run",
            retrieved_at="2026-07-22T10:00:00+00:00",
            sleep=lambda _seconds: None,
            log=lambda *_args: None,
        )

    resumed_client = FakeWikidataClient(
        pages=[
            _wikidata_response(
                [{"company": {"value": "http://www.wikidata.org/entity/Q2"}}]
            ),
            _wikidata_response([]),
        ]
    )
    progress_messages: list[str] = []

    result = assets.pull_wikidata_company_pages_for_seed_unit(
        client=resumed_client,
        object_store=object_store,
        config=config,
        partition_date="2026-07-20",
        seed_unit=_wikidata_seed_unit("QEX1"),
        source_run_id="different-dagster-retry-run",
        retrieved_at="2026-07-22T10:00:00+00:00",
        sleep=lambda _seconds: None,
        log=lambda message, *args: progress_messages.append(message % args),
    )

    assert first_client.offsets == [0, 1]
    assert resumed_client.offsets == [1, 2]
    assert result.metadata["row_count"] == 2
    assert result.metadata["reused"] is False
    assert any(
        "Reused Wikidata company page" in message for message in progress_messages
    )
    assert all(
        "run_id=" not in key
        for bucket, key in _s3_client.objects
        if bucket == WIKIDATA_TEST_BUCKET
    )

    class NoNetworkClient:
        def fetch(self, _query: str, *, user_agent: str) -> dict[str, Any]:
            raise AssertionError(
                f"completed partition performed a request as {user_agent}"
            )

    repeated_result = assets.pull_wikidata_company_pages_for_seed_unit(
        client=NoNetworkClient(),
        object_store=object_store,
        config=config,
        partition_date="2026-07-20",
        seed_unit=_wikidata_seed_unit("QEX1"),
        source_run_id="third-dagster-run",
        retrieved_at="2026-07-22T12:00:00+00:00",
        sleep=lambda _seconds: None,
        log=lambda *_args: None,
    )

    assert repeated_result.metadata["reused"] is True
    assert repeated_result.metadata["row_count"] == 2


def test_wikidata_split_stages_adopt_completed_manifest_without_network() -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    class NoNetworkClient:
        def fetch(self, _query: str, *, user_agent: str) -> dict[str, Any]:
            raise AssertionError(
                f"migrated partition performed a request as {user_agent}"
            )

    object_store, s3_client = _object_store()
    prefix = "partition_date=2026-07-20/exchange_id=QEX1"
    page_key = f"{prefix}/page=000001.json"
    augmentation_keys = [
        (
            f"{prefix}/augmentation_kind={augmentation_kind}/"
            "page=000001_batch=000001.json"
        )
        for augmentation_kind in ("profile", "identifiers", "relationships", "people")
    ]
    s3_client.objects[(WIKIDATA_TEST_BUCKET, page_key)] = json.dumps(
        _wikidata_response(
            [{"company": {"value": "http://www.wikidata.org/entity/Q1"}}]
        )
    ).encode("utf-8")
    for augmentation_key in augmentation_keys:
        s3_client.objects[(WIKIDATA_TEST_BUCKET, augmentation_key)] = json.dumps(
            _wikidata_response([])
        ).encode("utf-8")
    combined_manifest_key = f"{prefix}/manifest.json"
    s3_client.objects[(WIKIDATA_TEST_BUCKET, combined_manifest_key)] = json.dumps(
        {
            "source": "wikidata",
            "status": "complete",
            "query_mode": "exchange",
            "partition_date": "2026-07-20",
            "source_run_id": "2026-07-20",
            "exchange_id": "QEX1",
            "exchange_name": "Test Exchange",
            "listed_company_count_on_exchange": 1,
            "mics": ["XONE"],
            "country_wikidata_id": "Q30",
            "country_name": "United States",
            "country_iso2": "US",
            "registry_property_id": None,
            "page_size": 100,
            "max_pages": None,
            "query_hash": "hash",
            "row_count": 1,
            "page_count": 1,
            "augmentation_batch_size": 100,
            "augmentation_row_count": 0,
            "started_at": "2026-07-20T10:00:00+00:00",
            "completed_at": "2026-07-20T11:00:00+00:00",
            "objects": [page_key],
            "augmentation_objects": augmentation_keys,
        }
    ).encode("utf-8")
    config = WikidataRawPullConfig(
        request_delay_seconds=0,
        user_agent="test-agent",
    )

    results = [
        assets.pull_wikidata_company_pages_for_seed_unit(
            client=NoNetworkClient(),
            object_store=object_store,
            config=config,
            partition_date="2026-07-20",
            seed_unit=_wikidata_seed_unit("QEX1"),
            source_run_id="new-dagster-run",
            retrieved_at="2026-07-23T10:00:00+00:00",
            sleep=lambda _seconds: None,
            log=lambda *_args: None,
        ),
        *(
            assets.pull_wikidata_augmentation_for_seed_unit(
                client=NoNetworkClient(),
                object_store=object_store,
                config=config,
                partition_date="2026-07-20",
                seed_unit_id="QEX1",
                data_kind=data_kind,
                source_run_id="new-dagster-run",
                retrieved_at="2026-07-23T10:00:00+00:00",
                sleep=lambda _seconds: None,
                log=lambda *_args: None,
            )
            for data_kind in assets.WIKIDATA_NETWORK_DATA_KINDS
        ),
        assets.materialize_wikidata_persons_for_seed_unit(
            object_store=object_store,
            partition_date="2026-07-20",
            seed_unit_id="QEX1",
        ),
    ]

    assert all(result.metadata["reused"] is True for result in results)
    assert {
        key
        for bucket, key in s3_client.objects
        if bucket == WIKIDATA_TEST_BUCKET and "/data_kind=" in key
    } == {
        f"{prefix}/data_kind={data_kind}/manifest.json"
        for data_kind in assets.WIKIDATA_ALL_SEED_UNIT_DATA_KINDS
    }
    snapshot_result = assets.materialize_wikidata_company_source_snapshot(
        object_store=object_store,
        partition_date="2026-07-20",
        seed_unit_id="QEX1",
    )
    assert snapshot_result.metadata["reused"] is True
    assert snapshot_result.metadata["manifest_key"] == combined_manifest_key


def test_wikidata_step_retry_uses_bounded_exponential_delay() -> None:
    from dagster_v3.defs.wikidata import assets

    assert [
        assets.wikidata_step_retry_delay_seconds(retry_number)
        for retry_number in range(6)
    ] == [300, 600, 1_200, 2_400, 2_400, 2_400]


def test_wikidata_legacy_keys_map_to_the_week_download_started() -> None:
    from dagster_v3.defs.wikidata.migrate_raw_bucket import (
        legacy_object_target_key,
        weekly_partition_date,
    )

    assert weekly_partition_date("2026-07-20") == "2026-07-20"
    assert weekly_partition_date("2026-07-23") == "2026-07-20"
    assert (
        legacy_object_target_key(
            "raw/run_id=retry-run/retrieved_date=2026-07-23/"
            "exchange_id=QEX1/page=000001.json"
        )
        == "partition_date=2026-07-20/exchange_id=QEX1/page=000001.json"
    )


def test_wikidata_legacy_manifest_rewrite_removes_run_identity() -> None:
    from dagster_v3.defs.wikidata.migrate_raw_bucket import rewrite_legacy_manifest

    legacy_prefix = "raw/run_id=retry-run/retrieved_date=2026-07-23/exchange_id=QEX1"
    rewritten = rewrite_legacy_manifest(
        {
            "source": "wikidata",
            "run_id": "retry-run",
            "retrieved_date": "2026-07-23",
            "exchange_id": "QEX1",
            "query_mode": "exchange",
            "objects": [f"{legacy_prefix}/page=000001.json"],
            "augmentation_objects": [
                f"{legacy_prefix}/augmentation_kind=profile/"
                "page=000001_batch=000001.json"
            ],
        },
        target_key=("partition_date=2026-07-20/exchange_id=QEX1/manifest.json"),
    )

    assert rewritten["status"] == "complete"
    assert rewritten["partition_date"] == "2026-07-20"
    assert rewritten["source_run_id"] == "2026-07-20"
    assert "run_id" not in rewritten
    assert "retrieved_date" not in rewritten
    assert rewritten["objects"] == [
        "partition_date=2026-07-20/exchange_id=QEX1/page=000001.json"
    ]
    assert rewritten["augmentation_objects"] == [
        "partition_date=2026-07-20/exchange_id=QEX1/"
        "augmentation_kind=profile/page=000001_batch=000001.json"
    ]


def test_wikidata_client_translates_exhausted_http_retries() -> None:
    from dagster_v3.defs.wikidata.source import (
        WikidataSparqlClient,
        WikidataTransientRequestError,
    )

    class FailingSession:
        def get(self, *_args: object, **_kwargs: object) -> None:
            raise requests.exceptions.RetryError("too many 504 error responses")

    client = WikidataSparqlClient.__new__(WikidataSparqlClient)
    client._timeout_seconds = 120
    client._session = FailingSession()

    with pytest.raises(
        WikidataTransientRequestError,
        match="exhausting HTTP retries",
    ) as exc_info:
        client.fetch("SELECT * WHERE {}", user_agent="test-agent")

    assert isinstance(exc_info.value.__cause__, requests.exceptions.RetryError)


def test_wikidata_seed_discovery_writes_exchange_catalog_without_company_pages() -> (
    None
):
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    object_store, s3_client = _object_store()
    client = FakeWikidataClientWithDiscovery(
        exchange_payload=_wikidata_response(
            [
                {
                    "exchange": {"value": "http://www.wikidata.org/entity/QEX1"},
                    "exchangeLabel": {"value": "Test Exchange One"},
                    "mic": {"value": "XONE"},
                    "country": {"value": "http://www.wikidata.org/entity/Q30"},
                    "countryLabel": {"value": "United States"},
                    "countryIso2": {"value": "US"},
                    "listedCompanyCount": {"value": "2"},
                },
                {
                    "exchange": {"value": "http://www.wikidata.org/entity/QEX1"},
                    "exchangeLabel": {"value": "Test Exchange One"},
                    "mic": {"value": "XALT"},
                    "country": {"value": "http://www.wikidata.org/entity/Q30"},
                    "countryLabel": {"value": "United States"},
                    "countryIso2": {"value": "US"},
                    "listedCompanyCount": {"value": "2"},
                },
                {
                    "exchange": {"value": "http://www.wikidata.org/entity/QEX2"},
                    "exchangeLabel": {"value": "Test Exchange Two"},
                    "mic": {"value": "XTWO"},
                    "listedCompanyCount": {"value": "1"},
                },
            ]
        ),
        company_payloads_by_exchange={
            "QEX1": [
                _wikidata_response(
                    [
                        _wikidata_company_binding(
                            company_id="Q1",
                            company_label="Alpha Inc",
                            listing_id="Q1-L1",
                            ticker="AAA",
                            website="https://alpha.example",
                        )
                    ]
                ),
                _wikidata_response([]),
            ],
            "QEX2": [
                _wikidata_response(
                    [
                        _wikidata_company_binding(
                            company_id="Q2",
                            company_label="Beta Plc",
                            listing_id="Q2-L1",
                            ticker="BBB",
                            website="https://beta.example",
                        )
                    ]
                ),
                _wikidata_response([]),
            ],
        },
    )

    config = WikidataRawPullConfig(
        page_size=1,
        max_pages=None,
        max_exchanges=2,
        include_registry_seed=False,
        request_delay_seconds=0,
        user_agent="test-agent",
    )
    result = assets.discover_wikidata_company_sources(
        client=client,
        object_store=object_store,
        config=config,
        partition_date="2026-06-16",
        source_run_id="2026-06-16",
        retrieved_at="2026-06-19T10:00:00+00:00",
        log=lambda *_args: None,
    )

    exchange_list_key = "partition_date=2026-06-16/active_exchanges.json"
    catalog_key = "partition_date=2026-06-16/seed_units.json"
    catalog = json.loads(
        s3_client.objects[(WIKIDATA_TEST_BUCKET, catalog_key)].decode("utf-8")
    )
    first_unit = catalog["seed_units"][0]

    assert (WIKIDATA_TEST_BUCKET, exchange_list_key) in s3_client.objects
    assert first_unit["exchange_name"] == "Test Exchange One"
    assert first_unit["mics"] == ["XALT", "XONE"]
    assert first_unit["country_wikidata_id"] == "Q30"
    assert first_unit["country_name"] == "United States"
    assert first_unit["country_iso2"] == "US"
    assert first_unit["listed_company_count_on_exchange"] == 2
    assert result.metadata["exchange_count"] == 2
    assert result.metadata["unit_count"] == 2
    assert client.company_offsets == {}
    assert len(client.queries) == 1

    reused_result = assets.discover_wikidata_company_sources(
        client=client,
        object_store=object_store,
        config=config,
        partition_date="2026-06-16",
        source_run_id="different-run",
        retrieved_at="2026-06-19T11:00:00+00:00",
        log=lambda *_args: None,
    )
    assert reused_result.metadata["reused"] is True
    assert len(client.queries) == 1


def test_wikidata_partitioned_pull_keeps_registry_pseudo_exchanges_separate() -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    object_store, s3_client = _object_store()
    client = FakeWikidataClient(
        pages=[
            # QEX1 (real exchange) page 1: one binding, less than page_size -> loop
            # breaks after a single fetch.
            _wikidata_response(
                [
                    _wikidata_company_binding(
                        company_id="Q1",
                        company_label="Alpha Inc",
                        listing_id="Q1-L1",
                        ticker="AAA",
                        website="https://alpha.example",
                    )
                ]
            ),
            # registry_P6460 (pseudo-exchange) page 1: one unlisted company.
            _wikidata_response(
                [{"company": {"value": "http://www.wikidata.org/entity/Q9"}}]
            ),
            # registry_P2333 (pseudo-exchange) page 1: no companies for this property.
            _wikidata_response([]),
        ]
    )

    config = WikidataRawPullConfig(
        exchange_ids_csv="QEX1",
        registry_property_ids_csv="P6460,P2333",
        page_size=100,
        max_pages=None,
        request_delay_seconds=0,
        user_agent="test-agent",
    )
    discovery_result = assets.discover_wikidata_company_sources(
        client=client,
        object_store=object_store,
        config=config,
        partition_date="2026-07-20",
        source_run_id="2026-07-20",
        retrieved_at="2026-07-20T10:00:00+00:00",
        log=lambda *_args: None,
    )
    seed_units = assets.read_wikidata_seed_units(
        object_store=object_store,
        partition_date="2026-07-20",
    )
    for seed_unit in seed_units:
        seed_unit_id = str(seed_unit["exchange_wikidata_id"])
        assets.pull_wikidata_company_pages_for_seed_unit(
            client=client,
            object_store=object_store,
            config=config,
            partition_date="2026-07-20",
            seed_unit=seed_unit,
            source_run_id="2026-07-20",
            retrieved_at="2026-07-20T10:00:00+00:00",
            sleep=lambda _seconds: None,
            log=lambda *_args: None,
        )
        for data_kind in assets.WIKIDATA_NETWORK_DATA_KINDS:
            assets.pull_wikidata_augmentation_for_seed_unit(
                client=client,
                object_store=object_store,
                config=config,
                partition_date="2026-07-20",
                seed_unit_id=seed_unit_id,
                data_kind=data_kind,
                source_run_id="2026-07-20",
                retrieved_at="2026-07-20T10:00:00+00:00",
                sleep=lambda _seconds: None,
                log=lambda *_args: None,
            )
        assets.materialize_wikidata_persons_for_seed_unit(
            object_store=object_store,
            partition_date="2026-07-20",
            seed_unit_id=seed_unit_id,
        )
        assets.materialize_wikidata_company_source_snapshot(
            object_store=object_store,
            partition_date="2026-07-20",
            seed_unit_id=seed_unit_id,
        )
    result = assets.finalize_wikidata_raw_snapshot(
        object_store=object_store,
        partition_date="2026-07-20",
        completed_at="2026-07-20T11:00:00+00:00",
    )

    exchange_manifest_key = "partition_date=2026-07-20/exchange_id=QEX1/manifest.json"
    registry_p6460_manifest_key = (
        "partition_date=2026-07-20/exchange_id=registry_P6460/manifest.json"
    )
    registry_p2333_manifest_key = (
        "partition_date=2026-07-20/exchange_id=registry_P2333/manifest.json"
    )

    assert (WIKIDATA_TEST_BUCKET, exchange_manifest_key) in s3_client.objects
    assert (
        WIKIDATA_TEST_BUCKET,
        registry_p6460_manifest_key,
    ) in s3_client.objects
    assert (
        WIKIDATA_TEST_BUCKET,
        registry_p2333_manifest_key,
    ) in s3_client.objects

    exchange_manifest = json.loads(
        s3_client.objects[(WIKIDATA_TEST_BUCKET, exchange_manifest_key)].decode("utf-8")
    )
    p6460_manifest = json.loads(
        s3_client.objects[(WIKIDATA_TEST_BUCKET, registry_p6460_manifest_key)].decode(
            "utf-8"
        )
    )
    p2333_manifest = json.loads(
        s3_client.objects[(WIKIDATA_TEST_BUCKET, registry_p2333_manifest_key)].decode(
            "utf-8"
        )
    )

    assert exchange_manifest["query_mode"] == "exchange"
    assert exchange_manifest["registry_property_id"] is None

    assert p6460_manifest["query_mode"] == "registry_number"
    assert p6460_manifest["registry_property_id"] == "P6460"
    assert p6460_manifest["exchange_id"] == "registry_P6460"
    assert p6460_manifest["row_count"] == 1
    assert p6460_manifest["page_count"] == 1

    assert p2333_manifest["query_mode"] == "registry_number"
    assert p2333_manifest["registry_property_id"] == "P2333"
    assert p2333_manifest["exchange_id"] == "registry_P2333"
    assert p2333_manifest["row_count"] == 0
    assert p2333_manifest["page_count"] == 0
    assert p2333_manifest["objects"] == []

    assert discovery_result.metadata["exchange_ids"] == [
        "QEX1",
        "registry_P6460",
        "registry_P2333",
    ]
    assert result.metadata["unit_count"] == 3
    assert result.metadata["exchange_count"] == 1
    assert result.metadata["registry_property_count"] == 2


def test_wikidata_query_uses_stable_order_for_offset_pagination() -> None:
    from dagster_v3.defs.wikidata.source import (
        build_company_identifier_augmentation_query,
        build_company_people_augmentation_query,
        build_company_profile_augmentation_query,
        build_company_relationship_augmentation_query,
        build_listed_company_query,
    )

    query = build_listed_company_query(exchange_id="Q13677", limit=100, offset=200)
    profile_query = build_company_profile_augmentation_query(("Q1", "Q2"))
    identifier_query = build_company_identifier_augmentation_query(("Q1", "Q2"))
    relationship_query = build_company_relationship_augmentation_query(("Q1", "Q2"))
    people_query = build_company_people_augmentation_query(("Q1", "Q2"))

    assert (
        "OPTIONAL { ?headquarters wdt:P131*/wdt:P17 ?headquartersCountry . }" in query
    )
    assert (
        "OPTIONAL { ?headquartersCountry wdt:P297 ?headquartersCountryIso2 . }" in query
    )
    assert "?companyDescription" in query
    assert "?headquartersCountryLabel" in query
    assert (
        "ORDER BY ?company ?listing ?website ?ticker ?isin ?cik ?lei "
        "?headquarters ?headquartersCountry ?headquartersCountryIso2 ?industry" in query
    )
    assert "?company p:P749 ?parentOrganizationStatement" not in query
    assert "?company p:P1128 ?employeeCountStatement" not in query
    assert query.index("ORDER BY") < query.index("LIMIT 100")
    assert query.index("LIMIT 100") < query.index("OFFSET 200")

    assert "VALUES ?company { wd:Q1 wd:Q2 }" in profile_query
    assert "?company wdt:P571 ?inceptionDate" in profile_query
    assert "?company wdt:P1454 ?legalForm" in profile_query
    assert "?company p:P1128 ?employeeCountStatement" in profile_query
    assert "?company wdt:P154 ?logoImage" in profile_query
    assert "?company wdt:P1320 ?openCorporatesId" not in profile_query
    assert "?company p:P749 ?parentOrganizationStatement" not in profile_query

    assert "VALUES ?company { wd:Q1 wd:Q2 }" in identifier_query
    assert "?company wdt:P1320 ?openCorporatesId" in identifier_query
    assert "?company wdt:P3608 ?euVatNumber" in identifier_query
    assert "?company wdt:P2771 ?dunsNumber" in identifier_query
    assert "?company wdt:P3347 ?permId" in identifier_query
    assert "?company wdt:P3377 ?bloombergCompanyId" in identifier_query
    assert "?company wdt:P4264 ?linkedinCompanyId" in identifier_query
    assert "?company p:P749 ?parentOrganizationStatement" not in identifier_query

    assert "VALUES ?company { wd:Q1 wd:Q2 }" in relationship_query
    assert "?company p:P749 ?parentOrganizationStatement" in relationship_query
    assert "?company p:P355 ?childOrganizationStatement" in relationship_query
    assert "?company p:P127 ?ownedByStatement" in relationship_query
    assert "?company p:P1830 ?ownerOfStatement" in relationship_query
    assert "?company wdt:P1320 ?openCorporatesId" not in relationship_query

    assert "VALUES ?company { wd:Q1 wd:Q2 }" in people_query
    assert "?company p:P169 ?roleStatement" in people_query
    assert "?roleStatement ps:P169 ?person" in people_query
    assert 'BIND("P169" AS ?roleProperty)' in people_query
    assert "?company p:P112 ?roleStatement" in people_query
    assert 'BIND("P112" AS ?roleProperty)' in people_query
    assert "?company p:P488 ?roleStatement" in people_query
    assert 'BIND("P488" AS ?roleProperty)' in people_query
    assert "?company p:P3320 ?roleStatement" in people_query
    assert 'BIND("P3320" AS ?roleProperty)' in people_query
    # P127 (owned by) is filtered to person-valued targets only -- P127 usually points at
    # another company, so without this the branch would pull in corporate owners too.
    assert "?company p:P127 ?roleStatement" in people_query
    assert "?person wdt:P31 wd:Q5" in people_query
    assert 'BIND("P127" AS ?roleProperty)' in people_query
    assert "OPTIONAL { ?roleStatement pq:P580 ?startDate . }" in people_query
    assert "OPTIONAL { ?roleStatement pq:P582 ?endDate . }" in people_query
    assert "?person wdt:P569 ?personBirthDate" in people_query
    assert "BIND(YEAR(?personBirthDate) AS ?personBirthYear)" in people_query
    assert "?person wdt:P18 ?personImage" in people_query
    assert "https://commons.wikimedia.org/wiki/Special:FilePath/" in people_query
    assert "?company wdt:P1320 ?openCorporatesId" not in people_query
    assert people_query.index("ORDER BY") > people_query.rindex(
        "SERVICE wikibase:label"
    )


def test_wikidata_registry_number_query_anchors_on_property_and_drops_listing_triples() -> (
    None
):
    from dagster_v3.defs.wikidata.source import build_registry_number_company_query

    query = build_registry_number_company_query(
        property_id="P6460", limit=100, offset=200
    )

    # Anchors on the registry property, not a p:P414 listing.
    assert "?company wdt:P6460 ?registryValue ." in query
    assert "p:P414" not in query
    assert "ps:P414" not in query
    assert "pq:P249" not in query
    assert "pq:P946" not in query
    assert "VALUES ?exchange" not in query

    # Same SELECT shape as build_listed_company_query so the page-row parser
    # (listed_company_row_from_binding) works unchanged -- listing/exchange/ticker/isin
    # simply come back unbound for every row.
    for select_var in ("?listing", "?exchange", "?exchangeLabel", "?ticker", "?isin"):
        assert select_var in query

    # Same OPTIONAL blocks as build_listed_company_query (minus the listing/exchange
    # ones), so official name/website/cik/lei/headquarters/industry are still captured.
    assert "OPTIONAL { ?company wdt:P1448 ?officialName . }" in query
    assert "OPTIONAL { ?company wdt:P856 ?website . }" in query
    assert "OPTIONAL { ?company wdt:P5531 ?cik . }" in query
    assert "OPTIONAL { ?company wdt:P1278 ?lei . }" in query
    assert (
        "OPTIONAL { ?headquarters wdt:P131*/wdt:P17 ?headquartersCountry . }" in query
    )
    assert (
        "OPTIONAL { ?headquartersCountry wdt:P297 ?headquartersCountryIso2 . }" in query
    )
    assert "OPTIONAL { ?company wdt:P452 ?industry . }" in query
    assert "FILTER NOT EXISTS { ?company wdt:P576 ?dissolvedDate . }" in query

    assert query.startswith("SELECT DISTINCT")
    assert "ORDER BY ?company" in query
    assert query.index("ORDER BY") < query.index("LIMIT 100")
    assert query.index("LIMIT 100") < query.index("OFFSET 200")


def test_wikidata_registry_pseudo_exchange_id_helpers() -> None:
    from dagster_v3.defs.wikidata.source import (
        is_registry_pseudo_exchange_id,
        registry_property_id_from_pseudo_exchange_id,
        registry_pseudo_exchange_id,
    )

    assert registry_pseudo_exchange_id("P6460") == "registry_P6460"
    assert is_registry_pseudo_exchange_id("registry_P6460") is True
    assert is_registry_pseudo_exchange_id("QEX1") is False
    assert registry_property_id_from_pseudo_exchange_id("registry_P6460") == "P6460"

    with pytest.raises(ValueError, match="not a registry pseudo-exchange id"):
        registry_property_id_from_pseudo_exchange_id("QEX1")


def test_wikidata_raw_pull_config_registry_seed_defaults_and_overrides() -> None:
    from dagster_v3.defs.wikidata.registry_seed import (
        WIKIDATA_REGISTRY_NUMBER_PROPERTY_IDS,
    )
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    default_config = WikidataRawPullConfig()
    assert default_config.include_registry_seed is True
    assert (
        default_config.configured_registry_property_ids()
        == WIKIDATA_REGISTRY_NUMBER_PROPERTY_IDS
    )

    overridden_config = WikidataRawPullConfig(registry_property_ids_csv="P6460, P2333")
    assert overridden_config.configured_registry_property_ids() == ("P6460", "P2333")

    with pytest.raises(ValueError, match="registry_property_ids_csv must not be blank"):
        WikidataRawPullConfig(registry_property_ids_csv="   ")

    with pytest.raises(
        ValueError, match="registry_property_ids_csv must contain at least one"
    ):
        WikidataRawPullConfig(
            registry_property_ids_csv=",  ,"
        ).configured_registry_property_ids()


def test_wikidata_raw_pull_registry_rows_respects_include_toggle() -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    enabled_rows = assets.wikidata_raw_pull_registry_rows(
        config=WikidataRawPullConfig(registry_property_ids_csv="P6460,P1059"),
        source_run_id="run-1",
        retrieved_at="2026-07-20T10:00:00+00:00",
    )
    assert [row["exchange_wikidata_id"] for row in enabled_rows] == [
        "registry_P6460",
        "registry_P1059",
    ]
    assert [row["registry_property_id"] for row in enabled_rows] == ["P6460", "P1059"]
    assert [row["listed_company_count_on_exchange"] for row in enabled_rows] == [0, 0]

    disabled_rows = assets.wikidata_raw_pull_registry_rows(
        config=WikidataRawPullConfig(include_registry_seed=False),
        source_run_id="run-1",
        retrieved_at="2026-07-20T10:00:00+00:00",
    )
    assert disabled_rows == []


def test_wikidata_normalization_builds_final_duckdb_tables(tmp_path: Path) -> None:
    from dagster_v3.defs.wikidata import assets

    database_path = tmp_path / "wikidata.duckdb"
    _seed_wikidata_listed_companies(database_path)

    with duckdb.connect(str(database_path)) as connection:
        row_counts = assets.normalize_wikidata_snapshot_tables(
            connection,
            catalog_name=database_path.stem,
        )

    assert row_counts == {
        "wikidata_companies": 2,
        "wikidata_exchanges": 3,
        "wikidata_company_listings": 2,
        "wikidata_company_identifiers": 11,
        "wikidata_company_websites": 2,
        "wikidata_company_relationships": 4,
        "wikidata_company_people": 0,
        "wikidata_persons": 0,
        "wikidata_seed_extraction_runs": 1,
    }

    with duckdb.connect(str(database_path), read_only=True) as connection:
        companies = connection.execute(
            """
            select
                wikidata_id,
                name,
                company_description,
                headquarters_wikidata_id,
                headquarters_country_wikidata_id,
                headquarters_country_label,
                headquarters_country_iso2,
                country_resolution_method,
                country_resolution_confidence,
                inception_date,
                legal_form_wikidata_id,
                legal_form_label,
                employee_count,
                employee_count_point_in_time,
                logo_image,
                logo_image_url,
                industry_wikidata_id,
                listing_count,
                has_current_listing
            from wikidata.wikidata.wikidata_companies
            order by wikidata_id
            """
        ).fetchall()
        identifiers = connection.execute(
            """
            select wikidata_id, identifier_type, wikidata_property_id, identifier_value
            from wikidata.wikidata.wikidata_company_identifiers
            order by wikidata_id, identifier_type, identifier_value
            """
        ).fetchall()
        relationships = connection.execute(
            """
            select
                subject_wikidata_id,
                relationship_type,
                wikidata_property_id,
                object_wikidata_id,
                object_name,
                start_date,
                end_date,
                is_current
            from wikidata.wikidata.wikidata_company_relationships
            order by subject_wikidata_id, relationship_type, object_wikidata_id
            """
        ).fetchall()
        websites = connection.execute(
            """
            select wikidata_id, website_host, root_domain
            from wikidata.wikidata.wikidata_company_websites
            order by wikidata_id
            """
        ).fetchall()
        runs = connection.execute(
            """
            select query_mode, row_count, distinct_company_count, distinct_listing_count
            from wikidata.wikidata.wikidata_seed_extraction_runs
            """
        ).fetchall()
        exchanges = connection.execute(
            """
            select exchange_wikidata_id, exchange_name, mic, listed_company_count
            from wikidata.wikidata.wikidata_exchanges
            order by exchange_wikidata_id, mic
            """
        ).fetchall()

    assert companies == [
        (
            "Q1",
            "Alpha Inc",
            "public software company",
            "Q60",
            "Q30",
            "United States",
            "US",
            "wikidata_headquarters_p131_p17",
            "high",
            date(2001, 2, 3),
            "Q4830453",
            "business",
            1234,
            date(2025, 12, 31),
            "Alpha logo.svg",
            "https://commons.wikimedia.org/wiki/Special:FilePath/Alpha%20logo.svg",
            "Q7397",
            1,
            1,
        ),
        (
            "Q2",
            "Beta Plc",
            "public hardware company",
            "Q84",
            "Q145",
            "United Kingdom",
            "GB",
            "wikidata_headquarters_p131_p17",
            "high",
            date(1999, 1, 1),
            "Q891723",
            "public limited company",
            55,
            date(2024, 12, 31),
            "Beta logo.svg",
            "https://commons.wikimedia.org/wiki/Special:FilePath/Beta%20logo.svg",
            "Q3966",
            1,
            1,
        ),
    ]
    assert identifiers == [
        ("Q1", "bloomberg_company_id", "P3377", "ALPHA:US"),
        ("Q1", "cik", "P5531", "CIKAAA"),
        ("Q1", "duns_number", "P2771", "123456789"),
        ("Q1", "eu_vat_number", "P3608", "FI12345678"),
        ("Q1", "isin", "P946", "ISINAAA"),
        ("Q1", "lei", "P1278", "LEIAAA"),
        ("Q1", "linkedin_company_id", "P4264", "alpha-inc"),
        ("Q1", "opencorporates_company_id", "P1320", "us_de/2923466"),
        ("Q1", "permid", "P3347", "4295907168"),
        ("Q2", "isin", "P946", "ISINBBB"),
        ("Q2", "lei", "P1278", "LEIBBB"),
    ]
    assert relationships == [
        (
            "Q1",
            "child_organization",
            "P355",
            "Q101",
            "Alpha Subsidiary",
            date(2020, 1, 1),
            None,
            1,
        ),
        (
            "Q1",
            "parent_organization",
            "P749",
            "Q100",
            "Alpha Holdings",
            date(2010, 1, 1),
            None,
            1,
        ),
        ("Q2", "owned_by", "P127", "Q102", "Beta Owner", date(2015, 1, 1), None, 1),
        (
            "Q2",
            "owner_of",
            "P1830",
            "Q103",
            "Beta Owned Business",
            None,
            date(2022, 12, 31),
            0,
        ),
    ]
    assert websites == [
        ("Q1", "alpha.example", "alpha.example"),
        ("Q2", "beta.example", "beta.example"),
    ]
    assert exchanges == [
        ("QEX1", "Test Exchange One", "XALT", 2),
        ("QEX1", "Test Exchange One", "XONE", 2),
        ("QEX2", "Test Exchange Two", "XTWO", 1),
    ]
    assert runs == [("active_exchange_listing", 2, 2, 2)]


def test_wikidata_normalization_dedups_company_seeded_via_exchange_and_registry(
    tmp_path: Path,
) -> None:
    # The same company discovered via a real exchange listing AND a registry-number
    # pseudo-exchange must yield exactly ONE wikidata_companies row (dedup is by
    # company_wikidata_id, unaffected by which seed source(s) found the company) and
    # must NOT create a spurious wikidata_company_listings row for the registry pull
    # (which carries no listing_statement_id). A company found ONLY via a registry
    # property must not claim has_current_listing.
    from dagster_v3.defs.wikidata import assets

    database_path = tmp_path / "wikidata.duckdb"
    _seed_wikidata_registry_dedup_scenario(database_path)

    with duckdb.connect(str(database_path)) as connection:
        row_counts = assets.normalize_wikidata_snapshot_tables(
            connection,
            catalog_name=database_path.stem,
        )

    assert row_counts["wikidata_companies"] == 2
    assert row_counts["wikidata_company_listings"] == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        companies = connection.execute(
            """
            select wikidata_id, has_current_listing, listing_count
            from wikidata.wikidata.wikidata_companies
            order by wikidata_id
            """
        ).fetchall()
        listings = connection.execute(
            """
            select wikidata_id, exchange_wikidata_id
            from wikidata.wikidata.wikidata_company_listings
            """
        ).fetchall()

    assert companies == [
        ("Q1", 1, 1),  # exchange + registry seed -> one row, real listing counted
        ("Q3", 0, 0),  # registry-only -> no current listing
    ]
    assert listings == [("Q1", "QEX1")]


def test_wikidata_exchange_normalization_rejects_invalid_mic(tmp_path: Path) -> None:
    from dagster_v3.defs.wikidata import assets

    database_path = tmp_path / "wikidata.duckdb"
    _seed_wikidata_listed_companies(database_path)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            "update wikidata.wikidata_stage.exchanges set mic = 'BAD' "
            "where exchange_wikidata_id = 'QEX1'"
        )
        with pytest.raises(ValueError, match="Invalid Wikidata MIC values: BAD"):
            assets.normalize_wikidata_snapshot_tables(
                connection,
                catalog_name=database_path.stem,
            )


def test_wikidata_exchange_normalization_requires_listing_coverage(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.wikidata import assets

    database_path = tmp_path / "wikidata.duckdb"
    _seed_wikidata_listed_companies(database_path)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            "delete from wikidata.wikidata_stage.exchanges "
            "where exchange_wikidata_id = 'QEX2'"
        )
        with pytest.raises(
            ValueError,
            match="Wikidata listings reference missing exchanges: QEX2",
        ):
            assets.normalize_wikidata_snapshot_tables(
                connection,
                catalog_name=database_path.stem,
            )


_WIKIDATA_STAGE_TABLE_DDL = """
create table wikidata.wikidata_stage.listed_companies (
    source_run_id varchar,
    retrieved_at timestamp,
    exchange_wikidata_id varchar,
    exchange_name varchar,
    listed_company_count_on_exchange bigint,
    page_number bigint,
    page_offset bigint,
    page_row_number bigint,
    company_wikidata_id varchar,
    company_url varchar,
    company_label varchar,
    company_description varchar,
    official_name varchar,
    listing_statement_id varchar,
    listing_url varchar,
    ticker varchar,
    isin varchar,
    website_url varchar,
    cik varchar,
    lei varchar,
    headquarters_wikidata_id varchar,
    headquarters_label varchar,
    headquarters_country_wikidata_id varchar,
    headquarters_country_label varchar,
    headquarters_country_iso2 varchar,
    inception_date varchar,
    legal_form_wikidata_id varchar,
    legal_form_label varchar,
    employee_count varchar,
    employee_count_point_in_time varchar,
    logo_image varchar,
    logo_image_url varchar,
    industry_wikidata_id varchar,
    industry_label varchar,
    opencorporates_company_id varchar,
    eu_vat_number varchar,
    duns_number varchar,
    permid varchar,
    bloomberg_company_id varchar,
    linkedin_company_id varchar,
    parent_organization_statement_id varchar,
    parent_organization_wikidata_id varchar,
    parent_organization_label varchar,
    parent_organization_start_date varchar,
    parent_organization_end_date varchar,
    child_organization_statement_id varchar,
    child_organization_wikidata_id varchar,
    child_organization_label varchar,
    child_organization_start_date varchar,
    child_organization_end_date varchar,
    owned_by_statement_id varchar,
    owned_by_wikidata_id varchar,
    owned_by_label varchar,
    owned_by_start_date varchar,
    owned_by_end_date varchar,
    owner_of_statement_id varchar,
    owner_of_wikidata_id varchar,
    owner_of_label varchar,
    owner_of_start_date varchar,
    owner_of_end_date varchar,
    person_wikidata_id varchar,
    person_url varchar,
    person_label varchar,
    person_description varchar,
    person_image varchar,
    person_image_url varchar,
    person_birth_year varchar,
    role_property varchar,
    role_start_date varchar,
    role_end_date varchar,
    query_hash varchar,
    source_record_id varchar,
    source_payload_hash varchar,
    raw_binding_json varchar
)
"""


def _seed_wikidata_registry_dedup_scenario(database_path: Path) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema wikidata.wikidata_stage")
        connection.execute(_WIKIDATA_STAGE_TABLE_DDL)

        def insert_row(row: dict[str, Any]) -> None:
            columns = ", ".join(row.keys())
            placeholders = ", ".join("?" for _ in row)
            connection.execute(
                "insert into wikidata.wikidata_stage.listed_companies "
                f"({columns}) values ({placeholders})",
                list(row.values()),
            )

        # Q1: a real exchange listing row (QEX1) ...
        insert_row(
            {
                "source_run_id": "run-1",
                "retrieved_at": "2026-07-20 10:00:00",
                "exchange_wikidata_id": "QEX1",
                "exchange_name": "Test Exchange One",
                "listed_company_count_on_exchange": 1,
                "page_number": 1,
                "page_offset": 0,
                "page_row_number": 1,
                "company_wikidata_id": "Q1",
                "company_url": "http://www.wikidata.org/entity/Q1",
                "company_label": "Alpha Inc",
                "listing_statement_id": "Q1-L1",
                "listing_url": "http://www.wikidata.org/entity/statement/Q1-L1",
                "ticker": "AAA",
                "source_record_id": "QEX1:000001:000001:Q1:Q1-L1",
                "source_payload_hash": "a" * 64,
                "raw_binding_json": "{}",
            }
        )
        _seed_wikidata_exchanges_stage(connection)
        # ... PLUS a registry-number pseudo-exchange row (SE orgnr, P6460) for the SAME
        # company -- no listing binding, per build_registry_number_company_query.
        insert_row(
            {
                "source_run_id": "run-1",
                "retrieved_at": "2026-07-20 10:00:00",
                "exchange_wikidata_id": "registry_P6460",
                "exchange_name": "Wikidata registry-number seed: P6460",
                "listed_company_count_on_exchange": 0,
                "page_number": 1,
                "page_offset": 0,
                "page_row_number": 1,
                "company_wikidata_id": "Q1",
                "company_url": "http://www.wikidata.org/entity/Q1",
                "company_label": "Alpha Inc",
                "listing_statement_id": "",
                "listing_url": "",
                "source_record_id": "registry_P6460:000001:000001:Q1:",
                "source_payload_hash": "b" * 64,
                "raw_binding_json": "{}",
            }
        )
        # Q3: discovered ONLY via a registry-number pseudo-exchange (NO orgnr, P2333) --
        # never listed on a real exchange.
        insert_row(
            {
                "source_run_id": "run-1",
                "retrieved_at": "2026-07-20 10:00:00",
                "exchange_wikidata_id": "registry_P2333",
                "exchange_name": "Wikidata registry-number seed: P2333",
                "listed_company_count_on_exchange": 0,
                "page_number": 1,
                "page_offset": 0,
                "page_row_number": 1,
                "company_wikidata_id": "Q3",
                "company_url": "http://www.wikidata.org/entity/Q3",
                "company_label": "Gamma Unlisted AS",
                "listing_statement_id": "",
                "listing_url": "",
                "source_record_id": "registry_P2333:000001:000001:Q3:",
                "source_payload_hash": "c" * 64,
                "raw_binding_json": "{}",
            }
        )


def test_wikidata_normalization_builds_company_people_and_persons_tables(
    tmp_path: Path,
) -> None:
    # Acceptance case: Koenigsegg Automotive AB (Q500, discovered via the SE registry
    # seed P6460) must yield Christian von Koenigsegg (Q600) as founder AND CEO, with
    # description/image/birth year -- and dedup to ONE wikidata_persons row despite
    # appearing via three separate role links across two companies. Company/person
    # identity throughout is the Wikidata QID; nothing here matches on name.
    from dagster_v3.defs.wikidata import assets

    database_path = tmp_path / "wikidata.duckdb"
    _seed_wikidata_company_people_scenario(database_path)

    with duckdb.connect(str(database_path)) as connection:
        row_counts = assets.normalize_wikidata_snapshot_tables(
            connection,
            catalog_name=database_path.stem,
        )

    assert row_counts["wikidata_company_people"] == 6
    assert row_counts["wikidata_persons"] == 4

    with duckdb.connect(str(database_path), read_only=True) as connection:
        company_people = connection.execute(
            """
            select
                company_wikidata_id,
                person_wikidata_id,
                role_property,
                role_label,
                start_date,
                end_date,
                is_current
            from wikidata.wikidata.wikidata_company_people
            order by company_wikidata_id, role_property, person_wikidata_id
            """
        ).fetchall()
        persons = connection.execute(
            """
            select
                person_wikidata_id,
                name,
                name_normalized,
                description,
                birth_year,
                image_url,
                wikidata_url
            from wikidata.wikidata.wikidata_persons
            order by person_wikidata_id
            """
        ).fetchall()

    assert company_people == [
        ("Q500", "Q600", "P112", "founder", date(1994, 1, 1), None, 1),
        ("Q500", "Q600", "P169", "chief executive officer", date(1994, 1, 1), None, 1),
        (
            "Q500",
            "Q601",
            "P3320",
            "board member",
            date(2010, 1, 1),
            date(2015, 12, 31),
            0,
        ),
        ("Q501", "Q600", "P3320", "board member", None, None, 1),
        ("Q502", "Q603", "P127", "owned by", None, None, 1),
        ("Q502", "Q602", "P488", "chairperson", None, None, 1),
    ]
    assert persons == [
        (
            "Q600",
            "Christian von Koenigsegg",
            "christian von koenigsegg",
            "Swedish automotive engineer and entrepreneur",
            1972,
            "https://commons.wikimedia.org/wiki/Special:FilePath/Christian%20von%20Koenigsegg.jpg",
            "http://www.wikidata.org/entity/Q600",
        ),
        (
            "Q601",
            "Board Member X",
            "board member x",
            None,
            None,
            None,
            "http://www.wikidata.org/entity/Q601",
        ),
        (
            "Q602",
            "Chair Person Y",
            "chair person y",
            None,
            None,
            None,
            "http://www.wikidata.org/entity/Q602",
        ),
        (
            "Q603",
            "Owner Person Z",
            "owner person z",
            None,
            None,
            None,
            "http://www.wikidata.org/entity/Q603",
        ),
    ]


def _seed_wikidata_company_people_scenario(database_path: Path) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema wikidata.wikidata_stage")
        connection.execute(_WIKIDATA_STAGE_TABLE_DDL)

        def insert_row(row: dict[str, Any]) -> None:
            columns = ", ".join(row.keys())
            placeholders = ", ".join("?" for _ in row)
            connection.execute(
                "insert into wikidata.wikidata_stage.listed_companies "
                f"({columns}) values ({placeholders})",
                list(row.values()),
            )

        def person_row(
            *,
            exchange_wikidata_id: str,
            company_wikidata_id: str,
            company_label: str,
            person_wikidata_id: str,
            person_label: str,
            role_property: str,
            source_payload_hash: str,
            person_description: str = "",
            person_image: str = "",
            person_image_url: str = "",
            person_birth_year: str = "",
            role_start_date: str = "",
            role_end_date: str = "",
        ) -> dict[str, Any]:
            return {
                "source_run_id": "run-1",
                "retrieved_at": "2026-07-21 10:00:00",
                "exchange_wikidata_id": exchange_wikidata_id,
                "exchange_name": f"Wikidata registry-number seed: {exchange_wikidata_id}",
                "listed_company_count_on_exchange": 0,
                "page_number": 1,
                "page_offset": 0,
                "page_row_number": 1,
                "company_wikidata_id": company_wikidata_id,
                "company_url": f"http://www.wikidata.org/entity/{company_wikidata_id}",
                "company_label": company_label,
                "listing_statement_id": "",
                "listing_url": "",
                "person_wikidata_id": person_wikidata_id,
                "person_url": f"http://www.wikidata.org/entity/{person_wikidata_id}",
                "person_label": person_label,
                "person_description": person_description,
                "person_image": person_image,
                "person_image_url": person_image_url,
                "person_birth_year": person_birth_year,
                "role_property": role_property,
                "role_start_date": role_start_date,
                "role_end_date": role_end_date,
                "source_record_id": (
                    f"{exchange_wikidata_id}:{company_wikidata_id}:{role_property}:"
                    f"{person_wikidata_id}"
                ),
                "source_payload_hash": source_payload_hash,
                "raw_binding_json": "{}",
            }

        koenigsegg_image_url = (
            "https://commons.wikimedia.org/wiki/Special:FilePath/"
            "Christian%20von%20Koenigsegg.jpg"
        )
        # CEO (P169) and founder (P112) are TWO separate role rows for the SAME person
        # at the SAME company -- both must survive as distinct wikidata_company_people
        # rows (grouped by role_property), while wikidata_persons still dedups to one.
        insert_row(
            person_row(
                exchange_wikidata_id="registry_P6460",
                company_wikidata_id="Q500",
                company_label="Koenigsegg Automotive AB",
                person_wikidata_id="Q600",
                person_label="Christian von Koenigsegg",
                person_description="Swedish automotive engineer and entrepreneur",
                person_image="Christian von Koenigsegg.jpg",
                person_image_url=koenigsegg_image_url,
                person_birth_year="1972",
                role_property="P169",
                role_start_date="1994-01-01T00:00:00Z",
                source_payload_hash="a" * 64,
            )
        )
        insert_row(
            person_row(
                exchange_wikidata_id="registry_P6460",
                company_wikidata_id="Q500",
                company_label="Koenigsegg Automotive AB",
                person_wikidata_id="Q600",
                person_label="Christian von Koenigsegg",
                person_description="Swedish automotive engineer and entrepreneur",
                person_image="Christian von Koenigsegg.jpg",
                person_image_url=koenigsegg_image_url,
                person_birth_year="1972",
                role_property="P112",
                role_start_date="1994-01-01T00:00:00Z",
                source_payload_hash="b" * 64,
            )
        )
        # Historical board member (P3320) with an end date -> is_current must be 0.
        insert_row(
            person_row(
                exchange_wikidata_id="registry_P6460",
                company_wikidata_id="Q500",
                company_label="Koenigsegg Automotive AB",
                person_wikidata_id="Q601",
                person_label="Board Member X",
                role_property="P3320",
                role_start_date="2010-01-01T00:00:00Z",
                role_end_date="2015-12-31T00:00:00Z",
                source_payload_hash="c" * 64,
            )
        )
        # Same person (Q600) linked from a SECOND, different company -- proves
        # wikidata_persons dedups across companies, not just across roles.
        insert_row(
            person_row(
                exchange_wikidata_id="registry_P2333",
                company_wikidata_id="Q501",
                company_label="Koenigsegg Group AB",
                person_wikidata_id="Q600",
                person_label="Christian von Koenigsegg",
                person_description="Swedish automotive engineer and entrepreneur",
                person_image="Christian von Koenigsegg.jpg",
                person_image_url=koenigsegg_image_url,
                person_birth_year="1972",
                role_property="P3320",
                source_payload_hash="d" * 64,
            )
        )
        # Chairperson (P488) at a third company, no description/image/birth year --
        # exercises the nullable columns.
        insert_row(
            person_row(
                exchange_wikidata_id="registry_P1059",
                company_wikidata_id="Q502",
                company_label="Example Holding AB",
                person_wikidata_id="Q602",
                person_label="Chair Person Y",
                role_property="P488",
                source_payload_hash="e" * 64,
            )
        )
        # Person-valued owned-by (P127) at the same company -- the branch that requires
        # the ?person wdt:P31 wd:Q5 filter in the SPARQL query (not re-verified here,
        # that's build_company_people_augmentation_query's job; this only checks the
        # DuckDB pivot treats a P127 row like any other role).
        insert_row(
            person_row(
                exchange_wikidata_id="registry_P1059",
                company_wikidata_id="Q502",
                company_label="Example Holding AB",
                person_wikidata_id="Q603",
                person_label="Owner Person Z",
                role_property="P127",
                source_payload_hash="f" * 64,
            )
        )
        _seed_wikidata_exchanges_stage(connection)


def test_wikidata_clickhouse_export_uses_matching_table_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dagster_v3.defs.wikidata import assets, tables

    calls: dict[str, Any] = {}

    def fake_assert_clickhouse_tables_exist(
        clickhouse: ClickhouseResource,
        *,
        database: str,
        tables: tuple[str, ...],
    ) -> None:
        calls["assert"] = {
            "clickhouse": clickhouse,
            "database": database,
            "tables": tables,
        }

    def fake_replace_duckdb_connection_tables_in_clickhouse(
        **kwargs: Any,
    ) -> dict[str, int]:
        calls["replace"] = kwargs
        return {tables.WIKIDATA_COMPANIES_TABLE: 3}

    monkeypatch.setattr(
        assets,
        "assert_clickhouse_tables_exist",
        fake_assert_clickhouse_tables_exist,
    )
    monkeypatch.setattr(
        assets,
        "replace_duckdb_connection_tables_in_clickhouse",
        fake_replace_duckdb_connection_tables_in_clickhouse,
    )

    clickhouse = ClickhouseResource(host="localhost")
    client = object()
    database_path = tmp_path / "wikidata_companies.duckdb"
    duckdb.connect(str(database_path)).close()
    monkeypatch.setattr(
        assets,
        "wikidata_duckdb_path",
        lambda _table_name: database_path,
    )

    class FakeConnectionContext:
        def __enter__(self) -> object:
            return client

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        ClickhouseResource,
        "get_connection",
        lambda _self: FakeConnectionContext(),
    )

    result = assets._export_wikidata_table_to_clickhouse(
        clickhouse,
        table_name=tables.WIKIDATA_COMPANIES_TABLE,
    )

    assert calls["assert"] == {
        "clickhouse": clickhouse,
        "database": "corpscout",
        "tables": (tables.WIKIDATA_COMPANIES_TABLE,),
    }
    replace_call = calls["replace"]
    assert replace_call["clickhouse_client"] is client
    assert isinstance(replace_call["duckdb_connection"], duckdb.DuckDBPyConnection)
    assert replace_call["duckdb_schema"] == "wikidata_companies.wikidata"
    assert replace_call | {"duckdb_connection": None} == {
        "duckdb_connection": None,
        "clickhouse_client": client,
        "duckdb_schema": "wikidata_companies.wikidata",
        "clickhouse_database": "corpscout",
        "tables": (
            (
                tables.WIKIDATA_COMPANIES_TABLE,
                tables.WIKIDATA_TABLE_COLUMNS[tables.WIKIDATA_COMPANIES_TABLE],
            ),
        ),
        "allow_empty_tables": (),
    }
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "clickhouse_database": "corpscout",
        "clickhouse_table": tables.WIKIDATA_COMPANIES_TABLE,
        "duckdb_path": str(database_path),
        "row_count": 3,
    }


class FakeWikidataClient:
    def __init__(
        self,
        pages: list[dict[str, Any]],
        augmentation_payloads: list[dict[str, Any]] | None = None,
    ) -> None:
        self._pages = pages
        self._augmentation_payloads = augmentation_payloads or []
        self.offsets: list[int] = []
        self.augmentation_queries: list[str] = []

    def fetch(self, query: str, *, user_agent: str) -> dict[str, Any]:
        assert user_agent == "test-agent"
        if "VALUES ?company" in query:
            self.augmentation_queries.append(query)
            if self._augmentation_payloads:
                return self._augmentation_payloads.pop(0)
            return _wikidata_response([])

        offset_marker = "OFFSET "
        offset = int(query.split(offset_marker, 1)[1].splitlines()[0])
        self.offsets.append(offset)
        return self._pages.pop(0)


class FakeWikidataClientWithDiscovery:
    def __init__(
        self,
        *,
        exchange_payload: dict[str, Any],
        company_payloads_by_exchange: dict[str, list[dict[str, Any]]],
    ) -> None:
        self._exchange_payload = exchange_payload
        self._company_payloads_by_exchange = {
            exchange_id: list(payloads)
            for exchange_id, payloads in company_payloads_by_exchange.items()
        }
        self.queries: list[str] = []
        self.company_offsets: dict[str, list[int]] = {}

    def fetch(self, query: str, *, user_agent: str) -> dict[str, Any]:
        assert user_agent == "test-agent"
        self.queries.append(query)
        if "GROUP BY ?exchange ?exchangeLabel" in query:
            return self._exchange_payload
        if "VALUES ?company" in query:
            return _wikidata_response([])

        exchange_id = (
            query.split("VALUES ?exchange { wd:", 1)[1].split("}", 1)[0].strip()
        )
        offset_marker = "OFFSET "
        offset = int(query.split(offset_marker, 1)[1].splitlines()[0])
        self.company_offsets.setdefault(exchange_id, []).append(offset)
        return self._company_payloads_by_exchange[exchange_id].pop(0)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def create_bucket(self, Bucket: str) -> None:
        pass

    def head_object(self, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            raise FakeS3Error("404")

    def put_object(self, Bucket: str, Key: str, Body: bytes | str) -> None:
        self.objects[(Bucket, Key)] = (
            Body.encode("utf-8") if isinstance(Body, str) else Body
        )

    def get_object(self, Bucket: str, Key: str) -> dict[str, BytesIO]:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def get_paginator(self, operation_name: str) -> "FakeS3Paginator":
        assert operation_name == "list_objects_v2"
        return FakeS3Paginator(self)

    def delete_objects(self, Bucket: str, Delete: dict[str, Any]) -> None:
        for item in Delete["Objects"]:
            self.objects.pop((Bucket, item["Key"]), None)


class FakeS3Paginator:
    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    def paginate(self, Bucket: str, Prefix: str) -> Iterator[dict[str, Any]]:
        contents = [
            {"Key": key, "Size": len(body)}
            for (bucket, key), body in sorted(self._client.objects.items())
            if bucket == Bucket and key.startswith(Prefix)
        ]
        yield {"Contents": contents}


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


def _object_store() -> tuple[ObjectStoreResource, FakeS3Client]:
    s3_client = FakeS3Client()
    return (
        ObjectStoreResource(
            bucket=WIKIDATA_TEST_BUCKET,
            endpoint_url="http://test-s3",
            access_key="test-access-key",
            secret_key="test-secret-key",
            s3_client=s3_client,
        ),
        s3_client,
    )


def _write_snapshot_manifest(
    s3_client: FakeS3Client,
    *,
    partition_date: str,
    manifest_keys: list[str],
) -> None:
    snapshot_key = f"partition_date={partition_date}/snapshot_manifest.json"
    s3_client.objects[(WIKIDATA_TEST_BUCKET, snapshot_key)] = json.dumps(
        {
            "source": "wikidata",
            "status": "complete",
            "partition_date": partition_date,
            "source_run_id": partition_date,
            "manifest_keys": manifest_keys,
        }
    ).encode("utf-8")


def _wikidata_seed_unit(
    exchange_id: str,
    *,
    query_mode: str = "exchange",
    registry_property_id: str | None = None,
) -> dict[str, Any]:
    return {
        "exchange_wikidata_id": exchange_id,
        "exchange_name": f"Test Exchange {exchange_id}",
        "listed_company_count_on_exchange": None,
        "mics": [],
        "country_wikidata_id": None,
        "country_name": None,
        "country_iso2": None,
        "query_mode": query_mode,
        "registry_property_id": registry_property_id,
    }


def _seed_wikidata_listed_companies(database_path: Path) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema wikidata.wikidata_stage")
        connection.execute(
            """
            create table wikidata.wikidata_stage.listed_companies (
                source_run_id varchar,
                retrieved_at timestamp,
                exchange_wikidata_id varchar,
                exchange_name varchar,
                listed_company_count_on_exchange bigint,
                page_number bigint,
                page_offset bigint,
                page_row_number bigint,
                company_wikidata_id varchar,
                company_url varchar,
                company_label varchar,
                company_description varchar,
                official_name varchar,
                listing_statement_id varchar,
                listing_url varchar,
                ticker varchar,
                isin varchar,
                website_url varchar,
                cik varchar,
                lei varchar,
                headquarters_wikidata_id varchar,
                headquarters_label varchar,
                headquarters_country_wikidata_id varchar,
                headquarters_country_label varchar,
                headquarters_country_iso2 varchar,
                inception_date varchar,
                legal_form_wikidata_id varchar,
                legal_form_label varchar,
                employee_count varchar,
                employee_count_point_in_time varchar,
                logo_image varchar,
                logo_image_url varchar,
                industry_wikidata_id varchar,
                industry_label varchar,
                opencorporates_company_id varchar,
                eu_vat_number varchar,
                duns_number varchar,
                permid varchar,
                bloomberg_company_id varchar,
                linkedin_company_id varchar,
                parent_organization_statement_id varchar,
                parent_organization_wikidata_id varchar,
                parent_organization_label varchar,
                parent_organization_start_date varchar,
                parent_organization_end_date varchar,
                child_organization_statement_id varchar,
                child_organization_wikidata_id varchar,
                child_organization_label varchar,
                child_organization_start_date varchar,
                child_organization_end_date varchar,
                owned_by_statement_id varchar,
                owned_by_wikidata_id varchar,
                owned_by_label varchar,
                owned_by_start_date varchar,
                owned_by_end_date varchar,
                owner_of_statement_id varchar,
                owner_of_wikidata_id varchar,
                owner_of_label varchar,
                owner_of_start_date varchar,
                owner_of_end_date varchar,
                query_hash varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                raw_binding_json varchar
            )
            """
        )
        connection.executemany(
            """
            insert into wikidata.wikidata_stage.listed_companies values (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?
            )
            """,
            [
                (
                    "run-1",
                    "2026-06-19 10:00:00",
                    "QEX1",
                    "Test Exchange One",
                    2,
                    1,
                    0,
                    1,
                    "Q1",
                    "http://www.wikidata.org/entity/Q1",
                    "Alpha Inc",
                    "public software company",
                    "Alpha Incorporated",
                    "Q1-L1",
                    "http://www.wikidata.org/entity/statement/Q1-L1",
                    "AAA",
                    "ISINAAA",
                    "https://alpha.example",
                    "CIKAAA",
                    "LEIAAA",
                    "Q60",
                    "New York",
                    "Q30",
                    "United States",
                    "US",
                    "2001-02-03T00:00:00Z",
                    "Q4830453",
                    "business",
                    "1234",
                    "2025-12-31T00:00:00Z",
                    "Alpha logo.svg",
                    "https://commons.wikimedia.org/wiki/Special:FilePath/Alpha%20logo.svg",
                    "Q7397",
                    "Software",
                    "us_de/2923466",
                    "FI12345678",
                    "123456789",
                    "4295907168",
                    "ALPHA:US",
                    "alpha-inc",
                    "Q1-parent-statement",
                    "Q100",
                    "Alpha Holdings",
                    "2010-01-01T00:00:00Z",
                    "",
                    "Q1-child-statement",
                    "Q101",
                    "Alpha Subsidiary",
                    "2020-01-01T00:00:00Z",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "a" * 64,
                    "QEX1:000001:000001:Q1:Q1-L1",
                    "b" * 64,
                    "{}",
                ),
                (
                    "run-1",
                    "2026-06-19 10:00:00",
                    "QEX2",
                    "Test Exchange Two",
                    1,
                    1,
                    0,
                    1,
                    "Q2",
                    "http://www.wikidata.org/entity/Q2",
                    "Beta Plc",
                    "public hardware company",
                    "",
                    "Q2-L1",
                    "http://www.wikidata.org/entity/statement/Q2-L1",
                    "BBB",
                    "ISINBBB",
                    "https://beta.example",
                    "",
                    "LEIBBB",
                    "Q84",
                    "London",
                    "Q145",
                    "United Kingdom",
                    "GB",
                    "1999-01-01T00:00:00Z",
                    "Q891723",
                    "public limited company",
                    "55",
                    "2024-12-31T00:00:00Z",
                    "Beta logo.svg",
                    "https://commons.wikimedia.org/wiki/Special:FilePath/Beta%20logo.svg",
                    "Q3966",
                    "Hardware",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "Q2-owned-by-statement",
                    "Q102",
                    "Beta Owner",
                    "2015-01-01T00:00:00Z",
                    "",
                    "Q2-owner-of-statement",
                    "Q103",
                    "Beta Owned Business",
                    "",
                    "2022-12-31T00:00:00Z",
                    "c" * 64,
                    "QEX2:000001:000001:Q2:Q2-L1",
                    "d" * 64,
                    "{}",
                ),
            ],
        )
        _seed_wikidata_exchanges_stage(connection)
        connection.execute(
            """
            insert into wikidata.wikidata_stage.exchanges
            select
                source_run_id,
                retrieved_at,
                exchange_wikidata_id,
                exchange_name,
                'XALT',
                country_wikidata_id,
                country_name,
                country_iso2,
                listed_company_count,
                exchange_wikidata_id || ':XALT',
                source_payload_hash,
                raw_exchange_json
            from wikidata.wikidata_stage.exchanges
            where exchange_wikidata_id = 'QEX1'
            """
        )


def _seed_wikidata_exchanges_stage(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        create table wikidata.wikidata_stage.exchanges as
        select
            max(source_run_id) as source_run_id,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            exchange_wikidata_id,
            max(exchange_name) as exchange_name,
            case
                when exchange_wikidata_id = 'QEX1' then 'XONE'
                when exchange_wikidata_id = 'QEX2' then 'XTWO'
                else null
            end as mic,
            cast(null as varchar) as country_wikidata_id,
            cast(null as varchar) as country_name,
            cast(null as varchar) as country_iso2,
            max(listed_company_count_on_exchange) as listed_company_count,
            exchange_wikidata_id || ':seed' as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            '{}' as raw_exchange_json
        from wikidata.wikidata_stage.listed_companies
        where exchange_wikidata_id not like 'registry_%'
        group by exchange_wikidata_id
        """
    )


def _wikidata_response(bindings: list[dict[str, Any]]) -> dict[str, Any]:
    return {"head": {"vars": ["company"]}, "results": {"bindings": bindings}}


def _wikidata_company_binding(
    *,
    company_id: str,
    company_label: str,
    listing_id: str,
    ticker: str,
    website: str,
) -> dict[str, Any]:
    return {
        "company": {"value": f"http://www.wikidata.org/entity/{company_id}"},
        "companyLabel": {"value": company_label},
        "companyDescription": {"value": "test listed company"},
        "officialName": {"value": company_label},
        "listing": {"value": f"http://www.wikidata.org/entity/statement/{listing_id}"},
        "exchange": {"value": "http://www.wikidata.org/entity/QEX"},
        "exchangeLabel": {"value": "Ignored Exchange Label"},
        "ticker": {"value": ticker},
        "isin": {"value": f"ISIN{ticker}"},
        "website": {"value": website},
        "cik": {"value": f"CIK{ticker}"},
        "lei": {"value": f"LEI{ticker}"},
        "headquarters": {"value": "http://www.wikidata.org/entity/Q60"},
        "headquartersLabel": {"value": "HQ"},
        "headquartersCountry": {"value": "http://www.wikidata.org/entity/Q30"},
        "headquartersCountryLabel": {"value": "United States"},
        "headquartersCountryIso2": {"value": "US"},
        "inceptionDate": {"value": "2001-02-03T00:00:00Z"},
        "legalForm": {"value": "http://www.wikidata.org/entity/Q4830453"},
        "legalFormLabel": {"value": "business"},
        "employeeCount": {"value": "1234"},
        "employeeCountPointInTime": {"value": "2025-12-31T00:00:00Z"},
        "logoImage": {"value": "Alpha logo.svg"},
        "logoImageUrl": {
            "value": "https://commons.wikimedia.org/wiki/Special:FilePath/Alpha%20logo.svg"
        },
        "industry": {"value": "http://www.wikidata.org/entity/Q7397"},
        "industryLabel": {"value": "Software"},
        "openCorporatesId": {"value": "us_de/2923466"},
        "euVatNumber": {"value": "FI12345678"},
        "dunsNumber": {"value": "123456789"},
        "permId": {"value": "4295907168"},
        "bloombergCompanyId": {"value": "ALPHA:US"},
        "linkedinCompanyId": {"value": "alpha-inc"},
        "parentOrganizationStatement": {
            "value": "http://www.wikidata.org/entity/statement/Q1-parent-statement"
        },
        "parentOrganization": {"value": "http://www.wikidata.org/entity/Q100"},
        "parentOrganizationLabel": {"value": "Alpha Holdings"},
        "parentOrganizationStartDate": {"value": "2010-01-01T00:00:00Z"},
        "childOrganizationStatement": {
            "value": "http://www.wikidata.org/entity/statement/Q1-child-statement"
        },
        "childOrganization": {"value": "http://www.wikidata.org/entity/Q101"},
        "childOrganizationLabel": {"value": "Alpha Subsidiary"},
        "childOrganizationStartDate": {"value": "2020-01-01T00:00:00Z"},
        "ownedByStatement": {
            "value": "http://www.wikidata.org/entity/statement/Q1-owned-by-statement"
        },
        "ownedBy": {"value": "http://www.wikidata.org/entity/Q102"},
        "ownedByLabel": {"value": "Alpha Owner"},
        "ownedByStartDate": {"value": "2015-01-01T00:00:00Z"},
        "ownerOfStatement": {
            "value": "http://www.wikidata.org/entity/statement/Q1-owner-of-statement"
        },
        "ownerOf": {"value": "http://www.wikidata.org/entity/Q103"},
        "ownerOfLabel": {"value": "Alpha Owned Business"},
        "ownerOfEndDate": {"value": "2022-12-31T00:00:00Z"},
    }
