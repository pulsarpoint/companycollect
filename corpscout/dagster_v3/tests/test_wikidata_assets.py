import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
from dagster import AssetKey
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.definitions import defs as load_project_defs


def test_wikidata_clickhouse_asset_is_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    resource_keys = repository.get_top_level_resources().keys()

    assert "wikidata_company_seed_raw_objects" in asset_keys
    assert "wikidata_listed_companies_duckdb" in asset_keys
    assert "wikidata_company_seed_clickhouse" in asset_keys
    assert "clickhouse" in resource_keys
    assert "dlt" in resource_keys
    assert "object_store" in resource_keys
    assert (
        repository.get_top_level_resources()["clickhouse"].configurable_resource_cls
        is ClickhouseResource
    )


def test_wikidata_clickhouse_depends_on_duckdb_final_tables() -> None:
    repository = load_project_defs().get_repository_def()

    deps = repository.asset_graph.get(
        AssetKey(["wikidata_company_seed_clickhouse"])
    ).parent_keys

    assert {key.path[-1] for key in deps} == {
        "wikidata_companies",
        "wikidata_company_listings",
        "wikidata_company_identifiers",
        "wikidata_company_websites",
        "wikidata_company_relationships",
        "wikidata_seed_extraction_runs",
    }


def test_wikidata_normalized_tables_depend_on_listed_companies_duckdb() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph

    for asset_name in {
        "wikidata_companies",
        "wikidata_company_listings",
        "wikidata_company_identifiers",
        "wikidata_company_websites",
        "wikidata_company_relationships",
        "wikidata_seed_extraction_runs",
    }:
        asset = asset_graph.get(AssetKey([asset_name]))
        assert asset.is_executable
        assert asset.parent_keys == {AssetKey(["wikidata_listed_companies_duckdb"])}


def test_wikidata_listed_companies_duckdb_depends_on_raw_s3_snapshot() -> None:
    repository = load_project_defs().get_repository_def()

    dlt_asset = repository.asset_graph.get(AssetKey(["wikidata_listed_companies_duckdb"]))

    assert dlt_asset.parent_keys == {AssetKey(["wikidata_company_seed_raw_objects"])}


def test_wikidata_weekly_refresh_job_and_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    job_names = set(repository.job_names)
    schedule_names = {schedule.name for schedule in repository.schedule_defs}
    weekly_schedule = next(
        schedule
        for schedule in repository.schedule_defs
        if schedule.name == "wikidata_company_seed_weekly_schedule"
    )

    assert "wikidata_company_seed_weekly_job" in job_names
    assert "wikidata_company_seed_weekly_schedule" in schedule_names
    assert weekly_schedule.job_name == "wikidata_company_seed_weekly_job"
    assert weekly_schedule.cron_schedule == "0 3 * * 1"
    assert weekly_schedule.execution_timezone == "Europe/Belgrade"


def test_wikidata_raw_object_asset_has_no_upstream_dependencies() -> None:
    repository = load_project_defs().get_repository_def()

    raw_asset = repository.asset_graph.get(AssetKey(["wikidata_company_seed_raw_objects"]))

    assert raw_asset.parent_keys == set()


def test_wikidata_raw_object_asset_is_not_partitioned() -> None:
    repository = load_project_defs().get_repository_def()

    raw_asset = repository.asset_graph.get(AssetKey(["wikidata_company_seed_raw_objects"]))

    assert raw_asset.partitions_def is None


def test_wikidata_listed_companies_duckdb_materialization_uses_dlt_pipeline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dagster_v3.defs.wikidata import assets

    dlt_resource = FakeDagsterDltResource()
    object_store, _s3_client = _object_store()
    monkeypatch.setattr(assets, "WIKIDATA_DUCKDB_PATH", tmp_path / "wikidata.duckdb")

    result = dg.materialize(
        [assets.wikidata_listed_companies_duckdb_asset],
        resources={"dlt": dlt_resource, "object_store": object_store},
        run_config={
            "ops": {
                "wikidata_listed_companies_duckdb": {
                    "config": {
                        "max_exchanges": 1,
                    }
                }
            }
        },
    )

    assert result.success
    assert dlt_resource.pipeline_names == ["wikidata_listed_companies"]
    assert dlt_resource.resource_names == [["listed_companies"]]


def test_wikidata_listed_companies_source_reads_raw_s3_pages() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, s3_client = _object_store()
    page_key = (
        "raw/run_id=run-1/retrieved_date=2026-06-19/"
        "exchange_id=QEX1/page=000001.json"
    )
    manifest_key = (
        "raw/run_id=run-1/retrieved_date=2026-06-19/"
        "exchange_id=QEX1/manifest.json"
    )
    s3_client.objects[("source-wikidata-company-seed", page_key)] = json.dumps(
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
    s3_client.objects[("source-wikidata-company-seed", manifest_key)] = json.dumps(
        {
            "source": "wikidata",
            "query_mode": "exchange",
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

    source = wikidata_source.wikidata_listed_companies_source(
        object_store=object_store,
        source_run_id="run-1",
    )
    rows = list(source.resources[wikidata_source.WIKIDATA_LISTED_COMPANIES_TABLE])

    assert list(source.resources.keys()) == ["listed_companies"]
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
    assert rows[0]["source_run_id"] == "run-1"
    assert rows[0]["retrieved_at"] == "2026-06-19T10:00:00+00:00"
    assert rows[0]["source_record_id"] == "QEX1:000001:000001:Q1:Q1-L1"
    assert len(rows[0]["source_payload_hash"]) == 64
    assert len(rows[0]["query_hash"]) == 64


def test_wikidata_listed_companies_source_merges_manifest_augmentation_objects() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    object_store, s3_client = _object_store()
    page_key = (
        "raw/run_id=run-1/retrieved_date=2026-06-19/"
        "exchange_id=QEX1/page=000001.json"
    )
    augmentation_key = (
        "raw/run_id=run-1/retrieved_date=2026-06-19/"
        "exchange_id=QEX1/augmentation_kind=profile/page=000001_batch=000001.json"
    )
    manifest_key = (
        "raw/run_id=run-1/retrieved_date=2026-06-19/"
        "exchange_id=QEX1/manifest.json"
    )
    s3_client.objects[("source-wikidata-company-seed", page_key)] = json.dumps(
        _wikidata_response(
            [
                {
                    "company": {"value": "http://www.wikidata.org/entity/Q1"},
                    "companyLabel": {"value": "Alpha Inc"},
                    "listing": {"value": "http://www.wikidata.org/entity/statement/Q1-L1"},
                    "ticker": {"value": "AAA"},
                }
            ]
        )
    ).encode("utf-8")
    s3_client.objects[("source-wikidata-company-seed", augmentation_key)] = json.dumps(
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
    s3_client.objects[("source-wikidata-company-seed", manifest_key)] = json.dumps(
        {
            "source": "wikidata",
            "query_mode": "exchange",
            "run_id": "run-1",
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

    source = wikidata_source.wikidata_listed_companies_source(
        object_store=object_store,
        source_run_id="run-1",
    )
    rows = list(source.resources[wikidata_source.WIKIDATA_LISTED_COMPANIES_TABLE])

    assert len(rows) == 1
    assert rows[0]["source_record_id"] == "QEX1:000001:000001:Q1:Q1-L1:aug:000001"
    assert rows[0]["legal_form_wikidata_id"] == "Q4830453"
    assert rows[0]["linkedin_company_id"] == "alpha-inc"
    assert rows[0]["parent_organization_wikidata_id"] == "Q100"


def test_wikidata_listed_companies_resource_declares_explicit_schema() -> None:
    from dagster_v3.defs.wikidata import source as wikidata_source

    source = wikidata_source.wikidata_listed_companies_source(
        source_run_id="run-1",
    )
    schema = source.resources[
        wikidata_source.WIKIDATA_LISTED_COMPANIES_TABLE
    ].compute_table_schema()

    assert set(schema["columns"]) == set(wikidata_source.WIKIDATA_LISTED_COMPANIES_COLUMNS)
    assert schema["columns"]["exchange_wikidata_id"]["data_type"] == "text"
    assert schema["columns"]["listed_company_count_on_exchange"]["data_type"] == "bigint"
    assert schema["columns"]["page_number"]["data_type"] == "bigint"
    assert schema["columns"]["source_payload_hash"]["data_type"] == "text"
    assert schema["columns"]["company_description"]["data_type"] == "text"
    assert schema["columns"]["headquarters_wikidata_id"]["data_type"] == "text"
    assert schema["columns"]["headquarters_country_wikidata_id"]["data_type"] == "text"
    assert schema["columns"]["headquarters_country_label"]["data_type"] == "text"
    assert schema["columns"]["headquarters_country_iso2"]["data_type"] == "text"
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
        assert schema["columns"][column_name]["data_type"] == "text"


def test_wikidata_raw_pull_writes_pages_and_manifest() -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    object_store, s3_client = _object_store()
    old_key = (
        "raw/run_id=old-run/retrieved_date=2026-06-18/"
        "exchange_id=Q13677/page=000001.json"
    )
    s3_client.objects[("source-wikidata-company-seed", old_key)] = b"old"
    old_legacy_key = (
        "raw/retrieved_date=2026-06-18/run_id=old-run/"
        "exchange_id=Q13677/page=000001.json"
    )
    s3_client.objects[("source-wikidata-company-seed", old_legacy_key)] = b"old legacy"
    unrelated_key = "other/raw/run_id=old-run/page=000001.json"
    s3_client.objects[("source-wikidata-company-seed", unrelated_key)] = b"unrelated"
    client = FakeWikidataClient(
        pages=[
            _wikidata_response([{"company": {"value": "http://www.wikidata.org/entity/Q1"}}]),
            _wikidata_response([{"company": {"value": "http://www.wikidata.org/entity/Q2"}}]),
            _wikidata_response([]),
        ]
    )

    result = assets.pull_wikidata_company_seed_raw_objects(
        client=client,
        object_store=object_store,
        config=WikidataRawPullConfig(
            exchange_ids_csv="Q13677",
            page_size=1,
            max_pages=None,
            request_delay_seconds=0.25,
            user_agent="test-agent",
        ),
        run_id="run-123",
        retrieved_at="2026-06-19T10:00:00+00:00",
        sleep=lambda _seconds: None,
    )

    page_one_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/page=000001.json"
    )
    page_two_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/page=000002.json"
    )
    augmentation_page_one_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/augmentation_kind=profile/page=000001_batch=000001.json"
    )
    augmentation_page_two_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/augmentation_kind=profile/page=000002_batch=000001.json"
    )
    identifier_page_one_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/augmentation_kind=identifiers/page=000001_batch=000001.json"
    )
    relationship_page_one_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/augmentation_kind=relationships/page=000001_batch=000001.json"
    )
    manifest_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/manifest.json"
    )

    assert ("source-wikidata-company-seed", page_one_key) in s3_client.objects
    assert ("source-wikidata-company-seed", page_two_key) in s3_client.objects
    assert ("source-wikidata-company-seed", augmentation_page_one_key) in s3_client.objects
    assert ("source-wikidata-company-seed", augmentation_page_two_key) in s3_client.objects
    assert ("source-wikidata-company-seed", identifier_page_one_key) in s3_client.objects
    assert ("source-wikidata-company-seed", relationship_page_one_key) in s3_client.objects
    assert ("source-wikidata-company-seed", manifest_key) in s3_client.objects
    assert ("source-wikidata-company-seed", old_key) not in s3_client.objects
    assert ("source-wikidata-company-seed", old_legacy_key) not in s3_client.objects
    assert ("source-wikidata-company-seed", unrelated_key) in s3_client.objects
    page_three_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=Q13677/page=000003.json"
    )
    assert ("source-wikidata-company-seed", page_three_key) not in s3_client.objects

    manifest = json.loads(
        s3_client.objects[("source-wikidata-company-seed", manifest_key)].decode("utf-8")
    )
    assert manifest["source"] == "wikidata"
    assert manifest["query_mode"] == "exchange"
    assert manifest["exchange_id"] == "Q13677"
    assert "partition_month" not in manifest
    assert manifest["row_count"] == 2
    assert manifest["augmentation_row_count"] == 0
    assert manifest["page_count"] == 2
    assert manifest["objects"] == [page_one_key, page_two_key]
    assert manifest["augmentation_objects"] == [
        augmentation_page_one_key,
        identifier_page_one_key,
        relationship_page_one_key,
        augmentation_page_two_key,
        (
            "raw/run_id=run-123/retrieved_date=2026-06-19/"
            "exchange_id=Q13677/augmentation_kind=identifiers/page=000002_batch=000001.json"
        ),
        (
            "raw/run_id=run-123/retrieved_date=2026-06-19/"
            "exchange_id=Q13677/augmentation_kind=relationships/page=000002_batch=000001.json"
        ),
    ]
    assert len(manifest["query_hash"]) == 64
    assert result.metadata["row_count"] == 2
    assert result.metadata["page_count"] == 2
    assert result.metadata["exchange_count"] == 1
    assert result.metadata["deleted_old_raw_object_count"] == 2
    assert "partition_month" not in result.metadata
    assert client.offsets == [0, 1, 2]


def test_wikidata_raw_pull_discovers_active_exchanges_before_downloading_pages() -> None:
    from dagster_v3.defs.wikidata import assets
    from dagster_v3.defs.wikidata.source import WikidataRawPullConfig

    object_store, s3_client = _object_store()
    client = FakeWikidataDltClient(
        exchange_payload=_wikidata_response(
            [
                {
                    "exchange": {"value": "http://www.wikidata.org/entity/QEX1"},
                    "exchangeLabel": {"value": "Test Exchange One"},
                    "listedCompanyCount": {"value": "2"},
                },
                {
                    "exchange": {"value": "http://www.wikidata.org/entity/QEX2"},
                    "exchangeLabel": {"value": "Test Exchange Two"},
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

    result = assets.pull_wikidata_company_seed_raw_objects(
        client=client,
        object_store=object_store,
        config=WikidataRawPullConfig(
            page_size=1,
            max_pages=None,
            max_exchanges=2,
            request_delay_seconds=0,
            user_agent="test-agent",
        ),
        run_id="run-123",
        retrieved_at="2026-06-19T10:00:00+00:00",
        sleep=lambda _seconds: None,
    )

    manifest_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "exchange_id=QEX1/manifest.json"
    )
    exchange_list_key = (
        "raw/run_id=run-123/retrieved_date=2026-06-19/"
        "active_exchanges.json"
    )
    manifest = json.loads(
        s3_client.objects[("source-wikidata-company-seed", manifest_key)].decode("utf-8")
    )

    assert ("source-wikidata-company-seed", exchange_list_key) in s3_client.objects
    assert manifest["exchange_name"] == "Test Exchange One"
    assert manifest["listed_company_count_on_exchange"] == 2
    assert manifest["page_size"] == 1
    assert result.metadata["exchange_count"] == 2
    assert result.metadata["row_count"] == 2
    assert client.company_offsets == {"QEX1": [0, 1], "QEX2": [0, 1]}


def test_wikidata_query_uses_stable_order_for_offset_pagination() -> None:
    from dagster_v3.defs.wikidata.source import (
        build_company_identifier_augmentation_query,
        build_company_profile_augmentation_query,
        build_company_relationship_augmentation_query,
        build_listed_company_query,
    )

    query = build_listed_company_query(exchange_id="Q13677", limit=100, offset=200)
    profile_query = build_company_profile_augmentation_query(("Q1", "Q2"))
    identifier_query = build_company_identifier_augmentation_query(("Q1", "Q2"))
    relationship_query = build_company_relationship_augmentation_query(("Q1", "Q2"))

    assert (
        "OPTIONAL { ?headquarters wdt:P131*/wdt:P17 ?headquartersCountry . }"
        in query
    )
    assert "OPTIONAL { ?headquartersCountry wdt:P297 ?headquartersCountryIso2 . }" in query
    assert "?companyDescription" in query
    assert "?headquartersCountryLabel" in query
    assert (
        "ORDER BY ?company ?listing ?website ?ticker ?isin ?cik ?lei "
        "?headquarters ?headquartersCountry ?headquartersCountryIso2 ?industry"
        in query
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


def test_wikidata_normalization_builds_final_duckdb_tables(tmp_path: Path) -> None:
    from dagster_v3.defs.wikidata import assets

    database_path = tmp_path / "wikidata.duckdb"
    _seed_wikidata_listed_companies(database_path)

    row_counts = assets.normalize_wikidata_listed_companies_duckdb(database_path)

    assert row_counts == {
        "wikidata_companies": 2,
        "wikidata_company_listings": 2,
        "wikidata_company_identifiers": 11,
        "wikidata_company_websites": 2,
        "wikidata_company_relationships": 4,
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
    assert runs == [("active_exchange_listing", 2, 2, 2)]


def test_wikidata_clickhouse_export_uses_final_table_contract(monkeypatch) -> None:
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

    def fake_replace_duckdb_tables_in_clickhouse(**kwargs: Any) -> dict[str, int]:
        calls["replace"] = kwargs
        return {table_name: index + 1 for index, table_name in enumerate(tables.WIKIDATA_TABLES)}

    monkeypatch.setattr(
        assets,
        "assert_clickhouse_tables_exist",
        fake_assert_clickhouse_tables_exist,
    )
    monkeypatch.setattr(
        assets,
        "replace_duckdb_tables_in_clickhouse",
        fake_replace_duckdb_tables_in_clickhouse,
    )

    clickhouse = ClickhouseResource(host="localhost")
    client = object()

    @contextmanager
    def fake_connection(self: ClickhouseResource) -> Iterator[object]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_connection)

    result = assets._export_wikidata_tables_to_clickhouse(clickhouse)

    assert calls["assert"] == {
        "clickhouse": clickhouse,
        "database": "corpscout",
        "tables": tables.WIKIDATA_TABLES,
    }
    assert calls["replace"] == {
        "duckdb_path": Path("data/wikidata.duckdb"),
        "clickhouse_client": client,
        "duckdb_schema": "wikidata.wikidata",
        "clickhouse_database": "corpscout",
        "tables": tuple(
            (table_name, tables.WIKIDATA_TABLE_COLUMNS[table_name])
            for table_name in tables.WIKIDATA_TABLES
        ),
    }
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "wikidata_companies_row_count": 1,
        "wikidata_company_listings_row_count": 2,
        "wikidata_company_identifiers_row_count": 3,
        "wikidata_company_websites_row_count": 4,
        "wikidata_company_relationships_row_count": 5,
        "wikidata_seed_extraction_runs_row_count": 6,
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


class FakeWikidataDltClient:
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

        exchange_id = query.split("VALUES ?exchange { wd:", 1)[1].split("}", 1)[0].strip()
        offset_marker = "OFFSET "
        offset = int(query.split(offset_marker, 1)[1].splitlines()[0])
        self.company_offsets.setdefault(exchange_id, []).append(offset)
        return self._company_payloads_by_exchange[exchange_id].pop(0)


class FakeDagsterDltResource:
    def __init__(self) -> None:
        self.pipeline_names: list[str] = []
        self.resource_names: list[list[str]] = []

    def run(self, **kwargs: Any) -> Iterator[dg.MaterializeResult]:
        pipeline = kwargs["dlt_pipeline"]
        source = kwargs["dlt_source"]
        self.pipeline_names.append(pipeline.pipeline_name)
        self.resource_names.append(list(source.resources.keys()))
        yield dg.MaterializeResult(metadata={"pipeline_name": pipeline.pipeline_name})


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def create_bucket(self, Bucket: str) -> None:
        pass

    def head_object(self, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            raise FakeS3Error("404")

    def put_object(self, Bucket: str, Key: str, Body: bytes | str) -> None:
        self.objects[(Bucket, Key)] = Body.encode("utf-8") if isinstance(Body, str) else Body

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
            bucket="source-wikidata-company-seed",
            endpoint_url="http://test-s3",
            access_key="test-access-key",
            secret_key="test-secret-key",
            s3_client=s3_client,
        ),
        s3_client,
    )


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
