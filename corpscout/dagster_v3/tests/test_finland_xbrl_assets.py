from datetime import date
from io import BytesIO
from pathlib import Path

import dagster as dg
import duckdb
from dagster import AssetKey
import polars as pl
import pytest
from pydantic import ValidationError

import dagster_v3.defs.finland_xbrl.assets as xbrl_assets
from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.finland_xbrl.assets import (
    RAW_XML_DOCUMENTS_OBJECT_KEY,
    XbrlParsedConfig,
    XbrlRawConfig,
    download_finland_xbrl_raw_xml_documents,
    resolve_xbrl_documents_key,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlApiResource
from dagster_v3.defs.finland_xbrl.tables import (
    FACTS_TABLE,
    FINANCIAL_METRICS_TABLE,
    STATEMENT_DOCUMENTS_TABLE,
    TABLE_COLUMNS,
    XML_DOCUMENTS_TABLE,
)
from dagster_v3.defs.common.resources import LocalDuckDBResource, ObjectStoreResource


class FakeResponse:
    def __init__(self, payload: dict | None = None, content: bytes = b"", status_code: int = 200) -> None:
        self._payload = payload or {}
        self.content = content
        self.status_code = status_code
        self.headers: dict[str, str] = {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None, int]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict | None = None, timeout: int = 120) -> FakeResponse:
        self.calls.append((url, params, timeout))
        if url.endswith("/financial"):
            business_id = params["businessId"]
            financial_date = params["financialDate"]
            return FakeResponse(content=f"<xbrl>{business_id}:{financial_date}</xbrl>".encode())
        raise AssertionError(f"unexpected URL {url}")


class FakePagedFinancialReportsSession:
    def __init__(self, financials_by_page: dict[int, list[dict]] | None = None) -> None:
        self.calls: list[tuple[str, dict | None, int]] = []
        self.headers: dict[str, str] = {}
        self._financials_by_page = financials_by_page or {
            1: [
                {
                    "businessId": "active-web",
                    "financialDate": "2026-05-31",
                    "registrationDate": "2026-06-01",
                }
            ],
            2: [
                {
                    "businessId": "second-active-web",
                    "financialDate": "2026-04-30",
                    "registrationDate": "2026-06-02",
                }
            ],
            3: [],
        }

    def get(self, url: str, params: dict | None = None, timeout: int = 120) -> FakeResponse:
        self.calls.append((url, params, timeout))
        assert url.endswith("/all_financial_statements")
        page = int(params["page"])
        financials = self._financials_by_page.get(page, [])
        return FakeResponse(
            payload={
                "totalResults": len(financials),
                "financials": financials,
            }
        )


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def create_bucket(self, Bucket: str) -> None:
        pass

    def head_object(self, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            raise FakeS3Error("404")

    def put_object(self, Bucket: str, Key: str, Body: bytes | str) -> None:
        body = Body.encode("utf-8") if isinstance(Body, str) else Body
        self.objects[(Bucket, Key)] = body

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


def _object_store() -> tuple[ObjectStoreResource, FakeS3Client]:
    s3_client = FakeS3Client()
    return ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client), s3_client


def test_xbrl_financial_reports_config_has_launchpad_defaults() -> None:
    config = xbrl_assets.XbrlFinancialReportsConfig()

    assert config.request_delay_seconds == 1.0
    assert config.max_retries == 5
    assert config.retry_initial_delay_seconds == 10.0
    assert config.retry_max_delay_seconds == 120.0


def test_financial_reports_asset_is_monthly_partitioned():
    node = load_project_defs().get_repository_def().asset_graph.get(
        AssetKey(["finland_xbrl_financial_reports_duckdb"])
    )
    assert node.partitions_def is not None
    assert type(node.partitions_def).__name__ == "MonthlyPartitionsDefinition"


def test_financial_reports_merge_accumulates_across_windows(tmp_path: Path) -> None:
    db = tmp_path / "finland_ytj.duckdb"
    window_one_session = FakePagedFinancialReportsSession(
        {
            1: [
                {
                    "businessId": "a",
                    "financialDate": "2024-02-29",
                    "registrationDate": "2024-03-15",
                }
            ],
            2: [],
        }
    )
    window_two_session = FakePagedFinancialReportsSession(
        {
            1: [
                {
                    "businessId": "b",
                    "financialDate": "2024-03-31",
                    "registrationDate": "2024-04-15",
                }
            ],
            2: [],
        }
    )

    xbrl_assets.run_finland_xbrl_financial_reports_dlt_pipeline(
        database_path=db,
        registered_date_start="2024-03-01",
        registered_date_end="2024-03-31",
        run_id="r1",
        session=window_one_session,
    )
    xbrl_assets.run_finland_xbrl_financial_reports_dlt_pipeline(
        database_path=db,
        registered_date_start="2024-04-01",
        registered_date_end="2024-04-30",
        run_id="r2",
        session=window_two_session,
    )

    with duckdb.connect(str(db), read_only=True) as conn:
        ids = [
            row[0]
            for row in conn.execute(
                f"select business_id from "
                f"{xbrl_assets.XBRL_DLT_DATASET_NAME}.{xbrl_assets.XBRL_DLT_FINANCIAL_REPORTS_TABLE} "
                f"order by business_id"
            ).fetchall()
        ]

    assert ids == ["a", "b"]


def test_xbrl_raw_config_uses_duckdb_selection_by_default() -> None:
    config = XbrlRawConfig()

    assert config.financial_start_date is None
    assert config.max_reports is None
    assert config.refresh_existing is False
    assert config.download_delay_seconds == 1.0


def test_xbrl_raw_config_treats_blank_optional_dates_as_missing() -> None:
    config = XbrlRawConfig(
        financial_start_date=" ",
    )

    assert config.financial_start_date is None


def test_xbrl_transforms_are_dbt_assets():
    graph = load_project_defs().get_repository_def().asset_graph
    keys = {k.path[-1] for k in graph.get_all_asset_keys()}
    assert "finland_xbrl_eligible_financial_reports" in keys
    assert "fi_prh_xbrl_financial_metrics" in keys
    # the download re-pointed to the dbt eligible model
    deps = graph.get(AssetKey(["finland_xbrl_raw_xml_documents"])).parent_keys
    assert AssetKey(["finland_xbrl_eligible_financial_reports"]) in deps
    # eligible model depends on its two sources
    eligible_deps = graph.get(AssetKey(["finland_xbrl_eligible_financial_reports"])).parent_keys
    dep_names = {k.path[-1] for k in eligible_deps}
    assert {"finland_xbrl_financial_reports_duckdb", "finland_ytj_all_companies_duckdb"} <= dep_names


@pytest.mark.parametrize("value", [0, -1])
def test_xbrl_raw_config_rejects_non_positive_max_reports(value: int) -> None:
    with pytest.raises(ValidationError):
        XbrlRawConfig(max_reports=value)


def test_xbrl_parsed_config_uses_xml_documents_manifest_by_default() -> None:
    assert XbrlParsedConfig()
    assert XbrlParsedConfig(documents_key="raw/fi_prh_xbrl_xml_documents.parquet")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"documents_key": 1},
        {"listing_key": "windows/start=2026-01-01/end=2026-01-31/listing.json"},
        {"registered_date_start": "2026-01-01"},
        {"registered_date_end": "2026-01-31"},
    ],
)
def test_xbrl_parsed_config_rejects_non_manifest_config(kwargs: dict) -> None:
    with pytest.raises(ValidationError):
        XbrlParsedConfig(**kwargs)


def test_xbrl_asset_graph_models_eligible_companies_downstream_of_ytj_duckdb() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert dg.AssetKey("fi_prhytj_statuses") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("fi_prhytj_websites") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_financial_reports_duckdb") in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_eligible_companies") not in asset_graph.get_all_asset_keys()
    assert asset_graph.get(dg.AssetKey("finland_xbrl_eligible_financial_reports")).parent_keys == {
        dg.AssetKey("finland_ytj_all_companies_duckdb"),
        dg.AssetKey("finland_xbrl_financial_reports_duckdb"),
    }


def test_xbrl_financial_reports_are_modeled_as_dlt_source() -> None:
    repository = load_project_defs().get_repository_def()
    resource_keys = repository.get_top_level_resources().keys()

    assert "dlt" in resource_keys
    assert "finland_xbrl_financial_reports_source" in xbrl_assets.__dict__
    assert "dlt" in xbrl_assets.finland_xbrl_financial_reports_duckdb_asset.required_resource_keys


def test_xbrl_dlt_source_pages_financial_report_listing() -> None:
    session = FakePagedFinancialReportsSession()
    sleeps: list[float] = []

    source = xbrl_assets.finland_xbrl_financial_reports_source(
        registered_date_start="2026-06-01",
        registered_date_end="2026-06-30",
        request_delay_seconds=0.5,
        run_id="test-run",
        session=session,
        sleep=sleeps.append,
    )
    rows = list(source.resources[xbrl_assets.XBRL_DLT_FINANCIAL_REPORTS_TABLE])

    assert [row["business_id"] for row in rows] == ["active-web", "second-active-web"]
    assert rows[0]["financial_date"] == "2026-05-31"
    assert rows[0]["registration_date"] == "2026-06-01"
    assert rows[0]["source_run_id"] == "test-run"
    assert rows[0]["source_page_number"] == 1
    assert rows[1]["source_page_number"] == 2
    assert [call[1]["page"] for call in session.calls] == [1, 2, 3]
    assert session.calls[0][1]["registeredDateStart"] == "2026-06-01"
    assert session.calls[0][1]["registeredDateEnd"] == "2026-06-30"
    assert sleeps == [0.5, 0.5]


def test_xbrl_financial_reports_http_client_uses_dlt_retry_client() -> None:
    client = xbrl_assets._financial_reports_http_client(
        timeout_seconds=120,
        user_agent="test-agent",
        max_retries=5,
        retry_initial_delay_seconds=10.0,
        retry_max_delay_seconds=120.0,
    )

    assert client.__class__.__module__ == "dlt.sources.helpers.requests.retry"
    assert client._retry_kwargs["status_codes"] == (429, *range(500, 600))
    assert client._retry_kwargs["max_attempts"] == 5
    assert client._retry_kwargs["backoff_factor"] == 10.0
    assert client._retry_kwargs["respect_retry_after_header"] is True
    assert client._retry_kwargs["max_delay"] == 120.0


def test_xbrl_dlt_pipeline_loads_financial_reports_table(tmp_path: Path) -> None:
    session = FakePagedFinancialReportsSession()
    database_path = tmp_path / "source.duckdb"

    load_info = xbrl_assets.run_finland_xbrl_financial_reports_dlt_pipeline(
        database_path=database_path,
        registered_date_start="2026-06-01",
        registered_date_end="2026-06-30",
        run_id="test-run",
        session=session,
    )

    assert load_info

    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            select business_id, financial_date, registration_date, source_page_number
            from {xbrl_assets.XBRL_DLT_DATASET_NAME}.{xbrl_assets.XBRL_DLT_FINANCIAL_REPORTS_TABLE}
            order by business_id
            """
        ).fetchall()

    assert rows == [
        ("active-web", "2026-05-31", "2026-06-01", 1),
        ("second-active-web", "2026-04-30", "2026-06-02", 2),
    ]


def test_xbrl_asset_graph_models_xml_documents_catalog_as_bridge() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert dg.AssetKey(XML_DOCUMENTS_TABLE) in asset_graph.get_all_asset_keys()
    assert asset_graph.get(dg.AssetKey("finland_xbrl_raw_xml_documents")).parent_keys == {
        dg.AssetKey("finland_xbrl_eligible_financial_reports")
    }
    assert asset_graph.get(dg.AssetKey(XML_DOCUMENTS_TABLE)).parent_keys == {
        dg.AssetKey("finland_xbrl_raw_xml_documents")
    }
    assert asset_graph.get(dg.AssetKey(STATEMENT_DOCUMENTS_TABLE)).parent_keys == {
        dg.AssetKey(XML_DOCUMENTS_TABLE)
    }
    assert asset_graph.get(dg.AssetKey(FACTS_TABLE)).parent_keys == {
        dg.AssetKey(XML_DOCUMENTS_TABLE)
    }


def test_xbrl_asset_graph_keeps_quality_and_concept_profile_as_metadata_not_assets() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert dg.AssetKey("fi_prh_xbrl_parse_quality") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("fi_prh_xbrl_concept_profile") not in asset_graph.get_all_asset_keys()


def test_xbrl_asset_graph_models_financial_metrics_downstream_of_parsed_tables() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    # fi_prh_xbrl_financial_metrics is now a dbt model: its parents are the two
    # parsed-table sources plus the xbrl_metric_map seed.
    parent_keys = asset_graph.get(dg.AssetKey(FINANCIAL_METRICS_TABLE)).parent_keys
    assert {
        dg.AssetKey(STATEMENT_DOCUMENTS_TABLE),
        dg.AssetKey(FACTS_TABLE),
    } <= parent_keys
    assert dg.AssetKey("xbrl_metric_map") in parent_keys


def test_xbrl_api_resource_only_keeps_xml_download_methods() -> None:
    assert not hasattr(XbrlApiResource, "iter_registration_window")
    assert not hasattr(XbrlApiResource, "_list_registration_window")


def test_xbrl_api_resource_uses_dlt_retry_client_for_xml_downloads() -> None:
    api = XbrlApiResource()
    client = api.session()

    assert client.__class__.__module__ == "dlt.sources.helpers.requests.retry"
    assert client._retry_kwargs["status_codes"] == (429, *range(500, 600))
    assert client._retry_kwargs["max_attempts"] == 5
    assert client._retry_kwargs["backoff_factor"] == 10.0
    assert client._retry_kwargs["respect_retry_after_header"] is True
    assert client._retry_kwargs["max_delay"] == 120.0


def test_xbrl_raw_download_uses_eligible_financial_report_rows() -> None:
    session = FakeHttpSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()
    sleeps: list[float] = []

    result = download_finland_xbrl_raw_xml_documents(
        xbrl_api=api,
        object_store=object_store,
        financial_reports=_eligible_financial_reports(),
        financial_start_date="2024-06-15",
        max_reports=None,
        refresh_existing=False,
        download_delay_seconds=0.5,
        sleep=sleeps.append,
    )

    assert result.metadata["downloaded_count"] == 2
    assert result.metadata["selected_reports_count"] == 2
    assert ("source-finland-prh-xbrl", "companies/active-web/2026-05-31.xml") in s3_client.objects
    assert ("source-finland-prh-xbrl", "companies/second-active-web/2026-04-30.xml") in s3_client.objects
    assert [call[0] for call in session.calls] == [
        "https://avoindata.prh.fi/opendata-xbrl-api/v3/financial",
        "https://avoindata.prh.fi/opendata-xbrl-api/v3/financial",
    ]
    assert sleeps == [0.5]

    xml_documents_frame = pl.read_parquet(
        BytesIO(s3_client.objects[("source-finland-prh-xbrl", RAW_XML_DOCUMENTS_OBJECT_KEY)])
    )
    assert xml_documents_frame.columns == [
        "business_id",
        "financial_date",
        "registration_date",
        "source_url",
        "xml_object_key",
        "xml_sha256",
        "xml_size_bytes",
        "downloaded",
        "reused",
        "discovery_registered_date_start",
        "discovery_registered_date_end",
        "financial_start_date",
        "max_reports",
        "selected_at",
    ]
    assert xml_documents_frame["business_id"].to_list() == ["active-web", "second-active-web"]
    assert xml_documents_frame["xml_object_key"].to_list() == [
        "companies/active-web/2026-05-31.xml",
        "companies/second-active-web/2026-04-30.xml",
    ]
    assert xml_documents_frame["downloaded"].to_list() == [True, True]
    assert xml_documents_frame["reused"].to_list() == [False, False]
    assert xml_documents_frame["discovery_registered_date_start"].to_list() == [
        "2026-06-01",
        "2026-06-01",
    ]


def test_xbrl_raw_download_limits_accepted_financial_reports() -> None:
    session = FakeHttpSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()

    result = download_finland_xbrl_raw_xml_documents(
        xbrl_api=api,
        object_store=object_store,
        financial_reports=_eligible_financial_reports(),
        financial_start_date="2024-06-15",
        max_reports=1,
        refresh_existing=False,
        download_delay_seconds=0.0,
    )

    assert result.metadata["max_reports"] == 1
    assert result.metadata["documents_count"] == 1
    assert result.metadata["downloaded_count"] == 1
    assert ("source-finland-prh-xbrl", "companies/active-web/2026-05-31.xml") in s3_client.objects
    assert ("source-finland-prh-xbrl", "companies/second-active-web/2026-04-30.xml") not in s3_client.objects


def test_xbrl_raw_download_preserves_existing_xml_document_catalog_when_selection_is_empty() -> None:
    session = FakeHttpSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()
    s3_client.objects[("source-finland-prh-xbrl", RAW_XML_DOCUMENTS_OBJECT_KEY)] = (
        _xml_documents_parquet(
            [
                {
                    "business_id": "existing-business",
                    "financial_date": "2024-12-31",
                    "registration_date": "2025-06-01",
                    "source_url": "https://example.test/financial",
                    "xml_object_key": "companies/existing-business/2024-12-31.xml",
                    "xml_sha256": "old-hash",
                    "xml_size_bytes": 123,
                    "downloaded": True,
                    "reused": False,
                    "discovery_registered_date_start": "2025-06-01",
                    "discovery_registered_date_end": "2025-06-02",
                    "financial_start_date": "2023-01-01",
                    "max_reports": "1",
                    "selected_at": "2025-06-02T00:00:00+00:00",
                }
            ]
        )
    )

    result = download_finland_xbrl_raw_xml_documents(
        xbrl_api=api,
        object_store=object_store,
        financial_reports=[],
        financial_start_date="2024-06-15",
        max_reports=None,
        refresh_existing=False,
        download_delay_seconds=0.0,
    )

    assert result.metadata["documents_count"] == 0
    xml_documents_frame = pl.read_parquet(
        BytesIO(s3_client.objects[("source-finland-prh-xbrl", RAW_XML_DOCUMENTS_OBJECT_KEY)])
    )
    assert xml_documents_frame["business_id"].to_list() == ["existing-business"]
    assert xml_documents_frame["xml_object_key"].to_list() == [
        "companies/existing-business/2024-12-31.xml"
    ]


def test_load_eligible_financial_report_rows_filters_by_financial_date_and_limit(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {xbrl_assets.XBRL_DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {xbrl_assets.XBRL_DLT_DATASET_NAME}.{xbrl_assets.XBRL_ELIGIBLE_FINANCIAL_REPORTS_TABLE} (
                business_id varchar,
                financial_date varchar,
                registration_date varchar,
                discovery_registered_date_start varchar,
                discovery_registered_date_end varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {xbrl_assets.XBRL_DLT_DATASET_NAME}.{xbrl_assets.XBRL_ELIGIBLE_FINANCIAL_REPORTS_TABLE} values
            ('old-financial', '2023-12-31', '2024-01-01', '2024-01-01', '2024-01-31'),
            ('active-web', '2026-05-31', '2026-06-01', '2026-06-01', '2026-06-30'),
            ('second-active-web', '2026-04-30', '2026-06-02', '2026-06-01', '2026-06-30')
            """
        )

    rows = xbrl_assets.load_eligible_financial_report_rows(
        LocalDuckDBResource(database_path=str(database_path)),
        financial_start_date="2024-06-15",
        max_reports=1,
    )

    assert rows == [
        {
            "business_id": "second-active-web",
            "financial_date": "2026-04-30",
            "registration_date": "2026-06-02",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        }
    ]


def test_resolve_xbrl_documents_key_uses_default_manifest_when_no_config() -> None:
    object_store, s3_client = _object_store()

    documents_key = resolve_xbrl_documents_key(
        object_store=object_store,
        config=XbrlParsedConfig(),
        run_date=date(2026, 6, 15),
    )

    assert documents_key == RAW_XML_DOCUMENTS_OBJECT_KEY
    assert s3_client.objects == {}


def _xml_documents_parquet(rows: list[dict]) -> bytes:
    output = BytesIO()
    pl.DataFrame(
        rows,
        schema={
            column: _xml_document_column_dtype(column)
            for column in TABLE_COLUMNS[XML_DOCUMENTS_TABLE]
        },
    ).write_parquet(output)
    return output.getvalue()


def _xml_document_column_dtype(column: str) -> pl.DataType:
    if column == "xml_size_bytes":
        return pl.Int64
    if column in {"downloaded", "reused"}:
        return pl.Boolean
    return pl.Utf8


def _eligible_financial_reports() -> list[dict]:
    return [
        {
            "business_id": "active-web",
            "financial_date": "2026-05-31",
            "registration_date": "2026-06-01",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
        {
            "business_id": "second-active-web",
            "financial_date": "2026-04-30",
            "registration_date": "2026-06-02",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
    ]


def test_duckdb_xbrl_assets_share_the_finland_ytj_duckdb_pool():
    graph = load_project_defs().get_repository_def().asset_graph
    for key in (
        "finland_xbrl_financial_reports_duckdb",
        "finland_xbrl_eligible_financial_reports",
        "fi_prh_xbrl_statement_documents",
        "fi_prh_xbrl_financial_metrics",
    ):
        node = graph.get(AssetKey([key]))
        assert "finland_ytj_duckdb" in node.pools, f"{key} missing pool"
