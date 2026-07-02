from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
import json
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetKey
import duckdb
import pytest
from pydantic import ValidationError

import dagster_v3.defs.finland_xbrl.assets as xbrl_assets
from dagster_v3.defs.finland_xbrl import metric_mapping
from dagster_v3.defs.finland_xbrl.assets import financial_metrics
from dagster_v3.defs.finland_xbrl.assets import parse as parse_assets
from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.finland_xbrl.assets import (
    FINANCIAL_DATA_DAILY_KEY_PREFIX,
    FINANCIAL_DATA_S3_SNAPSHOT_KEY,
    FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END,
    FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START,
    FINLAND_XBRL_FINANCIAL_DATA_DAILY_DUCKDB_PATH,
    FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH,
    FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH,
    FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE,
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
    XbrlParsedConfig,
    XbrlRawConfig,
    XML_SNAPSHOT_PARTITIONS,
    build_financial_data_snapshot_csv,
    download_finland_xbrl_snapshot_xml_partition,
    download_finland_xbrl_raw_xml_documents,
    export_data_daily_duckdb_to_clickhouse,
    export_data_snapshot_duckdb_to_clickhouse,
    fetch_xml_snapshot_report_rows,
    financial_data_daily_key,
    materialize_data_daily_xml,
    materialize_data_daily_xml_duckdb,
    materialize_data_snapshot_xml_duckdb,
    materialize_data_daily_duckdb,
    materialize_data_snapshot_duckdb,
    read_xml_snapshot_manifest_rows,
    write_financial_data_daily_csv,
    write_financial_data_snapshot_csv,
    xml_snapshot_document_key,
    xml_snapshot_manifest_key,
    xml_snapshot_parse_duckdb_path,
    xml_snapshot_parse_temp_dir,
    xml_snapshot_partition_prefix,
    xml_snapshot_success_key,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.finland_xbrl.resources import (
    XbrlApiResource,
    XbrlParquetStorageResource,
)
from dagster_v3.defs.finland_xbrl.parser import ParsedStatement
from dagster_v3.defs.finland_xbrl.tables import (
    FACTS_TABLE,
    FINANCIAL_METRICS_TABLE,
    STATEMENT_DOCUMENTS_TABLE,
    XML_DOCUMENTS_TABLE,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

FINANCIAL_METRICS_USD_TABLE = "fi_prh_xbrl_financial_metrics_usd"


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


class FailingFinancialXmlSession(FakeHttpSession):
    def __init__(self, status_code: int) -> None:
        super().__init__()
        self.status_code = status_code

    def get(self, url: str, params: dict | None = None, timeout: int = 120) -> FakeResponse:
        self.calls.append((url, params, timeout))
        if url.endswith("/financial"):
            return FakeResponse(status_code=self.status_code)
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


class FakeClickHouseClient:
    def __init__(
        self,
        tables: set[str] | None = None,
        financial_statement_listing_rows: list[tuple[str, str, str]] | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.execute_calls: list[tuple[str, object | None]] = []
        self.tables = tables or {"fi_financial_metrics"}
        self.financial_statement_listing_rows = financial_statement_listing_rows or []

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str, ...]]:
        self.execute_calls.append((sql, params))
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = set(params.get("tables", ())) if isinstance(params, dict) else set()
            return [(table,) for table in sorted(self.tables & requested)]
        if "fi_xbrl_financial_statement_listings" in sql:
            return self.financial_statement_listing_rows
        return []


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeUsdRate:
    currency = "EUR"
    requested_rate_date = "2026-05-31"
    rate_date = "2026-05-30"
    rate = Decimal("1.10")
    source = "test-fx"


class FakeExchangeRates:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        self.requests.extend((request.currency, request.rate_date) for request in requests)
        return {
            (request.currency, request.rate_date): FakeUsdRate()
            for request in requests
        }


class FakeExchangeRatesWithMissing:
    def usd_rates(self, requests):
        raise LookupError("No USD exchange rate for EUR on, before, or after 2026-05-31")


class RecordingStatementParser:
    def __init__(self, *, fail_business_id: str | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail_business_id = fail_business_id

    def __call__(self, **kwargs) -> ParsedStatement:
        self.calls.append(kwargs)
        if kwargs["business_id"] == self.fail_business_id:
            raise ValueError("bad xml")
        statement_key = f"statement-{kwargs['business_id']}"
        return ParsedStatement(
            statement_key=statement_key,
            rows_by_table={
                STATEMENT_DOCUMENTS_TABLE: [
                    {
                        **_statement_document_row(statement_key),
                        "business_id": kwargs["business_id"],
                        "financial_date": kwargs["financial_date"],
                        "registration_date": kwargs["registration_date"],
                        "source_url": kwargs["source_url"],
                        "xml_object_key": kwargs["xml_object_key"],
                        "source_run_id": kwargs["source_run_id"],
                    }
                ],
                FACTS_TABLE: [
                    {
                        **_fact_row(statement_key, fact_ordinal=1),
                        "business_id": kwargs["business_id"],
                        "financial_date": kwargs["financial_date"],
                    }
                ],
            },
            warnings=[],
        )


def _object_store() -> tuple[ObjectStoreResource, FakeS3Client]:
    s3_client = FakeS3Client()
    return ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client), s3_client


def test_xbrl_financial_reports_config_has_launchpad_defaults() -> None:
    config = xbrl_assets.XbrlFinancialReportsConfig()

    assert config.request_delay_seconds == 1.0


def test_xbrl_parquet_storage_resource_maps_partition_paths(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))

    assert storage.financial_reports_backfill_path("2026-05-01") == (
        tmp_path
        / "parquet"
        / "financial_reports_backfill"
        / "partition_key=2026-05-01"
        / "data.parquet"
    )
    assert storage.financial_reports_incremental_path("2026-06-28") == (
        tmp_path
        / "parquet"
        / "financial_reports_incremental"
        / "partition_key=2026-06-28"
        / "data.parquet"
    )
    assert storage.raw_xml_documents_backfill_path("2026-05-01") == (
        tmp_path
        / "parquet"
        / "raw_xml_documents_backfill"
        / "partition_key=2026-05-01"
        / "data.parquet"
    )
    assert storage.raw_xml_documents_incremental_path("2026-06-28") == (
        tmp_path
        / "parquet"
        / "raw_xml_documents_incremental"
        / "partition_key=2026-06-28"
        / "data.parquet"
    )
    assert storage.statement_documents_backfill_path("2026-05-01") == (
        tmp_path
        / "parquet"
        / "statement_documents_backfill"
        / "partition_key=2026-05-01"
        / "data.parquet"
    )
    assert storage.statement_documents_incremental_path("2026-06-28") == (
        tmp_path
        / "parquet"
        / "statement_documents_incremental"
        / "partition_key=2026-06-28"
        / "data.parquet"
    )
    assert storage.facts_backfill_path("2026-05-01") == (
        tmp_path
        / "parquet"
        / "facts_backfill"
        / "partition_key=2026-05-01"
        / "data.parquet"
    )
    assert storage.facts_incremental_path("2026-06-28") == (
        tmp_path
        / "parquet"
        / "facts_incremental"
        / "partition_key=2026-06-28"
        / "data.parquet"
    )
    assert storage.financial_metrics_path() == (
        tmp_path
        / "parquet"
        / "financial_metrics"
        / "data.parquet"
    )
    assert storage.financial_metrics_usd_path() == (
        tmp_path
        / "parquet"
        / "financial_metrics_usd"
        / "data.parquet"
    )


def test_finland_xbrl_backfill_and_incremental_partitions() -> None:
    graph = load_project_defs().get_repository_def().asset_graph

    for key in (
        "finland_xbrl_financial_reports_backfill",
        "finland_xbrl_raw_xml_documents_backfill",
        "finland_xbrl_parse_backfill",
    ):
        node = graph.get(AssetKey(key))
        assert type(node.partitions_def).__name__ == "MonthlyPartitionsDefinition"
        partition_keys = node.partitions_def.get_partition_keys(
            current_time=datetime(2026, 6, 22)
        )
        assert partition_keys[0] == "2025-06-01"
        assert "2026-05-01" in partition_keys
        assert "2026-06-01" not in partition_keys

    for key in (
        "finland_xbrl_financial_reports_incremental",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_incremental",
    ):
        node = graph.get(AssetKey(key))
        assert type(node.partitions_def).__name__ == "DailyPartitionsDefinition"
        partition_keys = node.partitions_def.get_partition_keys(
            current_time=datetime(2026, 6, 22)
        )
        assert partition_keys[0] == "2026-06-01"


def test_finland_xbrl_jobs_and_incremental_schedule_registered() -> None:
    repo = load_project_defs().get_repository_def()

    with pytest.raises(Exception):
        repo.get_job("finland_xbrl_reference_refresh_job")

    historical_backfill = {
        key.path[-1]
        for key in repo.get_job(
            "finland_xbrl_historical_backfill_job"
        ).asset_layer.executable_asset_keys
    }
    assert historical_backfill == {
        "finland_xbrl_financial_reports_backfill",
        "finland_xbrl_raw_xml_documents_backfill",
        "finland_xbrl_parse_backfill",
    }
    assert type(
        repo.get_job("finland_xbrl_historical_backfill_job").partitions_def
    ).__name__ == "MonthlyPartitionsDefinition"
    with pytest.raises(Exception):
        repo.get_job("finland_xbrl_backfill_job")

    incremental = {
        key.path[-1]
        for key in repo.get_job("finland_xbrl_incremental_job").asset_layer.executable_asset_keys
    }
    assert incremental == {
        "data_daily",
        "data_daily_duckdb",
        "data_daily_duckdb_ch",
        "data_daily_xml",
        "data_daily_xml_duckdb",
        "finland_xbrl_financial_reports_incremental",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_incremental",
    }
    assert type(repo.get_job("finland_xbrl_incremental_job").partitions_def).__name__ == (
        "DailyPartitionsDefinition"
    )

    schedule = repo.get_schedule_def("finland_xbrl_incremental_schedule")
    assert schedule.cron_schedule == "0 6 * * *"

    data_snapshot = {
        key.path[-1]
        for key in repo.get_job("finland_xbrl_data_snapshot_job").asset_layer.executable_asset_keys
    }
    assert data_snapshot == {
        "data_snapshot",
        "data_snapshot_duckdb",
        "data_snapshot_duckdb_ch",
    }
    assert repo.get_job("finland_xbrl_data_snapshot_job").partitions_def is None

    xml_snapshot = {
        key.path[-1]
        for key in repo.get_job("finland_xbrl_xml_snapshot_job").asset_layer.executable_asset_keys
    }
    assert xml_snapshot == {"data_snapshot_xml", "data_snapshot_xml_duckdb"}
    assert type(repo.get_job("finland_xbrl_xml_snapshot_job").partitions_def).__name__ == (
        "MonthlyPartitionsDefinition"
    )

    publish = {
        key.path[-1]
        for key in repo.get_job("finland_xbrl_publish_job").asset_layer.executable_asset_keys
    }
    assert publish == {
        "fi_prh_xbrl_financial_metrics",
        "fi_prh_xbrl_financial_metrics_usd",
        "finland_xbrl_financial_metrics_clickhouse",
    }
    assert repo.get_job("finland_xbrl_publish_job").partitions_def is None


def test_financial_reports_write_separate_partition_parquet_files(tmp_path: Path) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
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

    storage.write_financial_reports_backfill(
        "2024-03-01",
        list(
            XbrlApiResource(session=window_one_session).iter_financial_report_rows(
                registered_date_start="2024-03-01",
                registered_date_end="2024-03-31",
                request_delay_seconds=0,
                run_id="r1",
                sleep=lambda _: None,
            )
        ),
    )
    storage.write_financial_reports_backfill(
        "2024-04-01",
        list(
            XbrlApiResource(session=window_two_session).iter_financial_report_rows(
                registered_date_start="2024-04-01",
                registered_date_end="2024-04-30",
                request_delay_seconds=0,
                run_id="r2",
                sleep=lambda _: None,
            )
        ),
    )

    assert storage.read_financial_reports_backfill("2024-03-01")[0]["business_id"] == "a"
    assert storage.read_financial_reports_backfill("2024-04-01")[0]["business_id"] == "b"
    assert storage.financial_reports_backfill_path("2024-03-01").exists()
    assert storage.financial_reports_backfill_path("2024-04-01").exists()


def test_xbrl_raw_config_defaults() -> None:
    config = XbrlRawConfig()

    assert config.refresh_existing is False
    assert config.download_delay_seconds == 1.0


def test_xbrl_transforms_are_python_assets():
    graph = load_project_defs().get_repository_def().asset_graph
    keys = {k.path[-1] for k in graph.get_all_asset_keys()}
    assert "fi_prh_xbrl_financial_metrics" in keys
    assert "fi_prh_xbrl_financial_metrics_usd" in keys
    assert "finland_xbrl_eligible_companies" not in keys
    assert "finland_xbrl_eligible_financial_reports" not in keys
    assert "xbrl_metric_map" not in keys
    assert "finland_xbrl_dbt_assets" not in xbrl_assets.__dict__
    deps = graph.get(AssetKey(["finland_xbrl_raw_xml_documents"])).parent_keys
    assert AssetKey(["finland_xbrl_raw_xml_documents_backfill"]) in deps
    assert AssetKey(["finland_xbrl_raw_xml_documents_incremental"]) in deps


def test_xbrl_parsed_config_has_no_manifest_override() -> None:
    assert XbrlParsedConfig()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"documents_key": 1},
        {"documents_key": "raw/fi_prh_xbrl_xml_documents.parquet"},
        {"listing_key": "windows/start=2026-01-01/end=2026-01-31/listing.json"},
        {"registered_date_start": "2026-01-01"},
        {"registered_date_end": "2026-01-31"},
    ],
)
def test_xbrl_parsed_config_rejects_non_manifest_config(kwargs: dict) -> None:
    with pytest.raises(ValidationError):
        XbrlParsedConfig(**kwargs)


def test_xbrl_asset_graph_does_not_model_ytj_eligibility() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert dg.AssetKey("fi_prhytj_statuses") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("fi_prhytj_websites") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_eligible_companies") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_financial_reports") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_financial_reports_duckdb") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_company_seed_duckdb") not in asset_graph.get_all_asset_keys()


def test_xbrl_financial_reports_are_modeled_as_partitioned_writer_assets() -> None:
    assert "finland_xbrl_financial_reports_source" not in xbrl_assets.__dict__
    assert "_financial_reports_resource" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_pipeline" not in xbrl_assets.__dict__
    assert "run_finland_xbrl_financial_reports_dlt_pipeline" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_backfill_duckdb" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_incremental_duckdb" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_duckdb" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports" not in xbrl_assets.__dict__
    graph = load_project_defs().get_repository_def().asset_graph
    assert AssetKey("finland_xbrl_financial_reports_backfill") in graph.get_all_asset_keys()
    assert AssetKey("finland_xbrl_financial_reports_incremental") in graph.get_all_asset_keys()
    assert AssetKey("finland_xbrl_financial_reports") not in graph.get_all_asset_keys()


def test_xbrl_api_resource_pages_financial_report_listing() -> None:
    session = FakePagedFinancialReportsSession()
    sleeps: list[float] = []
    log_messages: list[str] = []
    api = XbrlApiResource(session=session)

    listings = list(
        api.iter_financial_reports(
            registered_date_start="2026-06-01",
            registered_date_end="2026-06-30",
            request_delay_seconds=0.5,
            sleep=sleeps.append,
            log_info=log_messages.append,
        )
    )

    assert [listing.financial["businessId"] for listing in listings] == [
        "active-web",
        "second-active-web",
    ]
    assert listings[0].source_page_number == 1
    assert listings[0].source_page_record_number == 1
    assert listings[0].source_record_number == 1
    assert listings[1].source_page_number == 2
    assert listings[1].source_page_record_number == 1
    assert listings[1].source_record_number == 2
    assert [call[1]["page"] for call in session.calls] == [1, 2, 3]
    assert session.calls[0][1]["registeredDateStart"] == "2026-06-01"
    assert session.calls[0][1]["registeredDateEnd"] == "2026-06-30"
    assert sleeps == [0.5, 0.5]
    assert log_messages == [
        "PRH XBRL financial reports discovery 2026-06-01..2026-06-30 started",
        "PRH XBRL financial reports discovery 2026-06-01..2026-06-30 page 1 returned 1 reports",
        "PRH XBRL financial reports discovery 2026-06-01..2026-06-30 page 2 returned 1 reports",
        "PRH XBRL financial reports discovery 2026-06-01..2026-06-30 page 3 returned 0 reports; stopping",
        "PRH XBRL financial reports discovery 2026-06-01..2026-06-30 completed: 2 reports across 2 non-empty pages",
    ]


def test_xbrl_api_resource_produces_financial_report_rows() -> None:
    session = FakePagedFinancialReportsSession()
    api = XbrlApiResource(session=session)

    rows = list(
        api.iter_financial_report_rows(
            registered_date_start="2026-06-01",
            registered_date_end="2026-06-30",
            request_delay_seconds=0.5,
            run_id="test-run",
            sleep=lambda _: None,
        )
    )

    assert [row["business_id"] for row in rows] == ["active-web", "second-active-web"]
    assert rows[0]["financial_date"] == "2026-05-31"
    assert rows[0]["registration_date"] == "2026-06-01"
    assert rows[0]["source_run_id"] == "test-run"
    assert rows[0]["source_page_number"] == 1
    assert rows[1]["source_page_number"] == 2
    assert [call[1]["page"] for call in session.calls] == [1, 2, 3]


def test_financial_data_snapshot_csv_uses_api_columns_only() -> None:
    csv_body = build_financial_data_snapshot_csv(
        [
            {
                "businessId": "0100123-2",
                "financialDate": "2024-05-31",
                "registrationDate": "2024-08-17",
                "ignored": "not exported",
            }
        ]
    )

    assert csv_body == (
        "businessId,financialDate,registrationDate\r\n"
        "0100123-2,2024-05-31,2024-08-17\r\n"
    )


def test_financial_data_snapshot_writes_fixed_initial_listing_to_s3() -> None:
    session = FakePagedFinancialReportsSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()

    result = write_financial_data_snapshot_csv(
        xbrl_api=api,
        object_store=object_store,
        request_delay_seconds=0,
        sleep=lambda _: None,
    )

    assert result.metadata["downloaded"] is True
    assert result.metadata["row_count"] == 2
    assert result.metadata["registered_date_start"] == "2023-07-01"
    assert result.metadata["registered_date_end"] == "2026-06-01"
    assert result.metadata["s3_key"] == FINANCIAL_DATA_S3_SNAPSHOT_KEY
    assert FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START == "2023-07-01"
    assert FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END == "2026-06-01"
    assert [call[1]["registeredDateStart"] for call in session.calls] == [
        "2023-07-01",
        "2023-07-01",
        "2023-07-01",
    ]
    assert [call[1]["registeredDateEnd"] for call in session.calls] == [
        "2026-06-01",
        "2026-06-01",
        "2026-06-01",
    ]
    csv_body = s3_client.objects[
        ("source-finland-prh-xbrl", FINANCIAL_DATA_S3_SNAPSHOT_KEY)
    ].decode("utf-8")
    assert csv_body == (
        "businessId,financialDate,registrationDate\r\n"
        "active-web,2026-05-31,2026-06-01\r\n"
        "second-active-web,2026-04-30,2026-06-02\r\n"
    )


def test_financial_data_snapshot_reuses_existing_s3_csv_without_api_calls() -> None:
    session = FakePagedFinancialReportsSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()
    s3_client.objects[
        ("source-finland-prh-xbrl", FINANCIAL_DATA_S3_SNAPSHOT_KEY)
    ] = b"businessId,financialDate,registrationDate\r\n"

    result = write_financial_data_snapshot_csv(
        xbrl_api=api,
        object_store=object_store,
        request_delay_seconds=0,
        sleep=lambda _: None,
    )

    assert result.metadata["downloaded"] is False
    assert result.metadata["reused_existing_snapshot"] is True
    assert session.calls == []


def test_financial_data_daily_key_uses_one_day_registration_window() -> None:
    assert FINANCIAL_DATA_DAILY_KEY_PREFIX == "financial_data/daily"
    assert financial_data_daily_key("2026-06-01") == (
        "financial_data/daily/"
        "registeredDateStart=2026-06-01/"
        "registeredDateEnd=2026-06-01/"
        "financial_statements.csv"
    )


def test_financial_data_daily_writes_partition_listing_to_s3() -> None:
    session = FakePagedFinancialReportsSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()

    result = write_financial_data_daily_csv(
        partition_key="2026-06-01",
        xbrl_api=api,
        object_store=object_store,
        request_delay_seconds=0,
        sleep=lambda _: None,
    )

    s3_key = financial_data_daily_key("2026-06-01")
    assert result.metadata["downloaded"] is True
    assert result.metadata["row_count"] == 2
    assert result.metadata["registered_date_start"] == "2026-06-01"
    assert result.metadata["registered_date_end"] == "2026-06-01"
    assert result.metadata["s3_key"] == s3_key
    assert [call[1]["registeredDateStart"] for call in session.calls] == [
        "2026-06-01",
        "2026-06-01",
        "2026-06-01",
    ]
    assert [call[1]["registeredDateEnd"] for call in session.calls] == [
        "2026-06-01",
        "2026-06-01",
        "2026-06-01",
    ]
    assert s3_client.objects[("source-finland-prh-xbrl", s3_key)].decode("utf-8") == (
        "businessId,financialDate,registrationDate\r\n"
        "active-web,2026-05-31,2026-06-01\r\n"
        "second-active-web,2026-04-30,2026-06-02\r\n"
    )


def test_financial_data_daily_reuses_existing_s3_csv_without_api_calls() -> None:
    session = FakePagedFinancialReportsSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()
    s3_key = financial_data_daily_key("2026-06-01")
    s3_client.objects[
        ("source-finland-prh-xbrl", s3_key)
    ] = b"businessId,financialDate,registrationDate\r\n"

    result = write_financial_data_daily_csv(
        partition_key="2026-06-01",
        xbrl_api=api,
        object_store=object_store,
        request_delay_seconds=0,
        sleep=lambda _: None,
    )

    assert result.metadata["downloaded"] is False
    assert result.metadata["reused_existing_snapshot"] is True
    assert result.metadata["registered_date_start"] == "2026-06-01"
    assert result.metadata["registered_date_end"] == "2026-06-01"
    assert session.calls == []


def test_data_snapshot_asset_is_registered() -> None:
    graph = load_project_defs().get_repository_def().asset_graph
    node = graph.get(AssetKey("data_snapshot"))

    assert node.group_name == "finland_xbrl"
    assert node.description
    assert "fixed initial PRH XBRL financial statement listing" in node.description


def test_data_daily_asset_is_daily_partitioned_from_june_2026() -> None:
    graph = load_project_defs().get_repository_def().asset_graph
    node = graph.get(AssetKey("data_daily"))

    assert node.group_name == "finland_xbrl"
    assert type(node.partitions_def).__name__ == "DailyPartitionsDefinition"
    assert node.partitions_def.get_partition_keys(current_time=datetime(2026, 6, 22))[0] == (
        "2026-06-01"
    )
    assert node.description
    assert "daily PRH XBRL financial statement listing" in node.description


def test_data_daily_duckdb_writes_s3_partition_csv_to_duckdb(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    s3_client.objects[
        ("source-finland-prh-xbrl", financial_data_daily_key("2026-06-01"))
    ] = (
        "businessId,financialDate,registrationDate\r\n"
        "0100123-2,2024-05-31,2024-08-17\r\n"
        "0202020-2,2025-12-31,2026-02-01\r\n"
    ).encode("utf-8")
    duckdb_path = tmp_path / "daily.duckdb"

    result = materialize_data_daily_duckdb(
        partition_key="2026-06-01",
        object_store=object_store,
        daily_duckdb=duckdb_resource(duckdb_path),
    )

    assert result.metadata["row_count"] == 2
    assert result.metadata["partition"] == "2026-06-01"
    assert result.metadata["duckdb_schema"] == FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA
    assert result.metadata["duckdb_table"] == FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        columns = connection.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_schema = ?
              and table_name = ?
            order by ordinal_position
            """,
            [
                FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
                FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE,
            ],
        ).fetchall()
        rows = connection.execute(
            f"""
            select partition_key, "businessId", "financialDate", "registrationDate"
            from {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE}
            order by "businessId"
            """
        ).fetchall()

    assert columns == [
        ("partition_key", "VARCHAR"),
        ("businessId", "VARCHAR"),
        ("financialDate", "VARCHAR"),
        ("registrationDate", "VARCHAR"),
    ]
    assert rows == [
        ("2026-06-01", "0100123-2", "2024-05-31", "2024-08-17"),
        ("2026-06-01", "0202020-2", "2025-12-31", "2026-02-01"),
    ]


def test_data_daily_duckdb_replaces_only_current_partition(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    duckdb_path = tmp_path / "daily.duckdb"
    s3_client.objects[
        ("source-finland-prh-xbrl", financial_data_daily_key("2026-06-01"))
    ] = (
        "businessId,financialDate,registrationDate\r\n"
        "old-row,2024-05-31,2024-08-17\r\n"
    ).encode("utf-8")
    s3_client.objects[
        ("source-finland-prh-xbrl", financial_data_daily_key("2026-06-02"))
    ] = (
        "businessId,financialDate,registrationDate\r\n"
        "other-partition,2025-12-31,2026-02-01\r\n"
    ).encode("utf-8")

    materialize_data_daily_duckdb(
        partition_key="2026-06-01",
        object_store=object_store,
        daily_duckdb=duckdb_resource(duckdb_path),
    )
    materialize_data_daily_duckdb(
        partition_key="2026-06-02",
        object_store=object_store,
        daily_duckdb=duckdb_resource(duckdb_path),
    )
    s3_client.objects[
        ("source-finland-prh-xbrl", financial_data_daily_key("2026-06-01"))
    ] = (
        "businessId,financialDate,registrationDate\r\n"
        "new-row,2024-05-31,2024-08-18\r\n"
    ).encode("utf-8")

    materialize_data_daily_duckdb(
        partition_key="2026-06-01",
        object_store=object_store,
        daily_duckdb=duckdb_resource(duckdb_path),
    )

    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            select partition_key, "businessId", "registrationDate"
            from {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE}
            order by partition_key, "businessId"
            """
        ).fetchall()

    assert rows == [
        ("2026-06-01", "new-row", "2024-08-18"),
        ("2026-06-02", "other-partition", "2026-02-01"),
    ]


def test_data_daily_duckdb_ch_inserts_current_partition_into_clickhouse(
    tmp_path: Path,
) -> None:
    duckdb_path = tmp_path / "daily.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute(f"create schema {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}")
        connection.execute(
            f"""
            create table
              {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE} (
                partition_key varchar,
                "businessId" varchar,
                "financialDate" varchar,
                "registrationDate" varchar
              )
            """
        )
        connection.executemany(
            f"""
            insert into {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE}
              (partition_key, "businessId", "financialDate", "registrationDate")
            values (?, ?, ?, ?)
            """,
            [
                ("2026-06-01", "0100123-2", "2024-05-31", "2024-08-17"),
                ("2026-06-02", "not-exported", "2025-12-31", "2026-02-01"),
            ],
        )
    client = FakeClickHouseClient(tables={"fi_xbrl_financial_statement_listings"})

    row_count = export_data_daily_duckdb_to_clickhouse(
        partition_key="2026-06-01",
        daily_duckdb=duckdb_resource(duckdb_path),
        clickhouse=FakeClickHouseResource(client),
    )

    assert row_count == 1
    assert not any("EXCHANGE TABLES" in statement for statement in client.statements)
    insert_calls = [
        (sql, params)
        for sql, params in client.execute_calls
        if sql.strip().startswith("INSERT INTO")
    ]
    assert len(insert_calls) == 1
    insert_sql, insert_rows = insert_calls[0]
    assert "fi_xbrl_financial_statement_listings" in insert_sql
    assert isinstance(insert_rows, list)
    assert insert_rows == [
        ("0100123-2", date(2024, 5, 31), date(2024, 8, 17)),
    ]


def test_data_daily_duckdb_assets_are_partitioned_and_chained() -> None:
    graph = load_project_defs().get_repository_def().asset_graph
    duckdb_node = graph.get(AssetKey("data_daily_duckdb"))
    clickhouse_node = graph.get(AssetKey("data_daily_duckdb_ch"))
    xml_node = graph.get(AssetKey("data_daily_xml"))
    xml_duckdb_node = graph.get(AssetKey("data_daily_xml_duckdb"))

    assert duckdb_node.group_name == "finland_xbrl"
    assert clickhouse_node.group_name == "finland_xbrl"
    assert xml_node.group_name == "finland_xbrl"
    assert xml_duckdb_node.group_name == "finland_xbrl"
    assert type(duckdb_node.partitions_def).__name__ == "DailyPartitionsDefinition"
    assert type(clickhouse_node.partitions_def).__name__ == "DailyPartitionsDefinition"
    assert type(xml_node.partitions_def).__name__ == "DailyPartitionsDefinition"
    assert type(xml_duckdb_node.partitions_def).__name__ == "DailyPartitionsDefinition"
    assert duckdb_node.parent_keys == {AssetKey("data_daily")}
    assert clickhouse_node.parent_keys == {AssetKey("data_daily_duckdb")}
    assert xml_node.parent_keys == {AssetKey("data_daily_duckdb_ch")}
    assert xml_duckdb_node.parent_keys == {AssetKey("data_daily_xml")}
    assert FINLAND_XBRL_FINANCIAL_DATA_DAILY_DUCKDB_PATH == (
        "data/finland_xbrl/financial_data_daily.duckdb"
    )


def test_xml_snapshot_monthly_partitions_cover_prh_start_until_june_2026() -> None:
    partition_keys = XML_SNAPSHOT_PARTITIONS.get_partition_keys(
        current_time=datetime(2026, 7, 1)
    )

    assert partition_keys[0] == "2023-07-01"
    assert "2026-05-01" in partition_keys
    assert "2026-06-01" not in partition_keys


def test_data_snapshot_xml_duckdb_uses_xml_snapshot_partitions() -> None:
    graph = load_project_defs().get_repository_def().asset_graph
    node = graph.get(AssetKey("data_snapshot_xml_duckdb"))

    assert node.partitions_def is XML_SNAPSHOT_PARTITIONS


def test_xml_snapshot_s3_keys_are_scoped_by_registration_window() -> None:
    assert xml_snapshot_partition_prefix("2023-07-01", "2023-07-31") == (
        "financial_data/xml_snapshot/"
        "registeredDateStart=2023-07-01/"
        "registeredDateEnd=2023-07-31"
    )
    assert xml_snapshot_document_key(
        "2023-07-01",
        "2023-07-31",
        "0100123-2",
        "2022-12-31",
    ) == (
        "financial_data/xml_snapshot/"
        "registeredDateStart=2023-07-01/"
        "registeredDateEnd=2023-07-31/"
        "companies/0100123-2/2022-12-31.xml"
    )
    assert xml_snapshot_manifest_key("2023-07-01", "2023-07-31") == (
        "financial_data/xml_snapshot/"
        "registeredDateStart=2023-07-01/"
        "registeredDateEnd=2023-07-31/"
        "manifest.jsonl"
    )
    assert xml_snapshot_success_key("2023-07-01", "2023-07-31") == (
        "financial_data/xml_snapshot/"
        "registeredDateStart=2023-07-01/"
        "registeredDateEnd=2023-07-31/"
        "_SUCCESS.json"
    )


def test_xml_snapshot_parse_duckdb_path_helpers() -> None:
    assert str(FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH) == (
        "data/finland_xbrl/duckdb/xml_snapshot_parse"
    )
    assert xml_snapshot_parse_duckdb_path("2023-07-01") == (
        FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH
        / "partition_key=2023-07-01"
        / "data.duckdb"
    )
    assert xml_snapshot_parse_temp_dir("2023-07-01") == (
        Path("data/finland_xbrl/tmp/xml_snapshot_parse")
        / "partition_key=2023-07-01"
    )


def test_fetch_xml_snapshot_report_rows_reads_clickhouse_listing_table() -> None:
    client = FakeClickHouseClient(
        tables={"fi_xbrl_financial_statement_listings"},
        financial_statement_listing_rows=[
            ("0100123-2", "2022-12-31", "2023-07-02"),
            ("0202020-2", "2023-03-31", "2023-07-03"),
        ],
    )

    rows = fetch_xml_snapshot_report_rows(
        clickhouse=FakeClickHouseResource(client),
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
    )

    assert rows == [
        {
            "business_id": "0100123-2",
            "financial_date": "2022-12-31",
            "registration_date": "2023-07-02",
        },
        {
            "business_id": "0202020-2",
            "financial_date": "2023-03-31",
            "registration_date": "2023-07-03",
        },
    ]
    query_sql, query_params = client.execute_calls[-1]
    assert "fi_xbrl_financial_statement_listings" in query_sql
    assert "toDateOrNull(toString(registration_date)) AS registered_on" in query_sql
    assert "WHERE registered_on >= toDate(%(start)s)" in query_sql
    assert "registration_date >= toDate(%(start)s)" not in query_sql
    assert query_params == {
        "start": "2023-07-01",
        "end": "2023-07-31",
    }


def test_xml_snapshot_parse_requires_success_marker(tmp_path: Path) -> None:
    object_store, _s3_client = _object_store()

    with pytest.raises(FileNotFoundError, match="_SUCCESS.json"):
        materialize_data_snapshot_xml_duckdb(
            partition_key="2023-07-01",
            registered_date_start="2023-07-01",
            registered_date_end="2023-07-31",
            object_store=object_store,
            duckdb_path=tmp_path / "data.duckdb",
            temp_dir=tmp_path / "tmp",
            run_id="run-1",
        )

    assert not (tmp_path / "data.duckdb").exists()


def test_xml_snapshot_parse_requires_manifest(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    s3_client.objects[
        ("source-finland-prh-xbrl", xml_snapshot_success_key("2023-07-01", "2023-07-31"))
    ] = b"{}"

    with pytest.raises(FileNotFoundError, match="manifest.jsonl"):
        materialize_data_snapshot_xml_duckdb(
            partition_key="2023-07-01",
            registered_date_start="2023-07-01",
            registered_date_end="2023-07-31",
            object_store=object_store,
            duckdb_path=tmp_path / "data.duckdb",
            temp_dir=tmp_path / "tmp",
            run_id="run-1",
        )

    assert not (tmp_path / "data.duckdb").exists()


def test_read_xml_snapshot_manifest_rows_validates_required_fields() -> None:
    object_store, s3_client = _object_store()
    manifest_key = xml_snapshot_manifest_key("2023-07-01", "2023-07-31")
    s3_client.objects[
        ("source-finland-prh-xbrl", manifest_key)
    ] = b'{"business_id":"0100123-2"}\n'

    with pytest.raises(ValueError, match="financial_date"):
        read_xml_snapshot_manifest_rows(
            object_store=object_store,
            manifest_key=manifest_key,
        )


def test_xml_snapshot_parse_empty_manifest_creates_empty_duckdb(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2023-07-01",
        end="2023-07-31",
        rows=[],
    )

    result = materialize_data_snapshot_xml_duckdb(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        object_store=object_store,
        duckdb_path=tmp_path / "data.duckdb",
        temp_dir=tmp_path / "tmp",
        run_id="run-1",
    )

    with duckdb.connect(str(tmp_path / "data.duckdb"), read_only=True) as connection:
        assert connection.execute("select count(*) from statement_documents").fetchone()[0] == 0
        assert connection.execute("select count(*) from facts").fetchone()[0] == 0
    assert result.metadata["documents_in_manifest"] == 0
    assert result.metadata["statement_documents_row_count"] == 0
    assert result.metadata["facts_row_count"] == 0


def test_xml_snapshot_parse_writes_partition_duckdb_and_removes_temp_parquet(
    tmp_path: Path,
) -> None:
    object_store, s3_client = _object_store()
    xml_key = xml_snapshot_document_key(
        "2023-07-01",
        "2023-07-31",
        "0100123-2",
        "2022-12-31",
    )
    s3_client.objects[("source-finland-prh-xbrl", xml_key)] = b"<xbrl />"
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2023-07-01",
        end="2023-07-31",
        rows=[
            {
                "business_id": "0100123-2",
                "financial_date": "2022-12-31",
                "registration_date": "2023-07-02",
                "source_url": "https://example.test/financial",
                "xml_object_key": xml_key,
            }
        ],
    )
    parser = RecordingStatementParser()
    temp_dir = tmp_path / "tmp"

    result = materialize_data_snapshot_xml_duckdb(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        object_store=object_store,
        duckdb_path=tmp_path / "data.duckdb",
        temp_dir=temp_dir,
        run_id="run-1",
        parser=parser,
    )

    assert len(parser.calls) == 1
    assert parser.calls[0]["business_id"] == "0100123-2"
    assert parser.calls[0]["body"] == b"<xbrl />"
    with duckdb.connect(str(tmp_path / "data.duckdb"), read_only=True) as connection:
        assert connection.execute("select business_id from statement_documents").fetchall() == [
            ("0100123-2",)
        ]
        assert connection.execute("select business_id from facts").fetchall() == [
            ("0100123-2",)
        ]
    assert result.metadata["documents_parsed_this_run"] == 1
    assert result.metadata["documents_failed_this_run"] == 0
    assert result.metadata["statement_documents_row_count"] == 1
    assert result.metadata["facts_row_count"] == 1
    assert result.metadata["temporary_directory_removed"] is True
    assert not temp_dir.exists()


def test_xml_snapshot_parse_missing_xml_object_fails_partition(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    xml_key = xml_snapshot_document_key(
        "2023-07-01",
        "2023-07-31",
        "0100123-2",
        "2022-12-31",
    )
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2023-07-01",
        end="2023-07-31",
        rows=[
            {
                "business_id": "0100123-2",
                "financial_date": "2022-12-31",
                "registration_date": "2023-07-02",
                "source_url": "https://example.test/financial",
                "xml_object_key": xml_key,
            }
        ],
    )

    with pytest.raises(KeyError):
        materialize_data_snapshot_xml_duckdb(
            partition_key="2023-07-01",
            registered_date_start="2023-07-01",
            registered_date_end="2023-07-31",
            object_store=object_store,
            duckdb_path=tmp_path / "data.duckdb",
            temp_dir=tmp_path / "tmp",
            run_id="run-1",
            parser=RecordingStatementParser(),
        )

    assert not (tmp_path / "data.duckdb").exists()


def test_xml_snapshot_parse_records_parser_failures_and_continues(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    good_key = xml_snapshot_document_key(
        "2023-07-01",
        "2023-07-31",
        "0100123-2",
        "2022-12-31",
    )
    bad_key = xml_snapshot_document_key(
        "2023-07-01",
        "2023-07-31",
        "0202020-2",
        "2022-12-31",
    )
    s3_client.objects[("source-finland-prh-xbrl", good_key)] = b"<xbrl />"
    s3_client.objects[("source-finland-prh-xbrl", bad_key)] = b"<xbrl />"
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2023-07-01",
        end="2023-07-31",
        rows=[
            {
                "business_id": "0202020-2",
                "financial_date": "2022-12-31",
                "registration_date": "2023-07-02",
                "source_url": "https://example.test/bad",
                "xml_object_key": bad_key,
            },
            {
                "business_id": "0100123-2",
                "financial_date": "2022-12-31",
                "registration_date": "2023-07-02",
                "source_url": "https://example.test/good",
                "xml_object_key": good_key,
            },
        ],
    )

    result = materialize_data_snapshot_xml_duckdb(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        object_store=object_store,
        duckdb_path=tmp_path / "data.duckdb",
        temp_dir=tmp_path / "tmp",
        run_id="run-1",
        parser=RecordingStatementParser(fail_business_id="0202020-2"),
    )

    with duckdb.connect(str(tmp_path / "data.duckdb"), read_only=True) as connection:
        assert connection.execute("select business_id from statement_documents").fetchall() == [
            ("0100123-2",)
        ]
    assert result.metadata["documents_in_manifest"] == 2
    assert result.metadata["documents_parsed_this_run"] == 1
    assert result.metadata["documents_failed_this_run"] == 1


def test_xml_snapshot_existing_success_marker_skips_clickhouse_and_prh() -> None:
    object_store, s3_client = _object_store()
    success_key = xml_snapshot_success_key("2023-07-01", "2023-07-31")
    s3_client.objects[("source-finland-prh-xbrl", success_key)] = b"{}"
    session = FakeHttpSession()
    client = FakeClickHouseClient(tables={"fi_xbrl_financial_statement_listings"})

    result = download_finland_xbrl_snapshot_xml_partition(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        xbrl_api=XbrlApiResource(session=session),
        clickhouse=FakeClickHouseResource(client),
        object_store=object_store,
        download_delay_seconds=0,
        sleep=lambda _: None,
    )

    assert result.metadata["skipped_existing_partition"] is True
    assert result.metadata["selected_reports_count"] == 0
    assert session.calls == []
    assert client.execute_calls == []


def test_xml_snapshot_downloads_missing_xml_and_writes_manifest_and_success() -> None:
    object_store, s3_client = _object_store()
    session = FakeHttpSession()
    client = FakeClickHouseClient(
        tables={"fi_xbrl_financial_statement_listings"},
        financial_statement_listing_rows=[
            ("0100123-2", "2022-12-31", "2023-07-02"),
            ("0202020-2", "2023-03-31", "2023-07-03"),
        ],
    )

    result = download_finland_xbrl_snapshot_xml_partition(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        xbrl_api=XbrlApiResource(session=session),
        clickhouse=FakeClickHouseResource(client),
        object_store=object_store,
        download_delay_seconds=0,
        sleep=lambda _: None,
    )

    first_xml_key = xml_snapshot_document_key(
        "2023-07-01", "2023-07-31", "0100123-2", "2022-12-31"
    )
    second_xml_key = xml_snapshot_document_key(
        "2023-07-01", "2023-07-31", "0202020-2", "2023-03-31"
    )
    manifest_key = xml_snapshot_manifest_key("2023-07-01", "2023-07-31")
    success_key = xml_snapshot_success_key("2023-07-01", "2023-07-31")

    assert result.metadata["skipped_existing_partition"] is False
    assert result.metadata["selected_reports_count"] == 2
    assert result.metadata["downloaded_count"] == 2
    assert result.metadata["reused_count"] == 0
    assert s3_client.objects[("source-finland-prh-xbrl", first_xml_key)] == (
        b"<xbrl>0100123-2:2022-12-31</xbrl>"
    )
    assert s3_client.objects[("source-finland-prh-xbrl", second_xml_key)] == (
        b"<xbrl>0202020-2:2023-03-31</xbrl>"
    )
    manifest_rows = [
        json.loads(line)
        for line in s3_client.objects[
            ("source-finland-prh-xbrl", manifest_key)
        ].decode("utf-8").splitlines()
    ]
    assert [row["xml_object_key"] for row in manifest_rows] == [first_xml_key, second_xml_key]
    assert [row["downloaded"] for row in manifest_rows] == [True, True]
    assert [row["reused"] for row in manifest_rows] == [False, False]
    success = json.loads(
        s3_client.objects[("source-finland-prh-xbrl", success_key)].decode("utf-8")
    )
    assert success["selected_reports_count"] == 2
    assert success["downloaded_count"] == 2


def test_data_daily_xml_downloads_daily_listing_window() -> None:
    object_store, s3_client = _object_store()
    session = FakeHttpSession()
    client = FakeClickHouseClient(
        tables={"fi_xbrl_financial_statement_listings"},
        financial_statement_listing_rows=[
            ("0100123-2", "2022-12-31", "2026-06-01"),
        ],
    )

    result = materialize_data_daily_xml(
        partition_key="2026-06-01",
        xbrl_api=XbrlApiResource(session=session),
        clickhouse=FakeClickHouseResource(client),
        object_store=object_store,
        download_delay_seconds=0,
        sleep=lambda _: None,
    )

    xml_key = xml_snapshot_document_key(
        "2026-06-01",
        "2026-06-01",
        "0100123-2",
        "2022-12-31",
    )
    assert ("source-finland-prh-xbrl", xml_key) in s3_client.objects
    assert result.metadata["partition"] == "2026-06-01"
    assert result.metadata["registered_date_start"] == "2026-06-01"
    assert result.metadata["registered_date_end"] == "2026-06-01"
    assert result.metadata["selected_reports_count"] == 1
    assert result.metadata["manifest_key"] == xml_snapshot_manifest_key(
        "2026-06-01", "2026-06-01"
    )


def test_data_daily_xml_duckdb_parses_daily_xml_partition(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    xml_key = xml_snapshot_document_key(
        "2026-06-01",
        "2026-06-01",
        "0100123-2",
        "2022-12-31",
    )
    s3_client.objects[("source-finland-prh-xbrl", xml_key)] = b"<xbrl />"
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2026-06-01",
        end="2026-06-01",
        rows=[
            {
                "business_id": "0100123-2",
                "financial_date": "2022-12-31",
                "registration_date": "2026-06-01",
                "source_url": "https://example.test/financial",
                "xml_object_key": xml_key,
            }
        ],
    )

    result = materialize_data_daily_xml_duckdb(
        partition_key="2026-06-01",
        object_store=object_store,
        duckdb_path=tmp_path / "daily.duckdb",
        temp_dir=tmp_path / "tmp",
        run_id="run-1",
        parser=RecordingStatementParser(),
    )

    with duckdb.connect(str(tmp_path / "daily.duckdb"), read_only=True) as connection:
        assert connection.execute("select business_id from statement_documents").fetchall() == [
            ("0100123-2",)
        ]
    assert result.metadata["registered_date_start"] == "2026-06-01"
    assert result.metadata["registered_date_end"] == "2026-06-01"
    assert result.metadata["documents_parsed_this_run"] == 1


def test_xml_snapshot_reuses_existing_xml_without_success_marker() -> None:
    object_store, s3_client = _object_store()
    existing_xml_key = xml_snapshot_document_key(
        "2023-07-01", "2023-07-31", "0100123-2", "2022-12-31"
    )
    s3_client.objects[("source-finland-prh-xbrl", existing_xml_key)] = b"<xbrl>cached</xbrl>"
    session = FakeHttpSession()
    client = FakeClickHouseClient(
        tables={"fi_xbrl_financial_statement_listings"},
        financial_statement_listing_rows=[
            ("0100123-2", "2022-12-31", "2023-07-02"),
        ],
    )

    result = download_finland_xbrl_snapshot_xml_partition(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        xbrl_api=XbrlApiResource(session=session),
        clickhouse=FakeClickHouseResource(client),
        object_store=object_store,
        download_delay_seconds=0,
        sleep=lambda _: None,
    )

    assert result.metadata["downloaded_count"] == 0
    assert result.metadata["reused_count"] == 1
    assert session.calls == []
    manifest_key = xml_snapshot_manifest_key("2023-07-01", "2023-07-31")
    manifest_row = json.loads(
        s3_client.objects[("source-finland-prh-xbrl", manifest_key)].decode("utf-8").strip()
    )
    assert manifest_row["downloaded"] is False
    assert manifest_row["reused"] is True
    assert manifest_row["xml_sha256"]
    assert manifest_row["xml_size_bytes"] == len(b"<xbrl>cached</xbrl>")


def test_xml_snapshot_failure_does_not_write_success_marker() -> None:
    object_store, s3_client = _object_store()
    client = FakeClickHouseClient(
        tables={"fi_xbrl_financial_statement_listings"},
        financial_statement_listing_rows=[
            ("fail-business", "2022-12-31", "2023-07-02"),
        ],
    )

    with pytest.raises(RuntimeError, match="HTTP 500"):
        download_finland_xbrl_snapshot_xml_partition(
            partition_key="2023-07-01",
            registered_date_start="2023-07-01",
            registered_date_end="2023-07-31",
            xbrl_api=XbrlApiResource(
                session=FailingFinancialXmlSession(status_code=500)
            ),
            clickhouse=FakeClickHouseResource(client),
            object_store=object_store,
            download_delay_seconds=0,
            sleep=lambda _: None,
        )

    assert (
        "source-finland-prh-xbrl",
        xml_snapshot_success_key("2023-07-01", "2023-07-31"),
    ) not in s3_client.objects


def test_data_snapshot_duckdb_writes_s3_csv_to_duckdb(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    s3_client.objects[
        ("source-finland-prh-xbrl", FINANCIAL_DATA_S3_SNAPSHOT_KEY)
    ] = (
        "businessId,financialDate,registrationDate\r\n"
        "0100123-2,2024-05-31,2024-08-17\r\n"
        "0202020-2,2025-12-31,2026-02-01\r\n"
    ).encode("utf-8")
    duckdb_path = tmp_path / "snapshot.duckdb"

    result = materialize_data_snapshot_duckdb(
        object_store=object_store,
        snapshot_duckdb=duckdb_resource(duckdb_path),
    )

    assert result.metadata["row_count"] == 2
    assert result.metadata["duckdb_schema"] == FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA
    assert result.metadata["duckdb_table"] == FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        columns = connection.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_schema = ?
              and table_name = ?
            order by ordinal_position
            """,
            [
                FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
                FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
            ],
        ).fetchall()
        rows = connection.execute(
            f"""
            select "businessId", "financialDate", "registrationDate"
            from {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE}
            order by "businessId"
            """
        ).fetchall()

    assert columns == [
        ("businessId", "VARCHAR"),
        ("financialDate", "VARCHAR"),
        ("registrationDate", "VARCHAR"),
    ]
    assert rows == [
        ("0100123-2", "2024-05-31", "2024-08-17"),
        ("0202020-2", "2025-12-31", "2026-02-01"),
    ]


def test_data_snapshot_duckdb_asset_is_registered_after_s3_snapshot() -> None:
    graph = load_project_defs().get_repository_def().asset_graph
    node = graph.get(AssetKey("data_snapshot_duckdb"))

    assert node.group_name == "finland_xbrl"
    assert node.parent_keys == {AssetKey("data_snapshot")}
    assert FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH == (
        "data/finland_xbrl/financial_data_snapshot.duckdb"
    )


def test_data_snapshot_duckdb_ch_replaces_clickhouse_listing_table(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "snapshot.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute(f"create schema {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}")
        connection.execute(
            f"""
            create table
              {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE} (
                "businessId" varchar,
                "financialDate" varchar,
                "registrationDate" varchar
              )
            """
        )
        connection.executemany(
            f"""
            insert into {FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA}.{FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE}
              ("businessId", "financialDate", "registrationDate")
            values (?, ?, ?)
            """,
            [
                ("0100123-2", "2024-05-31", "2024-08-17"),
                ("0202020-2", "2025-12-31", "2026-02-01"),
            ],
        )
    client = FakeClickHouseClient(tables={"fi_xbrl_financial_statement_listings"})

    row_count = export_data_snapshot_duckdb_to_clickhouse(
        snapshot_duckdb=duckdb_resource(duckdb_path),
        clickhouse=FakeClickHouseResource(client),
    )

    assert row_count == 2
    assert any("CREATE TABLE" in statement for statement in client.statements)
    assert any("EXCHANGE TABLES" in statement for statement in client.statements)
    insert_calls = [
        (sql, params)
        for sql, params in client.execute_calls
        if sql.strip().startswith("INSERT INTO")
    ]
    assert len(insert_calls) == 1
    insert_sql, insert_rows = insert_calls[0]
    assert "_tmp_fi_xbrl_financial_statement_listings_" in insert_sql
    assert isinstance(insert_rows, list)
    assert insert_rows == [
        ("0100123-2", date(2024, 5, 31), date(2024, 8, 17)),
        ("0202020-2", date(2025, 12, 31), date(2026, 2, 1)),
    ]


def test_data_snapshot_duckdb_ch_asset_is_registered_after_duckdb_snapshot() -> None:
    graph = load_project_defs().get_repository_def().asset_graph
    node = graph.get(AssetKey("data_snapshot_duckdb_ch"))

    assert node.group_name == "finland_xbrl"
    assert node.parent_keys == {AssetKey("data_snapshot_duckdb")}


def test_xbrl_api_resource_rejects_registration_start_before_prh_floor() -> None:
    session = FakePagedFinancialReportsSession()
    api = XbrlApiResource(session=session)

    with pytest.raises(ValueError, match="2023-07-01"):
        list(
            api.iter_financial_report_rows(
                registered_date_start="2014-01-01",
                registered_date_end="2014-01-31",
                request_delay_seconds=0,
                run_id="test-run",
                sleep=lambda _: None,
            )
        )

    assert session.calls == []


def test_xbrl_parquet_storage_overwrites_financial_report_partition(tmp_path: Path) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    rows = list(
        XbrlApiResource(session=FakePagedFinancialReportsSession()).iter_financial_report_rows(
            registered_date_start="2026-06-01",
            registered_date_end="2026-06-30",
            request_delay_seconds=0,
            run_id="first-run",
            sleep=lambda _: None,
        )
    )

    storage.write_financial_reports_incremental("2026-06-01", rows)
    storage.write_financial_reports_incremental(
        "2026-06-01",
        [
            {
                **rows[0],
                "source_run_id": "second-run",
                "source_page_number": 9,
            }
        ],
    )

    stored = storage.read_financial_reports_incremental("2026-06-01")

    assert stored == [
        {
            **rows[0],
            "source_run_id": "second-run",
            "source_page_number": 9,
        }
    ]


def test_xbrl_parquet_storage_writes_empty_financial_report_partition(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))

    storage.write_financial_reports_incremental("2026-06-28", [])

    assert storage.read_financial_reports_incremental("2026-06-28") == []


def test_xbrl_parquet_storage_writes_raw_xml_document_partitions(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    rows = [
        {
            "business_id": "active-web",
            "financial_date": "2026-05-31",
            "registration_date": "2026-06-01",
            "source_url": "https://example.test/financial",
            "xml_object_key": "companies/active-web/2026-05-31.xml",
            "xml_sha256": "hash",
            "xml_size_bytes": 34,
            "downloaded": True,
            "reused": False,
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
            "financial_start_date": "",
            "max_reports": "",
            "selected_at": "2026-06-01T00:00:00+00:00",
        }
    ]

    storage.write_raw_xml_documents_incremental("2026-06-01", rows)
    storage.write_raw_xml_documents_backfill("2026-05-01", [])

    assert storage.read_raw_xml_documents_incremental("2026-06-01") == rows
    assert storage.read_raw_xml_documents_backfill("2026-05-01") == []
    assert storage.raw_xml_documents_incremental_row_count() == 1
    assert storage.raw_xml_documents_backfill_row_count() == 0


def test_xbrl_parquet_storage_writes_parsed_output_partitions(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    statement = _statement_document_row("statement-1")
    fact = _fact_row("statement-1", fact_ordinal=1)

    storage.write_statement_documents_incremental("2026-06-01", [statement])
    storage.write_facts_incremental("2026-06-01", [fact])
    storage.write_statement_documents_backfill("2026-05-01", [])
    storage.write_facts_backfill("2026-05-01", [])

    assert storage.read_statement_documents_incremental("2026-06-01") == [statement]
    assert storage.read_facts_incremental("2026-06-01") == [fact]
    assert storage.read_statement_documents_backfill("2026-05-01") == []
    assert storage.read_facts_backfill("2026-05-01") == []
    assert storage.statement_documents_row_count() == 1
    assert storage.facts_row_count() == 1


def test_xbrl_parquet_storage_writes_financial_metrics(tmp_path: Path) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    rows = [_financial_metric_row("statement-1")]

    storage.write_financial_metrics(rows)

    assert storage.read_financial_metrics() == rows
    assert storage.financial_metrics_row_count() == 1


def test_xbrl_parquet_storage_fails_when_required_financial_report_partition_is_missing(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))

    with pytest.raises(
        FileNotFoundError,
        match="financial_reports_incremental/partition_key=2026-06-01/data.parquet",
    ):
        storage.read_financial_reports_incremental("2026-06-01")


def test_xbrl_parquet_storage_fails_when_required_raw_xml_partition_is_missing(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))

    with pytest.raises(
        FileNotFoundError,
        match="raw_xml_documents_incremental/partition_key=2026-06-01/data.parquet",
    ):
        storage.read_raw_xml_documents_incremental("2026-06-01")


def test_xbrl_metric_mapping_is_code_backed() -> None:
    rows = metric_mapping.xbrl_metric_mapping_rows()

    assert {
        "concept_qname": "fi_met:md103",
        "mcy_member_code": "fi_MC:x673",
        "metric_code": "revenue",
    } in rows
    assert len(rows) == len(
        {(row["concept_qname"], row["mcy_member_code"]) for row in rows}
    )
    assert {
        "concept_qname": "fi_met:ii52",
        "mcy_member_code": "",
        "metric_code": "employees",
    } in rows


def test_build_financial_metric_rows_maps_current_numeric_facts() -> None:
    statement = _statement_document_row("statement-1")
    fact = _fact_row("statement-1", fact_ordinal=1)
    employee_fact = {
        **_fact_row("statement-1", fact_ordinal=2),
        "concept_qname": "fi_met:ii52",
        "concept_local_name": "ii52",
        "mcy_member_code": "",
        "numeric_value": "12",
        "raw_value": "12",
    }
    unmapped_fact = {
        **_fact_row("statement-1", fact_ordinal=3),
        "concept_qname": "unknown:metric",
        "mcy_member_code": "unknown:member",
        "numeric_value": "7",
    }
    comparative_fact = {
        **_fact_row("statement-1", fact_ordinal=4),
        "numeric_value": "999",
        "is_comparative": True,
    }

    rows = financial_metrics.build_financial_metric_rows(
        statement_documents=[statement],
        facts=[fact, employee_fact, unmapped_fact, comparative_fact],
        built_at="2026-06-01T00:00:00+00:00",
    )

    assert rows == [
        {
            "statement_key": "statement-1",
            "business_id": "active-web",
            "financial_date": "2026-05-31",
            "registration_date": "2026-06-01",
            "period_start": "2026-01-01",
            "period_end": "2026-05-31",
            "reported_company_name": "Active Oy",
            "source_url": "https://example.test/financial",
            "xml_object_key": "companies/active-web/2026-05-31.xml",
            "xml_sha256": "hash",
            "xml_size_bytes": 8,
            "source_run_id": "test-run",
            "revenue": 100.0,
            "operating_profit_loss": None,
            "profit_loss": None,
            "total_assets": None,
            "equity": None,
            "liabilities": None,
            "cash_and_bank": None,
            "current_assets": None,
            "current_receivables": None,
            "current_liabilities": None,
            "personnel_expenses": None,
            "wages_and_salaries": None,
            "employees": 12,
            "source_fact_count": 4,
            "mapped_fact_count": 2,
            "unmapped_numeric_fact_count": 1,
            "metric_warnings": "[\"unmapped numeric facts: 1\"]",
            "mapping_version": "finland-prh-xbrl-metrics-v1",
            "built_at": "2026-06-01T00:00:00+00:00",
        }
    ]


def test_build_financial_metric_rows_keeps_all_parsed_companies() -> None:
    active_statement = _statement_document_row("statement-active")
    inactive_statement = {
        **_statement_document_row("statement-inactive"),
        "business_id": "inactive-company",
        "reported_business_id": "inactive-company",
    }

    rows = financial_metrics.build_financial_metric_rows(
        statement_documents=[active_statement, inactive_statement],
        facts=[
            _fact_row("statement-active", fact_ordinal=1),
            _fact_row("statement-inactive", fact_ordinal=1),
        ],
        built_at="2026-06-01T00:00:00+00:00",
    )

    assert [row["statement_key"] for row in rows] == [
        "statement-active",
        "statement-inactive",
    ]
    assert [row["business_id"] for row in rows] == ["active-web", "inactive-company"]


def test_build_financial_metric_usd_rows_converts_eur_amounts() -> None:
    exchange_rates = FakeExchangeRates()

    rows = financial_metrics.build_financial_metric_usd_rows(
        financial_metrics=[
            {
                **_financial_metric_row("statement-1"),
                "revenue": 100.0,
                "profit_loss": -25.5,
                "employees": 12,
            }
        ],
        exchange_rates=exchange_rates,
        converted_at="2026-06-02T00:00:00+00:00",
    )

    assert exchange_rates.requests == [("EUR", "2026-05-31")]
    assert len(rows) == 1
    row = rows[0]
    assert row["statement_key"] == "statement-1"
    assert row["currency_original"] == "EUR"
    assert row["revenue_amount_original"] == 100.0
    assert row["revenue_amount_usd"] == 110.0
    assert row["profit_loss_amount_original"] == -25.5
    assert row["profit_loss_amount_usd"] == -28.05
    assert row["employees"] == 12
    assert row["fx_rate_to_usd"] == 1.1
    assert row["fx_rate_date"] == "2026-05-30"
    assert row["fx_converted_at"] == "2026-06-02T00:00:00+00:00"
    assert row["source_system"] == "finland_prh_xbrl"
    assert row["source_record_id"] == "statement-1"
    assert row["source_payload_hash"] == "0" * 64


def test_build_financial_metric_usd_rows_fails_when_required_rate_is_missing() -> None:
    with pytest.raises(LookupError, match="Missing EUR/USD exchange rates"):
        financial_metrics.build_financial_metric_usd_rows(
            financial_metrics=[_financial_metric_row("statement-1")],
            exchange_rates=FakeExchangeRatesWithMissing(),
            converted_at="2026-06-02T00:00:00+00:00",
        )


def test_xbrl_parquet_storage_writes_financial_metrics_usd(tmp_path: Path) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    rows = financial_metrics.build_financial_metric_usd_rows(
        financial_metrics=[_financial_metric_row("statement-1")],
        exchange_rates=FakeExchangeRates(),
        converted_at="2026-06-02T00:00:00+00:00",
    )

    storage.write_financial_metrics_usd(rows)

    assert storage.read_financial_metrics_usd() == rows
    assert storage.financial_metrics_usd_row_count() == 1


def test_financial_report_materialization_loads_api_rows(tmp_path: Path) -> None:
    session = FakePagedFinancialReportsSession()
    api = XbrlApiResource(session=session)
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    context = dg.build_asset_context(
        partition_key="2026-06-01",
    )

    result = xbrl_assets.materialize_financial_reports_window(
        context,
        xbrl_assets.XbrlFinancialReportsConfig(request_delay_seconds=0),
        api,
        registered_date_start="2026-06-01",
        registered_date_end="2026-06-30",
        run_id="test-run",
        write_financial_reports=storage.write_financial_reports_incremental,
    )

    assert result.metadata["row_count"] == 2
    assert result.metadata["parquet_path"] == str(
        storage.financial_reports_incremental_path("2026-06-01")
    )
    assert [
        (row["business_id"], row["financial_date"], row["registration_date"], row["source_page_number"])
        for row in storage.read_financial_reports_incremental("2026-06-01")
    ] == [
        ("active-web", "2026-05-31", "2026-06-01", 1),
        ("second-active-web", "2026-04-30", "2026-06-02", 2),
    ]


def test_xbrl_asset_graph_models_xml_documents_catalog_as_bridge() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert dg.AssetKey(XML_DOCUMENTS_TABLE) in asset_graph.get_all_asset_keys()
    assert asset_graph.get(dg.AssetKey("finland_xbrl_raw_xml_documents")).parent_keys == {
        dg.AssetKey("finland_xbrl_raw_xml_documents_backfill"),
        dg.AssetKey("finland_xbrl_raw_xml_documents_incremental"),
    }
    assert asset_graph.get(dg.AssetKey("finland_xbrl_raw_xml_documents_backfill")).parent_keys == {
        dg.AssetKey("finland_xbrl_financial_reports_backfill"),
    }
    assert asset_graph.get(
        dg.AssetKey("finland_xbrl_raw_xml_documents_incremental")
    ).parent_keys == {
        dg.AssetKey("finland_xbrl_financial_reports_incremental"),
    }
    assert asset_graph.get(dg.AssetKey(XML_DOCUMENTS_TABLE)).parent_keys == {
        dg.AssetKey("finland_xbrl_raw_xml_documents")
    }


def test_xbrl_asset_graph_keeps_quality_and_concept_profile_as_metadata_not_assets() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert dg.AssetKey("fi_prh_xbrl_parse_quality") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("fi_prh_xbrl_concept_profile") not in asset_graph.get_all_asset_keys()


def test_xbrl_asset_graph_models_financial_metrics_downstream_of_parse_parquet_assets() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    parent_keys = asset_graph.get(dg.AssetKey(FINANCIAL_METRICS_TABLE)).parent_keys
    assert parent_keys == {
        dg.AssetKey("finland_xbrl_parse_backfill"),
        dg.AssetKey("finland_xbrl_parse_incremental"),
    }
    assert asset_graph.get(dg.AssetKey(FINANCIAL_METRICS_USD_TABLE)).parent_keys == {
        dg.AssetKey(FINANCIAL_METRICS_TABLE),
    }
    assert asset_graph.get(
        dg.AssetKey("finland_xbrl_financial_metrics_clickhouse")
    ).parent_keys == {
        dg.AssetKey(FINANCIAL_METRICS_USD_TABLE),
    }


def test_finland_xbrl_clickhouse_export_casts_employees_to_nullable_uint(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.finland_xbrl.clickhouse import (
        export_finland_xbrl_financial_metrics_clickhouse,
    )

    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    storage.write_financial_metrics_usd(
        financial_metrics.build_financial_metric_usd_rows(
            financial_metrics=[
                {
                    **_financial_metric_row("statement-with-employees"),
                    "employees": 12,
                },
                {
                    **_financial_metric_row("statement-without-employees"),
                    "employees": None,
                },
            ],
            exchange_rates=FakeExchangeRates(),
            converted_at="2026-06-02T00:00:00+00:00",
        )
    )
    client = FakeClickHouseClient()

    row_count = export_finland_xbrl_financial_metrics_clickhouse(
        xbrl_parquet_storage=storage,
        clickhouse=FakeClickHouseResource(client),
    )

    assert row_count == 2
    insert_calls = [
        (sql, params)
        for sql, params in client.execute_calls
        if sql.strip().startswith("INSERT INTO corpscout.fi_financial_metrics__stage_")
    ]
    assert len(insert_calls) == 1
    insert_sql, insert_rows = insert_calls[0]
    assert isinstance(insert_rows, list)
    assert len(insert_rows) == 2
    column_block = insert_sql.split("(", 1)[1].rsplit(")", 1)[0]
    insert_columns = [
        line.strip().rstrip(",")
        for line in column_block.splitlines()
        if line.strip()
    ]
    first_row = dict(zip(insert_columns, insert_rows[0], strict=True))
    second_row = dict(zip(insert_columns, insert_rows[1], strict=True))
    assert first_row["employees"] == 12
    assert second_row["employees"] is None
    assert first_row["revenue_amount_usd"] == Decimal("110.000000")
    assert first_row["fx_rate_to_usd"] == Decimal("1.100000000000")
    assert any(statement.startswith("EXCHANGE TABLES") for statement in client.statements)


def test_xbrl_api_resource_keeps_source_api_methods() -> None:
    assert not hasattr(XbrlApiResource, "iter_registration_window")
    assert not hasattr(XbrlApiResource, "_list_registration_window")
    assert hasattr(XbrlApiResource, "iter_financial_reports")
    assert hasattr(XbrlApiResource, "download_statement_xml")


def test_xbrl_api_resource_uses_dlt_retry_client_for_xml_downloads() -> None:
    api = XbrlApiResource()
    client = api.session()

    assert client.__class__.__module__ == "dlt.sources.helpers.requests.retry"
    assert client._retry_kwargs["status_codes"] == (429, *range(500, 600))
    assert client._retry_kwargs["max_attempts"] == 6
    assert client._retry_kwargs["backoff_factor"] == 30.0
    assert client._retry_kwargs["respect_retry_after_header"] is True
    assert client._retry_kwargs["max_delay"] == 480.0


def test_xbrl_raw_download_uses_eligible_financial_report_rows() -> None:
    session = FakeHttpSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()
    sleeps: list[float] = []
    log_messages: list[str] = []

    result = download_finland_xbrl_raw_xml_documents(
        xbrl_api=api,
        object_store=object_store,
        financial_reports=_financial_reports(),
        refresh_existing=False,
        download_delay_seconds=0.5,
        sleep=sleeps.append,
        log_info=log_messages.append,
        progress_interval=1,
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

    assert result.rows
    assert list(result.rows[0]) == [
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
    assert [row["business_id"] for row in result.rows] == ["active-web", "second-active-web"]
    assert [row["xml_object_key"] for row in result.rows] == [
        "companies/active-web/2026-05-31.xml",
        "companies/second-active-web/2026-04-30.xml",
    ]
    assert [row["downloaded"] for row in result.rows] == [True, True]
    assert [row["reused"] for row in result.rows] == [False, False]
    assert [row["discovery_registered_date_start"] for row in result.rows] == [
        "2026-06-01",
        "2026-06-01",
    ]
    assert [row["financial_start_date"] for row in result.rows] == ["", ""]
    assert [row["max_reports"] for row in result.rows] == ["", ""]
    assert ("source-finland-prh-xbrl", "raw/fi_prh_xbrl_xml_documents.parquet") not in (
        s3_client.objects
    )
    assert log_messages == [
        "XBRL raw XML download started: reports=2 refresh_existing=False",
        "XBRL raw XML document 1/2: business_id=active-web financial_date=2026-05-31 action=downloaded downloaded=1 reused=0 bytes_downloaded=34",
        "XBRL raw XML document 2/2: business_id=second-active-web financial_date=2026-04-30 action=downloaded downloaded=2 reused=0 bytes_downloaded=75",
        "XBRL raw XML download completed: selected_reports=2 documents=2 downloaded=2 reused=0 bytes_downloaded=75",
    ]


def test_xbrl_raw_download_returns_empty_manifest_when_selection_is_empty() -> None:
    session = FakeHttpSession()
    api = XbrlApiResource(session=session)
    object_store, s3_client = _object_store()

    result = download_finland_xbrl_raw_xml_documents(
        xbrl_api=api,
        object_store=object_store,
        financial_reports=[],
        refresh_existing=False,
        download_delay_seconds=0.0,
    )

    assert result.metadata["documents_count"] == 0
    assert result.rows == []
    assert s3_client.objects == {}


def test_xbrl_parse_window_uses_partition_manifest_without_global_catalog(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    storage.write_raw_xml_documents_incremental("2026-06-01", [])
    object_store, _s3_client = _object_store()
    context = dg.build_asset_context(partition_key="2026-06-01")

    result = parse_assets._materialize_parse_window(
        context,
        object_store,
        window_start=date(2026, 6, 1),
        window_end=date(2026, 6, 2),
        documents=storage.read_raw_xml_documents_incremental("2026-06-01"),
        documents_manifest_path=storage.raw_xml_documents_incremental_path(
            "2026-06-01"
        ),
        run_id="test-run",
        write_statement_documents=storage.write_statement_documents_incremental,
        write_facts=storage.write_facts_incremental,
    )

    assert result.metadata["documents_in_window"] == 0
    assert result.metadata["xml_documents_manifest_path"] == str(
        storage.raw_xml_documents_incremental_path("2026-06-01")
    )
    assert result.metadata["statement_documents_parquet_path"] == str(
        storage.statement_documents_incremental_path("2026-06-01")
    )
    assert result.metadata["facts_parquet_path"] == str(
        storage.facts_incremental_path("2026-06-01")
    )
    assert storage.read_statement_documents_incremental("2026-06-01") == []
    assert storage.read_facts_incremental("2026-06-01") == []


def test_xbrl_parse_window_writes_parsed_partition_parquet(
    tmp_path: Path,
) -> None:
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    object_store, _s3_client = _object_store()
    object_store.write_bytes(
        "companies/active-web/2026-05-31.xml",
        b"<xbrl />",
        bucket="source-finland-prh-xbrl",
    )
    document = {
        "business_id": "active-web",
        "financial_date": "2026-05-31",
        "registration_date": "2026-06-01",
        "source_url": "https://example.test/financial",
        "xml_object_key": "companies/active-web/2026-05-31.xml",
    }

    result = parse_assets._materialize_parse_window(
        dg.build_asset_context(partition_key="2026-06-01"),
        object_store,
        window_start=date(2026, 6, 1),
        window_end=date(2026, 6, 2),
        documents=[document],
        documents_manifest_path=storage.raw_xml_documents_incremental_path(
            "2026-06-01"
        ),
        run_id="test-run",
        write_statement_documents=storage.write_statement_documents_incremental,
        write_facts=storage.write_facts_incremental,
        parser=_fake_statement_parser,
    )

    assert result.metadata["documents_parsed_this_run"] == 1
    assert storage.read_statement_documents_incremental("2026-06-01") == [
        _statement_document_row("statement-active-web")
    ]
    assert storage.read_facts_incremental("2026-06-01") == [
        _fact_row("statement-active-web", fact_ordinal=1)
    ]


def test_xbrl_parse_outputs_are_parquet_without_duckdb_bridge() -> None:
    assert "finland_xbrl_parsed_tables" not in xbrl_assets.__dict__
    assert "rebuild_parsed_duckdb_tables" not in xbrl_assets.__dict__
    assert "load_parsed_table_frame" not in xbrl_assets.__dict__
    assert "parsed_duckdb_row_counts" not in xbrl_assets.__dict__
    assert "parsed_duckdb_observability_metadata" not in xbrl_assets.__dict__


def test_finland_xbrl_parse_assets_use_lxml_parser() -> None:
    assert "run_finland_xbrl_parse" in xbrl_assets.__dict__

    graph = load_project_defs().get_repository_def().asset_graph
    for key in ("finland_xbrl_parse_backfill", "finland_xbrl_parse_incremental"):
        node = graph.get(AssetKey([key]))
        assert node.kinds == {"lxml", "parquet", "python"}



def test_financial_report_rows_in_registration_window_keeps_all_companies() -> None:
    financial_reports = [
        {
            "business_id": "old-financial-date",
            "financial_date": "2023-12-31",
            "registration_date": "2026-06-01",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
        {
            "business_id": "outside-window",
            "financial_date": "2026-05-31",
            "registration_date": "2026-05-31",
            "discovery_registered_date_start": "2026-05-01",
            "discovery_registered_date_end": "2026-05-31",
        },
        {
            "business_id": "inactive-company",
            "financial_date": "2026-04-30",
            "registration_date": "2026-06-02",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
        {
            "business_id": "missing-website",
            "financial_date": "2026-04-30",
            "registration_date": "2026-06-02",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
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

    rows = xbrl_assets.financial_report_rows_in_registration_window(
        financial_reports=financial_reports,
        registered_date_start="2026-06-01",
        registered_date_end="2026-06-30",
    )

    assert rows == [
        {
            "business_id": "old-financial-date",
            "financial_date": "2023-12-31",
            "registration_date": "2026-06-01",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
        {
            "business_id": "active-web",
            "financial_date": "2026-05-31",
            "registration_date": "2026-06-01",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
        {
            "business_id": "inactive-company",
            "financial_date": "2026-04-30",
            "registration_date": "2026-06-02",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
        {
            "business_id": "missing-website",
            "financial_date": "2026-04-30",
            "registration_date": "2026-06-02",
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


def _financial_reports() -> list[dict]:
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


def _write_xml_snapshot_manifest_fixture(
    s3_client: FakeS3Client,
    *,
    start: str,
    end: str,
    rows: list[dict[str, Any]],
) -> None:
    s3_client.objects[("source-finland-prh-xbrl", xml_snapshot_success_key(start, end))] = b"{}"
    s3_client.objects[("source-finland-prh-xbrl", xml_snapshot_manifest_key(start, end))] = (
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows).encode("utf-8")
    )


def _statement_document_row(statement_key: str) -> dict:
    return {
        "statement_key": statement_key,
        "source_run_id": "test-run",
        "business_id": "active-web",
        "financial_date": "2026-05-31",
        "registration_date": "2026-06-01",
        "source_url": "https://example.test/financial",
        "xml_object_key": "companies/active-web/2026-05-31.xml",
        "xml_sha256": "hash",
        "xml_size_bytes": 8,
        "root_name": "xbrl",
        "schema_refs": "[]",
        "taxonomy_entrypoint": "",
        "reported_business_id": "active-web",
        "reported_company_name": "Active Oy",
        "reported_period_start": "2026-01-01",
        "reported_period_end": "2026-05-31",
        "contexts_count": 1,
        "units_count": 1,
        "facts_count": 1,
        "validation_warnings": "[]",
        "parser_version": "test-parser",
        "parsed_at": "2026-06-01T00:00:00+00:00",
    }


def _financial_metric_row(statement_key: str) -> dict[str, Any]:
    return {
        "statement_key": statement_key,
        "business_id": "active-web",
        "financial_date": "2026-05-31",
        "registration_date": "2026-06-01",
        "period_start": "2026-01-01",
        "period_end": "2026-05-31",
        "reported_company_name": "Active Oy",
        "source_url": "https://example.test/financial",
        "xml_object_key": "companies/active-web/2026-05-31.xml",
        "xml_sha256": "0" * 64,
        "xml_size_bytes": 8,
        "source_run_id": "test-run",
        "revenue": 100.0,
        "operating_profit_loss": None,
        "profit_loss": None,
        "total_assets": None,
        "equity": None,
        "liabilities": None,
        "cash_and_bank": None,
        "current_assets": None,
        "current_receivables": None,
        "current_liabilities": None,
        "personnel_expenses": None,
        "wages_and_salaries": None,
        "employees": None,
        "source_fact_count": 1,
        "mapped_fact_count": 1,
        "unmapped_numeric_fact_count": 0,
        "metric_warnings": "[]",
        "mapping_version": "finland-prh-xbrl-metrics-v1",
        "built_at": "2026-06-01T00:00:00+00:00",
    }


def _fact_row(statement_key: str, fact_ordinal: int) -> dict:
    return {
        "statement_key": statement_key,
        "business_id": "active-web",
        "financial_date": "2026-05-31",
        "fact_ordinal": fact_ordinal,
        "concept_qname": "fi_met:md103",
        "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
        "concept_local_name": "md103",
        "context_id": "current",
        "unit_id": "EUR",
        "decimals": "0",
        "precision": "",
        "value_kind": "numeric",
        "raw_value": "100",
        "numeric_value": "100",
        "date_value": "",
        "text_value": "",
        "mcy_member_code": "fi_MC:x673",
        "mcy_member_label_fi": "",
        "ref_member_code": "",
        "ref_member_label_fi": "",
        "is_comparative": False,
        "dimensions": "{}",
        "parser_version": "test-parser",
        "parsed_at": "2026-06-01T00:00:00+00:00",
    }


def _fake_statement_parser(**kwargs) -> ParsedStatement:
    del kwargs
    return ParsedStatement(
        statement_key="statement-active-web",
        rows_by_table={
            STATEMENT_DOCUMENTS_TABLE: [_statement_document_row("statement-active-web")],
            FACTS_TABLE: [_fact_row("statement-active-web", fact_ordinal=1)],
        },
        warnings=[],
    )


def test_duckdb_xbrl_assets_use_dedicated_finland_xbrl_duckdb_pool():
    graph = load_project_defs().get_repository_def().asset_graph
    for key in (
        "finland_xbrl_financial_reports_backfill",
        "finland_xbrl_financial_reports_incremental",
        "finland_xbrl_raw_xml_documents_backfill",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_backfill",
        "finland_xbrl_parse_incremental",
        "fi_prh_xbrl_financial_metrics",
        "fi_prh_xbrl_financial_metrics_usd",
    ):
        node = graph.get(AssetKey([key]))
        assert "finland_ytj_duckdb" not in node.pools, f"{key} should not use YTJ pool"
        assert "finland_xbrl_duckdb" not in node.pools, f"{key} should not use DuckDB pool"


def test_finland_xbrl_no_longer_exposes_dedicated_duckdb_file() -> None:
    assert "_XBRL_DUCKDB_PATH" not in xbrl_assets.__dict__
    assert "FINLAND_XBRL_DUCKDB_POOL" not in xbrl_assets.__dict__
