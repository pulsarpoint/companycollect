from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import duckdb
from dagster import AssetKey
import pytest
from pydantic import ValidationError

import dagster_v3.defs.finland_xbrl.assets as xbrl_assets
from dagster_v3.defs.finland_xbrl import metric_mapping
from dagster_v3.defs.finland_xbrl.assets import financial_metrics
from dagster_v3.defs.finland_xbrl.assets import parse as parse_assets
from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.finland_xbrl.assets import (
    XbrlParsedConfig,
    XbrlRawConfig,
    download_finland_xbrl_raw_xml_documents,
)
from dagster_v3.defs.finland_xbrl.resources import (
    XbrlApiResource,
    XbrlParquetStorageResource,
)
from dagster_v3.defs.finland_xbrl.tables import (
    FINANCIAL_METRICS_TABLE,
    XML_DOCUMENTS_TABLE,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource


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
    assert storage.eligible_companies_path() == (
        tmp_path
        / "parquet"
        / "eligible_companies"
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

    reference_refresh = {
        key.path[-1]
        for key in repo.get_job(
            "finland_xbrl_reference_refresh_job"
        ).asset_layer.executable_asset_keys
    }
    assert reference_refresh == {
        "finland_ytj_all_companies_duckdb",
        "finland_xbrl_eligible_companies",
    }
    assert repo.get_job("finland_xbrl_reference_refresh_job").partitions_def is None

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
        "finland_xbrl_financial_reports_incremental",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_incremental",
    }
    assert type(repo.get_job("finland_xbrl_incremental_job").partitions_def).__name__ == (
        "DailyPartitionsDefinition"
    )

    schedule = repo.get_schedule_def("finland_xbrl_incremental_schedule")
    assert schedule.cron_schedule == "0 6 * * *"


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


def test_xbrl_asset_graph_models_eligible_companies_parquet_downstream_of_ytj_duckdb() -> None:
    asset_graph = load_project_defs().resolve_asset_graph()

    assert dg.AssetKey("fi_prhytj_statuses") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("fi_prhytj_websites") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_financial_reports") in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_financial_reports_duckdb") not in asset_graph.get_all_asset_keys()
    assert dg.AssetKey("finland_xbrl_company_seed_duckdb") not in asset_graph.get_all_asset_keys()
    assert asset_graph.get(dg.AssetKey("finland_xbrl_eligible_companies")).parent_keys == {
        dg.AssetKey("finland_ytj_all_companies_duckdb")
    }


def test_xbrl_financial_reports_are_modeled_as_partitioned_writer_assets() -> None:
    assert "finland_xbrl_financial_reports_source" not in xbrl_assets.__dict__
    assert "_financial_reports_resource" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_pipeline" not in xbrl_assets.__dict__
    assert "run_finland_xbrl_financial_reports_dlt_pipeline" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_backfill_duckdb" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_incremental_duckdb" not in xbrl_assets.__dict__
    assert "finland_xbrl_financial_reports_duckdb" not in xbrl_assets.__dict__
    graph = load_project_defs().get_repository_def().asset_graph
    canonical = graph.get(AssetKey("finland_xbrl_financial_reports"))
    assert canonical.partitions_def is None
    assert canonical.parent_keys == {
        AssetKey("finland_xbrl_financial_reports_backfill"),
        AssetKey("finland_xbrl_financial_reports_incremental"),
    }


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
    rows = [
        {
            "statement_key": "statement-1",
            "business_id": "active-web",
            "financial_date": "2026-05-31",
            "period_start": "2026-01-01",
            "period_end": "2026-05-31",
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
    ]

    storage.write_financial_metrics(rows)

    assert storage.read_financial_metrics() == rows
    assert storage.financial_metrics_row_count() == 1


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


def test_build_financial_metric_rows_maps_current_numeric_facts() -> None:
    statement = _statement_document_row("statement-1")
    fact = _fact_row("statement-1", fact_ordinal=1)
    unmapped_fact = {
        **_fact_row("statement-1", fact_ordinal=2),
        "concept_qname": "unknown:metric",
        "mcy_member_code": "unknown:member",
        "numeric_value": "7",
    }
    comparative_fact = {
        **_fact_row("statement-1", fact_ordinal=3),
        "numeric_value": "999",
        "is_comparative": True,
    }

    rows = financial_metrics.build_financial_metric_rows(
        statement_documents=[statement],
        facts=[fact, unmapped_fact, comparative_fact],
        built_at="2026-06-01T00:00:00+00:00",
    )

    assert rows == [
        {
            "statement_key": "statement-1",
            "business_id": "active-web",
            "financial_date": "2026-05-31",
            "period_start": "2026-01-01",
            "period_end": "2026-05-31",
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
            "source_fact_count": 3,
            "mapped_fact_count": 1,
            "unmapped_numeric_fact_count": 1,
            "metric_warnings": "[\"unmapped numeric facts: 1\"]",
            "mapping_version": "finland-prh-xbrl-metrics-v1",
            "built_at": "2026-06-01T00:00:00+00:00",
        }
    ]


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
        dg.AssetKey("finland_xbrl_eligible_companies"),
    }
    assert asset_graph.get(
        dg.AssetKey("finland_xbrl_raw_xml_documents_incremental")
    ).parent_keys == {
        dg.AssetKey("finland_xbrl_financial_reports_incremental"),
        dg.AssetKey("finland_xbrl_eligible_companies"),
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
        financial_reports=_eligible_financial_reports(),
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
        parser=_fake_arelle_parser,
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



def test_load_eligible_financial_report_rows_filters_by_registration_window_only() -> None:
    eligible_companies = [
        {
            "business_id": "old-financial-date",
            "primary_name": "Old Oy",
            "website_normalized_url": "https://old.example",
        },
        {
            "business_id": "outside-window",
            "primary_name": "Outside Oy",
            "website_normalized_url": "https://outside.example",
        },
        {
            "business_id": "active-web",
            "primary_name": "Active Oy",
            "website_normalized_url": "https://active.example",
        },
        {
            "business_id": "second-active-web",
            "primary_name": "Second Oy",
            "website_normalized_url": "https://second.example",
        },
    ]
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

    rows = xbrl_assets.load_eligible_financial_report_rows(
        eligible_companies=eligible_companies,
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
            "business_id": "second-active-web",
            "financial_date": "2026-04-30",
            "registration_date": "2026-06-02",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        },
    ]


def test_build_finland_xbrl_eligible_companies_writes_active_companies_with_websites_parquet(
    tmp_path: Path,
) -> None:
    ytj_database_path = tmp_path / "finland_ytj.duckdb"
    storage = XbrlParquetStorageResource(base_path=str(tmp_path / "parquet"))
    log_messages: list[str] = []
    with duckdb.connect(str(ytj_database_path)) as connection:
        connection.execute("create schema finland_prhytj")
        connection.execute(
            """
            create table finland_prhytj.all_companies (
                business_id varchar,
                primary_name varchar,
                is_active boolean,
                website_normalized_url varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_prhytj.all_companies values
            ('active-web', 'Active Oy', true, 'https://active.example'),
            ('inactive-web', 'Inactive Oy', false, 'https://inactive.example'),
            ('active-missing-web', 'No Web Oy', true, ''),
            ('second-active-web', 'Second Oy', true, 'https://second.example')
            """
        )

    result = xbrl_assets.build_finland_xbrl_eligible_companies(
        ytj_duckdb=duckdb_resource(ytj_database_path),
        xbrl_parquet_storage=storage,
        log_info=log_messages.append,
    )

    assert result.metadata["eligible_companies_row_count"] == 2
    assert result.metadata["parquet_path"] == str(storage.eligible_companies_path())
    assert log_messages == [
        f"Building Finland XBRL eligible companies parquet from YTJ DuckDB {ytj_database_path}",
        "Finland XBRL eligible companies parquet built: eligible_companies_row_count=2",
    ]
    assert storage.read_eligible_companies() == [
        {
            "business_id": "active-web",
            "primary_name": "Active Oy",
            "website_normalized_url": "https://active.example",
        },
        {
            "business_id": "second-active-web",
            "primary_name": "Second Oy",
            "website_normalized_url": "https://second.example",
        },
    ]


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


def _fake_arelle_parser(**kwargs) -> SimpleNamespace:
    del kwargs
    return SimpleNamespace(
        statement_document=_statement_document_row("statement-active-web"),
        facts=[_fact_row("statement-active-web", fact_ordinal=1)],
        warnings=[],
    )


def test_duckdb_xbrl_assets_use_dedicated_finland_xbrl_duckdb_pool():
    graph = load_project_defs().get_repository_def().asset_graph
    for key in (
        "finland_xbrl_eligible_companies",
    ):
        node = graph.get(AssetKey([key]))
        assert "finland_ytj_duckdb" in node.pools, f"{key} missing YTJ pool"
        assert "finland_xbrl_duckdb" not in node.pools, f"{key} still uses XBRL pool"
    for key in (
        "finland_xbrl_financial_reports_backfill",
        "finland_xbrl_financial_reports_incremental",
        "finland_xbrl_financial_reports",
        "finland_xbrl_raw_xml_documents_backfill",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_backfill",
        "finland_xbrl_parse_incremental",
        "fi_prh_xbrl_financial_metrics",
    ):
        node = graph.get(AssetKey([key]))
        assert "finland_xbrl_duckdb" not in node.pools, f"{key} should not use DuckDB pool"


def test_finland_xbrl_no_longer_exposes_dedicated_duckdb_file() -> None:
    assert "_XBRL_DUCKDB_PATH" not in xbrl_assets.__dict__
    assert "FINLAND_XBRL_DUCKDB_POOL" not in xbrl_assets.__dict__
