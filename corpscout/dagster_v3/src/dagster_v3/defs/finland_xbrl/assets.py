import json
import os
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any, Callable

import dagster as dg
import dlt
import polars as pl
from dateutil.relativedelta import relativedelta
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.helpers.requests import Client as DltRequestsClient

from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.arelle_parser import (
    ArelleStatementParser,
    parse_statement_xml_with_arelle,
)
from dagster_v3.defs.finland_xbrl.resources import HttpSession, XbrlApiResource
from dagster_v3.defs.common.resources import LocalDuckDBResource, ObjectStoreResource
from pydantic import ConfigDict, field_validator

XBRL_BUCKET = "source-finland-prh-xbrl"
RAW_XML_DOCUMENTS_OBJECT_KEY = f"raw/{tables.XML_DOCUMENTS_TABLE}.parquet"
XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
XBRL_TIMEOUT_SECONDS = 120
XBRL_DLT_DATASET_NAME = "finland_prh_xbrl"
XBRL_DLT_FINANCIAL_REPORTS_TABLE = "financial_reports"
XBRL_ELIGIBLE_FINANCIAL_REPORTS_TABLE = "eligible_financial_reports"
DEFAULT_XBRL_REGISTERED_DATE_END = date.today().isoformat()
DEFAULT_XBRL_REGISTERED_DATE_START = (
    date.fromisoformat(DEFAULT_XBRL_REGISTERED_DATE_END) - relativedelta(months=1)
).isoformat()
DEFAULT_XBRL_REQUEST_DELAY_SECONDS = 1.0
DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS = 5
DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS = 10.0
DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS = 120.0
FI_XBRL_PARSE_PARTITION_START = "2014-01-01"  # earliest registration month in scope; widen if needed
fi_xbrl_parse_partitions = dg.MonthlyPartitionsDefinition(start_date=FI_XBRL_PARSE_PARTITION_START)

FINLAND_XBRL_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
_XBRL_DUCKDB_PATH = Path(LocalDuckDBResource().database_path).expanduser()
if not _XBRL_DUCKDB_PATH.is_absolute():
    _XBRL_DUCKDB_PATH = _XBRL_DUCKDB_PATH.resolve()
os.environ["FINLAND_XBRL_DUCKDB_PATH"] = str(_XBRL_DUCKDB_PATH)

finland_xbrl_dbt_project = DbtProject(
    project_dir=FINLAND_XBRL_DBT_PROJECT_DIR,
    profiles_dir=FINLAND_XBRL_DBT_PROJECT_DIR,
)
finland_xbrl_dbt_project.prepare_if_dev()


class FinlandXbrlDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, props: Mapping[str, Any]) -> dg.AssetKey:
        if props["resource_type"] == "source":
            return super().get_asset_key(props)
        if props["name"] == "eligible_financial_reports":
            return dg.AssetKey("finland_xbrl_eligible_financial_reports")
        return dg.AssetKey(props["name"])  # fi_prh_xbrl_financial_metrics keeps its name

    def get_group_name(self, props: Mapping[str, Any]) -> str:
        return "finland_xbrl"


@dbt_assets(
    manifest=finland_xbrl_dbt_project.manifest_path,
    project=finland_xbrl_dbt_project,
    dagster_dbt_translator=FinlandXbrlDbtTranslator(),
    pool="finland_ytj_duckdb",
)
def finland_xbrl_dbt_assets(
    context: dg.AssetExecutionContext,
    finland_xbrl_dbt: DbtCliResource,
):
    yield from finland_xbrl_dbt.cli(["build"], context=context).stream()


class FinlandXbrlDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != XBRL_DLT_FINANCIAL_REPORTS_TABLE:
            return spec
        return spec.replace_attributes(
            key="finland_xbrl_financial_reports_duckdb",
            deps=[],
            group_name="finland_xbrl",
            description="Finland PRH XBRL financial report listing loaded to local DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb"},
        )


class XbrlFinancialReportsConfig(dg.Config):
    registered_date_start: str = DEFAULT_XBRL_REGISTERED_DATE_START
    registered_date_end: str = DEFAULT_XBRL_REGISTERED_DATE_END
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS
    max_retries: int = DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS
    retry_initial_delay_seconds: float = DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS
    retry_max_delay_seconds: float = DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS

    @field_validator("registered_date_start", "registered_date_end")
    @classmethod
    def validate_required_iso_date(cls, value: str) -> str:
        return _required_iso_date(value, field_name="registered date")

    @field_validator(
        "request_delay_seconds",
        "retry_initial_delay_seconds",
        "retry_max_delay_seconds",
    )
    @classmethod
    def validate_non_negative_seconds(cls, value: float) -> float:
        if value < 0:
            raise ValueError("delay values must be zero or greater")
        return value

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_retries must be greater than zero")
        return value


class XbrlRawConfig(dg.Config):
    financial_start_date: str | None = None
    max_reports: int | None = None
    refresh_existing: bool = False
    download_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS

    @field_validator("financial_start_date", mode="before")
    @classmethod
    def normalize_optional_iso_date(cls, value: object) -> str | None:
        return _optional_iso_date(value)

    @field_validator("max_reports")
    @classmethod
    def validate_max_reports(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("max_reports must be greater than zero")
        return value

    @field_validator("download_delay_seconds")
    @classmethod
    def validate_download_delay_seconds(cls, value: float) -> float:
        if value < 0:
            raise ValueError("download_delay_seconds must be zero or greater")
        return value


class XbrlParsedConfig(dg.Config):
    model_config = ConfigDict(extra="forbid")

    documents_key: str | None = None

    @field_validator("documents_key", mode="before")
    @classmethod
    def normalize_object_key(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("object keys must be strings")
        stripped = value.strip()
        return stripped or None


@dlt.source(name="finland_xbrl")
def finland_xbrl_financial_reports_source(
    *,
    registered_date_start: str,
    registered_date_end: str,
    base_url: str = XBRL_BASE_URL,
    timeout_seconds: int = XBRL_TIMEOUT_SECONDS,
    user_agent: str = "corpscout-dagster-v3-dev/0.1",
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    max_retries: int = DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    run_id: str = "",
    session: HttpSession | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> DltResource:
    return _financial_reports_resource(
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        request_delay_seconds=request_delay_seconds,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        run_id=run_id,
        session=session,
        sleep=sleep,
    )


@dlt.resource(
    name=XBRL_DLT_FINANCIAL_REPORTS_TABLE,
    write_disposition="replace",
    primary_key=("business_id", "financial_date", "registration_date"),
)
def _financial_reports_resource(
    *,
    registered_date_start: str,
    registered_date_end: str,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    request_delay_seconds: float,
    max_retries: int,
    retry_initial_delay_seconds: float,
    retry_max_delay_seconds: float,
    run_id: str,
    session: HttpSession | None,
    sleep: Callable[[float], None],
) -> Iterator[dict[str, Any]]:
    page_number = 1
    source_record_number = 1
    http_client = session or _financial_reports_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
    )
    while True:
        payload = _download_financial_reports_page(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            page_number=page_number,
            session=http_client,
        )
        financials = _financials_from_payload(payload)
        if not financials:
            break
        for page_record_number, financial in enumerate(financials, start=1):
            yield _dlt_financial_report_row(
                financial,
                registered_date_start=registered_date_start,
                registered_date_end=registered_date_end,
                source_page_number=page_number,
                source_page_record_number=page_record_number,
                source_record_number=source_record_number,
                run_id=run_id,
            )
            source_record_number += 1
        page_number += 1
        if request_delay_seconds > 0:
            sleep(request_delay_seconds)


def run_finland_xbrl_financial_reports_dlt_pipeline(
    *,
    database_path: str | Path,
    registered_date_start: str,
    registered_date_end: str,
    run_id: str,
    session: HttpSession | None = None,
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    max_retries: int = DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
) -> Any:
    return finland_xbrl_financial_reports_pipeline(database_path).run(
        finland_xbrl_financial_reports_source(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            run_id=run_id,
            session=session,
            request_delay_seconds=request_delay_seconds,
            max_retries=max_retries,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
        )
    )


def finland_xbrl_financial_reports_pipeline(database_path: str | Path) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="finland_xbrl_financial_reports",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=XBRL_DLT_DATASET_NAME,
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=finland_xbrl_financial_reports_source(
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-01",
    ),
    dlt_pipeline=finland_xbrl_financial_reports_pipeline(LocalDuckDBResource().path()),
    name="finland_xbrl_financial_reports_duckdb",
    dagster_dlt_translator=FinlandXbrlDltTranslator(),
    pool="finland_ytj_duckdb",
)
def finland_xbrl_financial_reports_duckdb_asset(
    context: dg.AssetExecutionContext,
    config: XbrlFinancialReportsConfig,
    dlt: DagsterDltResource,
    source_duckdb: LocalDuckDBResource,
) -> Iterator[Any]:
    """Load PRH XBRL financial report listings to a local DuckDB database with dlt."""
    yield from dlt.run(
        context=context,
        dlt_source=finland_xbrl_financial_reports_source(
            registered_date_start=config.registered_date_start,
            registered_date_end=config.registered_date_end,
            request_delay_seconds=config.request_delay_seconds,
            max_retries=config.max_retries,
            retry_initial_delay_seconds=config.retry_initial_delay_seconds,
            retry_max_delay_seconds=config.retry_max_delay_seconds,
            run_id=context.run_id,
        ),
        dlt_pipeline=finland_xbrl_financial_reports_pipeline(source_duckdb.path()),
    )


def download_finland_xbrl_raw_xml_documents(
    *,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    financial_reports: list[dict[str, Any]],
    financial_start_date: str,
    max_reports: int | None,
    refresh_existing: bool,
    download_delay_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(XBRL_BUCKET)

    documents: list[dict[str, Any]] = []
    downloaded_count = 0
    reused_count = 0
    bytes_downloaded = 0
    selected_at = datetime.now(UTC).isoformat()
    selected_financial_reports = (
        financial_reports[:max_reports] if max_reports is not None else financial_reports
    )

    for report_index, report in enumerate(selected_financial_reports):
        business_id = str(report["business_id"])
        financial_date = str(report["financial_date"])
        registration_date = _optional_string(report.get("registration_date"))
        object_key = document_object_key(business_id, financial_date)
        if not refresh_existing and object_store.exists(object_key, bucket=XBRL_BUCKET):
            reused_count += 1
            documents.append(
                {
                    "business_id": business_id,
                    "financial_date": financial_date,
                    "registration_date": registration_date,
                    "object_key": object_key,
                    "source_url": xbrl_api.statement_xml_url(business_id, financial_date),
                    "xml_sha256": "",
                    "xml_size_bytes": 0,
                    "downloaded": False,
                    "discovery_registered_date_start": _optional_string(
                        report.get("discovery_registered_date_start")
                    ),
                    "discovery_registered_date_end": _optional_string(
                        report.get("discovery_registered_date_end")
                    ),
                }
            )
            continue

        body, source_url = xbrl_api.download_statement_xml(
            business_id,
            financial_date,
        )
        object_store.write_bytes(object_key, body, bucket=XBRL_BUCKET)
        downloaded_count += 1
        bytes_downloaded += len(body)
        documents.append(
            {
                "business_id": business_id,
                "financial_date": financial_date,
                "registration_date": registration_date,
                "object_key": object_key,
                "source_url": source_url,
                "xml_sha256": sha256(body).hexdigest(),
                "xml_size_bytes": len(body),
                "downloaded": True,
                "discovery_registered_date_start": _optional_string(
                    report.get("discovery_registered_date_start")
                ),
                "discovery_registered_date_end": _optional_string(
                    report.get("discovery_registered_date_end")
                ),
            }
        )
        if download_delay_seconds > 0 and report_index < len(selected_financial_reports) - 1:
            sleep(download_delay_seconds)

    xml_document_rows = [
        _xml_document_row(
            document,
            discovery_registered_date_start=document["discovery_registered_date_start"],
            discovery_registered_date_end=document["discovery_registered_date_end"],
            financial_start_date=financial_start_date,
            max_reports=max_reports,
            selected_at=selected_at,
        )
        for document in documents
    ]
    xml_documents_catalog = merge_xml_document_catalog(
        object_store=object_store,
        new_rows=xml_document_rows,
    )

    return dg.MaterializeResult(
        metadata={
            "financial_start_date": financial_start_date,
            "max_reports": max_reports,
            "selected_reports_count": len(selected_financial_reports),
            "documents_count": len(documents),
            "downloaded_count": downloaded_count,
            "reused_count": reused_count,
            "bytes_downloaded": bytes_downloaded,
            "xml_documents_object_key": RAW_XML_DOCUMENTS_OBJECT_KEY,
            "xml_documents_catalog_count": xml_documents_catalog.height,
        }
    )


@dataclass(frozen=True)
class XbrlParseRunResult:
    load_info: Any
    parsed: int
    failed: int


def run_finland_xbrl_arelle_dlt_pipeline(
    *,
    database_path: str | Path,
    object_store: ObjectStoreResource,
    documents: list[dict[str, Any]],
    run_id: str,
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> XbrlParseRunResult:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    _log_parse_progress(log_info, f"Parsing {len(documents)} XBRL XML documents")
    statement_rows, fact_rows, failed = parse_xbrl_documents(
        documents,
        object_store=object_store,
        run_id=run_id,
        parser=parser,
        log_info=log_info,
        progress_interval=progress_interval,
    )
    pipeline = dlt.pipeline(
        pipeline_name="finland_xbrl_arelle_parsed_tables",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=XBRL_DLT_DATASET_NAME,
        dev_mode=False,
    )
    load_info = pipeline.run(
        finland_xbrl_arelle_source(
            statement_rows=statement_rows,
            fact_rows=fact_rows,
        )
    )
    _ensure_parsed_duckdb_tables(database_file)
    if log_info is not None:
        log_info("dlt loaded parsed XBRL tables into DuckDB")
    return XbrlParseRunResult(
        load_info=load_info,
        parsed=len(statement_rows),
        failed=len(failed),
    )


def _parse_registration_date(raw: object) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def documents_in_registration_window(
    documents: list[dict[str, Any]],
    *,
    window_start: date,
    window_end: date,
) -> list[dict[str, Any]]:
    """Keep docs whose registration_date is in [window_start, window_end)."""
    selected: list[dict[str, Any]] = []
    for document in documents:
        registered = _parse_registration_date(document.get("registration_date"))
        if registered is not None and window_start <= registered < window_end:
            selected.append(document)
    return selected


def documents_missing_registration_date(
    documents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Docs with no/unparseable registration_date — excluded from every month partition."""
    return [
        document
        for document in documents
        if _parse_registration_date(document.get("registration_date")) is None
    ]


def unparsed_documents(
    documents: list[dict[str, Any]],
    *,
    parsed_object_keys: set[str],
) -> list[dict[str, Any]]:
    """Drop docs whose S3 object key is already present in the parsed output."""
    return [
        document
        for document in documents
        if document["xml_object_key"] not in parsed_object_keys
    ]


def parse_xbrl_documents(
    documents: list[dict[str, Any]],
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    parser: ArelleStatementParser = parse_statement_xml_with_arelle,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    parsed_at = datetime.now(UTC)
    total_documents = len(documents)
    statement_rows: list[dict[str, Any]] = []
    fact_rows: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    warning_count = 0
    started_at = datetime.now(UTC)
    for document_index, document in enumerate(documents, start=1):
        xml_object_key = document["xml_object_key"]
        try:
            body = object_store.read_bytes(xml_object_key, bucket=XBRL_BUCKET)
            parsed = parser(
                business_id=document["business_id"],
                financial_date=document["financial_date"],
                registration_date=document.get("registration_date"),
                source_url=document.get("source_url", ""),
                xml_object_key=xml_object_key,
                source_run_id=run_id,
                body=body,
                parsed_at=parsed_at,
            )
        except Exception as exc:  # noqa: BLE001 - skip and record one bad document
            failed.append(
                {
                    "xml_object_key": xml_object_key,
                    "business_id": document["business_id"],
                    "financial_date": document["financial_date"],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            _log_parse_progress(
                log_info,
                f"Skipping unparseable XBRL document {xml_object_key}: "
                f"business_id={document['business_id']} "
                f"financial_date={document['financial_date']} "
                f"error={type(exc).__name__}: {exc}",
            )
            continue
        warning_count += len(parsed.warnings)
        statement_rows.append(
            _table_row(tables.STATEMENT_DOCUMENTS_TABLE, parsed.statement_document)
        )
        fact_rows.extend(_table_row(tables.FACTS_TABLE, fact) for fact in parsed.facts)
        if _should_log_parse_progress(
            document_index=document_index,
            total_documents=total_documents,
            progress_interval=progress_interval,
        ):
            elapsed_seconds = (datetime.now(UTC) - started_at).total_seconds()
            _log_parse_progress(
                log_info,
                "Parsed XBRL XML document "
                f"{document_index}/{total_documents}: "
                f"business_id={document['business_id']} "
                f"financial_date={document['financial_date']} "
                f"facts={len(parsed.facts)} "
                f"warnings={len(parsed.warnings)} "
                f"elapsed_seconds={elapsed_seconds:.1f}",
            )

    elapsed_seconds = (datetime.now(UTC) - started_at).total_seconds()
    _log_parse_progress(
        log_info,
        "Parsed XBRL XML documents complete: "
        f"documents={total_documents} "
        f"statement_rows={len(statement_rows)} "
        f"fact_rows={len(fact_rows)} "
        f"failed={len(failed)} "
        f"parser_warnings={warning_count} "
        f"elapsed_seconds={elapsed_seconds:.1f}",
    )
    return statement_rows, fact_rows, failed


@dlt.source(name="finland_xbrl_arelle")
def finland_xbrl_arelle_source(
    *,
    statement_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
) -> list[DltResource]:
    return _finland_xbrl_arelle_resources(
        statement_rows=statement_rows,
        fact_rows=fact_rows,
    )


def _finland_xbrl_arelle_resources(
    *,
    statement_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
) -> list[DltResource]:
    return [
        dlt.resource(
            statement_rows,
            name=tables.STATEMENT_DOCUMENTS_TABLE,
            write_disposition="append",
            primary_key="statement_key",
        ),
        dlt.resource(
            fact_rows,
            name=tables.FACTS_TABLE,
            write_disposition="append",
            primary_key=("statement_key", "fact_ordinal"),
        ),
    ]


def _should_log_parse_progress(
    *,
    document_index: int,
    total_documents: int,
    progress_interval: int,
) -> bool:
    if total_documents == 0:
        return False
    if document_index == 1 or document_index == total_documents:
        return True
    return progress_interval > 0 and document_index % progress_interval == 0


def _log_parse_progress(log_info: Callable[[str], None] | None, message: str) -> None:
    if log_info is not None:
        log_info(message)


@dg.asset(
    name="finland_xbrl_raw_xml_documents",
    group_name="finland_xbrl",
    deps=["finland_xbrl_eligible_financial_reports"],
    kinds={"python", "s3", "xml"},
)
def finland_xbrl_raw_xml_documents(
    config: XbrlRawConfig,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    source_duckdb: LocalDuckDBResource,
) -> dg.MaterializeResult:
    """Download PRH XBRL XML statements for eligible DuckDB financial report rows."""
    run_date = datetime.now(UTC).date()
    financial_start_date = config.financial_start_date or (
        run_date - relativedelta(years=2)
    ).isoformat()
    return download_finland_xbrl_raw_xml_documents(
        xbrl_api=xbrl_api,
        object_store=object_store,
        financial_reports=load_eligible_financial_report_rows(
            source_duckdb,
            financial_start_date=financial_start_date,
            max_reports=config.max_reports,
        ),
        financial_start_date=financial_start_date,
        max_reports=config.max_reports,
        refresh_existing=config.refresh_existing,
        download_delay_seconds=config.download_delay_seconds,
    )


@dg.asset(
    name=tables.XML_DOCUMENTS_TABLE,
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents],
    kinds={"python", "s3", "parquet"},
)
def finland_xbrl_xml_documents(object_store: ObjectStoreResource) -> dg.MaterializeResult:
    """Catalog of raw Finland PRH XBRL XML documents available in object storage."""
    frame = load_xml_document_catalog_frame(object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY)
    return dg.MaterializeResult(
        metadata={
            "bucket": XBRL_BUCKET,
            "object_key": RAW_XML_DOCUMENTS_OBJECT_KEY,
            "row_count": frame.height,
        }
    )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            table,
            deps=[tables.XML_DOCUMENTS_TABLE],
            group_name="finland_xbrl",
            kinds={"python", "dlt", "duckdb", "arelle"},
            description=f"Parsed Finland PRH XBRL DuckDB table for `{table}`.",
        )
        for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE)
    ],
    partitions_def=fi_xbrl_parse_partitions,
    pool="finland_ytj_duckdb",
)
def finland_xbrl_parsed_tables(
    context: dg.AssetExecutionContext,
    config: XbrlParsedConfig,
    object_store: ObjectStoreResource,
    source_duckdb: LocalDuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    documents_key = resolve_xbrl_documents_key(config=config)
    window = context.partition_time_window
    window_start = window.start.date()
    window_end = window.end.date()

    documents, _meta = load_xbrl_document_manifest(
        object_store=object_store, documents_key=documents_key
    )
    # Docs without a parseable registration_date fall outside every month partition,
    # so they are never parsed. Surface that gap rather than dropping it silently.
    missing_registration = len(documents_missing_registration_date(documents))
    if missing_registration:
        context.log.warning(
            "XBRL parse partition %s: %d catalog docs have no parseable registration_date "
            "and are excluded from all month partitions",
            context.partition_key, missing_registration,
        )
    in_window = documents_in_registration_window(
        documents, window_start=window_start, window_end=window_end
    )
    to_parse = unparsed_documents(
        in_window, parsed_object_keys=load_parsed_object_keys(source_duckdb)
    )
    context.log.info(
        "XBRL parse partition %s: %d catalog docs, %d in window, %d to parse",
        context.partition_key, len(documents), len(in_window), len(to_parse),
    )

    parsed_this_run = 0
    failed_this_run = 0
    if to_parse:
        result = run_finland_xbrl_arelle_dlt_pipeline(
            database_path=source_duckdb.path(),
            object_store=object_store,
            documents=to_parse,
            run_id=context.run_id,
            log_info=context.log.info,
        )
        parsed_this_run = result.parsed
        failed_this_run = result.failed
        if failed_this_run:
            # Failed docs are not written, so they are re-attempted on the next run
            # (and keep failing until the source document is fixed/removed).
            context.log.warning(
                "XBRL parse partition %s: %d documents failed to parse and were skipped "
                "(will retry next run)",
                context.partition_key, failed_this_run,
            )

    row_counts = parsed_duckdb_row_counts(source_duckdb)
    for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE):
        yield dg.MaterializeResult(
            asset_key=table,
            metadata={
                "duckdb_schema": XBRL_DLT_DATASET_NAME,
                "duckdb_table": table,
                "partition": context.partition_key,
                "documents_in_window": len(in_window),
                "documents_parsed_this_run": parsed_this_run,
                "documents_failed_this_run": failed_this_run,
                "documents_missing_registration_date": missing_registration,
                "total_table_row_count": row_counts[table],
                "xml_documents_object_key": documents_key,
            },
        )


defs = dg.Definitions(
    assets=[
        finland_xbrl_financial_reports_duckdb_asset,
        finland_xbrl_dbt_assets,
        finland_xbrl_raw_xml_documents,
        finland_xbrl_xml_documents,
        finland_xbrl_parsed_tables,
    ],
    resources={
        "xbrl_api": XbrlApiResource(),
        "object_store": ObjectStoreResource(),
        "source_duckdb": LocalDuckDBResource(),
        "finland_xbrl_dbt": DbtCliResource(
            project_dir=finland_xbrl_dbt_project,
            profiles_dir=FINLAND_XBRL_DBT_PROJECT_DIR,
        ),
    },
)


def _download_financial_reports_page(
    *,
    registered_date_start: str,
    registered_date_end: str,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    page_number: int,
    session: HttpSession | None,
) -> dict[str, Any]:
    http_session = session or _financial_reports_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        max_retries=DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
        retry_initial_delay_seconds=DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
        retry_max_delay_seconds=DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    )
    headers = getattr(http_session, "headers", None)
    if isinstance(headers, dict):
        headers["User-Agent"] = user_agent
    response = http_session.get(
        f"{base_url}/all_financial_statements",
        params={
            "registeredDateStart": registered_date_start,
            "registeredDateEnd": registered_date_end,
            "page": page_number,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _financial_reports_http_client(
    *,
    timeout_seconds: int,
    user_agent: str,
    max_retries: int,
    retry_initial_delay_seconds: float,
    retry_max_delay_seconds: float,
) -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=timeout_seconds,
        request_max_attempts=max_retries,
        request_backoff_factor=retry_initial_delay_seconds,
        request_max_retry_delay=retry_max_delay_seconds,
        respect_retry_after_header=True,
        session_attrs={"headers": {"User-Agent": user_agent}},
    )


def _financials_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    financials = payload.get("financials") or []
    return [financial for financial in financials if isinstance(financial, dict)]


def _dlt_financial_report_row(
    financial: dict[str, Any],
    *,
    registered_date_start: str,
    registered_date_end: str,
    source_page_number: int,
    source_page_record_number: int,
    source_record_number: int,
    run_id: str,
) -> dict[str, Any]:
    business_id = str(financial.get("businessId") or "").strip()
    financial_date = str(financial.get("financialDate") or "").strip()
    registration_date = str(financial.get("registrationDate") or "").strip()
    return {
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date,
        "discovery_registered_date_start": registered_date_start,
        "discovery_registered_date_end": registered_date_end,
        "source_run_id": run_id,
        "source_page_number": source_page_number,
        "source_page_record_number": source_page_record_number,
        "source_record_number": source_record_number,
        "source_payload_hash": _source_payload_hash(financial),
        "raw_financial": json.dumps(financial, ensure_ascii=False, separators=(",", ":")),
    }


def _source_payload_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(body.encode("utf-8")).hexdigest()


def load_eligible_financial_report_rows(
    source_duckdb: LocalDuckDBResource,
    *,
    financial_start_date: str,
    max_reports: int | None,
) -> list[dict[str, Any]]:
    limit_clause = "" if max_reports is None else f"limit {max_reports}"
    with source_duckdb.connect(read_only=True) as connection:
        rows = connection.execute(
            f"""
            select
                business_id,
                financial_date,
                registration_date,
                discovery_registered_date_start,
                discovery_registered_date_end
            from {XBRL_DLT_DATASET_NAME}.{XBRL_ELIGIBLE_FINANCIAL_REPORTS_TABLE}
            where financial_date >= ?
            order by financial_date, business_id
            {limit_clause}
            """,
            [financial_start_date],
        ).fetchall()
    return [
        {
            "business_id": business_id,
            "financial_date": financial_date,
            "registration_date": registration_date,
            "discovery_registered_date_start": discovery_registered_date_start,
            "discovery_registered_date_end": discovery_registered_date_end,
        }
        for (
            business_id,
            financial_date,
            registration_date,
            discovery_registered_date_start,
            discovery_registered_date_end,
        ) in rows
    ]


def _optional_string(value: Any) -> str:
    return str(value or "")


def document_object_key(business_id: str, financial_date: str) -> str:
    return f"companies/{business_id}/{financial_date}.xml"


def resolve_registration_window(
    *,
    run_date: date,
    registered_date_start: str,
    registered_date_end: str | None,
) -> tuple[str, str]:
    return (
        registered_date_start,
        registered_date_end or run_date.isoformat(),
    )


def resolve_xbrl_documents_key(
    *,
    config: XbrlParsedConfig,
    object_store: ObjectStoreResource | None = None,
    run_date: date | None = None,
) -> str:
    del object_store, run_date
    if config.documents_key is not None:
        return config.documents_key
    return RAW_XML_DOCUMENTS_OBJECT_KEY


def load_xbrl_document_manifest(
    *,
    object_store: ObjectStoreResource,
    documents_key: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    frame = load_xml_document_catalog_frame(object_store, documents_key=documents_key)
    return (
        [_normalize_xml_document_row(row) for row in frame.to_dicts()],
        {"xml_documents_object_key": documents_key},
    )


def load_xml_document_catalog_frame(
    object_store: ObjectStoreResource,
    *,
    documents_key: str,
) -> pl.DataFrame:
    if not object_store.exists(documents_key, bucket=XBRL_BUCKET):
        raise ValueError(
            f"XBRL XML document manifest {documents_key!r} does not exist in "
            f"{XBRL_BUCKET!r}. Materialize finland_xbrl_raw_xml_documents first."
        )

    return pl.read_parquet(
        BytesIO(object_store.read_bytes(documents_key, bucket=XBRL_BUCKET))
    )


def load_parsed_table_frame(source_duckdb: LocalDuckDBResource, *, table: str) -> pl.DataFrame:
    if table not in {tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE}:
        raise ValueError(f"Unsupported parsed XBRL DuckDB table {table!r}")
    with source_duckdb.connect(read_only=True) as connection:
        if not _duckdb_table_exists(connection, table=table):
            raise ValueError(
                f"XBRL parsed DuckDB table {XBRL_DLT_DATASET_NAME}.{table} does not exist. "
                "Materialize fi_prh_xbrl_statement_documents and fi_prh_xbrl_facts_raw first."
            )
        arrow_table = connection.execute(
            f"select {', '.join(tables.TABLE_COLUMNS[table])} "
            f"from {XBRL_DLT_DATASET_NAME}.{table}"
        ).to_arrow_table()
    return pl.from_arrow(arrow_table)


def parsed_duckdb_row_counts(source_duckdb: LocalDuckDBResource) -> dict[str, int]:
    parsed_tables = (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE)
    if not source_duckdb.path().exists():
        return {table: 0 for table in parsed_tables}
    with source_duckdb.connect(read_only=True) as connection:
        return {
            table: (
                int(
                    connection.execute(
                        f"select count(*) from {XBRL_DLT_DATASET_NAME}.{table}"
                    ).fetchone()[0]
                )
                if _duckdb_table_exists(connection, table=table)
                else 0
            )
            for table in parsed_tables
        }


def load_parsed_object_keys(source_duckdb: LocalDuckDBResource) -> set[str]:
    """Set of xml_object_keys already present in the parsed statement table."""
    if not source_duckdb.path().exists():
        return set()
    with source_duckdb.connect(read_only=True) as connection:
        if not _duckdb_table_exists(connection, table=tables.STATEMENT_DOCUMENTS_TABLE):
            return set()
        rows = connection.execute(
            f"select distinct xml_object_key "
            f"from {XBRL_DLT_DATASET_NAME}.{tables.STATEMENT_DOCUMENTS_TABLE}"
        ).fetchall()
    return {row[0] for row in rows}


def parsed_duckdb_observability_metadata(
    *,
    object_store: ObjectStoreResource,
    source_duckdb: LocalDuckDBResource,
    documents_key: str,
) -> dict[str, Any]:
    xml_documents_frame = load_xml_document_catalog_frame(
        object_store,
        documents_key=documents_key,
    )
    statement_documents_frame = load_parsed_table_frame(
        source_duckdb,
        table=tables.STATEMENT_DOCUMENTS_TABLE,
    )
    facts_frame = load_parsed_table_frame(
        source_duckdb,
        table=tables.FACTS_TABLE,
    )
    quality = build_parse_quality_row(
        xml_documents_frame=xml_documents_frame,
        statement_documents_frame=statement_documents_frame,
        facts_frame=facts_frame,
        generated_at=datetime.now(UTC).isoformat(),
    )
    concept_rows = build_concept_profile_rows(facts_frame, example_limit=3)
    return {
        "xml_documents_count": quality["xml_documents_count"],
        "statement_documents_count": quality["statement_documents_count"],
        "facts_count": quality["facts_count"],
        "statements_with_zero_facts_count": quality["statements_with_zero_facts_count"],
        "duplicate_statement_keys_count": quality["duplicate_statement_keys_count"],
        "parser_warning_statements_count": quality["parser_warning_statements_count"],
        "missing_statement_business_ids_count": quality["missing_statement_business_ids_count"],
        "missing_statement_financial_dates_count": quality[
            "missing_statement_financial_dates_count"
        ],
        "missing_fact_business_ids_count": quality["missing_fact_business_ids_count"],
        "missing_fact_financial_dates_count": quality["missing_fact_financial_dates_count"],
        "facts_per_statement_min": quality["facts_per_statement_min"],
        "facts_per_statement_avg": quality["facts_per_statement_avg"],
        "facts_per_statement_max": quality["facts_per_statement_max"],
        "latest_parsed_at": quality["latest_parsed_at"],
        "top_concept_namespaces": json.loads(quality["top_concept_namespaces"]),
        "top_concepts": json.loads(quality["top_concepts"]),
        "concept_count": len(concept_rows),
    }


def _ensure_parsed_duckdb_tables(database_path: Path) -> None:
    with LocalDuckDBResource(database_path=str(database_path)).connect() as connection:
        connection.execute(f"create schema if not exists {XBRL_DLT_DATASET_NAME}")
        for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE):
            column_definitions = ", ".join(
                f"{column} {_duckdb_column_type(column)}"
                for column in tables.TABLE_COLUMNS[table]
            )
            connection.execute(
                f"create table if not exists {XBRL_DLT_DATASET_NAME}.{table} ({column_definitions})"
            )


def _duckdb_table_exists(connection: Any, *, table: str) -> bool:
    return bool(
        connection.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = ?
              and table_name = ?
            """,
            [XBRL_DLT_DATASET_NAME, table],
        ).fetchone()[0]
    )


def _duckdb_column_type(column: str) -> str:
    dtype = _xbrl_column_dtype(column)
    if dtype == pl.Boolean:
        return "boolean"
    if dtype == pl.Int64:
        return "bigint"
    if dtype == pl.Float64:
        return "double"
    return "varchar"


def build_parse_quality_row(
    *,
    xml_documents_frame: pl.DataFrame,
    statement_documents_frame: pl.DataFrame,
    facts_frame: pl.DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "xml_documents_count": xml_documents_frame.height,
        "statement_documents_count": statement_documents_frame.height,
        "facts_count": facts_frame.height,
        "statements_with_zero_facts_count": _zero_facts_statement_count(
            statement_documents_frame
        ),
        "duplicate_statement_keys_count": _duplicate_count(
            statement_documents_frame,
            column="statement_key",
        ),
        "parser_warning_statements_count": _non_empty_string_count(
            statement_documents_frame,
            column="validation_warnings",
            ignore_values={"[]"},
        ),
        "missing_statement_business_ids_count": _missing_string_count(
            statement_documents_frame,
            column="business_id",
        ),
        "missing_statement_financial_dates_count": _missing_string_count(
            statement_documents_frame,
            column="financial_date",
        ),
        "missing_fact_business_ids_count": _missing_string_count(facts_frame, column="business_id"),
        "missing_fact_financial_dates_count": _missing_string_count(
            facts_frame,
            column="financial_date",
        ),
        "facts_per_statement_min": _numeric_column_stat(
            statement_documents_frame,
            column="facts_count",
            statistic="min",
            default=0,
        ),
        "facts_per_statement_avg": _numeric_column_stat(
            statement_documents_frame,
            column="facts_count",
            statistic="mean",
            default=0.0,
        ),
        "facts_per_statement_max": _numeric_column_stat(
            statement_documents_frame,
            column="facts_count",
            statistic="max",
            default=0,
        ),
        "latest_parsed_at": _max_string(statement_documents_frame, column="parsed_at"),
        "top_concept_namespaces": json.dumps(
            _top_counts(facts_frame, column="concept_namespace", count_name="count"),
            ensure_ascii=False,
        ),
        "top_concepts": json.dumps(
            _top_counts(facts_frame, column="concept_qname", count_name="count"),
            ensure_ascii=False,
        ),
    }


def build_concept_profile_rows(
    facts_frame: pl.DataFrame,
    *,
    example_limit: int = 5,
) -> list[dict[str, Any]]:
    if facts_frame.height == 0:
        return []

    concept_columns = ["concept_qname", "concept_namespace", "concept_local_name"]
    concepts = facts_frame.select(concept_columns).unique().sort("concept_qname").to_dicts()
    rows: list[dict[str, Any]] = []
    for concept in concepts:
        concept_facts = facts_frame.filter(
            (pl.col("concept_qname") == concept["concept_qname"])
            & (pl.col("concept_namespace") == concept["concept_namespace"])
            & (pl.col("concept_local_name") == concept["concept_local_name"])
        )
        rows.append(
            {
                "concept_qname": concept["concept_qname"],
                "concept_namespace": concept["concept_namespace"],
                "concept_local_name": concept["concept_local_name"],
                "facts_count": concept_facts.height,
                "statement_count": _unique_non_empty_count(concept_facts, column="statement_key"),
                "business_count": _unique_non_empty_count(concept_facts, column="business_id"),
                "financial_date_count": _unique_non_empty_count(
                    concept_facts,
                    column="financial_date",
                ),
                "numeric_count": _value_kind_count(concept_facts, value_kind="numeric"),
                "date_count": _value_kind_count(concept_facts, value_kind="date"),
                "text_count": _value_kind_count(concept_facts, value_kind="text"),
                "current_period_count": _current_period_count(concept_facts),
                "comparative_count": _comparative_count(concept_facts),
                "example_numeric_values": json.dumps(
                    _example_values(concept_facts, column="numeric_value", limit=example_limit),
                    ensure_ascii=False,
                ),
                "example_date_values": json.dumps(
                    _example_values(concept_facts, column="date_value", limit=example_limit),
                    ensure_ascii=False,
                ),
                "example_text_values": json.dumps(
                    _example_values(concept_facts, column="text_value", limit=example_limit),
                    ensure_ascii=False,
                ),
            }
        )

    return sorted(rows, key=lambda row: (-int(row["facts_count"]), str(row["concept_qname"])))


def merge_xml_document_catalog(
    *,
    object_store: ObjectStoreResource,
    new_rows: list[dict[str, Any]],
) -> pl.DataFrame:
    new_frame = _xbrl_table_frame(tables.XML_DOCUMENTS_TABLE, new_rows)
    if object_store.exists(RAW_XML_DOCUMENTS_OBJECT_KEY, bucket=XBRL_BUCKET):
        existing_frame = pl.read_parquet(
            BytesIO(object_store.read_bytes(RAW_XML_DOCUMENTS_OBJECT_KEY, bucket=XBRL_BUCKET))
        )
        catalog = pl.concat([existing_frame, new_frame], how="vertical_relaxed")
    else:
        catalog = new_frame

    if catalog.height > 0:
        catalog = catalog.unique(
            subset=["business_id", "financial_date", "xml_object_key"],
            keep="last",
            maintain_order=True,
        )
    output = BytesIO()
    catalog.select(tables.TABLE_COLUMNS[tables.XML_DOCUMENTS_TABLE]).write_parquet(output)
    object_store.write_bytes(RAW_XML_DOCUMENTS_OBJECT_KEY, output.getvalue(), bucket=XBRL_BUCKET)
    return catalog


def _zero_facts_statement_count(frame: pl.DataFrame) -> int:
    if "facts_count" not in frame.columns or frame.height == 0:
        return 0
    return int(frame.select((pl.col("facts_count").fill_null(0) == 0).sum()).item())


def _duplicate_count(frame: pl.DataFrame, *, column: str) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    return frame.height - int(frame.select(pl.col(column).n_unique()).item())


def _missing_string_count(frame: pl.DataFrame, *, column: str) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    return int(
        frame.select((pl.col(column).fill_null("").str.strip_chars() == "").sum()).item()
    )


def _non_empty_string_count(
    frame: pl.DataFrame,
    *,
    column: str,
    ignore_values: set[str] | None = None,
) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    ignored = ignore_values or set()
    value = pl.col(column).fill_null("").str.strip_chars()
    return int(frame.select(((value != "") & value.is_in(list(ignored)).not_()).sum()).item())


def _numeric_column_stat(
    frame: pl.DataFrame,
    *,
    column: str,
    statistic: str,
    default: int | float,
) -> int | float:
    if column not in frame.columns or frame.height == 0:
        return default
    expression = getattr(pl.col(column), statistic)()
    value = frame.select(expression).item()
    return default if value is None else value


def _max_string(frame: pl.DataFrame, *, column: str) -> str:
    if column not in frame.columns or frame.height == 0:
        return ""
    value = frame.select(pl.col(column).fill_null("").max()).item()
    return str(value or "")


def _top_counts(
    frame: pl.DataFrame,
    *,
    column: str,
    count_name: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if column not in frame.columns or frame.height == 0:
        return []
    return (
        frame.filter(pl.col(column).fill_null("").str.strip_chars() != "")
        .group_by(column)
        .len(count_name)
        .sort([count_name, column], descending=[True, False])
        .head(limit)
        .to_dicts()
    )


def _unique_non_empty_count(frame: pl.DataFrame, *, column: str) -> int:
    if column not in frame.columns or frame.height == 0:
        return 0
    return int(
        frame.filter(pl.col(column).fill_null("").str.strip_chars() != "")
        .select(pl.col(column).n_unique())
        .item()
    )


def _value_kind_count(frame: pl.DataFrame, *, value_kind: str) -> int:
    if "value_kind" not in frame.columns or frame.height == 0:
        return 0
    return int(frame.select((pl.col("value_kind") == value_kind).sum()).item())


def _comparative_count(frame: pl.DataFrame) -> int:
    if "is_comparative" not in frame.columns or frame.height == 0:
        return 0
    return int(frame.select(pl.col("is_comparative").fill_null(False).sum()).item())


def _current_period_count(frame: pl.DataFrame) -> int:
    if "is_comparative" not in frame.columns or frame.height == 0:
        return frame.height
    return int(frame.select(pl.col("is_comparative").fill_null(False).not_().sum()).item())


def _example_values(frame: pl.DataFrame, *, column: str, limit: int) -> list[str]:
    if column not in frame.columns or frame.height == 0:
        return []
    return (
        frame.select(pl.col(column).fill_null("").cast(pl.Utf8).str.strip_chars())
        .filter(pl.col(column) != "")
        .unique(maintain_order=True)
        .head(limit)
        .to_series()
        .to_list()
    )


def _required_iso_date(value: str, *, field_name: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} is required")
    _parse_iso_date(stripped, field_name=field_name)
    return stripped


def _optional_iso_date(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("date config values must be strings")
    stripped = value.strip()
    if not stripped:
        return None
    _parse_iso_date(stripped, field_name="date config value")
    return stripped


def _parse_iso_date(value: str, *, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from error


def _xml_document_row(
    document: dict[str, Any],
    *,
    discovery_registered_date_start: str,
    discovery_registered_date_end: str,
    financial_start_date: str,
    max_reports: int | None,
    selected_at: str,
) -> dict[str, Any]:
    downloaded = bool(document["downloaded"])
    return {
        "business_id": document["business_id"],
        "financial_date": document["financial_date"],
        "registration_date": document.get("registration_date", ""),
        "source_url": document.get("source_url", ""),
        "xml_object_key": document["object_key"],
        "xml_sha256": document.get("xml_sha256", ""),
        "xml_size_bytes": int(document.get("xml_size_bytes") or 0),
        "downloaded": downloaded,
        "reused": not downloaded,
        "discovery_registered_date_start": discovery_registered_date_start,
        "discovery_registered_date_end": discovery_registered_date_end,
        "financial_start_date": financial_start_date,
        "max_reports": "" if max_reports is None else str(max_reports),
        "selected_at": selected_at,
    }


def _normalize_xml_document_row(row: dict[str, Any]) -> dict[str, Any]:
    xml_object_key = str(row.get("xml_object_key") or row.get("object_key") or "").strip()
    if not xml_object_key:
        raise ValueError("XBRL document manifest row is missing xml_object_key")
    return {
        "business_id": str(row.get("business_id") or ""),
        "financial_date": str(row.get("financial_date") or ""),
        "registration_date": row.get("registration_date"),
        "source_url": str(row.get("source_url") or ""),
        "xml_object_key": xml_object_key,
    }


def _xbrl_table_frame(table: str, rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={column: _xbrl_column_dtype(column) for column in tables.TABLE_COLUMNS[table]},
    )


def _table_row(table: str, row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.TABLE_COLUMNS[table]}


def _xbrl_column_dtype(column: str) -> pl.DataType:
    if column in {
        "xml_size_bytes",
        "statement_count",
        "business_count",
        "financial_date_count",
        "numeric_count",
        "date_count",
        "text_count",
        "current_period_count",
        "comparative_count",
        "xml_documents_count",
        "statement_documents_count",
        "facts_count",
        "statements_with_zero_facts_count",
        "duplicate_statement_keys_count",
        "parser_warning_statements_count",
        "missing_statement_business_ids_count",
        "missing_statement_financial_dates_count",
        "missing_fact_business_ids_count",
        "missing_fact_financial_dates_count",
        "facts_per_statement_min",
        "facts_per_statement_max",
        "contexts_count",
        "units_count",
        "fact_ordinal",
        "source_fact_count",
        "mapped_fact_count",
        "unmapped_numeric_fact_count",
    }:
        return pl.Int64
    if column == "facts_per_statement_avg":
        return pl.Float64
    if column in {"downloaded", "reused", "is_comparative"}:
        return pl.Boolean
    return pl.Utf8
