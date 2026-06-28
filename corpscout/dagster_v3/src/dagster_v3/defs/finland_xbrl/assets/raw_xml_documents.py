import time
from datetime import UTC, date, datetime
from hashlib import sha256
from io import BytesIO
from typing import Any, Callable

import dagster as dg
import polars as pl
from dagster_duckdb import DuckDBResource
from pydantic import field_validator

from dagster_v3.defs.common.duckdb_resources import read_only_duckdb_connection
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.common import (
    BACKFILL_PARTITIONS,
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    FINLAND_XBRL_DUCKDB_POOL,
    RAW_XML_DOCUMENTS_OBJECT_KEY,
    XBRL_BUCKET,
    XBRL_DLT_DATASET_NAME,
    XBRL_DLT_FINANCIAL_REPORTS_TABLE,
    XBRL_ELIGIBLE_COMPANIES_TABLE,
    _registration_window,
)
from dagster_v3.defs.finland_xbrl.assets.eligible_companies import (
    finland_xbrl_eligible_companies,
)
from dagster_v3.defs.finland_xbrl.assets.financial_reports import (
    finland_xbrl_financial_reports_backfill_duckdb,
    finland_xbrl_financial_reports_incremental_duckdb,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlApiResource

class XbrlRawConfig(dg.Config):
    refresh_existing: bool = False
    download_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS

    @field_validator("download_delay_seconds")
    @classmethod
    def validate_download_delay_seconds(cls, value: float) -> float:
        if value < 0:
            raise ValueError("download_delay_seconds must be zero or greater")
        return value


def download_finland_xbrl_raw_xml_documents(
    *,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    financial_reports: list[dict[str, Any]],
    refresh_existing: bool,
    download_delay_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
    log_info: Callable[[str], None] | None = None,
    progress_interval: int = 25,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(XBRL_BUCKET)

    documents: list[dict[str, Any]] = []
    downloaded_count = 0
    reused_count = 0
    bytes_downloaded = 0
    selected_at = datetime.now(UTC).isoformat()
    selected_financial_reports = financial_reports
    total_reports = len(selected_financial_reports)
    _log_raw_xml_download(
        log_info,
        f"XBRL raw XML download started: reports={total_reports} "
        f"refresh_existing={refresh_existing}",
    )

    for report_index, report in enumerate(selected_financial_reports, start=1):
        business_id = str(report["business_id"])
        financial_date = str(report["financial_date"])
        registration_date = _optional_string(report.get("registration_date"))
        object_key = document_object_key(business_id, financial_date)
        action = "downloaded"
        if not refresh_existing and object_store.exists(object_key, bucket=XBRL_BUCKET):
            action = "reused"
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
        else:
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
        if _should_log_raw_xml_progress(
            report_index=report_index,
            total_reports=total_reports,
            progress_interval=progress_interval,
        ):
            _log_raw_xml_download(
                log_info,
                "XBRL raw XML document "
                f"{report_index}/{total_reports}: "
                f"business_id={business_id} "
                f"financial_date={financial_date} "
                f"action={action} "
                f"downloaded={downloaded_count} "
                f"reused={reused_count} "
                f"bytes_downloaded={bytes_downloaded}",
            )
        if download_delay_seconds > 0 and report_index < total_reports:
            sleep(download_delay_seconds)

    xml_document_rows = [
        _xml_document_row(
            document,
            discovery_registered_date_start=document["discovery_registered_date_start"],
            discovery_registered_date_end=document["discovery_registered_date_end"],
            selected_at=selected_at,
        )
        for document in documents
    ]
    xml_documents_catalog = merge_xml_document_catalog(
        object_store=object_store,
        new_rows=xml_document_rows,
    )
    _log_raw_xml_download(
        log_info,
        "XBRL raw XML download completed: "
        f"selected_reports={len(selected_financial_reports)} "
        f"documents={len(documents)} "
        f"downloaded={downloaded_count} "
        f"reused={reused_count} "
        f"bytes_downloaded={bytes_downloaded} "
        f"catalog_rows={xml_documents_catalog.height}",
    )

    return dg.MaterializeResult(
        metadata={
            "selected_reports_count": len(selected_financial_reports),
            "documents_count": len(documents),
            "downloaded_count": downloaded_count,
            "reused_count": reused_count,
            "bytes_downloaded": bytes_downloaded,
            "xml_documents_object_key": RAW_XML_DOCUMENTS_OBJECT_KEY,
            "xml_documents_catalog_count": xml_documents_catalog.height,
        }
    )



def _materialize_raw_xml_window(
    context: dg.AssetExecutionContext,
    config: XbrlRawConfig,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    source_duckdb: DuckDBResource,
    *,
    registered_date_start: str,
    registered_date_end: str,
) -> dg.MaterializeResult:
    context.log.info(
        "XBRL raw XML partition %s: loading eligible reports registered %s..%s",
        context.partition_key,
        registered_date_start,
        registered_date_end,
    )
    financial_reports = load_eligible_financial_report_rows(
        source_duckdb,
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
    )
    context.log.info(
        "XBRL raw XML partition %s: %d eligible financial reports selected",
        context.partition_key,
        len(financial_reports),
    )
    result = download_finland_xbrl_raw_xml_documents(
        xbrl_api=xbrl_api,
        object_store=object_store,
        financial_reports=financial_reports,
        refresh_existing=config.refresh_existing,
        download_delay_seconds=config.download_delay_seconds,
        log_info=context.log.info,
    )
    context.log.info(
        "XBRL raw XML partition %s complete: selected=%s downloaded=%s reused=%s catalog_rows=%s",
        context.partition_key,
        result.metadata["selected_reports_count"],
        result.metadata["downloaded_count"],
        result.metadata["reused_count"],
        result.metadata["xml_documents_catalog_count"],
    )
    return dg.MaterializeResult(
        metadata={
            **result.metadata,
            "partition": context.partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
        }
    )


@dg.asset(
    name="finland_xbrl_raw_xml_documents_backfill",
    group_name="finland_xbrl",
    deps=[
        finland_xbrl_financial_reports_backfill_duckdb,
        finland_xbrl_eligible_companies,
    ],
    partitions_def=BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "s3", "xml"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
)
def finland_xbrl_raw_xml_documents_backfill(
    context: dg.AssetExecutionContext,
    config: XbrlRawConfig,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return _materialize_raw_xml_window(
        context,
        config,
        xbrl_api,
        object_store,
        source_duckdb,
        registered_date_start=start,
        registered_date_end=end,
    )


@dg.asset(
    name="finland_xbrl_raw_xml_documents_incremental",
    group_name="finland_xbrl",
    deps=[
        finland_xbrl_financial_reports_incremental_duckdb,
        finland_xbrl_eligible_companies,
    ],
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "s3", "xml"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
)
def finland_xbrl_raw_xml_documents_incremental(
    context: dg.AssetExecutionContext,
    config: XbrlRawConfig,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return _materialize_raw_xml_window(
        context,
        config,
        xbrl_api,
        object_store,
        source_duckdb,
        registered_date_start=start,
        registered_date_end=end,
    )


@dg.asset(
    name="finland_xbrl_raw_xml_documents",
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents_backfill, finland_xbrl_raw_xml_documents_incremental],
    kinds={"python", "s3", "xml"},
)
def finland_xbrl_raw_xml_documents(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    """Catalog marker for PRH XBRL XML statements downloaded into object storage."""
    context.log.info(
        "Loading Finland XBRL raw XML document catalog from s3://%s/%s",
        XBRL_BUCKET,
        RAW_XML_DOCUMENTS_OBJECT_KEY,
    )
    frame = load_xml_document_catalog_frame(
        object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY
    )
    context.log.info("Finland XBRL raw XML document catalog row_count=%d", frame.height)
    return dg.MaterializeResult(
        metadata={
            "bucket": XBRL_BUCKET,
            "object_key": RAW_XML_DOCUMENTS_OBJECT_KEY,
            "row_count": frame.height,
        }
    )


@dg.asset(
    name=tables.XML_DOCUMENTS_TABLE,
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents],
    kinds={"python", "s3", "parquet"},
)
def finland_xbrl_xml_documents(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    """Catalog of raw Finland PRH XBRL XML documents available in object storage."""
    context.log.info(
        "Loading Finland XBRL XML document table marker from s3://%s/%s",
        XBRL_BUCKET,
        RAW_XML_DOCUMENTS_OBJECT_KEY,
    )
    frame = load_xml_document_catalog_frame(object_store, documents_key=RAW_XML_DOCUMENTS_OBJECT_KEY)
    context.log.info("Finland XBRL XML document table marker row_count=%d", frame.height)
    return dg.MaterializeResult(
        metadata={
            "bucket": XBRL_BUCKET,
            "object_key": RAW_XML_DOCUMENTS_OBJECT_KEY,
            "row_count": frame.height,
        }
    )


def load_eligible_financial_report_rows(
    source_duckdb: DuckDBResource,
    *,
    registered_date_start: str,
    registered_date_end: str,
) -> list[dict[str, Any]]:
    with read_only_duckdb_connection(source_duckdb) as connection:
        rows = connection.execute(
            f"""
            select
                reports.business_id,
                reports.financial_date,
                reports.registration_date,
                reports.discovery_registered_date_start,
                reports.discovery_registered_date_end
            from {XBRL_DLT_DATASET_NAME}.{XBRL_DLT_FINANCIAL_REPORTS_TABLE} as reports
            inner join {XBRL_DLT_DATASET_NAME}.{XBRL_ELIGIBLE_COMPANIES_TABLE} as companies
                on reports.business_id = companies.business_id
            where reports.registration_date >= ?
              and reports.registration_date <= ?
            order by reports.registration_date, reports.financial_date, reports.business_id
            """,
            [registered_date_start, registered_date_end],
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


def _should_log_raw_xml_progress(
    *,
    report_index: int,
    total_reports: int,
    progress_interval: int,
) -> bool:
    if total_reports == 0:
        return False
    if report_index == 1 or report_index == total_reports:
        return True
    return progress_interval > 0 and report_index % progress_interval == 0


def _log_raw_xml_download(
    log_info: Callable[[str], None] | None,
    message: str,
) -> None:
    if log_info is not None:
        log_info(message)


def resolve_xbrl_documents_key(
    *,
    config: Any,
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


def merge_xml_document_catalog(
    *,
    object_store: ObjectStoreResource,
    new_rows: list[dict[str, Any]],
) -> pl.DataFrame:
    new_frame = pl.DataFrame(new_rows, schema=tables.XML_DOCUMENTS_POLARS_SCHEMA)
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
    catalog.select(tables.XML_DOCUMENTS_COLUMNS).write_parquet(output)
    object_store.write_bytes(RAW_XML_DOCUMENTS_OBJECT_KEY, output.getvalue(), bucket=XBRL_BUCKET)
    return catalog

def _xml_document_row(
    document: dict[str, Any],
    *,
    discovery_registered_date_start: str,
    discovery_registered_date_end: str,
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
        "financial_start_date": "",
        "max_reports": "",
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
