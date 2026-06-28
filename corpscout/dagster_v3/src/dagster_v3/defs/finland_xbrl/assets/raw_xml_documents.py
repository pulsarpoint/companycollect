import time
from datetime import UTC, datetime
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable

import dagster as dg
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.common import (
    BACKFILL_PARTITIONS,
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    XBRL_BUCKET,
    _registration_window,
)
from dagster_v3.defs.finland_xbrl.assets.eligible_companies import (
    finland_xbrl_eligible_companies,
)
from dagster_v3.defs.finland_xbrl.assets.financial_reports import (
    finland_xbrl_financial_reports_backfill,
    finland_xbrl_financial_reports_incremental,
)
from dagster_v3.defs.finland_xbrl.resources import (
    XbrlApiResource,
    XbrlParquetStorageResource,
)

class XbrlRawConfig(dg.Config):
    refresh_existing: bool = False
    download_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS

    @field_validator("download_delay_seconds")
    @classmethod
    def validate_download_delay_seconds(cls, value: float) -> float:
        if value < 0:
            raise ValueError("download_delay_seconds must be zero or greater")
        return value


@dataclass(frozen=True)
class RawXmlDownloadResult:
    rows: list[dict[str, Any]]
    metadata: dict[str, Any]


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
) -> RawXmlDownloadResult:
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
    _log_raw_xml_download(
        log_info,
        "XBRL raw XML download completed: "
        f"selected_reports={len(selected_financial_reports)} "
        f"documents={len(documents)} "
        f"downloaded={downloaded_count} "
        f"reused={reused_count} "
        f"bytes_downloaded={bytes_downloaded}",
    )

    return RawXmlDownloadResult(
        rows=xml_document_rows,
        metadata={
            "selected_reports_count": len(selected_financial_reports),
            "documents_count": len(documents),
            "downloaded_count": downloaded_count,
            "reused_count": reused_count,
            "bytes_downloaded": bytes_downloaded,
        },
    )



def _materialize_raw_xml_window(
    context: dg.AssetExecutionContext,
    config: XbrlRawConfig,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    *,
    registered_date_start: str,
    registered_date_end: str,
    read_financial_reports: Callable[[str], list[dict[str, Any]]],
    read_eligible_companies: Callable[[], list[dict[str, Any]]],
    write_raw_xml_documents: Callable[[str, list[dict[str, Any]]], Path],
) -> dg.MaterializeResult:
    context.log.info(
        "XBRL raw XML partition %s: loading eligible reports registered %s..%s",
        context.partition_key,
        registered_date_start,
        registered_date_end,
    )
    financial_report_rows = read_financial_reports(context.partition_key)
    financial_reports = load_eligible_financial_report_rows(
        eligible_companies=read_eligible_companies(),
        financial_reports=financial_report_rows,
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
    parquet_path = write_raw_xml_documents(context.partition_key, result.rows)
    context.log.info(
        "XBRL raw XML partition %s complete: selected=%s downloaded=%s reused=%s manifest_rows=%s parquet_path=%s",
        context.partition_key,
        result.metadata["selected_reports_count"],
        result.metadata["downloaded_count"],
        result.metadata["reused_count"],
        len(result.rows),
        parquet_path,
    )
    return dg.MaterializeResult(
        metadata={
            **result.metadata,
            "partition": context.partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "raw_xml_documents_parquet_path": str(parquet_path),
            "raw_xml_documents_row_count": len(result.rows),
        }
    )


@dg.asset(
    name="finland_xbrl_raw_xml_documents_backfill",
    group_name="finland_xbrl",
    deps=[
        finland_xbrl_financial_reports_backfill,
        finland_xbrl_eligible_companies,
    ],
    partitions_def=BACKFILL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "s3", "xml"},
)
def finland_xbrl_raw_xml_documents_backfill(
    context: dg.AssetExecutionContext,
    config: XbrlRawConfig,
    xbrl_api: XbrlApiResource,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return _materialize_raw_xml_window(
        context,
        config,
        xbrl_api,
        object_store,
        registered_date_start=start,
        registered_date_end=end,
        read_financial_reports=xbrl_parquet_storage.read_financial_reports_backfill,
        read_eligible_companies=xbrl_parquet_storage.read_eligible_companies,
        write_raw_xml_documents=xbrl_parquet_storage.write_raw_xml_documents_backfill,
    )


@dg.asset(
    name="finland_xbrl_raw_xml_documents_incremental",
    group_name="finland_xbrl",
    deps=[
        finland_xbrl_financial_reports_incremental,
        finland_xbrl_eligible_companies,
    ],
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "s3", "xml"},
)
def finland_xbrl_raw_xml_documents_incremental(
    context: dg.AssetExecutionContext,
    config: XbrlRawConfig,
    xbrl_api: XbrlApiResource,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    start, end = _registration_window(context)
    return _materialize_raw_xml_window(
        context,
        config,
        xbrl_api,
        object_store,
        registered_date_start=start,
        registered_date_end=end,
        read_financial_reports=xbrl_parquet_storage.read_financial_reports_incremental,
        read_eligible_companies=xbrl_parquet_storage.read_eligible_companies,
        write_raw_xml_documents=xbrl_parquet_storage.write_raw_xml_documents_incremental,
    )


@dg.asset(
    name="finland_xbrl_raw_xml_documents",
    group_name="finland_xbrl",
    deps=[finland_xbrl_raw_xml_documents_backfill, finland_xbrl_raw_xml_documents_incremental],
    kinds={"python", "s3", "xml"},
)
def finland_xbrl_raw_xml_documents(
    context: dg.AssetExecutionContext,
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    """Catalog marker for PRH XBRL XML statements downloaded into object storage."""
    backfill_count = xbrl_parquet_storage.raw_xml_documents_backfill_row_count()
    incremental_count = xbrl_parquet_storage.raw_xml_documents_incremental_row_count()
    row_count = backfill_count + incremental_count
    context.log.info(
        "Finland XBRL raw XML document partition manifests row_count=%d backfill=%d incremental=%d",
        row_count,
        backfill_count,
        incremental_count,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket": XBRL_BUCKET,
            "row_count": row_count,
            "backfill_row_count": backfill_count,
            "incremental_row_count": incremental_count,
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
    xbrl_parquet_storage: XbrlParquetStorageResource,
) -> dg.MaterializeResult:
    """Catalog of raw Finland PRH XBRL XML documents available in object storage."""
    backfill_count = xbrl_parquet_storage.raw_xml_documents_backfill_row_count()
    incremental_count = xbrl_parquet_storage.raw_xml_documents_incremental_row_count()
    row_count = backfill_count + incremental_count
    context.log.info(
        "Finland XBRL XML document table marker row_count=%d backfill=%d incremental=%d",
        row_count,
        backfill_count,
        incremental_count,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket": XBRL_BUCKET,
            "row_count": row_count,
            "backfill_row_count": backfill_count,
            "incremental_row_count": incremental_count,
        }
    )


def load_eligible_financial_report_rows(
    *,
    eligible_companies: list[dict[str, Any]],
    financial_reports: list[dict[str, Any]],
    registered_date_start: str,
    registered_date_end: str,
) -> list[dict[str, Any]]:
    eligible_business_ids = {company["business_id"] for company in eligible_companies}
    return sorted(
        [
            report
            for report in financial_reports
            if report["business_id"] in eligible_business_ids
            and registered_date_start <= report["registration_date"] <= registered_date_end
        ],
        key=lambda report: (
            report["registration_date"],
            report["financial_date"],
            report["business_id"],
        ),
    )


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
