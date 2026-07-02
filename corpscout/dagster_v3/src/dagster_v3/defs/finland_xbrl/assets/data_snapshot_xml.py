import json
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import (
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    XBRL_BUCKET,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb_ch import (
    DATA_SNAPSHOT_CLICKHOUSE_TABLE,
)
from dagster_v3.defs.finland_xbrl.clickhouse import CLICKHOUSE_DATABASE
from dagster_v3.defs.finland_xbrl.resources import XbrlApiResource

XML_SNAPSHOT_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2023-07-01",
    end_date="2026-06-01",
)
XML_SNAPSHOT_PREFIX = "financial_data/xml_snapshot"


def xml_snapshot_partition_prefix(registered_date_start: str, registered_date_end: str) -> str:
    return (
        f"{XML_SNAPSHOT_PREFIX}/"
        f"registeredDateStart={registered_date_start}/"
        f"registeredDateEnd={registered_date_end}"
    )


def xml_snapshot_document_key(
    registered_date_start: str,
    registered_date_end: str,
    business_id: str,
    financial_date: str,
) -> str:
    return (
        f"{xml_snapshot_partition_prefix(registered_date_start, registered_date_end)}/"
        f"companies/{business_id}/{financial_date}.xml"
    )


def xml_snapshot_manifest_key(
    registered_date_start: str,
    registered_date_end: str,
) -> str:
    return f"{xml_snapshot_partition_prefix(registered_date_start, registered_date_end)}/manifest.jsonl"


def xml_snapshot_success_key(
    registered_date_start: str,
    registered_date_end: str,
) -> str:
    return f"{xml_snapshot_partition_prefix(registered_date_start, registered_date_end)}/_SUCCESS.json"


def fetch_xml_snapshot_report_rows(
    *,
    clickhouse: ClickhouseResource,
    registered_date_start: str,
    registered_date_end: str,
) -> list[dict[str, str]]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(DATA_SNAPSHOT_CLICKHOUSE_TABLE,),
    )
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT DISTINCT
                business_id,
                toString(financial_date) AS financial_date,
                toString(registration_date) AS registration_date
            FROM {CLICKHOUSE_DATABASE}.{DATA_SNAPSHOT_CLICKHOUSE_TABLE}
            WHERE registration_date >= toDate(%(start)s)
              AND registration_date <= toDate(%(end)s)
            ORDER BY registration_date, business_id, financial_date
            """,
            {
                "start": registered_date_start,
                "end": registered_date_end,
            },
        )
    return [
        {
            "business_id": str(row[0]),
            "financial_date": str(row[1]),
            "registration_date": str(row[2]),
        }
        for row in rows
    ]


def download_finland_xbrl_snapshot_xml_partition(
    *,
    partition_key: str,
    registered_date_start: str,
    registered_date_end: str,
    xbrl_api: XbrlApiResource,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    download_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    prefix = xml_snapshot_partition_prefix(registered_date_start, registered_date_end)
    manifest_key = xml_snapshot_manifest_key(registered_date_start, registered_date_end)
    success_key = xml_snapshot_success_key(registered_date_start, registered_date_end)

    if object_store.exists(success_key, bucket=XBRL_BUCKET):
        _log_xml_snapshot(
            log_info,
            "Finland XBRL XML snapshot partition already complete; skipping: "
            f"partition={partition_key} bucket={XBRL_BUCKET} success_key={success_key}",
        )
        return _materialize_result(
            partition_key=partition_key,
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            s3_prefix=prefix,
            manifest_key=manifest_key,
            success_key=success_key,
            selected_reports_count=0,
            downloaded_count=0,
            reused_count=0,
            bytes_downloaded=0,
            skipped_existing_partition=True,
        )

    object_store.ensure_bucket(XBRL_BUCKET)
    reports = fetch_xml_snapshot_report_rows(
        clickhouse=clickhouse,
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
    )
    _log_xml_snapshot(
        log_info,
        "Finland XBRL XML snapshot partition download started: "
        f"partition={partition_key} reports={len(reports)} "
        f"registered_date_start={registered_date_start} registered_date_end={registered_date_end}",
    )

    manifest_rows: list[dict[str, Any]] = []
    downloaded_count = 0
    reused_count = 0
    bytes_downloaded = 0
    total_reports = len(reports)
    for report_index, report in enumerate(reports, start=1):
        business_id = report["business_id"]
        financial_date = report["financial_date"]
        registration_date = report["registration_date"]
        xml_key = xml_snapshot_document_key(
            registered_date_start,
            registered_date_end,
            business_id,
            financial_date,
        )
        source_url = xbrl_api.statement_xml_url(business_id, financial_date)
        downloaded = False
        if object_store.exists(xml_key, bucket=XBRL_BUCKET):
            body = object_store.read_bytes(xml_key, bucket=XBRL_BUCKET)
            reused_count += 1
        else:
            body, source_url = xbrl_api.download_statement_xml(business_id, financial_date)
            object_store.write_bytes(xml_key, body, bucket=XBRL_BUCKET)
            downloaded = True
            downloaded_count += 1
            bytes_downloaded += len(body)

        manifest_rows.append(
            {
                "business_id": business_id,
                "financial_date": financial_date,
                "registration_date": registration_date,
                "source_url": source_url,
                "xml_object_key": xml_key,
                "xml_sha256": sha256(body).hexdigest(),
                "xml_size_bytes": len(body),
                "downloaded": downloaded,
                "reused": not downloaded,
                "registered_date_start": registered_date_start,
                "registered_date_end": registered_date_end,
                "downloaded_at": datetime.now(UTC).isoformat(),
            }
        )
        if _should_log_xml_snapshot_progress(report_index, total_reports):
            _log_xml_snapshot(
                log_info,
                "Finland XBRL XML snapshot progress: "
                f"partition={partition_key} {report_index}/{total_reports} "
                f"business_id={business_id} financial_date={financial_date} "
                f"downloaded={downloaded_count} reused={reused_count}",
            )
        if download_delay_seconds > 0 and report_index < total_reports:
            sleep(download_delay_seconds)

    manifest_body = "".join(
        json.dumps(row, sort_keys=True) + "\n" for row in manifest_rows
    ).encode("utf-8")
    object_store.write_bytes(manifest_key, manifest_body, bucket=XBRL_BUCKET)
    success_body = json.dumps(
        {
            "partition": partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "selected_reports_count": total_reports,
            "downloaded_count": downloaded_count,
            "reused_count": reused_count,
            "bytes_downloaded": bytes_downloaded,
            "manifest_key": manifest_key,
            "completed_at": datetime.now(UTC).isoformat(),
        },
        sort_keys=True,
    ).encode("utf-8")
    object_store.write_bytes(success_key, success_body, bucket=XBRL_BUCKET)
    _log_xml_snapshot(
        log_info,
        "Finland XBRL XML snapshot partition complete: "
        f"partition={partition_key} selected={total_reports} "
        f"downloaded={downloaded_count} reused={reused_count} bytes={bytes_downloaded}",
    )

    return _materialize_result(
        partition_key=partition_key,
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
        s3_prefix=prefix,
        manifest_key=manifest_key,
        success_key=success_key,
        selected_reports_count=total_reports,
        downloaded_count=downloaded_count,
        reused_count=reused_count,
        bytes_downloaded=bytes_downloaded,
        skipped_existing_partition=False,
    )


@dg.asset(
    name="data_snapshot_xml",
    group_name="finland_xbrl",
    deps=[dg.AssetKey("data_snapshot_duckdb_ch")],
    partitions_def=XML_SNAPSHOT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "s3", "xml", "clickhouse"},
    description=(
        "Downloads historical Finland XBRL statement XML files into monthly "
        "S3 snapshot folders from ClickHouse financial statement listings."
    ),
)
def data_snapshot_xml(
    context: dg.AssetExecutionContext,
    xbrl_api: XbrlApiResource,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    start, end = _xml_snapshot_registration_window(context)
    return download_finland_xbrl_snapshot_xml_partition(
        partition_key=context.partition_key,
        registered_date_start=start,
        registered_date_end=end,
        xbrl_api=xbrl_api,
        clickhouse=clickhouse,
        object_store=object_store,
        log_info=context.log.info,
    )


def _xml_snapshot_registration_window(
    context: dg.AssetExecutionContext,
) -> tuple[str, str]:
    window = context.partition_time_window
    start = window.start.date().isoformat()
    end = (window.end.date() - timedelta(days=1)).isoformat()
    return start, end


def _materialize_result(
    *,
    partition_key: str,
    registered_date_start: str,
    registered_date_end: str,
    s3_prefix: str,
    manifest_key: str,
    success_key: str,
    selected_reports_count: int,
    downloaded_count: int,
    reused_count: int,
    bytes_downloaded: int,
    skipped_existing_partition: bool,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "partition": partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "selected_reports_count": selected_reports_count,
            "downloaded_count": downloaded_count,
            "reused_count": reused_count,
            "bytes_downloaded": bytes_downloaded,
            "s3_bucket": XBRL_BUCKET,
            "s3_prefix": s3_prefix,
            "manifest_key": manifest_key,
            "success_key": success_key,
            "skipped_existing_partition": skipped_existing_partition,
        }
    )


def _should_log_xml_snapshot_progress(report_index: int, total_reports: int) -> bool:
    if total_reports == 0:
        return False
    return report_index == 1 or report_index == total_reports or report_index % 25 == 0


def _log_xml_snapshot(log_info: Callable[[str], None] | None, message: str) -> None:
    if log_info is not None:
        log_info(message)
