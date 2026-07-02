import csv
import time
from collections.abc import Callable
from io import StringIO
from typing import Any

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import (
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    XBRL_BUCKET,
)
from dagster_v3.defs.finland_xbrl.resources import (
    PRH_XBRL_REGISTRATION_SEARCH_START,
    XbrlApiResource,
)

FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START = PRH_XBRL_REGISTRATION_SEARCH_START
FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END = "2026-06-01"
FINANCIAL_DATA_S3_SNAPSHOT_KEY = (
    "financial_data/snapshot/"
    f"registeredDateStart={FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START}/"
    f"registeredDateEnd={FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END}/"
    "financial_statements.csv"
)
FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS = (
    "businessId",
    "financialDate",
    "registrationDate",
)


def build_financial_data_snapshot_csv(financials: list[dict[str, Any]]) -> str:
    output = StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=list(FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS),
        extrasaction="ignore",
    )
    writer.writeheader()
    for financial in financials:
        writer.writerow(
            {
                "businessId": _csv_value(financial.get("businessId")),
                "financialDate": _csv_value(financial.get("financialDate")),
                "registrationDate": _csv_value(financial.get("registrationDate")),
            }
        )
    return output.getvalue()


def write_financial_data_snapshot_csv(
    *,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    if object_store.exists(FINANCIAL_DATA_S3_SNAPSHOT_KEY, bucket=XBRL_BUCKET):
        _log_snapshot(
            log_info,
            "Reusing existing Finland XBRL financial data S3 snapshot: "
            f"bucket={XBRL_BUCKET} key={FINANCIAL_DATA_S3_SNAPSHOT_KEY}",
        )
        return _materialize_result(
            downloaded=False,
            reused_existing_snapshot=True,
            row_count=None,
            csv_size_bytes=None,
        )

    _log_snapshot(
        log_info,
        "Downloading Finland XBRL financial data S3 snapshot: "
        f"registeredDateStart={FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START} "
        f"registeredDateEnd={FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END}",
    )
    financials = [
        listing.financial
        for listing in xbrl_api.iter_financial_reports(
            registered_date_start=FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START,
            registered_date_end=FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END,
            request_delay_seconds=request_delay_seconds,
            sleep=sleep,
            log_info=log_info,
        )
    ]
    csv_body = build_financial_data_snapshot_csv(financials).encode("utf-8")
    object_store.ensure_bucket(XBRL_BUCKET)
    object_store.write_bytes(
        FINANCIAL_DATA_S3_SNAPSHOT_KEY,
        csv_body,
        bucket=XBRL_BUCKET,
    )
    _log_snapshot(
        log_info,
        "Completed Finland XBRL financial data S3 snapshot: "
        f"bucket={XBRL_BUCKET} key={FINANCIAL_DATA_S3_SNAPSHOT_KEY} "
        f"rows={len(financials)} bytes={len(csv_body)}",
    )
    return _materialize_result(
        downloaded=True,
        reused_existing_snapshot=False,
        row_count=len(financials),
        csv_size_bytes=len(csv_body),
    )


@dg.asset(
    name="finacial_data_s3_snapshot",
    group_name="finland_xbrl",
    kinds={"python", "s3", "csv", "prh"},
    description=(
        "Creates the fixed initial PRH XBRL financial statement listing snapshot "
        "as CSV in S3. It pulls /all_financial_statements from registeredDateStart "
        f"{FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START} through "
        f"registeredDateEnd {FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END}, "
        "stores only businessId, financialDate, and registrationDate, and skips "
        "the download when the fixed S3 object already exists."
    ),
)
def finacial_data_s3_snapshot(
    context: dg.AssetExecutionContext,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return write_financial_data_snapshot_csv(
        xbrl_api=xbrl_api,
        object_store=object_store,
        log_info=context.log.info,
    )


def _materialize_result(
    *,
    downloaded: bool,
    reused_existing_snapshot: bool,
    row_count: int | None,
    csv_size_bytes: int | None,
) -> dg.MaterializeResult:
    metadata: dict[str, Any] = {
        "s3_bucket": XBRL_BUCKET,
        "s3_key": FINANCIAL_DATA_S3_SNAPSHOT_KEY,
        "registered_date_start": FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START,
        "registered_date_end": FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END,
        "columns": list(FINANCIAL_DATA_S3_SNAPSHOT_COLUMNS),
        "downloaded": downloaded,
        "reused_existing_snapshot": reused_existing_snapshot,
    }
    if row_count is not None:
        metadata["row_count"] = row_count
    if csv_size_bytes is not None:
        metadata["csv_size_bytes"] = csv_size_bytes
    return dg.MaterializeResult(metadata=metadata)


def _csv_value(value: Any) -> str:
    return str(value or "").strip()


def _log_snapshot(log_info: Callable[[str], None] | None, message: str) -> None:
    if log_info is not None:
        log_info(message)
