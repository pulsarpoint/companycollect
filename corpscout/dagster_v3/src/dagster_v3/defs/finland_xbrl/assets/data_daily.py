import time
from collections.abc import Callable
from typing import Any

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import (
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    XBRL_BUCKET,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot import (
    build_financial_data_snapshot_csv,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlApiResource

FINANCIAL_DATA_DAILY_KEY_PREFIX = "financial_data/daily"


def financial_data_daily_key(partition_key: str) -> str:
    return (
        f"{FINANCIAL_DATA_DAILY_KEY_PREFIX}/"
        f"registeredDateStart={partition_key}/"
        f"registeredDateEnd={partition_key}/"
        "financial_statements.csv"
    )


def write_financial_data_daily_csv(
    *,
    partition_key: str,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    s3_key = financial_data_daily_key(partition_key)
    if object_store.exists(s3_key, bucket=XBRL_BUCKET):
        _log_daily(
            log_info,
            "Reusing existing Finland XBRL financial data daily CSV: "
            f"bucket={XBRL_BUCKET} key={s3_key}",
        )
        return _materialize_result(
            partition_key=partition_key,
            s3_key=s3_key,
            downloaded=False,
            reused_existing_snapshot=True,
            row_count=None,
            csv_size_bytes=None,
        )

    _log_daily(
        log_info,
        "Downloading Finland XBRL financial data daily CSV: "
        f"registeredDateStart={partition_key} registeredDateEnd={partition_key}",
    )
    financials = [
        listing.financial
        for listing in xbrl_api.iter_financial_reports(
            registered_date_start=partition_key,
            registered_date_end=partition_key,
            request_delay_seconds=request_delay_seconds,
            sleep=sleep,
            log_info=log_info,
        )
    ]
    csv_body = build_financial_data_snapshot_csv(financials).encode("utf-8")
    object_store.ensure_bucket(XBRL_BUCKET)
    object_store.write_bytes(s3_key, csv_body, bucket=XBRL_BUCKET)
    _log_daily(
        log_info,
        "Completed Finland XBRL financial data daily CSV: "
        f"bucket={XBRL_BUCKET} key={s3_key} rows={len(financials)} bytes={len(csv_body)}",
    )
    return _materialize_result(
        partition_key=partition_key,
        s3_key=s3_key,
        downloaded=True,
        reused_existing_snapshot=False,
        row_count=len(financials),
        csv_size_bytes=len(csv_body),
    )


@dg.asset(
    name="data_daily",
    group_name="finland_xbrl",
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "s3", "csv", "prh"},
    description=(
        "Creates one daily PRH XBRL financial statement listing CSV in S3. Each "
        "partition calls /all_financial_statements with registeredDateStart and "
        "registeredDateEnd both equal to the partition date, starting 2026-06-01."
    ),
)
def data_daily(
    context: dg.AssetExecutionContext,
    xbrl_api: XbrlApiResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return write_financial_data_daily_csv(
        partition_key=context.partition_key,
        xbrl_api=xbrl_api,
        object_store=object_store,
        log_info=context.log.info,
    )


def _materialize_result(
    *,
    partition_key: str,
    s3_key: str,
    downloaded: bool,
    reused_existing_snapshot: bool,
    row_count: int | None,
    csv_size_bytes: int | None,
) -> dg.MaterializeResult:
    metadata: dict[str, Any] = {
        "partition": partition_key,
        "s3_bucket": XBRL_BUCKET,
        "s3_key": s3_key,
        "registered_date_start": partition_key,
        "registered_date_end": partition_key,
        "downloaded": downloaded,
        "reused_existing_snapshot": reused_existing_snapshot,
    }
    if row_count is not None:
        metadata["row_count"] = row_count
    if csv_size_bytes is not None:
        metadata["csv_size_bytes"] = csv_size_bytes
    return dg.MaterializeResult(metadata=metadata)


def _log_daily(log_info: Callable[[str], None] | None, message: str) -> None:
    if log_info is not None:
        log_info(message)
