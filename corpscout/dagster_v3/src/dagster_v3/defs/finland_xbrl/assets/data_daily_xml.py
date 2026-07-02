import time
from collections.abc import Callable

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import (
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily_duckdb_ch import data_daily_duckdb_ch
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
    download_finland_xbrl_snapshot_xml_partition,
)
from dagster_v3.defs.finland_xbrl.resources import XbrlApiResource


def materialize_data_daily_xml(
    *,
    partition_key: str,
    xbrl_api: XbrlApiResource,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    download_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    return download_finland_xbrl_snapshot_xml_partition(
        partition_key=partition_key,
        registered_date_start=partition_key,
        registered_date_end=partition_key,
        xbrl_api=xbrl_api,
        clickhouse=clickhouse,
        object_store=object_store,
        download_delay_seconds=download_delay_seconds,
        sleep=sleep,
        log_info=log_info,
    )


@dg.asset(
    name="data_daily_xml",
    group_name="finland_xbrl",
    deps=[data_daily_duckdb_ch],
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "s3", "xml", "clickhouse"},
    description=(
        "Downloads daily Finland XBRL statement XML files for the daily "
        "registration-date partition."
    ),
)
def data_daily_xml(
    context: dg.AssetExecutionContext,
    xbrl_api: XbrlApiResource,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return materialize_data_daily_xml(
        partition_key=context.partition_key,
        xbrl_api=xbrl_api,
        clickhouse=clickhouse,
        object_store=object_store,
        log_info=context.log.info,
    )
