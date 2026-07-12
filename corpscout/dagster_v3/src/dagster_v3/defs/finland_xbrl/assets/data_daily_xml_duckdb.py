from collections.abc import Callable
from pathlib import Path

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import (
    DAILY_PARTITIONS,
    FINLAND_XBRL_DUCKDB_POOL,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily_xml import data_daily_xml
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    StatementParser,
    materialize_data_snapshot_xml_duckdb,
    xml_daily_parse_duckdb_path,
    xml_daily_parse_temp_dir,
)
from dagster_v3.defs.finland_xbrl.parser import parse_statement_xml


def materialize_data_daily_xml_duckdb(
    *,
    partition_key: str,
    object_store: ObjectStoreResource,
    duckdb_path: Path,
    temp_dir: Path,
    run_id: str,
    log_info: Callable[[str], None] | None = None,
    parser: StatementParser = parse_statement_xml,
) -> dg.MaterializeResult:
    return materialize_data_snapshot_xml_duckdb(
        partition_key=partition_key,
        registered_date_start=partition_key,
        registered_date_end=partition_key,
        object_store=object_store,
        duckdb_path=duckdb_path,
        temp_dir=temp_dir,
        run_id=run_id,
        log_info=log_info,
        parser=parser,
    )


@dg.asset(
    name="data_daily_xml_duckdb",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[data_daily_xml],
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "duckdb", "parquet", "xml"},
    description=(
        "Parses daily Finland XBRL XML files into a partition-scoped DuckDB database."
    ),
)
def data_daily_xml_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return materialize_data_daily_xml_duckdb(
        partition_key=context.partition_key,
        object_store=object_store,
        duckdb_path=xml_daily_parse_duckdb_path(context.partition_key),
        temp_dir=xml_daily_parse_temp_dir(context.partition_key),
        run_id=context.run.run_id,
        log_info=context.log.info,
    )
