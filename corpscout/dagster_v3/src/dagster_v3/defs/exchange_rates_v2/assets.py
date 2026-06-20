from collections.abc import Iterator, Mapping
from datetime import date, timedelta
import json
import os
from pathlib import Path
from typing import Any

import dagster as dg
import dlt as dlt_lib
import duckdb
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
    get_asset_key_for_model,
)
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.clickhouse.resolved import export_duckdb_table_to_clickhouse
from dagster_v3.defs.duckdb.schema_contract import validate_duckdb_table_contract
from dagster_v3.defs.exchange_rates_v2 import tables
from dagster_v3.defs.exchange_rates_v2.source import (
    EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME,
    EXCHANGE_RATES_V2_DUCKDB_PIPELINE_NAME,
    EXCHANGE_RATES_V2_RAW_DLT_TABLE,
    exchange_rates_v2_raw_range_source,
)

GROUP_NAME = "exchange_rates_v2"
EXCHANGE_RATES_V2_DUCKDB_PATH = Path(
    os.environ.get("EXCHANGE_RATES_V2_DUCKDB_PATH", "data/exchange_rates_v2_source.duckdb")
).expanduser()
if not EXCHANGE_RATES_V2_DUCKDB_PATH.is_absolute():
    EXCHANGE_RATES_V2_DUCKDB_PATH = EXCHANGE_RATES_V2_DUCKDB_PATH.resolve()
# Monthly (not daily) partitions: this is a tiny daily-grained reference dataset,
# but per-partition materialization bookkeeping is written to the Postgres event
# log, so a daily full-history backfill emits ~1265 events and exhausts Postgres
# connections. Monthly cuts that ~30x while each partition still fetches/builds a
# whole month of daily rates. end_offset=1 keeps the in-progress current month a
# valid partition so a daily schedule can refresh month-to-date.
EXCHANGE_RATES_V2_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2023-01-01", end_offset=1
)
# multi_run (one partition per run) + a concurrency pool is how partition
# materialization is throttled internally: a UI/daemon backfill of N months
# creates N small runs (~2 materializations each, so no event-log connection
# spike), and the pool caps how many run at once. The instance defaults every
# pool to limit 1 (dagster.yaml concurrency.pools.default_limit), so the runs
# execute one at a time — which is also required because all three assets write
# one single-writer DuckDB file. Raise the pool limit to run more concurrently
# (`dagster instance concurrency set exchange_rates_v2_duckdb N`), but N>1 needs
# per-run DuckDB isolation to be safe.
EXCHANGE_RATES_V2_BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
EXCHANGE_RATES_V2_DUCKDB_POOL = "exchange_rates_v2_duckdb"
EXCHANGE_RATES_V2_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
CLICKHOUSE_EXPORT_TABLE = "clickhouse_exchange_rates"

os.environ["EXCHANGE_RATES_V2_DUCKDB_PATH"] = str(EXCHANGE_RATES_V2_DUCKDB_PATH)
exchange_rates_v2_dbt_project = DbtProject(
    project_dir=EXCHANGE_RATES_V2_DBT_PROJECT_DIR,
    profiles_dir=EXCHANGE_RATES_V2_DBT_PROJECT_DIR,
)
exchange_rates_v2_dbt_project.prepare_if_dev()


class ExchangeRatesV2DltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.table_name != EXCHANGE_RATES_V2_RAW_DLT_TABLE:
            return spec
        return spec.replace_attributes(
            key="exchange_rates_v2_raw_duckdb",
            deps=[],
            group_name=GROUP_NAME,
            description="Raw ECB exchange-rate API payloads stored in v2 DuckDB.",
            kinds={"dlt", "duckdb", "reference", "fx"},
        )


class ExchangeRatesV2DbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return super().get_asset_key(dbt_resource_props)
        return dg.AssetKey(f"exchange_rates_v2_{dbt_resource_props['name']}")

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return GROUP_NAME


class ExchangeRatesV2Config(dg.Config):
    # USD is always fetched (required base); EUR is the base currency and is never
    # a quote, so it is intentionally not listed here.
    currencies: list[str] = ["USD", "NOK", "GBP", "SEK", "DKK"]


def month_partition_window(partition_key: str) -> tuple[str, str]:
    """Map a monthly partition key (first-of-month) to its [first, last] day."""
    first = date.fromisoformat(partition_key)
    next_first = (
        first.replace(year=first.year + 1, month=1)
        if first.month == 12
        else first.replace(month=first.month + 1)
    )
    last = next_first - timedelta(days=1)
    return first.isoformat(), last.isoformat()


def month_partition_range_window(
    *,
    start_partition_key: str,
    end_partition_key: str,
) -> tuple[str, str]:
    start_date, _ = month_partition_window(start_partition_key)
    _, end_date = month_partition_window(end_partition_key)
    return start_date, end_date


@dlt_assets(
    dlt_source=exchange_rates_v2_raw_range_source(
        start_date="2023-01-01",
        end_date="2023-01-01",
        currencies=[],
    ),
    dlt_pipeline=dlt_lib.pipeline(
        pipeline_name=EXCHANGE_RATES_V2_DUCKDB_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(EXCHANGE_RATES_V2_DUCKDB_PATH)),
        dataset_name=EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME,
        dev_mode=False,
    ),
    name="exchange_rates_v2_raw_duckdb",
    dagster_dlt_translator=ExchangeRatesV2DltTranslator(),
    partitions_def=EXCHANGE_RATES_V2_PARTITIONS,
    backfill_policy=EXCHANGE_RATES_V2_BACKFILL_POLICY,
    pool=EXCHANGE_RATES_V2_DUCKDB_POOL,
)
def exchange_rates_v2_raw_duckdb_asset(
    context: AssetExecutionContext,
    config: ExchangeRatesV2Config,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    start_date, end_date = _context_partition_range(context)
    EXCHANGE_RATES_V2_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Loading raw ECB exchange-rate v2 payload to DuckDB: duckdb_path=%s, "
        "start_date=%s, end_date=%s, currencies=%s",
        EXCHANGE_RATES_V2_DUCKDB_PATH,
        start_date,
        end_date,
        config.currencies,
    )
    yield from dlt.run(
        context=context,
        dlt_source=exchange_rates_v2_raw_range_source(
            start_date=start_date,
            end_date=end_date,
            currencies=config.currencies,
            source_run_id=context.run_id,
        ),
        dlt_pipeline=dlt_lib.pipeline(
            pipeline_name=EXCHANGE_RATES_V2_DUCKDB_PIPELINE_NAME,
            destination=dlt_lib.destinations.duckdb(str(EXCHANGE_RATES_V2_DUCKDB_PATH)),
            dataset_name=EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME,
            dev_mode=False,
        ),
    )


@dbt_assets(
    manifest=exchange_rates_v2_dbt_project.manifest_path,
    project=exchange_rates_v2_dbt_project,
    dagster_dbt_translator=ExchangeRatesV2DbtTranslator(),
    partitions_def=EXCHANGE_RATES_V2_PARTITIONS,
    backfill_policy=EXCHANGE_RATES_V2_BACKFILL_POLICY,
    pool=EXCHANGE_RATES_V2_DUCKDB_POOL,
)
def exchange_rates_v2_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
) -> Iterator[Any]:
    start_date, end_date = _context_partition_range(context)
    yield from dbt.cli(
        [
            "build",
            "--vars",
            json.dumps({"start_date": start_date, "end_date": end_date}),
        ],
        context=context,
    ).stream()


@dg.asset(
    deps=[
        get_asset_key_for_model([exchange_rates_v2_dbt_assets], "ecb_rates"),
        get_asset_key_for_model([exchange_rates_v2_dbt_assets], "identity_rates"),
    ],
    partitions_def=EXCHANGE_RATES_V2_PARTITIONS,
    backfill_policy=EXCHANGE_RATES_V2_BACKFILL_POLICY,
    pool=EXCHANGE_RATES_V2_DUCKDB_POOL,
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    description="Exchange-rate v2 dbt rows exported from DuckDB to migrated ClickHouse table.",
)
def exchange_rates_v2_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    start_date, end_date = _context_partition_range(context)
    counts = export_exchange_rates_v2_clickhouse(
        duckdb_path=EXCHANGE_RATES_V2_DUCKDB_PATH,
        clickhouse=clickhouse,
        start_date=start_date,
        end_date=end_date,
    )
    context.log.info("Exported exchange-rate v2 rows to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def export_exchange_rates_v2_clickhouse(
    *,
    duckdb_path: str | Path,
    clickhouse: ClickhouseResource,
    start_date: str,
    end_date: str,
) -> dict[str, int | str]:
    with duckdb.connect(str(duckdb_path)) as connection:
        _validate_exchange_rates_v2_duckdb_table(connection, "ecb_rates")
        _validate_exchange_rates_v2_duckdb_table(connection, "identity_rates")
        connection.execute(
            f"""
            create or replace table {EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME}.{CLICKHOUSE_EXPORT_TABLE}
            as
            select {", ".join(tables.EXCHANGE_RATES_V2_COLUMNS)}
            from {EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME}.ecb_rates
            where rate_date >= '{_sql_escape(start_date)}' and rate_date <= '{_sql_escape(end_date)}'
            union all
            select {", ".join(tables.EXCHANGE_RATES_V2_COLUMNS)}
            from {EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME}.identity_rates
            where rate_date >= '{_sql_escape(start_date)}' and rate_date <= '{_sql_escape(end_date)}'
            """
        )
    with clickhouse.get_connection() as client:
        delete_exchange_rates_v2_window(client, start_date=start_date, end_date=end_date)
        row_count = export_duckdb_table_to_clickhouse(
            duckdb_path=duckdb_path,
            clickhouse_client=client,
            duckdb_schema=EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME,
            duckdb_table=CLICKHOUSE_EXPORT_TABLE,
            clickhouse_database=tables.EXCHANGE_RATES_V2_DATABASE,
            clickhouse_table=tables.EXCHANGE_RATES_V2_TABLE,
            columns=tables.EXCHANGE_RATES_V2_COLUMNS,
            truncate=False,
        )
    return {
        "rows": row_count,
        "table": tables.QUALIFIED_EXCHANGE_RATES_V2_TABLE,
        "start_date": start_date,
        "end_date": end_date,
    }


def delete_exchange_rates_v2_window(
    client: Any,
    *,
    start_date: str,
    end_date: str,
) -> None:
    client.execute(
        f"ALTER TABLE {tables.QUALIFIED_EXCHANGE_RATES_V2_TABLE} DELETE WHERE "
        "source IN ('ECB EXR', 'identity') "
        f"AND rate_date >= '{_sql_escape(start_date)}' "
        f"AND rate_date <= '{_sql_escape(end_date)}'"
    )


exchange_rates_v2_selection = dg.AssetSelection.assets(
    "exchange_rates_v2_clickhouse"
).upstream()

exchange_rates_v2_job = dg.define_asset_job(
    "exchange_rates_v2_job",
    selection=exchange_rates_v2_selection,
    # Explicitly partition the job: the selection spans partitioned assets and
    # non-partitioned upstream nodes, so without this the job resolves
    # non-partitioned and the assets fail on context.partition_key_range. Pin it
    # to the asset partitions so it runs per-partition. To repopulate history,
    # launch a UI/daemon backfill over the month range: the multi_run policy
    # makes one small run per partition and the exchange_rates_v2_duckdb pool
    # throttles how many run at once (default 1).
    partitions_def=EXCHANGE_RATES_V2_PARTITIONS,
)


def _validate_exchange_rates_v2_duckdb_table(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> None:
    validate_duckdb_table_contract(
        connection,
        schema=EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME,
        table=table,
        contract=tables.EXCHANGE_RATES_V2_DUCKDB_CONTRACT,
    )


def _context_partition_range(context: AssetExecutionContext) -> tuple[str, str]:
    partition_key_range = context.partition_key_range
    return month_partition_range_window(
        start_partition_key=partition_key_range.start,
        end_partition_key=partition_key_range.end,
    )


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


defs = dg.Definitions(
    assets=[
        exchange_rates_v2_raw_duckdb_asset,
        exchange_rates_v2_dbt_assets,
        exchange_rates_v2_clickhouse,
    ],
    jobs=[exchange_rates_v2_job],
    resources={
        "dbt": DbtCliResource(
            project_dir=exchange_rates_v2_dbt_project,
            profiles_dir=EXCHANGE_RATES_V2_DBT_PROJECT_DIR,
        )
    },
)
