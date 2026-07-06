from collections.abc import Iterator, Mapping
from datetime import date
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
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import export_duckdb_connection_table_to_clickhouse
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.duckdb.schema_contract import validate_duckdb_table_contract
from dagster_v3.defs.exchange_rates_v2 import tables
from dagster_v3.defs.exchange_rates_v2.source import (
    ECB_REFERENCE_CURRENCIES,
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
# No partitions. A single non-partitioned run pulls [START_DATE, today], with the
# ECB API requests split into calendar-year batches to keep each payload bounded.
# The run rebuilds the DuckDB dbt tables and republishes the entire ClickHouse
# window. This also means one materialization event per asset per run (not one
# per month), so it can never storm the shared Postgres event log — the original
# reason partitions existed. The daily schedule just re-runs the full pull;
# ReplacingMergeTree(pulled_at) + the per-run DELETE window keep it idempotent.
EXCHANGE_RATES_V2_START_DATE = "2006-01-01"
# All three DuckDB-writing assets share one pool so overlapping runs (e.g. a
# manual run racing the daily schedule) serialize against the single-writer file.
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
    # Pull the full ECB euro reference-rate currency set so every country we add
    # already has EUR->its-currency for USD conversion — no per-country re-fetch.
    # USD is always fetched regardless; EUR is the base and is never a quote.
    currencies: list[str] = list(ECB_REFERENCE_CURRENCIES)


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
    pool=EXCHANGE_RATES_V2_DUCKDB_POOL,
)
def exchange_rates_v2_raw_duckdb_asset(
    context: AssetExecutionContext,
    config: ExchangeRatesV2Config,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    start_date, end_date = _full_range()
    EXCHANGE_RATES_V2_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Loading raw ECB exchange-rate v2 payload to DuckDB: duckdb_path=%s, "
        "start_date=%s, end_date=%s, currencies=%s",
        EXCHANGE_RATES_V2_DUCKDB_PATH,
        start_date,
        end_date,
        config.currencies,
    )
    with duckdb.connect(str(EXCHANGE_RATES_V2_DUCKDB_PATH)) as connection:
        _clear_exchange_rates_v2_raw_window(
            connection,
            start_date=start_date,
            end_date=end_date,
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
    pool=EXCHANGE_RATES_V2_DUCKDB_POOL,
)
def exchange_rates_v2_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
) -> Iterator[Any]:
    start_date, end_date = _full_range()
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
    pool=EXCHANGE_RATES_V2_DUCKDB_POOL,
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    description="Exchange-rate v2 dbt rows exported from DuckDB to migrated ClickHouse table.",
)
def exchange_rates_v2_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    exchange_rates_v2_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    start_date, end_date = _full_range()
    with exchange_rates_v2_duckdb.get_connection() as connection:
        counts = export_exchange_rates_v2_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            start_date=start_date,
            end_date=end_date,
        )
    context.log.info("Exported exchange-rate v2 rows to ClickHouse", extra=counts)
    return dg.MaterializeResult(metadata=counts)


def export_exchange_rates_v2_clickhouse(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
    start_date: str,
    end_date: str,
) -> dict[str, int | str]:
    _validate_exchange_rates_v2_duckdb_table(duckdb_connection, "ecb_rates")
    _validate_exchange_rates_v2_duckdb_table(duckdb_connection, "identity_rates")
    duckdb_connection.execute(
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
        row_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
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
)

# Replaces the retired v1 daily schedule. Weekdays 18:30 Belgrade, after ECB's
# ~16:00 CET reference-rate publish. Non-partitioned: each run re-pulls the full
# [START_DATE, today] window in one request, so it both backfills history and
# picks up the new day.
exchange_rates_v2_daily_schedule = dg.ScheduleDefinition(
    name="exchange_rates_v2_daily_schedule",
    job=exchange_rates_v2_job,
    cron_schedule="30 18 * * 1-5",
    execution_timezone="Europe/Belgrade",
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


def _full_range() -> tuple[str, str]:
    """The whole reference window: [START_DATE, today]. The source batches it by year."""
    return EXCHANGE_RATES_V2_START_DATE, date.today().isoformat()


def _clear_exchange_rates_v2_raw_window(
    connection: duckdb.DuckDBPyConnection,
    *,
    start_date: str,
    end_date: str,
) -> None:
    table_exists = bool(
        connection.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME, EXCHANGE_RATES_V2_RAW_DLT_TABLE],
        ).fetchone()[0]
    )
    if not table_exists:
        return
    connection.execute(
        f"""
        delete from {EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME}.{EXCHANGE_RATES_V2_RAW_DLT_TABLE}
        where cast(start_date as date) <= cast(? as date)
          and cast(end_date as date) >= cast(? as date)
        """,
        [end_date, start_date],
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
    schedules=[exchange_rates_v2_daily_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=exchange_rates_v2_dbt_project,
            profiles_dir=EXCHANGE_RATES_V2_DBT_PROJECT_DIR,
        ),
        "exchange_rates_v2_duckdb": duckdb_resource(EXCHANGE_RATES_V2_DUCKDB_PATH),
    },
)
