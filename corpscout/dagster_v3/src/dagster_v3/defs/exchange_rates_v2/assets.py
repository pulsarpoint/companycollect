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

from dagster_v3.defs.clickhouse.resolved import export_duckdb_table_to_clickhouse
from dagster_v3.defs.duckdb.schema_contract import validate_duckdb_table_contract
from dagster_v3.defs.exchange_rates_v2 import tables
from dagster_v3.defs.exchange_rates_v2.source import (
    DEFAULT_CLICKHOUSE_NATIVE_PORT,
    EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME,
    EXCHANGE_RATES_V2_DUCKDB_PIPELINE_NAME,
    EXCHANGE_RATES_V2_RAW_DLT_TABLE,
    exchange_rates_v2_raw_range_source,
)

GROUP_NAME = "exchange_rates_v2"
EXCHANGE_RATES_V2_DUCKDB_PATH = Path("data/exchange_rates_v2_source.duckdb")
EXCHANGE_RATES_V2_PARTITIONS = dg.DailyPartitionsDefinition(start_date="2023-01-01")
EXCHANGE_RATES_V2_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
CLICKHOUSE_EXPORT_TABLE = "clickhouse_exchange_rates"

os.environ.setdefault("EXCHANGE_RATES_V2_DUCKDB_PATH", str(EXCHANGE_RATES_V2_DUCKDB_PATH))
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
    currencies: list[str] = ["NOK", "USD", "EUR", "GBP", "SEK", "DKK"]


def day_partition_window(partition_key: str) -> tuple[str, str]:
    day = date.fromisoformat(partition_key)
    return day.isoformat(), day.isoformat()


def day_partition_range_window(
    *,
    start_partition_key: str,
    end_partition_key: str,
) -> tuple[str, str]:
    start_date, _ = day_partition_window(start_partition_key)
    _, end_date = day_partition_window(end_partition_key)
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
            where rate_date >= '{_duckdb_string(start_date)}' and rate_date <= '{_duckdb_string(end_date)}'
            union all
            select {", ".join(tables.EXCHANGE_RATES_V2_COLUMNS)}
            from {EXCHANGE_RATES_V2_DUCKDB_DATASET_NAME}.identity_rates
            where rate_date >= '{_duckdb_string(start_date)}' and rate_date <= '{_duckdb_string(end_date)}'
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
        "ALTER TABLE reference.exchange_rates DELETE WHERE "
        "source IN ('ECB EXR', 'identity') "
        f"AND rate_date >= '{_sql_string(start_date)}' "
        f"AND rate_date <= '{_sql_string(end_date)}'"
    )


exchange_rates_v2_selection = dg.AssetSelection.assets(
    "exchange_rates_v2_clickhouse"
).upstream()

exchange_rates_v2_job = dg.define_asset_job(
    "exchange_rates_v2_job",
    selection=exchange_rates_v2_selection,
)


def clickhouse_resource_from_env() -> ClickhouseResource:
    return ClickhouseResource(
        host=dg.EnvVar("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_NATIVE_PORT", DEFAULT_CLICKHOUSE_NATIVE_PORT),
        user=dg.EnvVar("CLICKHOUSE_USER"),
        password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
        database=dg.EnvVar("CLICKHOUSE_DATABASE"),
        secure=_bool_env("CLICKHOUSE_SECURE", False),
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
    return day_partition_range_window(
        start_partition_key=partition_key_range.start,
        end_partition_key=partition_key_range.end,
    )


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _duckdb_string(value: str) -> str:
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
