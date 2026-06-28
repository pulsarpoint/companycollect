import asyncio
import os
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol

import dagster as dg
import dlt
import duckdb
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    read_only_duckdb_connection,
)
from dagster_v3.defs.norway_brreg import resources
from dagster_v3.defs.norway_brreg import tables
from dagster_v3.defs.norway_brreg.financial_fetches import (
    BRREG_FINANCIAL_FETCHES_COLUMNS,
    FINANCIAL_FETCHES_TABLE,
    run_brreg_financial_statement_fetches,
)
from dagster_v3.defs.norway_brreg.financial_normalize import (
    build_financial_statement_rows as build_normalized_financial_statement_rows,
)
from dagster_v3.defs.norway_brreg.financial_normalize import (
    build_financial_statement_rows_from_fetch_rows,
)
from exchange_rates import ExchangeRateClient, ExchangeRateRequest
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from translator.task_queues import BUILD_TASK_QUEUE
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    BuildQueueWorkflowInput,
)

BRREG_BASE_URL = resources.BRREG_BASE_URL
BRREG_FINANCIAL_STATEMENTS_COLUMNS = resources.BRREG_FINANCIAL_STATEMENTS_COLUMNS
BRREG_REGNSKAP_BASE_URL = resources.BRREG_REGNSKAP_BASE_URL
DLT_DATASET_NAME = resources.DLT_DATASET_NAME
ENTITIES_TABLE = resources.ENTITIES_TABLE

GROUP_NAME = "norway_brreg"
NORWAY_BRREG_DUCKDB_POOL = "norway_brreg_duckdb"
FINANCIAL_STATEMENTS_TABLE = "financial_statements"
FINANCIAL_SOURCE_SLUG = "norway_brregregnskap"
NORWAY_BRREG_DUCKDB_PATH = Path("data/norway_brreg_source.duckdb")


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[ExchangeRateRequest]) -> dict[tuple[str, str], Any]: ...


class NorwayBrregDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        resource_name = data.resource.name
        if resource_name == ENTITIES_TABLE:
            return spec.replace_attributes(
                key=dg.AssetKey("norway_brreg_entities_duckdb"),
                deps=[],
                group_name=GROUP_NAME,
                description="Norway Brreg entity bulk data loaded to local DuckDB with dlt.",
                kinds={"python", "dlt", "duckdb"},
            )
        return spec


@dlt_assets(
    dlt_source=resources.norway_brreg_entities_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="norway_brreg_entities",
        destination=dlt.destinations.duckdb(str(NORWAY_BRREG_DUCKDB_PATH)),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
    ),
    name="norway_brreg_entities_duckdb",
    dagster_dlt_translator=NorwayBrregDltTranslator(),
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_brreg_entities_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    norway_brreg_duckdb: DuckDBResource,
) -> Iterator[Any]:
    """Load Brreg entity bulk data to local DuckDB with dlt."""
    NORWAY_BRREG_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Norway Brreg entity dlt load: source_url=%s, duckdb_path=%s, "
        "dataset=%s, table=%s",
        f"{BRREG_BASE_URL}/enheter/lastned",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
    )
    yield from dlt.run(context=context)
    with read_only_duckdb_connection(norway_brreg_duckdb) as connection:
        row_count = _duckdb_table_count(
            duckdb_connection=connection,
            table_name=f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}",
        )
    context.log.info(
        "Completed Norway Brreg entity dlt load: duckdb_path=%s, dataset=%s, table=%s, rows=%s",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        row_count,
    )


@dg.asset(
    name="norway_brreg_financial_fetches_duckdb",
    deps=[dg.AssetKey("norway_brreg_entities_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Resumable Norway Brreg annual-account fetch outcomes stored in DuckDB.",
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_brreg_financial_fetches_duckdb_asset(
    context: AssetExecutionContext,
    norway_brreg_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    duckdb_path = duckdb_database_path(norway_brreg_duckdb)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Norway Brreg financial fetch load: source_url=%s, duckdb_path=%s, "
        "input_table=%s.%s, output_table=%s.%s",
        BRREG_REGNSKAP_BASE_URL,
        duckdb_path,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        DLT_DATASET_NAME,
        FINANCIAL_FETCHES_TABLE,
    )
    with norway_brreg_duckdb.get_connection() as connection:
        counts = run_brreg_financial_statement_fetches(
            duckdb_connection=connection,
            source_run_id=context.run_id,
            base_url=BRREG_REGNSKAP_BASE_URL,
            log=context.log.info,
        )
        row_count = _duckdb_table_count(
            duckdb_connection=connection,
            table_name=f"{DLT_DATASET_NAME}.{FINANCIAL_FETCHES_TABLE}",
        )
        status_counts = _duckdb_fetch_status_counts(
            duckdb_connection=connection,
            table_name=f"{DLT_DATASET_NAME}.{FINANCIAL_FETCHES_TABLE}",
        )
    context.log.info(
        "Completed Norway Brreg financial fetch load: duckdb_path=%s, table=%s.%s, "
        "rows=%s, statuses=%s",
        duckdb_path,
        DLT_DATASET_NAME,
        FINANCIAL_FETCHES_TABLE,
        row_count,
        status_counts,
    )
    return dg.MaterializeResult(metadata={**counts, "rows": row_count})


@dg.asset(
    name="norway_brreg_financial_statements_duckdb",
    deps=[dg.AssetKey("norway_brreg_financial_fetches_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Norway Brreg normalized annual-account rows derived from successful fetch outcomes.",
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_brreg_financial_statements_duckdb_asset(
    context: AssetExecutionContext,
    norway_brreg_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    duckdb_path = duckdb_database_path(norway_brreg_duckdb)
    context.log.info(
        "Starting Norway Brreg financial statement normalization: duckdb_path=%s, "
        "input_table=%s.%s, output_table=%s.%s",
        duckdb_path,
        DLT_DATASET_NAME,
        FINANCIAL_FETCHES_TABLE,
        DLT_DATASET_NAME,
        FINANCIAL_STATEMENTS_TABLE,
    )
    with norway_brreg_duckdb.get_connection() as connection:
        counts = normalize_norway_brreg_financial_statements_duckdb(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID = "build-queue-norway_brreg"


class NorwayBrregTranslationConfig(dg.Config):
    batch_size: int = 25
    max_tokens: int = 32768
    extra_body_json: str = '{"chat_template_kwargs": {"enable_thinking": false}}'


def build_norway_brreg_build_queue_input(
    config: NorwayBrregTranslationConfig,
) -> BuildQueueWorkflowInput:
    return BuildQueueWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path="data/translator/norway_brreg.duckdb",
        translate_workflow_id="translate-norway_brreg",
        translate_task_queue=BUILD_TASK_QUEUE,
        batch_size=config.batch_size,
        max_tokens=config.max_tokens,
        extra_body_json=config.extra_body_json,
    )


async def _start_norway_brreg_build_queue(
    temporal_address: str, config: NorwayBrregTranslationConfig
) -> str:
    client = await Client.connect(temporal_address)
    handle = await client.start_workflow(
        BuildQueueWorkflow.run,
        build_norway_brreg_build_queue_input(config),
        id=NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
        task_queue=BUILD_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return handle.result_run_id


@dg.asset(
    deps=[dg.AssetKey("norway_resolved_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "temporal"},
    description=(
        "Fire-and-forget: start (or reuse) the BuildQueueWorkflow Temporal workflow "
        "after no_companies lands in ClickHouse. BuildQueueWorkflow seeds the DuckDB "
        "queue then starts TranslateWorkflow autonomously. Does not wait for completion."
    ),
)
def norway_brreg_translation_trigger(
    context: AssetExecutionContext, config: NorwayBrregTranslationConfig
) -> dg.MaterializeResult:
    import asyncio as _asyncio
    import os

    address = os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    run_id = _asyncio.run(_start_norway_brreg_build_queue(address, config))
    context.log.info(
        "Started Norway Brreg BuildQueueWorkflow: workflow_id=%s run_id=%s",
        NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
        run_id,
    )
    return dg.MaterializeResult(
        metadata={
            "workflow_id": NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
            "workflow_run_id": run_id,
            "task_queue": BUILD_TASK_QUEUE,
        }
    )


# Monthly coordinated refresh (see dagster_v3/CLAUDE.md "Scheduling"). Loads
# entities once, runs the resolved chain (dbt -> norway_resolved_clickhouse, which
# lands corpscout.no_companies), then fires the BuildQueueWorkflow
# (returns immediately; the workflow runs async).
norway_brreg_refresh_job = dg.define_asset_job(
    "norway_brreg_refresh_job",
    selection=dg.AssetSelection.assets("norway_brreg_translation_trigger").upstream(),
)
norway_brreg_refresh_schedule = dg.ScheduleDefinition(
    name="norway_brreg_refresh_schedule",
    job=norway_brreg_refresh_job,
    cron_schedule="0 6 7 * *",  # monthly, 7th 06:00 (staggered vs estonia/latvia)
    execution_timezone="Europe/Belgrade",
)


def build_financial_statement_rows(
    records: list[dict[str, Any]],
    *,
    org: dict[str, Any],
    exchange_rates: ExchangeRates,
    run_id: str,
    source_url: str,
) -> list[dict[str, Any]]:
    return build_normalized_financial_statement_rows(
        records,
        org=org,
        exchange_rates=exchange_rates,
        run_id=run_id,
        source_url=source_url,
    )


def normalize_norway_brreg_financial_statements_duckdb(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    exchange_rates: ExchangeRates,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    fetch_rows = _fetch_duckdb_dicts(
        duckdb_connection,
        dataset=DLT_DATASET_NAME,
        table=FINANCIAL_FETCHES_TABLE,
        columns=tuple(BRREG_FINANCIAL_FETCHES_COLUMNS),
    )
    rows = build_financial_statement_rows_from_fetch_rows(
        fetch_rows,
        exchange_rates=exchange_rates,
    )
    _replace_duckdb_table_from_rows(
        duckdb_connection,
        dataset=DLT_DATASET_NAME,
        table=FINANCIAL_STATEMENTS_TABLE,
        columns=tables.copy_dlt_columns(BRREG_FINANCIAL_STATEMENTS_COLUMNS),
        rows=rows,
    )

    counts = {
        "financial_fetches": len(fetch_rows),
        "financial_statements": len(rows),
        "successful_fetches": sum(1 for row in fetch_rows if row.get("fetch_status") == "success"),
        "failed_fetches": sum(1 for row in fetch_rows if row.get("fetch_status") != "success"),
    }
    _log(
        log,
        "Normalized Norway Brreg financial statements: fetches=%s, rows=%s, "
        "successful_fetches=%s, failed_fetches=%s",
        counts["financial_fetches"],
        counts["financial_statements"],
        counts["successful_fetches"],
        counts["failed_fetches"],
    )
    return counts


def _log(log: Callable[..., None] | None, message: str, *args: Any) -> None:
    if log is not None:
        log(message, *args)


def _fetch_duckdb_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    dataset: str,
    table: str,
    columns: tuple[str, ...],
) -> list[tuple[Any, ...]]:
    select_list = ", ".join(columns)
    return connection.execute(
        f"select {select_list} from {dataset}.{table} order by org_number"
    ).fetchall()


def _fetch_duckdb_dicts(
    connection: duckdb.DuckDBPyConnection,
    *,
    dataset: str,
    table: str,
    columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows = _fetch_duckdb_rows(
        connection,
        dataset=dataset,
        table=table,
        columns=columns,
    )
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _replace_duckdb_table_from_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    dataset: str,
    table: str,
    columns: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    qualified_table = f"{dataset}.{table}"
    column_names = tuple(columns)
    column_defs = ", ".join(
        f"{column_name} {_duckdb_type_for_dlt_column(column_schema)}"
        for column_name, column_schema in columns.items()
    )
    connection.execute(f"create schema if not exists {dataset}")
    connection.execute(f"drop table if exists {qualified_table}")
    connection.execute(f"create table {qualified_table} ({column_defs})")
    if not rows:
        return
    placeholders = ", ".join("?" for _ in column_names)
    connection.executemany(
        f"""
        insert into {qualified_table} ({", ".join(column_names)})
        values ({placeholders})
        """,
        [tuple(row.get(column_name) for column_name in column_names) for row in rows],
    )


def _duckdb_type_for_dlt_column(column_schema: dict[str, Any]) -> str:
    data_type = column_schema["data_type"]
    if data_type == "text":
        return "varchar"
    if data_type == "bigint":
        return "bigint"
    if data_type == "decimal":
        return "decimal(38, 3)"
    if data_type == "bool":
        return "boolean"
    if data_type == "date":
        return "date"
    if data_type == "timestamp":
        return "timestamp"
    raise ValueError(f"Unsupported DuckDB column type: {data_type}")


def _duckdb_table_count(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> int:
    value = duckdb_connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)


def _duckdb_fetch_status_counts(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> dict[str, int]:
    rows = duckdb_connection.execute(
        f"""
        select fetch_status, count(*) as row_count
        from {table_name}
        group by fetch_status
        order by fetch_status
        """
    ).fetchall()
    return {str(status): int(row_count) for status, row_count in rows}
