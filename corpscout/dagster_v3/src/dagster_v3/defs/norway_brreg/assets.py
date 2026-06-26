import asyncio
import os
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path
from time import perf_counter
from time import sleep
from typing import Any, Protocol

import dagster as dg
import dlt
import duckdb
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.clickhouse.resolved import export_duckdb_connection_table_to_clickhouse
from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.norway_brreg import resources
from dagster_v3.defs.norway_brreg import tables
from dagster_v3.defs.norway_brreg.clickhouse import (
    prepare_norway_brreg_clickhouse_companies_table,
    prepare_norway_brreg_clickhouse_financial_statements_table,
)
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
from dagster_v3.defs.translations.assets import (
    TemporalClient,
    start_translation_workflow,
)
from exchange_rates import ExchangeRateClient, ExchangeRateRequest
from temporal.translations.queue import TranslationQueueWorkflowInput
from temporalio.client import Client
from translations.queue import (
    TranslationQueue,
    TranslationQueueItem,
    TranslationQueueSummary,
)
from translations.types import SmokeTranslationResult

BRREG_BASE_URL = resources.BRREG_BASE_URL
BRREG_FINANCIAL_STATEMENTS_COLUMNS = resources.BRREG_FINANCIAL_STATEMENTS_COLUMNS
BRREG_REGNSKAP_BASE_URL = resources.BRREG_REGNSKAP_BASE_URL
DLT_DATASET_NAME = resources.DLT_DATASET_NAME
ENTITIES_TABLE = resources.ENTITIES_TABLE

GROUP_NAME = "norway_brreg"
NORWAY_BRREG_DUCKDB_POOL = "norway_brreg_duckdb"
FINANCIAL_STATEMENTS_TABLE = "financial_statements"
FINANCIAL_SOURCE_SLUG = "norway_brregregnskap"
NORWAY_BRREG_TRANSLATION_SOURCE_SLUG = "norway-brreg"
NORWAY_BRREG_TRANSLATION_WORKFLOW_ID = "translation-norway-brreg"
NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY = dg.AssetKey(
    "norway_brreg_translation_workflow_status"
)
NORWAY_BRREG_DUCKDB_PATH = Path("data/norway_brreg_source.duckdb")
NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH = Path("data/norway_brreg_translation_queue.duckdb")
NORWAY_BRREG_LLM_TRANSLATION_FIELDS = (
    ("articles_purpose_original", "articles_purpose_en"),
    ("activity_text_original", "activity_text_en"),
    ("company_description_original", "company_description_en"),
)
NORWAY_BRREG_EN_FIELD_BY_ORIGINAL_FIELD = dict(NORWAY_BRREG_LLM_TRANSLATION_FIELDS)


class NorwayBrregTranslationConfig(dg.Config):
    batch_size: int = 50
    timeout_seconds: int = 120
    max_batch_failures: int = 0
    refresh_existing_queue: bool = False
    worker_id: str = "translation-temporal-worker"
    max_tokens: int = 4096
    extra_body_json: str = '{"chat_template_kwargs":{"enable_thinking":false}}'
    initialize_timeout_seconds: int = 300
    batch_timeout_buffer_seconds: int = 600
    summarize_timeout_seconds: int = 30
    activity_maximum_attempts: int = 1
    lease_timeout_seconds: int = 1800
    temporal_address: str = ""


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


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_entities_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    description="Norway Brreg final companies table exported to ClickHouse.",
    metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_brreg_clickhouse_companies(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    norway_brreg_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    duckdb_path = duckdb_database_path(norway_brreg_duckdb)
    context.log.info(
        "Starting Norway Brreg companies ClickHouse export: duckdb_path=%s, table=%s",
        duckdb_path,
        tables.QUALIFIED_COMPANIES_TABLE,
    )
    with read_only_duckdb_connection(norway_brreg_duckdb) as connection:
        rows = export_norway_brreg_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    context.log.info(
        "Completed Norway Brreg companies ClickHouse export: rows=%s",
        rows,
    )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": tables.QUALIFIED_COMPANIES_TABLE,
        },
    )


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    description="Norway Brreg final financial statements table exported to ClickHouse.",
    metadata={"table": tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE},
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_brreg_clickhouse_financial_statements(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    norway_brreg_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    duckdb_path = duckdb_database_path(norway_brreg_duckdb)
    context.log.info(
        "Starting Norway Brreg financial statements ClickHouse export: duckdb_path=%s, "
        "table=%s",
        duckdb_path,
        tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
    )
    with read_only_duckdb_connection(norway_brreg_duckdb) as connection:
        rows = export_norway_brreg_clickhouse_financial_statements(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    context.log.info(
        "Completed Norway Brreg financial statements ClickHouse export: rows=%s",
        rows,
    )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "table": tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
        },
    )


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_entities_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "temporal"},
    description=(
        "Seed the Norway Brreg translation queue table and start or reuse the "
        "serialized Temporal translation workflow."
    ),
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_brreg_translation_queue(
    context: AssetExecutionContext,
    config: NorwayBrregTranslationConfig,
    norway_brreg_duckdb: DuckDBResource,
    norway_brreg_translation_queue_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    source_duckdb_path = duckdb_database_path(norway_brreg_duckdb)
    queue_duckdb_path = duckdb_database_path(norway_brreg_translation_queue_duckdb)
    queue_duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    seed_started = perf_counter()
    context.log.info(
        "Starting Norway Brreg translation queue seed: queue_duckdb_path=%s "
        "source_duckdb_path=%s refresh_existing_queue=%s",
        queue_duckdb_path,
        source_duckdb_path,
        config.refresh_existing_queue,
    )
    with norway_brreg_translation_queue_duckdb.get_connection() as queue_connection:
        counts = seed_norway_brreg_translation_queue(
            source_duckdb_path=source_duckdb_path,
            queue_duckdb_connection=queue_connection,
            queue_duckdb_path=queue_duckdb_path,
            log=context.log.info,
            refresh_existing_queue=config.refresh_existing_queue,
        )
    context.log.info(
        "Finished Norway Brreg translation queue seed: elapsed_seconds=%.3f "
        "source_scan_skipped=%s queue_total_items=%s queue_location_items=%s "
        "queue_remaining_items=%s",
        perf_counter() - seed_started,
        counts["source_scan_skipped"],
        counts["queue_total_items"],
        counts["queue_location_items"],
        counts["queue_remaining_items"],
    )
    workflow_started = perf_counter()
    context.log.info(
        "Starting Norway Brreg translation Temporal workflow: workflow_id=%s",
        NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
    )
    workflow = start_translation_workflow(
        workflow_id=NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
        params=TranslationQueueWorkflowInput(
            duckdb_path=str(queue_duckdb_path),
            batch_size=config.batch_size,
            timeout_seconds=config.timeout_seconds,
            max_batch_failures=config.max_batch_failures,
            worker_id=config.worker_id,
            max_tokens=config.max_tokens,
            extra_body_json=config.extra_body_json,
            initialize_timeout_seconds=config.initialize_timeout_seconds,
            batch_timeout_buffer_seconds=config.batch_timeout_buffer_seconds,
            summarize_timeout_seconds=config.summarize_timeout_seconds,
            activity_maximum_attempts=config.activity_maximum_attempts,
            lease_timeout_seconds=config.lease_timeout_seconds,
        ),
        temporal_address=config.temporal_address,
    )
    context.log.info(
        "Finished Norway Brreg translation Temporal workflow start: elapsed_seconds=%.3f "
        "workflow_id=%s workflow_run_id=%s workflow_task_queue=%s",
        perf_counter() - workflow_started,
        workflow["workflow_id"],
        workflow["run_id"],
        workflow["task_queue"],
    )
    metadata = {
        **counts,
        "workflow_id": workflow["workflow_id"],
        "workflow_run_id": workflow["run_id"],
        "workflow_task_queue": workflow["task_queue"],
    }
    context.log.info(
        "Norway Brreg translation queue status: source_scan_skipped=%s "
        "source_rows=%s candidate_items=%s "
        "inserted_items=%s inserted_locations=%s queue_total_items=%s "
        "queue_location_items=%s queue_remaining_items=%s queue_pending_items=%s "
        "queue_completed_items=%s queue_leased_items=%s queue_failed_retryable_items=%s "
        "results=%s batch_attempts=%s "
        "successful_batches=%s failed_batches=%s workflow_id=%s workflow_run_id=%s "
        "workflow_task_queue=%s",
        metadata["source_scan_skipped"],
        metadata["source_rows"],
        metadata["candidate_items"],
        metadata["inserted_items"],
        metadata["inserted_locations"],
        metadata["queue_total_items"],
        metadata["queue_location_items"],
        metadata["queue_remaining_items"],
        metadata["queue_pending_items"],
        metadata["queue_completed_items"],
        metadata["queue_leased_items"],
        metadata["queue_failed_retryable_items"],
        metadata["queue_result_items"],
        metadata["queue_batch_attempts"],
        metadata["queue_successful_batches"],
        metadata["queue_failed_batches"],
        metadata["workflow_id"],
        metadata["workflow_run_id"],
        metadata["workflow_task_queue"],
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.observable_source_asset(
    key=NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
    group_name=GROUP_NAME,
    description="Observed Temporal status for the serialized Norway Brreg translation workflow.",
    tags={
        "system": "temporal",
        "temporal": "true",
        "dagster/kind/temporal": "",
        "source_slug": NORWAY_BRREG_TRANSLATION_SOURCE_SLUG,
    },
)
def norway_brreg_translation_workflow_status(
    context: AssetExecutionContext,
    norway_brreg_translation_queue_duckdb: DuckDBResource,
) -> dg.ObserveResult:
    queue_metadata = norway_brreg_translation_queue_status_metadata(
        queue_duckdb_path=duckdb_database_path(norway_brreg_translation_queue_duckdb),
    )
    try:
        workflow = describe_norway_brreg_translation_workflow()
    except Exception as exc:
        metadata = norway_brreg_translation_workflow_status_metadata(
            error=str(exc),
            queue_metadata=queue_metadata,
        )
        log_norway_brreg_translation_workflow_status(context.log.info, metadata)
        return dg.ObserveResult(
            metadata=metadata
        )
    metadata = norway_brreg_translation_workflow_status_metadata(
        workflow,
        queue_metadata=queue_metadata,
    )
    log_norway_brreg_translation_workflow_status(context.log.info, metadata)
    return dg.ObserveResult(
        metadata=metadata
    )


@dg.asset(
    deps=[
        dg.AssetKey("norway_brreg_translation_queue"),
        NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Apply completed Norway Brreg translation queue results back to entity _en fields.",
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_brreg_translations_applied(
    context: AssetExecutionContext,
    norway_brreg_duckdb: DuckDBResource,
    norway_brreg_translation_queue_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    try:
        workflow = describe_norway_brreg_translation_workflow()
    except Exception as exc:
        metadata = {
            "applied": False,
            "workflow_id": NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
            "workflow_status": "unavailable",
            "workflow_error": str(exc),
        }
        context.log.info("Skipping Norway Brreg translation application", extra=metadata)
        return dg.MaterializeResult(metadata=metadata)

    if workflow["workflow_status"] != "COMPLETED":
        metadata = {
            "applied": False,
            "workflow_id": workflow["workflow_id"],
            "workflow_run_id": workflow["workflow_run_id"],
            "workflow_status": workflow["workflow_status"],
        }
        context.log.info("Skipping Norway Brreg translation application", extra=metadata)
        return dg.MaterializeResult(metadata=metadata)

    with norway_brreg_duckdb.get_connection() as connection:
        counts = apply_norway_brreg_translation_queue_results(
            source_duckdb_connection=connection,
            queue_duckdb_path=duckdb_database_path(
                norway_brreg_translation_queue_duckdb
            ),
        )
    metadata = {
        **counts,
        "applied": True,
        "workflow_id": workflow["workflow_id"],
        "workflow_run_id": workflow["workflow_run_id"],
        "workflow_status": workflow["workflow_status"],
    }
    context.log.info("Applied Norway Brreg translation queue results", extra=metadata)
    return dg.MaterializeResult(metadata=metadata)


norway_brreg_translation_completion_job = dg.define_asset_job(
    "norway_brreg_translation_completion_job",
    # translations_applied writes the EN columns into DuckDB; clickhouse_companies
    # then publishes the final companies table. Run both so the sensor-driven
    # completion lands companies in ClickHouse automatically when translation finishes.
    selection=dg.AssetSelection.assets("norway_brreg_translations_applied")
    | dg.AssetSelection.assets("norway_brreg_clickhouse_companies"),
)


# Monthly coordinated refresh (see dagster_v3/CLAUDE.md "Scheduling"). Loads
# entities once, then kicks off the start-or-reuse Temporal translation workflow
# (returns immediately; the workflow runs async) AND runs the financials chain.
# Companies land via the existing completion sensor when translation finishes.
# Monthly because the translation is long-running and the per-company financial
# fetch is heavy. Excludes translations_applied/clickhouse_companies (sensor's job).
norway_brreg_refresh_job = dg.define_asset_job(
    "norway_brreg_refresh_job",
    selection=dg.AssetSelection.assets("norway_brreg_clickhouse_financial_statements").upstream()
    | dg.AssetSelection.assets("norway_brreg_translation_queue"),
)
norway_brreg_refresh_schedule = dg.ScheduleDefinition(
    name="norway_brreg_refresh_schedule",
    job=norway_brreg_refresh_job,
    cron_schedule="0 6 7 * *",  # monthly, 7th 06:00 (staggered vs estonia/latvia)
    execution_timezone="Europe/Belgrade",
)


def describe_norway_brreg_translation_workflow(
    *,
    temporal_client: TemporalClient | None = None,
) -> dict[str, str]:
    return asyncio.run(_describe_norway_brreg_translation_workflow(temporal_client=temporal_client))


async def _describe_norway_brreg_translation_workflow(
    *,
    temporal_client: TemporalClient | None = None,
) -> dict[str, str]:
    client = temporal_client or await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    )
    description = await client.get_workflow_handle(NORWAY_BRREG_TRANSLATION_WORKFLOW_ID).describe()
    return {
        "workflow_id": NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
        "workflow_run_id": description.run_id,
        "workflow_status": _workflow_status_name(description.status),
    }


def _workflow_status_name(status: Any) -> str:
    return str(getattr(status, "name", status))


def norway_brreg_translation_workflow_status_metadata(
    workflow: dict[str, str] | None = None,
    *,
    error: str = "",
    queue_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    queue_status = queue_metadata or _empty_translation_queue_status_metadata(
        queue_available=False,
        queue_error="queue status not loaded",
    )
    if workflow is None:
        return {
            "workflow_id": NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
            "workflow_status": "unavailable",
            "workflow_available": False,
            "workflow_error": error,
            **queue_status,
        }
    return {
        "workflow_id": workflow["workflow_id"],
        "workflow_run_id": workflow["workflow_run_id"],
        "workflow_status": workflow["workflow_status"],
        "workflow_available": True,
        "workflow_complete": workflow["workflow_status"] == "COMPLETED",
        **queue_status,
    }


def norway_brreg_translation_queue_status_metadata(
    *,
    queue_duckdb_connection: duckdb.DuckDBPyConnection | None = None,
    queue_duckdb_path: str | Path = NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH,
) -> dict[str, Any]:
    queue_path = Path(queue_duckdb_path)
    if not queue_path.exists():
        return _empty_translation_queue_status_metadata(
            queue_available=False,
            queue_error=f"queue DuckDB does not exist: {queue_path}",
            queue_duckdb_path=queue_path,
        )
    try:
        if queue_duckdb_connection is None:
            with read_only_duckdb_connection(duckdb_resource(queue_path)) as connection:
                summary = _translation_queue_summary_from_connection(
                    connection,
                    table_prefix="main",
                )
        else:
            summary = _translation_queue_summary_from_connection(
                queue_duckdb_connection,
                table_prefix="main",
            )
    except Exception as exc:
        return _empty_translation_queue_status_metadata(
            queue_available=False,
            queue_error=str(exc),
            queue_duckdb_path=queue_path,
        )
    remaining_items = (
        summary.pending_items + summary.leased_items + summary.failed_retryable_items
    )
    completed_percent = (
        round((summary.completed_items / summary.total_items) * 100, 3)
        if summary.total_items > 0
        else 0.0
    )
    return {
        "queue_available": True,
        "queue_duckdb_path": str(queue_path),
        "queue_total_items": summary.total_items,
        "queue_location_items": summary.location_items,
        "queue_remaining_items": remaining_items,
        "queue_pending_items": summary.pending_items,
        "queue_leased_items": summary.leased_items,
        "queue_completed_items": summary.completed_items,
        "queue_failed_retryable_items": summary.failed_retryable_items,
        "queue_result_items": summary.result_items,
        "queue_batch_attempts": summary.batch_attempts,
        "queue_successful_batches": summary.successful_batches,
        "queue_failed_batches": summary.failed_batches,
        "queue_completed_percent": completed_percent,
        "queue_error": "",
    }


def _empty_translation_queue_status_metadata(
    *,
    queue_available: bool,
    queue_error: str,
    queue_duckdb_path: str | Path = NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH,
) -> dict[str, Any]:
    return {
        "queue_available": queue_available,
        "queue_duckdb_path": str(queue_duckdb_path),
        "queue_total_items": 0,
        "queue_location_items": 0,
        "queue_remaining_items": 0,
        "queue_pending_items": 0,
        "queue_leased_items": 0,
        "queue_completed_items": 0,
        "queue_failed_retryable_items": 0,
        "queue_result_items": 0,
        "queue_batch_attempts": 0,
        "queue_successful_batches": 0,
        "queue_failed_batches": 0,
        "queue_completed_percent": 0.0,
        "queue_error": queue_error,
    }


def log_norway_brreg_translation_workflow_status(
    log: Callable[..., None] | None,
    metadata: dict[str, Any],
) -> None:
    _log(
        log,
        "Norway Brreg translation workflow status: workflow_status=%s "
        "workflow_run_id=%s queue_available=%s queue_total_items=%s "
        "queue_location_items=%s queue_remaining_items=%s queue_pending_items=%s "
        "queue_leased_items=%s queue_completed_items=%s queue_failed_retryable_items=%s "
        "queue_result_items=%s queue_batch_attempts=%s "
        "queue_successful_batches=%s queue_failed_batches=%s "
        "queue_completed_percent=%s queue_error=%s",
        metadata.get("workflow_status", ""),
        metadata.get("workflow_run_id", ""),
        metadata.get("queue_available", False),
        metadata.get("queue_total_items", 0),
        metadata.get("queue_location_items", 0),
        metadata.get("queue_remaining_items", 0),
        metadata.get("queue_pending_items", 0),
        metadata.get("queue_leased_items", 0),
        metadata.get("queue_completed_items", 0),
        metadata.get("queue_failed_retryable_items", 0),
        metadata.get("queue_result_items", 0),
        metadata.get("queue_batch_attempts", 0),
        metadata.get("queue_successful_batches", 0),
        metadata.get("queue_failed_batches", 0),
        metadata.get("queue_completed_percent", 0.0),
        metadata.get("queue_error", ""),
    )


def build_norway_brreg_translation_queue_items(
    rows: list[dict[str, Any]],
    *,
    source_duckdb_path: str | Path,
) -> list[TranslationQueueItem]:
    items: list[TranslationQueueItem] = []
    for row in rows:
        org_number = _string(row.get("org_number"))
        if org_number == "":
            continue
        for original_field, english_field in NORWAY_BRREG_LLM_TRANSLATION_FIELDS:
            source_text = _string(row.get(original_field))
            if source_text == "" or _string(row.get(english_field)) != "":
                continue
            items.append(
                TranslationQueueItem(
                    source_duckdb_path=str(source_duckdb_path),
                    source_table=f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}",
                    source_pk=org_number,
                    source_field=original_field,
                    source_text=source_text,
                    target_language="en",
                )
            )
    return items


def seed_norway_brreg_translation_queue(
    *,
    source_duckdb_path: str | Path,
    queue_duckdb_connection: duckdb.DuckDBPyConnection,
    queue_duckdb_path: str | Path,
    log: Callable[..., None] | None = None,
    lock_timeout_seconds: float = 120.0,
    refresh_existing_queue: bool = False,
) -> dict[str, Any]:
    source_path = str(source_duckdb_path)
    queue_path = str(queue_duckdb_path)
    Path(queue_path).parent.mkdir(parents=True, exist_ok=True)
    source_table = f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}"
    start = perf_counter()
    if Path(queue_path).exists() and not refresh_existing_queue:
        try:
            summary = _translation_queue_summary_from_connection(
                queue_duckdb_connection,
                table_prefix="main",
            )
        except duckdb.CatalogException:
            summary = None
        if summary is not None and summary.location_items > 0:
            remaining_items = (
                summary.pending_items + summary.leased_items + summary.failed_retryable_items
            )
            _log(
                log,
                "Skipping Norway Brreg translation queue source scan because queue is already "
                "seeded: queue_duckdb_path=%s queue_total_items=%s queue_location_items=%s "
                "queue_remaining_items=%s elapsed_seconds=%.3f",
                queue_path,
                summary.total_items,
                summary.location_items,
                remaining_items,
                perf_counter() - start,
            )
            return {
                "source_scan_skipped": True,
                "source_rows": 0,
                "candidate_items": 0,
                "inserted_items": 0,
                "inserted_locations": 0,
                "queue_total_items": summary.total_items,
                "queue_location_items": summary.location_items,
                "queue_remaining_items": remaining_items,
                "queue_pending_items": summary.pending_items,
                "queue_leased_items": summary.leased_items,
                "queue_completed_items": summary.completed_items,
                "queue_failed_retryable_items": summary.failed_retryable_items,
                "queue_result_items": summary.result_items,
                "queue_batch_attempts": summary.batch_attempts,
                "queue_successful_batches": summary.successful_batches,
                "queue_failed_batches": summary.failed_batches,
            }
    _log(
        log,
        "Counting Norway Brreg translation candidates: source_duckdb_path=%s, "
        "queue_duckdb_path=%s",
        source_path,
        queue_path,
    )
    connection = queue_duckdb_connection
    attached_source = False
    try:
        _execute_with_duckdb_lock_retry(
            lambda: connection.execute(
                f"attach {_duckdb_string_literal(source_path)} as source_db (read_only)"
            ),
            description=f"attach source DuckDB {source_path}",
            log=log,
            timeout_seconds=lock_timeout_seconds,
        )
        attached_source = True
        TranslationQueue.initialize_tables(connection, table_prefix="main")
        source_rows = int(
            connection.execute(
                f"select count(*) from source_db.{DLT_DATASET_NAME}.{ENTITIES_TABLE}"
            ).fetchone()[0]
        )
        candidate_items = int(
            connection.execute(_norway_brreg_translation_candidates_count_sql()).fetchone()[0]
        )
        queue_items_before = int(
            connection.execute("select count(*) from main.translation_items").fetchone()[0]
        )
        queue_locations_before = int(
            connection.execute("select count(*) from main.translation_locations").fetchone()[0]
        )
        _log(
            log,
            "Inserting Norway Brreg translation queue candidates: source_rows=%s, "
            "candidate_items=%s",
            source_rows,
            candidate_items,
        )
        _execute_with_duckdb_lock_retry(
            lambda: connection.execute(
                f"""
                insert into main.translation_items (
                    item_id,
                    source_text,
                    source_text_hash,
                    target_language,
                    status,
                    attempt_count,
                    created_at,
                    updated_at
                )
                select
                    sha256(concat_ws('|', sha256(source_text), 'en')) as item_id,
                    any_value(source_text) as source_text,
                    sha256(source_text) as source_text_hash,
                    'en' as target_language,
                    'pending' as status,
                    0 as attempt_count,
                    min(current_timestamp) as created_at,
                    max(current_timestamp) as updated_at
                from ({_norway_brreg_translation_candidates_sql()})
                group by sha256(source_text)
                on conflict (item_id) do nothing
                """
            ),
            description="insert Norway Brreg translation queue items",
            log=log,
            timeout_seconds=lock_timeout_seconds,
        )
        _execute_with_duckdb_lock_retry(
            lambda: connection.execute(
                f"""
                insert into main.translation_locations (
                    location_id,
                    item_id,
                    source_duckdb_path,
                    source_table,
                    source_pk,
                    source_field,
                    created_at,
                    updated_at
                )
                select
                    sha256(
                        concat_ws(
                            '|',
                            {_duckdb_string_literal(source_path)},
                            {_duckdb_string_literal(source_table)},
                            org_number,
                            source_field,
                            sha256(source_text),
                            'en'
                        )
                    ) as location_id,
                    sha256(concat_ws('|', sha256(source_text), 'en')) as item_id,
                    {_duckdb_string_literal(source_path)} as source_duckdb_path,
                    {_duckdb_string_literal(source_table)} as source_table,
                    org_number as source_pk,
                    source_field,
                    current_timestamp as created_at,
                    current_timestamp as updated_at
                from ({_norway_brreg_translation_candidates_sql()})
                on conflict (location_id) do nothing
                """
            ),
            description="insert Norway Brreg translation queue locations",
            log=log,
            timeout_seconds=lock_timeout_seconds,
        )
        queue_items_after = int(
            connection.execute("select count(*) from main.translation_items").fetchone()[0]
        )
        queue_locations_after = int(
            connection.execute("select count(*) from main.translation_locations").fetchone()[0]
        )
        summary = _translation_queue_summary_from_connection(connection, table_prefix="main")
    finally:
        if attached_source:
            connection.execute("detach source_db")

    inserted_items = queue_items_after - queue_items_before
    inserted_locations = queue_locations_after - queue_locations_before
    _log(
        log,
        "Inserted Norway Brreg translation queue candidates: inserted_items=%s, "
        "inserted_locations=%s, queue_total_items=%s, queue_location_items=%s, "
        "queue_remaining_items=%s, queue_pending_items=%s, queue_completed_items=%s, "
        "queue_leased_items=%s, queue_failed_retryable_items=%s, queue_result_items=%s, "
        "queue_batch_attempts=%s, queue_successful_batches=%s, queue_failed_batches=%s, "
        "elapsed_seconds=%.3f",
        inserted_items,
        inserted_locations,
        summary.total_items,
        summary.location_items,
        summary.pending_items + summary.leased_items + summary.failed_retryable_items,
        summary.pending_items,
        summary.completed_items,
        summary.leased_items,
        summary.failed_retryable_items,
        summary.result_items,
        summary.batch_attempts,
        summary.successful_batches,
        summary.failed_batches,
        perf_counter() - start,
    )
    remaining_items = summary.pending_items + summary.leased_items + summary.failed_retryable_items
    return {
        "source_scan_skipped": False,
        "source_rows": source_rows,
        "candidate_items": candidate_items,
        "inserted_items": inserted_items,
        "inserted_locations": inserted_locations,
        "queue_total_items": summary.total_items,
        "queue_location_items": summary.location_items,
        "queue_remaining_items": remaining_items,
        "queue_pending_items": summary.pending_items,
        "queue_leased_items": summary.leased_items,
        "queue_completed_items": summary.completed_items,
        "queue_failed_retryable_items": summary.failed_retryable_items,
        "queue_result_items": summary.result_items,
        "queue_batch_attempts": summary.batch_attempts,
        "queue_successful_batches": summary.successful_batches,
        "queue_failed_batches": summary.failed_batches,
    }


def _norway_brreg_translation_candidates_count_sql() -> str:
    return f"select count(*) from ({_norway_brreg_translation_candidates_sql()})"


def _translation_queue_summary_from_connection(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_prefix: str,
) -> TranslationQueueSummary:
    return TranslationQueueSummary(
        total_items=_count_attached_translation_queue_rows(connection, table_prefix, None),
        location_items=int(
            connection.execute(
                f"select count(*) from {table_prefix}.translation_locations"
            ).fetchone()[0]
        ),
        pending_items=_count_attached_translation_queue_rows(
            connection,
            table_prefix,
            "pending",
        ),
        leased_items=_count_attached_translation_queue_rows(
            connection,
            table_prefix,
            "leased",
        ),
        completed_items=_count_attached_translation_queue_rows(
            connection,
            table_prefix,
            "completed",
        ),
        failed_retryable_items=_count_attached_translation_queue_rows(
            connection,
            table_prefix,
            "failed_retryable",
        ),
        result_items=int(
            connection.execute(
                f"select count(*) from {table_prefix}.translation_results"
            ).fetchone()[0]
        ),
        batch_attempts=int(
            connection.execute(
                f"select count(*) from {table_prefix}.translation_batch_attempts"
            ).fetchone()[0]
        ),
        successful_batches=int(
            connection.execute(
                f"""
                select count(*)
                from {table_prefix}.translation_batch_attempts
                where status = 'success'
                """
            ).fetchone()[0]
        ),
        failed_batches=int(
            connection.execute(
                f"""
                select count(*)
                from {table_prefix}.translation_batch_attempts
                where status = 'failed'
                """
            ).fetchone()[0]
        ),
    )


def _count_attached_translation_queue_rows(
    connection: duckdb.DuckDBPyConnection,
    table_prefix: str,
    status: str | None,
) -> int:
    if status is None:
        return int(
            connection.execute(f"select count(*) from {table_prefix}.translation_items").fetchone()[
                0
            ]
        )
    return int(
        connection.execute(
            f"""
            select count(*)
            from {table_prefix}.translation_items
            where status = ?
            """,
            [status],
        ).fetchone()[0]
    )


def _norway_brreg_translation_candidates_sql() -> str:
    return f"""
        select
            org_number,
            'articles_purpose_original' as source_field,
            articles_purpose_original as source_text
        from source_db.{DLT_DATASET_NAME}.{ENTITIES_TABLE}
        where nullif(trim(coalesce(org_number, '')), '') is not null
          and nullif(trim(coalesce(articles_purpose_original, '')), '') is not null
          and nullif(trim(coalesce(articles_purpose_en, '')), '') is null
        union all
        select
            org_number,
            'activity_text_original' as source_field,
            activity_text_original as source_text
        from source_db.{DLT_DATASET_NAME}.{ENTITIES_TABLE}
        where nullif(trim(coalesce(org_number, '')), '') is not null
          and nullif(trim(coalesce(activity_text_original, '')), '') is not null
          and nullif(trim(coalesce(activity_text_en, '')), '') is null
        union all
        select
            org_number,
            'company_description_original' as source_field,
            company_description_original as source_text
        from source_db.{DLT_DATASET_NAME}.{ENTITIES_TABLE}
        where nullif(trim(coalesce(org_number, '')), '') is not null
          and nullif(trim(coalesce(company_description_original, '')), '') is not null
          and nullif(trim(coalesce(company_description_en, '')), '') is null
    """


def apply_norway_brreg_translation_queue_results(
    *,
    source_duckdb_connection: duckdb.DuckDBPyConnection,
    queue_duckdb_path: str | Path,
) -> dict[str, int]:
    completed_results = TranslationQueue(queue_duckdb_path).completed_results()
    counts = {
        "completed_translations": len(completed_results),
        "rows_updated": 0,
        "fields_updated": 0,
        "skipped_non_norway": 0,
        "skipped_non_free_text": 0,
        "skipped_missing_source_row": 0,
        "skipped_already_applied": 0,
    }
    updated_org_numbers: set[str] = set()
    for result in completed_results:
        if result.source_table != f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}":
            counts["skipped_non_norway"] += 1
            continue
        target_field = NORWAY_BRREG_EN_FIELD_BY_ORIGINAL_FIELD.get(result.source_field)
        if target_field is None:
            counts["skipped_non_free_text"] += 1
            continue
        existing = source_duckdb_connection.execute(
            f"""
            select {target_field}
            from {DLT_DATASET_NAME}.{ENTITIES_TABLE}
            where org_number = ?
            """,
            [result.source_pk],
        ).fetchone()
        if existing is None:
            counts["skipped_missing_source_row"] += 1
            continue
        if _string(existing[0]) == result.translated_text:
            counts["skipped_already_applied"] += 1
            continue
        source_duckdb_connection.execute(
            f"""
            update {DLT_DATASET_NAME}.{ENTITIES_TABLE}
            set {target_field} = ?
            where org_number = ?
            """,
            [result.translated_text, result.source_pk],
        )
        counts["fields_updated"] += 1
        updated_org_numbers.add(result.source_pk)
    counts["rows_updated"] = len(updated_org_numbers)
    return counts


def _complete_all_translation_queue_items_for_test(
    *,
    queue_duckdb_path: str | Path,
    translations_by_field: dict[str, str],
) -> None:
    queue = TranslationQueue(queue_duckdb_path)
    with read_only_duckdb_connection(duckdb_resource(queue_duckdb_path)) as connection:
        source_fields_by_item_id: dict[str, list[str]] = {}
        for item_id, source_field in connection.execute(
            """
            select item_id, source_field
            from translation_locations
            order by item_id, source_field
            """
        ).fetchall():
            source_fields_by_item_id.setdefault(item_id, []).append(source_field)
    while True:
        claimed = queue.claim_batch(limit=1000, worker_id="test-worker")
        if not claimed:
            break
        queue.complete_batch(
            claimed,
            [
                SmokeTranslationResult(
                    item_id=item.item_id,
                    translated_text=next(
                        (
                            translations_by_field[source_field]
                            for source_field in source_fields_by_item_id.get(item.item_id, [])
                            if source_field in translations_by_field
                        ),
                        f"Translated {item.source_text}",
                    ),
                )
                for item in claimed
            ],
            provider="test",
            model="test-model",
            duration_seconds=0.0,
        )


def _insert_completed_translation_for_test(
    *,
    queue_duckdb_path: str | Path,
    source_duckdb_path: str | Path,
    source_pk: str,
    source_field: str,
    source_text: str,
    translated_text: str,
) -> None:
    queue = TranslationQueue(queue_duckdb_path)
    queue.initialize()
    item = TranslationQueueItem(
        source_duckdb_path=str(source_duckdb_path),
        source_table=f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}",
        source_pk=source_pk,
        source_field=source_field,
        source_text=source_text,
        target_language="en",
    )
    queue.enqueue_items([item])
    claimed = queue.claim_batch(limit=1, worker_id="test-worker")
    queue.complete_batch(
        claimed,
        [SmokeTranslationResult(item_id=claimed[0].item_id, translated_text=translated_text)],
        provider="test",
        model="test-model",
        duration_seconds=0.0,
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


def export_norway_brreg_clickhouse_tables(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    companies = export_norway_brreg_clickhouse_companies(
        duckdb_connection=duckdb_connection,
        clickhouse=clickhouse,
        log=log,
    )
    financial_statements = export_norway_brreg_clickhouse_financial_statements(
        duckdb_connection=duckdb_connection,
        clickhouse=clickhouse,
        log=log,
    )
    return {
        "companies": companies,
        "financial_statements": financial_statements,
    }


def export_norway_brreg_clickhouse_companies(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
    log: Callable[..., None] | None = None,
) -> int:
    _log(
        log,
        "Preparing Norway Brreg companies ClickHouse table: database=%s, table=%s",
        tables.NORWAY_BRREG_DATABASE,
        tables.QUALIFIED_COMPANIES_TABLE,
    )
    prepare_norway_brreg_clickhouse_companies_table(clickhouse)
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=ENTITIES_TABLE,
            clickhouse_database=tables.NORWAY_BRREG_DATABASE,
            clickhouse_table=tables.COMPANIES_TABLE,
            columns=tables.COMPANIES_EXPORT_COLUMNS,
            truncate=False,
        )

    _log(log, "Finished Norway Brreg companies ClickHouse export: rows=%s", rows)
    return rows


def export_norway_brreg_clickhouse_financial_statements(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
    log: Callable[..., None] | None = None,
) -> int:
    _log(
        log,
        "Preparing Norway Brreg financial statements ClickHouse table: database=%s, table=%s",
        tables.NORWAY_BRREG_DATABASE,
        tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
    )
    prepare_norway_brreg_clickhouse_financial_statements_table(clickhouse)
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=FINANCIAL_STATEMENTS_TABLE,
            clickhouse_database=tables.NORWAY_BRREG_DATABASE,
            clickhouse_table=tables.FINANCIAL_STATEMENTS_TABLE,
            columns=tables.FINANCIAL_STATEMENTS_EXPORT_COLUMNS,
            truncate=False,
        )

    _log(
        log,
        "Finished Norway Brreg financial statements ClickHouse export: rows=%s",
        rows,
    )
    return rows


def _duckdb_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _execute_with_duckdb_lock_retry(
    operation: Callable[[], Any],
    *,
    description: str,
    log: Callable[..., None] | None,
    timeout_seconds: float,
) -> Any:
    started_at = perf_counter()
    attempt = 1
    while True:
        try:
            return operation()
        except duckdb.IOException as exc:
            if not _is_duckdb_lock_error(exc):
                raise
            elapsed_seconds = perf_counter() - started_at
            if elapsed_seconds >= timeout_seconds:
                raise RuntimeError(
                    f"DuckDB is still locked while trying to {description} after "
                    f"{elapsed_seconds:.1f}s. Stop the process holding the DuckDB file "
                    "or wait for the active translation/Dagster run to finish before "
                    "materializing this asset again."
                ) from exc
            wait_seconds = min(5.0, max(0.5, timeout_seconds - elapsed_seconds))
            _log(
                log,
                "Waiting for DuckDB lock before %s: attempt=%s elapsed_seconds=%.1f "
                "sleep_seconds=%.1f error=%s",
                description,
                attempt,
                elapsed_seconds,
                wait_seconds,
                exc,
            )
            sleep(wait_seconds)
            attempt += 1


def _is_duckdb_lock_error(exc: duckdb.IOException) -> bool:
    return "Could not set lock on file" in str(exc)


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


def _string(value: Any) -> str:
    return "" if value is None else str(value)
