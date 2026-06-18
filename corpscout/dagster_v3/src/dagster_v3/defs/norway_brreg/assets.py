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
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

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
    worker_id: str = "translation-temporal-worker"
    max_tokens: int = 4096
    extra_body_json: str = '{"chat_template_kwargs":{"enable_thinking":false}}'
    initialize_timeout_seconds: int = 300
    batch_timeout_buffer_seconds: int = 30
    summarize_timeout_seconds: int = 30
    activity_maximum_attempts: int = 1
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
)
def norway_brreg_entities_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
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
    row_count = _duckdb_table_count(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
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
)
def norway_brreg_financial_fetches_duckdb_asset(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    NORWAY_BRREG_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Norway Brreg financial fetch load: source_url=%s, duckdb_path=%s, "
        "input_table=%s.%s, output_table=%s.%s",
        BRREG_REGNSKAP_BASE_URL,
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        DLT_DATASET_NAME,
        FINANCIAL_FETCHES_TABLE,
    )
    counts = run_brreg_financial_statement_fetches(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        source_run_id=context.run_id,
        base_url=BRREG_REGNSKAP_BASE_URL,
        log=context.log.info,
    )
    row_count = _duckdb_table_count(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        table_name=f"{DLT_DATASET_NAME}.{FINANCIAL_FETCHES_TABLE}",
    )
    status_counts = _duckdb_fetch_status_counts(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        table_name=f"{DLT_DATASET_NAME}.{FINANCIAL_FETCHES_TABLE}",
    )
    context.log.info(
        "Completed Norway Brreg financial fetch load: duckdb_path=%s, table=%s.%s, "
        "rows=%s, statuses=%s",
        NORWAY_BRREG_DUCKDB_PATH,
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
)
def norway_brreg_financial_statements_duckdb_asset(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Norway Brreg financial statement normalization: duckdb_path=%s, "
        "input_table=%s.%s, output_table=%s.%s",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        FINANCIAL_FETCHES_TABLE,
        DLT_DATASET_NAME,
        FINANCIAL_STATEMENTS_TABLE,
    )
    counts = normalize_norway_brreg_financial_statements_duckdb(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        exchange_rates=ExchangeRateClient.from_env(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_translations_applied")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    description="Norway Brreg final companies table exported to ClickHouse.",
    metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
)
def norway_brreg_clickhouse_companies(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Norway Brreg companies ClickHouse export: duckdb_path=%s, table=%s",
        NORWAY_BRREG_DUCKDB_PATH,
        tables.QUALIFIED_COMPANIES_TABLE,
    )
    rows = export_norway_brreg_clickhouse_companies(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
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
)
def norway_brreg_clickhouse_financial_statements(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Norway Brreg financial statements ClickHouse export: duckdb_path=%s, "
        "table=%s",
        NORWAY_BRREG_DUCKDB_PATH,
        tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
    )
    rows = export_norway_brreg_clickhouse_financial_statements(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
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
)
def norway_brreg_translation_queue(
    context: AssetExecutionContext,
    config: NorwayBrregTranslationConfig,
) -> dg.MaterializeResult:
    counts = seed_norway_brreg_translation_queue(
        source_duckdb_path=NORWAY_BRREG_DUCKDB_PATH,
        queue_duckdb_path=NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH,
        log=context.log.info,
    )
    workflow = start_translation_workflow(
        workflow_id=NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
        params=TranslationQueueWorkflowInput(
            duckdb_path=str(NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH),
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
        ),
        temporal_address=config.temporal_address,
    )
    metadata = {
        **counts,
        "workflow_id": workflow["workflow_id"],
        "workflow_run_id": workflow["run_id"],
        "workflow_task_queue": workflow["task_queue"],
    }
    context.log.info("Seeded Norway Brreg translation queue and started workflow", extra=metadata)
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
def norway_brreg_translation_workflow_status() -> dg.ObserveResult:
    try:
        workflow = describe_norway_brreg_translation_workflow()
    except Exception as exc:
        return dg.ObserveResult(
            metadata=norway_brreg_translation_workflow_status_metadata(error=str(exc))
        )
    return dg.ObserveResult(
        metadata=norway_brreg_translation_workflow_status_metadata(workflow)
    )


@dg.asset(
    deps=[
        dg.AssetKey("norway_brreg_translation_queue"),
        NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Apply completed Norway Brreg translation queue results back to entity _en fields.",
)
def norway_brreg_translations_applied(context: AssetExecutionContext) -> dg.MaterializeResult:
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

    counts = apply_norway_brreg_translation_queue_results(
        source_duckdb_path=NORWAY_BRREG_DUCKDB_PATH,
        queue_duckdb_path=NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH,
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
    selection=dg.AssetSelection.assets("norway_brreg_translations_applied"),
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
) -> dict[str, Any]:
    if workflow is None:
        return {
            "workflow_id": NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
            "workflow_status": "unavailable",
            "workflow_available": False,
            "workflow_error": error,
        }
    return {
        "workflow_id": workflow["workflow_id"],
        "workflow_run_id": workflow["workflow_run_id"],
        "workflow_status": workflow["workflow_status"],
        "workflow_available": True,
        "workflow_complete": workflow["workflow_status"] == "COMPLETED",
    }


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
    queue_duckdb_path: str | Path,
    log: Callable[..., None] | None = None,
    lock_timeout_seconds: float = 120.0,
) -> dict[str, int]:
    source_path = str(source_duckdb_path)
    queue_path = str(queue_duckdb_path)
    Path(queue_path).parent.mkdir(parents=True, exist_ok=True)
    source_table = f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}"
    start = perf_counter()
    _log(
        log,
        "Counting Norway Brreg translation candidates: source_duckdb_path=%s, "
        "queue_duckdb_path=%s",
        source_path,
        queue_path,
    )
    with duckdb.connect() as connection:
        _execute_with_duckdb_lock_retry(
            lambda: connection.execute(
                f"attach {_duckdb_string_literal(source_path)} as source_db (read_only)"
            ),
            description=f"attach source DuckDB {source_path}",
            log=log,
            timeout_seconds=lock_timeout_seconds,
        )
        _execute_with_duckdb_lock_retry(
            lambda: connection.execute(f"attach {_duckdb_string_literal(queue_path)} as queue_db"),
            description=f"attach translation queue DuckDB {queue_path}",
            log=log,
            timeout_seconds=lock_timeout_seconds,
        )
        TranslationQueue.initialize_tables(connection, table_prefix="queue_db")
        source_rows = int(
            connection.execute(
                f"select count(*) from source_db.{DLT_DATASET_NAME}.{ENTITIES_TABLE}"
            ).fetchone()[0]
        )
        candidate_items = int(
            connection.execute(_norway_brreg_translation_candidates_count_sql()).fetchone()[0]
        )
        queue_items_before = int(
            connection.execute("select count(*) from queue_db.translation_items").fetchone()[0]
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
                insert into queue_db.translation_items (
                    item_id,
                    source_duckdb_path,
                    source_table,
                    source_pk,
                    source_field,
                    source_text,
                    source_text_hash,
                    target_language,
                    status,
                    attempt_count,
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
                    ) as item_id,
                    {_duckdb_string_literal(source_path)} as source_duckdb_path,
                    {_duckdb_string_literal(source_table)} as source_table,
                    org_number as source_pk,
                    source_field,
                    source_text,
                    sha256(source_text) as source_text_hash,
                    'en' as target_language,
                    'pending' as status,
                    0 as attempt_count,
                    current_timestamp as created_at,
                    current_timestamp as updated_at
                from ({_norway_brreg_translation_candidates_sql()})
                on conflict (item_id) do nothing
                """
            ),
            description="insert Norway Brreg translation queue candidates",
            log=log,
            timeout_seconds=lock_timeout_seconds,
        )
        queue_items_after = int(
            connection.execute("select count(*) from queue_db.translation_items").fetchone()[0]
        )
        summary = _translation_queue_summary_from_connection(connection, table_prefix="queue_db")

    inserted_items = queue_items_after - queue_items_before
    _log(
        log,
        "Inserted Norway Brreg translation queue candidates: inserted_items=%s, "
        "queue_total_items=%s, queue_pending_items=%s, elapsed_seconds=%.3f",
        inserted_items,
        summary.total_items,
        summary.pending_items,
        perf_counter() - start,
    )
    return {
        "source_rows": source_rows,
        "candidate_items": candidate_items,
        "inserted_items": inserted_items,
        "queue_total_items": summary.total_items,
        "queue_pending_items": summary.pending_items,
        "queue_completed_items": summary.completed_items,
        "queue_failed_retryable_items": summary.failed_retryable_items,
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
    source_duckdb_path: str | Path,
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
    with duckdb.connect(str(source_duckdb_path)) as connection:
        for result in completed_results:
            if result.source_table != f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}":
                counts["skipped_non_norway"] += 1
                continue
            target_field = NORWAY_BRREG_EN_FIELD_BY_ORIGINAL_FIELD.get(result.source_field)
            if target_field is None:
                counts["skipped_non_free_text"] += 1
                continue
            existing = connection.execute(
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
            connection.execute(
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
    claimed = queue.claim_batch(limit=1000, worker_id="test-worker")
    translations = [
        SmokeTranslationResult(
            item_id=item.item_id,
            translated_text=translations_by_field[item.source_field],
        )
        for item in claimed
        if item.source_field in translations_by_field
    ]
    queue.complete_batch(
        claimed,
        translations,
        provider="test",
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
    database_path: str | Path,
    exchange_rates: ExchangeRates,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    with duckdb.connect(str(database_path)) as connection:
        fetch_rows = _fetch_duckdb_dicts(
            connection,
            dataset=DLT_DATASET_NAME,
            table=FINANCIAL_FETCHES_TABLE,
            columns=tuple(BRREG_FINANCIAL_FETCHES_COLUMNS),
        )
        rows = build_financial_statement_rows_from_fetch_rows(
            fetch_rows,
            exchange_rates=exchange_rates,
        )
        _replace_duckdb_table_from_rows(
            connection,
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
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    companies = export_norway_brreg_clickhouse_companies(
        database_path=database_path,
        clickhouse=clickhouse,
        log=log,
    )
    financial_statements = export_norway_brreg_clickhouse_financial_statements(
        database_path=database_path,
        clickhouse=clickhouse,
        log=log,
    )
    return {
        "companies": companies,
        "financial_statements": financial_statements,
    }


def export_norway_brreg_clickhouse_companies(
    *,
    database_path: str | Path,
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
    _log(log, "Opening Norway Brreg DuckDB staging database: path=%s", database_path)
    with duckdb.connect(str(database_path), read_only=True) as connection:
        _log(
            log,
            "Reading Norway Brreg company rows from DuckDB: table=%s.%s",
            DLT_DATASET_NAME,
            ENTITIES_TABLE,
        )
        company_rows = _fetch_duckdb_rows(
            connection,
            dataset=DLT_DATASET_NAME,
            table=ENTITIES_TABLE,
            columns=tables.COMPANIES_COLUMNS,
        )
        _log(log, "Read Norway Brreg company rows from DuckDB: rows=%s", len(company_rows))

    with clickhouse.get_connection() as client:
        if company_rows:
            _log(
                log,
                "Inserting Norway Brreg company rows into ClickHouse: table=%s, rows=%s",
                tables.QUALIFIED_COMPANIES_TABLE,
                len(company_rows),
            )
            client.insert(
                tables.QUALIFIED_COMPANIES_TABLE,
                company_rows,
                column_names=tables.COMPANIES_COLUMNS,
            )
        else:
            _log(log, "Skipping Norway Brreg company ClickHouse insert: rows=0")

    _log(log, "Finished Norway Brreg companies ClickHouse export: rows=%s", len(company_rows))
    return len(company_rows)


def export_norway_brreg_clickhouse_financial_statements(
    *,
    database_path: str | Path,
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
    _log(log, "Opening Norway Brreg DuckDB staging database: path=%s", database_path)
    with duckdb.connect(str(database_path), read_only=True) as connection:
        _log(
            log,
            "Reading Norway Brreg financial statement rows from DuckDB: table=%s.%s",
            DLT_DATASET_NAME,
            FINANCIAL_STATEMENTS_TABLE,
        )
        financial_rows = _fetch_duckdb_rows(
            connection,
            dataset=DLT_DATASET_NAME,
            table=FINANCIAL_STATEMENTS_TABLE,
            columns=tables.FINANCIAL_STATEMENTS_COLUMNS,
        )
        _log(
            log,
            "Read Norway Brreg financial statement rows from DuckDB: rows=%s",
            len(financial_rows),
        )

    with clickhouse.get_connection() as client:
        if financial_rows:
            _log(
                log,
                "Inserting Norway Brreg financial statement rows into ClickHouse: "
                "table=%s, rows=%s",
                tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
                len(financial_rows),
            )
            client.insert(
                tables.QUALIFIED_FINANCIAL_STATEMENTS_TABLE,
                financial_rows,
                column_names=tables.FINANCIAL_STATEMENTS_COLUMNS,
            )
        else:
            _log(log, "Skipping Norway Brreg financial statement ClickHouse insert: rows=0")

    _log(
        log,
        "Finished Norway Brreg financial statements ClickHouse export: rows=%s",
        len(financial_rows),
    )
    return len(financial_rows)


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


def _duckdb_table_count(*, database_path: str | Path, table_name: str) -> int:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)


def _duckdb_fetch_status_counts(
    *,
    database_path: str | Path,
    table_name: str,
) -> dict[str, int]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
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
