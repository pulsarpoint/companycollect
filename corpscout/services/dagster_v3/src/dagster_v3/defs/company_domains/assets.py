"""Brave searches checkpoint completed outcomes directly in ClickHouse."""

import json
import os
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime
from threading import Lock
from time import monotonic
from typing import Literal
from uuid import UUID, uuid5

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import ConfigDict, Field, field_validator

from dagster_v3.defs.common.clickhouse_queue import (
    ClickHouseInputQueue,
    validate_relation,
)
from dagster_v3.defs.common.processing import ProcessingResource, render_query
from dagster_v3.defs.common.encrypted_llm import EncryptedLLMConfig
from dagster_v3.defs.company_domains.browser import (
    DEFAULT_BROWSER_API_URL,
    ROUTES,
    BraveBrowserResource,
    BraveSearchResult,
    CompanySearchInput,
)
from dagster_v3.defs.company_domains.results import (
    RESULT_TABLE,
    insert_results,
    page_outcomes,
    repair_current_answers,
    result_record,
    search_is_due,
)

PROCESSOR_VERSION = "brave-v2"
EXECUTION_TAG = "brave/execution"


class BraveSearchConfig(dg.Config):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    task_id: str | None = None
    llm: EncryptedLLMConfig | None = Field(
        default=None,
        description="Selected browser assistant profile with an encrypted API key. Omit only for legacy browser-service credentials.",
    )
    execution_id: str | None = Field(
        default=None,
        description="Original Dagster run ID to resume, including a forced execution.",
    )
    mode: Literal["process", "publish"] = "process"
    input_relation: str | None = None
    input_namespace: str = Field(default="company", min_length=1)
    query_type: str = Field(default="official_website", min_length=1)
    query_template: str = Field(
        default="Find the official website of {company_name}.", min_length=1
    )
    force: bool = Field(
        default=False, description="Search again regardless of previous outcomes."
    )
    rescan_old: bool = Field(
        default=False, description="Rescan completed searches older than 30 days."
    )
    requests_per_route: int = Field(default=1, ge=1, le=8)
    input_batch_size: int = Field(default=100, ge=4, le=10_000)
    answer_timeout_seconds: int = Field(default=60, ge=1, le=600)
    progress_log_every: int = Field(default=100, ge=1, le=100_000)
    progress_log_interval_seconds: int = Field(default=30, ge=1, le=3600)

    @field_validator("input_relation")
    @classmethod
    def named_relation(cls, value: str | None) -> str | None:
        return validate_relation(value) if value is not None else None

    @field_validator("task_id", "execution_id")
    @classmethod
    def stable_task_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None


def prepare_execution(
    context: dg.AssetExecutionContext,
    config: BraveSearchConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dict:
    """Keep one execution's selection, query and age cutoff fixed across Dagster retries."""
    execution_id = config.execution_id or context.run.root_run_id or context.run.run_id
    original = context.instance.get_run_by_id(execution_id)
    if original is None:
        raise ValueError("execution_id must identify the original Dagster run")
    supplied = (
        context.run.run_config.get("ops", {})
        .get("company_brave_search_results", {})
        .get("config", {})
    )
    current_run = context.instance.get_run_by_id(context.run.run_id)
    tagged_task = current_run.tags.get("processing/task_id")
    if config.task_id and tagged_task and config.task_id != tagged_task:
        raise ValueError("processing task_id differs from the prepared selection")
    if EXECUTION_TAG in original.tags:
        execution = json.loads(original.tags[EXECUTION_TAG])
        task_id = config.task_id or tagged_task
        if task_id is not None and task_id != execution["task_id"]:
            raise ValueError("execution_id belongs to a different input task")
        for name in (
            "query_type",
            "query_template",
            "force",
            "rescan_old",
            "input_relation",
        ):
            if name in supplied and getattr(config, name) != execution.get(name):
                raise ValueError(
                    f"resume must keep {name} unchanged; start a new execution instead"
                )
        if config.llm is not None:
            selected_llm = config.llm.model_dump()
            saved_llm = execution.get("llm")
            if saved_llm is None:
                # Upgrade an interrupted legacy execution without replaying its saved outcomes.
                execution["llm"] = selected_llm
                context.instance.add_run_tags(
                    execution_id, {EXECUTION_TAG: json.dumps(execution)}
                )
            elif config.llm.model_dump(exclude={"api_key_encrypted"}) != {
                key: value for key, value in saved_llm.items() if key != "api_key_encrypted"
            }:
                raise ValueError(
                    "resume must keep the selected LLM provider, endpoint and model unchanged; start a new execution instead"
                )
        context.instance.add_run_tags(
            context.run.run_id, {EXECUTION_TAG: json.dumps(execution)}
        )
        return execution
    if config.execution_id is not None:
        raise ValueError("the original run has no saved Brave execution to resume")
    if config.mode == "publish":
        raise ValueError(
            "publish mode requires execution_id from a previous Brave execution"
        )
    task_id = config.task_id or tagged_task or context.run.run_id
    # Initialization stores a single selection manifest in PostgreSQL. No item,
    # result, lease, retry or progress record is written there by this processor.
    with processing.get_store() as store:
        task = store.task(task_id)
        if task is None:
            if config.input_relation is None:
                raise ValueError(
                    "initialize company_brave_search_input or supply a prepared input_relation"
                )
            source = ClickHouseInputQueue(clickhouse, config.input_relation)
            store.register(
                task_id,
                processor=PROCESSOR_VERSION,
                config={
                    "input_relation": config.input_relation,
                    "query_type": config.query_type,
                    "query_template": config.query_template,
                },
                work_config={},
                source_info=source.inspect(),
            )
            task = store.task(task_id)
        if task["processor"] != PROCESSOR_VERSION or task["status"] not in {
            "selected",
            "ready",
        }:
            raise ValueError("the Brave input task is not ready")
        query = {
            name: getattr(config, name)
            if name in supplied
            else task["config"].get(name, getattr(config, name))
            for name in ("query_type", "query_template")
        }
        if task["status"] == "selected":
            task = store.activate_selection(task_id, config=query, work_config={})
    execution = {
        "execution_id": execution_id,
        "task_id": task_id,
        "started_at": datetime.now(UTC).isoformat(),
        "source_info": task["source_info"],
        "input_relation": task["source_info"]["relation"],
        "force": config.force,
        "rescan_old": config.rescan_old,
        "llm": config.llm.model_dump() if config.llm is not None else None,
        **query,
    }
    tags = {EXECUTION_TAG: json.dumps(execution), "processing/task_id": task_id}
    context.instance.add_run_tags(execution_id, tags)
    if execution_id != context.run.run_id:
        context.instance.add_run_tags(context.run.run_id, tags)
    return execution


@dg.asset(
    deps=["company_brave_search_input"],
    group_name="brave_domain_search",
    kinds={"python", "browser", "clickhouse"},
    pool="company_domains_brave",
    tags={"source": "brave", "country": "SE"},
    metadata={"dagster/table_name": RESULT_TABLE},
    description="Search selected Swedish companies and save completed answers or errors directly "
    "in company_brave_search_results. Existing outcomes are skipped unless force=true, or "
    "rescan_old=true and the latest outcome is older than 30 days. Successful answers feed "
    "se_company_brave_search_results_latest_success. Resume with execution_id; saved outcomes are never searched twice "
    "within that execution, including forced executions.",
)
def company_brave_search_results(
    context: dg.AssetExecutionContext,
    config: BraveSearchConfig,
    clickhouse: ClickhouseResource,
    processing_clickhouse: ClickhouseResource,
    company_brave_browser: BraveBrowserResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    if config.input_batch_size < len(ROUTES) * config.requests_per_route:
        raise ValueError("input_batch_size must cover all configured request slots")
    execution = prepare_execution(context, config, clickhouse, processing)
    execution_id = execution["execution_id"]
    outcome_counts_sql = (
        f"SELECT countIf(status='success'),countIf(status='error') FROM {RESULT_TABLE} FINAL "
        "WHERE execution_id=%(execution_id)s"
    )
    context.add_output_metadata(
        {"task_id": execution["task_id"], "execution_id": execution_id}
    )
    with processing_clickhouse.get_connection() as client:
        for relation in (
            RESULT_TABLE,
            "corpscout.company_brave_search_results_latest",
            "corpscout.se_company_brave_search_results_latest_success",
        ):
            if client.execute(f"EXISTS TABLE {relation}") != [(1,)]:
                raise ValueError(
                    f"apply the Brave ClickHouse migration before processing: {relation}"
                )
        repair_current_answers(client, execution_id=execution_id)
        [(succeeded, failed)] = client.execute(
            outcome_counts_sql, {"execution_id": execution_id}
        )
    source_info = execution["source_info"]
    source = ClickHouseInputQueue(
        clickhouse,
        source_info["relation"],
        selection_task_id=source_info.get("selection_task_id"),
    )
    if config.mode == "process":
        with clickhouse.get_connection() as client:
            if source.identity(client) != source_info["table_uuid"]:
                raise ValueError("input queue was replaced; resume requires the fixed selection")
            predicate = "input_id <= %(upper)s"
            params = {"upper": source_info["upper_id"]}
            if source.selection_task_id is not None:
                predicate += " AND task_id=%(task_id)s"
                params["task_id"] = source.selection_task_id
            [(total, companies_count)] = client.execute(
                f"SELECT count(),uniqExact(tuple(country_code,company_id)) "
                f"FROM {source.relation} WHERE {predicate}", params,
            )
            if total != source_info["total"] or companies_count != total:
                raise ValueError("input selection changed or contains duplicate companies")
    counts = {"skipped": 0, "scanned": 0, "succeeded": succeeded, "failed": failed}
    counts_lock = Lock()
    resumed_count = succeeded + failed
    last_reported_count = resumed_count
    last_reported_at = monotonic()
    pending: dict[str, dict] = {}
    started_at = datetime.fromisoformat(execution["started_at"])

    def report_progress(*, phase: str = "running") -> None:
        nonlocal last_reported_count, last_reported_at
        with counts_lock:
            processed = counts["succeeded"] + counts["failed"]
            accounted = processed + counts["skipped"]
            now = monotonic()
            if (
                phase == "running"
                and accounted - last_reported_count < config.progress_log_every
                and now - last_reported_at < config.progress_log_interval_seconds
            ):
                return
            total = source_info["total"]
            context.log.info(
                "Brave progress execution=%s phase=%s total=%d processed=%d remaining=%d "
                "succeeded=%d failed=%d skipped=%d progress=%.2f%% new_results=%d",
                execution_id,
                phase,
                total,
                processed,
                total - accounted,
                counts["succeeded"],
                counts["failed"],
                counts["skipped"],
                100 * accounted / total if total else 100,
                processed - resumed_count,
            )
            last_reported_count = accounted
            last_reported_at = now

    def companies() -> Iterator[CompanySearchInput]:
        after = None
        while rows := source.read(
            source_info, after=after, limit=config.input_batch_size
        ):
            for values in rows:
                if (
                    values.get("country_code") != "SE"
                    or not values.get("company_id")
                    or not values.get("company_name")
                ):
                    raise ValueError(
                        "the Swedish Brave asset requires SE company_id and company_name inputs"
                    )
            with processing_clickhouse.get_connection() as client:
                latest, completed = page_outcomes(
                    client,
                    rows,
                    query_type=execution["query_type"],
                    execution_id=execution_id,
                )
            for values in rows:
                counts["scanned"] += 1
                if values["input_id"] in completed:
                    report_progress()
                    continue
                if not search_is_due(
                    latest.get((values["country_code"], values["company_id"])),
                    force=execution["force"],
                    rescan_old=execution["rescan_old"],
                    started_at=started_at,
                ):
                    with counts_lock:
                        counts["skipped"] += 1
                    report_progress()
                    continue
                query = render_query(execution["query_template"], values)
                request_id = str(uuid5(UUID(execution_id), values["input_id"]))
                pending[request_id] = values
                yield CompanySearchInput(
                    values["input_id"],
                    values["company_name"],
                    query,
                    request_id,
                    config.answer_timeout_seconds * 1000,
                )
            after = rows[-1]["input_id"]
        if counts["scanned"] != source_info["total"]:
            raise ValueError(
                "fixed input selection changed; refusing to mark missing inputs complete"
            )

    def save(result: BraveSearchResult) -> None:
        record = result_record(
            result,
            pending[result.company.request_id],
            task_id=execution["task_id"],
            execution_id=execution_id,
            query_type=execution["query_type"],
            source_run_id=context.run.run_id,
            processor_version=PROCESSOR_VERSION,
        )
        # Each browser route waits for durable acknowledgement before taking another input.
        # Connections are per writer: the native ClickHouse client is not thread-safe.
        with processing_clickhouse.get_connection() as client:
            insert_results(client, [record])
        del pending[result.company.request_id]
        # Only acknowledged writes advance live totals; resume reloads them from ClickHouse.
        with counts_lock:
            counts["succeeded" if result.status == "success" else "failed"] += 1
        report_progress()

    if config.mode == "process":
        report_progress(phase="start")
        with closing(
            company_brave_browser.iter_answers(
                companies(),
                requests_per_route=config.requests_per_route,
                on_result=save,
                llm=execution.get("llm"),
            )
        ) as answers:
            for result in answers:
                context.log.info(
                    "Brave execution=%s input=%s route=%s status=%s error=%s stage=%s elapsed_ms=%s",
                    execution_id,
                    result.company.company_id,
                    result.route,
                    result.status,
                    result.error_type,
                    result.error_stage,
                    result.elapsed_ms,
                )
    with processing_clickhouse.get_connection() as client:
        repair_current_answers(client, execution_id=execution_id)
        [(succeeded, failed)] = client.execute(
            outcome_counts_sql, {"execution_id": execution_id}
        )
    metadata = {
        "task_id": execution["task_id"],
        "execution_id": execution_id,
        "total": source_info["total"],
        "succeeded": succeeded,
        "failed": failed,
        "skipped": counts["skipped"],
        "force": execution["force"],
        "rescan_old": execution["rescan_old"],
        "results_table": RESULT_TABLE,
        "output_tables": "corpscout.se_company_brave_search_results_latest_success",
        "request_slots": len(ROUTES) * config.requests_per_route,
    }
    if config.mode == "process":
        remaining = source_info["total"] - counts["skipped"] - succeeded - failed
        if remaining != 0:
            raise ValueError("ClickHouse outcomes do not account for the complete input selection")
        metadata["remaining"] = remaining
        with counts_lock:
            counts.update(succeeded=succeeded, failed=failed)
        report_progress(phase="finished")
    if config.mode == "process" and failed:
        raise dg.Failure(
            "Brave completed with failed searches; outcomes are saved. Use a new forced execution to retry.",
            metadata=metadata,
            allow_retries=False,
        )
    return dg.MaterializeResult(metadata=metadata)


company_brave_search_job = dg.define_asset_job(
    name="company_brave_search_job",
    selection=dg.AssetSelection.assets(company_brave_search_results),
)

defs = dg.Definitions(
    assets=[company_brave_search_results],
    jobs=[company_brave_search_job],
    resources={
        "company_brave_browser": BraveBrowserResource(
            api_url=os.getenv("BROWSER_API_URL") or DEFAULT_BROWSER_API_URL,
            api_token=dg.EnvVar("BROWSER_API_TOKEN"),
        ),
        "processing": ProcessingResource(postgres_url=dg.EnvVar("PROCESSING_PG_URL")),
        "processing_clickhouse": ClickhouseResource(
            host=dg.EnvVar("CLICKHOUSE_HOST"),
            port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
            user=dg.EnvVar("PROCESSING_CLICKHOUSE_USER"),
            password=dg.EnvVar("PROCESSING_CLICKHOUSE_PASSWORD"),
            database=dg.EnvVar("CLICKHOUSE_DATABASE"),
            secure=os.getenv("CLICKHOUSE_SECURE", "false").lower()
            in ("1", "true", "yes"),
        ),
    },
)
