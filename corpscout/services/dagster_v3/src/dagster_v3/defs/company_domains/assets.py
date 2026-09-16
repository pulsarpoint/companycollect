"""Fixed ClickHouse inputs with durable PostgreSQL progress and Brave responses."""

import os
from contextlib import closing
from threading import Event, Thread
from time import monotonic
from typing import Literal, Self
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common.clickhouse_queue import (
    ClickHouseInputQueue,
    validate_relation,
)
from dagster_v3.defs.common.processing import (
    ProcessingResource,
    render_query,
    work_key,
)
from dagster_v3.defs.company_domains.publication import publish_results
from dagster_v3.defs.company_domains.browser import (
    ROUTES,
    BraveBrowserResource,
    BraveSearchResult,
    CompanySearchInput,
)

PROCESSOR_VERSION = "brave-v2"


class BraveSearchConfig(dg.Config):
    task_id: str | None = None
    mode: Literal["process", "publish"] = "process"
    input_relation: str | None = None
    input_namespace: str = Field(default="company", min_length=1)
    query_type: str = Field(default="official_website", min_length=1)
    query_template: str = Field(
        default="Find the official website of {company_name}.", min_length=1
    )
    requests_per_route: int = Field(default=1, ge=1, le=8)
    input_batch_size: int = Field(default=100, ge=4, le=10_000)
    freshness_days: int = Field(default=30, ge=0)
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_failed: bool = False
    answer_timeout_seconds: int = Field(default=60, ge=1, le=600)
    max_answer_timeout_seconds: int = Field(default=180, ge=1, le=600)
    retry_seconds: int = Field(default=60, ge=0, le=3600)
    lease_seconds: int = Field(default=300, ge=30, le=3600)
    export_batch_size: int = Field(default=100, ge=1, le=10_000)
    export_interval_seconds: int = Field(default=30, ge=1, le=3600)

    @model_validator(mode="after")
    def validate_timeouts(self) -> Self:
        if self.max_answer_timeout_seconds < self.answer_timeout_seconds:
            raise ValueError(
                "maximum answer timeout must cover the initial answer timeout"
            )
        if self.retry_failed and self.mode != "process":
            raise ValueError("retry_failed requires process mode")
        return self

    @field_validator("input_relation")
    @classmethod
    def named_relation(cls, value: str | None) -> str | None:
        return validate_relation(value) if value is not None else None

    @field_validator("task_id")
    @classmethod
    def stable_task_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None


@dg.asset(
    deps=["company_brave_search_input"],
    group_name="brave_domain_search",
    kinds={"python", "browser", "postgres", "clickhouse"},
    pool="company_domains_brave",
    tags={"source": "brave", "country": "SE"},
    metadata={"dagster/table_name": "corpscout.se_company_brave_domains"},
    description="Latest successful Brave answers for Swedish companies in corpscout.se_company_brave_domains. "
    "Read a fixed ClickHouse input queue, render the query template, and collect copied Brave "
    "responses with four continuously refilled routes. PostgreSQL holds task progress and responses; "
    "closed batches are archived to S3 and latest successful answers are imported into the Swedish output table by ClickHouse SQL. Supply task_id to resume or mode=publish to replay exports.",
)
def se_company_brave_domains(
    context: dg.AssetExecutionContext,
    config: BraveSearchConfig,
    clickhouse: ClickhouseResource,
    processing_clickhouse: ClickhouseResource,
    company_brave_browser: BraveBrowserResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    if (
        config.mode == "process"
        and config.input_batch_size < len(ROUTES) * config.requests_per_route
    ):
        raise ValueError("input_batch_size must cover all configured request slots")
    current_run = context.instance.get_run_by_id(context.run.run_id)
    tagged_task_id = current_run.tags.get("processing/task_id") if current_run else None
    if config.task_id and tagged_task_id and config.task_id != tagged_task_id:
        raise ValueError(
            "processing task_id differs from the task prepared in this run"
        )
    task_id = config.task_id or tagged_task_id or context.run.run_id
    # Persist identity in the event log before selection so interrupted preparation is traceable.
    context.add_output_metadata({"task_id": task_id})
    context.log.info("Brave task_id=%s mode=%s", task_id, config.mode)
    with processing.get_store() as store:
        task = store.task(task_id)
        if task is None:
            if config.mode == "publish":
                raise ValueError("publish mode requires an existing task_id")
            if config.input_relation is None:
                raise ValueError(
                    "initialize company_brave_search_input and supply its task_id, or provide a prepared input_relation"
                )
            saved_config = {
                key: getattr(config, key)
                for key in (
                    "input_relation",
                    "input_namespace",
                    "query_type",
                    "query_template",
                    "freshness_days",
                )
            }
            source = ClickHouseInputQueue(clickhouse, config.input_relation)
            store.register(
                task_id,
                processor=PROCESSOR_VERSION,
                config=saved_config,
                work_config={
                    key: saved_config[key] for key in ("input_namespace", "query_type")
                },
                source_info=source.inspect(),
            )
            task = store.task(task_id)
        if task["processor"] != PROCESSOR_VERSION:
            raise ValueError("task belongs to a different processor version")
        if task["status"] == "selected" and config.mode == "process":
            task = store.activate_selection(
                task_id,
                config={
                    key: getattr(config, key)
                    for key in (
                        "input_namespace",
                        "query_type",
                        "query_template",
                        "freshness_days",
                    )
                },
                work_config={
                    key: getattr(config, key)
                    for key in ("input_namespace", "query_type")
                },
            )
        if task["status"] != "ready":
            raise ValueError("task is not ready for processing")
        if config.retry_failed:
            requeued = store.retry_failed(task_id, max_attempts=config.max_attempts)
            context.log.info("Brave task=%s requeued_failed=%s", task_id, requeued)
        # A resume always uses the registered selection and template.
        saved_config = task["config"]
        source_info = task["source_info"]
        source = (
            ClickHouseInputQueue(
                clickhouse,
                source_info["relation"],
                selection_task_id=source_info.get("selection_task_id"),
            )
            if source_info
            else None
        )
        input_cache = {}
        errors = []
        stopped = Event()

        def maintain_leases():
            while not stopped.wait(config.lease_seconds / 3):
                try:
                    store.heartbeat(
                        context.run.run_id, lease_seconds=config.lease_seconds
                    )
                except Exception as error:
                    errors.append(type(error).__name__)
                    stopped.set()

        heartbeat = Thread(target=maintain_leases, name="brave_leases", daemon=True)
        claims = {}

        def companies():
            # iter_answers serializes this generator while browser workers run independently.
            while not stopped.is_set():
                item = store.claim(
                    task_id,
                    owner=context.run.run_id,
                    lease_seconds=config.lease_seconds,
                    max_attempts=config.max_attempts,
                )
                if item is None:
                    current = store.task(task_id)
                    if current["admitted_count"] == current["total"]:
                        return
                    open_count = current["admitted_count"] - sum(
                        current[key]
                        for key in (
                            "succeeded_count",
                            "terminal_failed_count",
                            "skipped_count",
                            "cancelled_count",
                        )
                    )
                    available = config.input_batch_size - open_count
                    if available <= 0:
                        return
                    rows = source.read(
                        source_info, after=current["source_cursor"], limit=available
                    )
                    if not rows:
                        raise ValueError(
                            "fixed input queue ended before its registered total"
                        )
                    for values in rows:
                        render_query(saved_config["query_template"], values)
                    if store.admit(
                        task_id,
                        after=current["source_cursor"],
                        input_ids=[row["input_id"] for row in rows],
                        capacity=config.input_batch_size,
                    ):
                        input_cache.update((row["input_id"], row) for row in rows)
                        context.log.info(
                            "Brave task=%s admitted=%s/%s",
                            task_id,
                            current["admitted_count"] + len(rows),
                            current["total"],
                        )
                    continue
                values = input_cache.pop(item.input_id, None)
                if values is None:
                    values = source.read(source_info, input_id=item.input_id)[0]
                query = render_query(saved_config["query_template"], values)
                key = work_key(
                    PROCESSOR_VERSION,
                    task["work_config"],
                    saved_config["query_template"],
                    values,
                    query,
                )
                if store.skip_if_fresh(
                    item, work_key=key, freshness_days=saved_config["freshness_days"]
                ):
                    continue
                timeout_count = 0
                if item.attempt > 1:
                    # Indexed by task/input/attempt; archived response text is not needed.
                    # Older failures have no stage: conservatively treat those timeouts
                    # as eligible, without inventing a stage for their history.
                    with store.transaction() as cursor:
                        cursor.execute(
                            """SELECT count(*) AS timeouts FROM processing.results
                            WHERE task_id=%s AND input_id=%s AND attempt<%s
                              AND status='error' AND payload->>'error_type'='TimeoutError'
                              AND coalesce(payload->>'error_stage','') IN ('','answer_generation')""",
                            (task_id, item.input_id, item.attempt),
                        )
                        timeout_count = cursor.fetchone()["timeouts"]
                answer_timeout_ms = 1000 * min(
                    config.answer_timeout_seconds * (1 + timeout_count),
                    config.max_answer_timeout_seconds,
                )
                # Retain only request/result attribution while the browser is active.
                claims[item.lease_token] = (
                    item,
                    key,
                    {
                        "query": query,
                        "company_id": str(values.get("company_id") or item.input_id),
                        "company_name": str(values.get("company_name") or ""),
                        "country_code": str(values.get("country_code") or ""),
                    },
                )
                yield CompanySearchInput(
                    item.input_id,
                    str(values.get("company_name") or item.input_id),
                    query,
                    item.lease_token,
                    answer_timeout_ms,
                )

        def save(result: BraveSearchResult):
            item, key, attribution = claims.pop(result.company.request_id)
            if result.status == "success" and not result.answer.strip():
                raise ValueError("cannot save an empty successful Brave response")
            store.complete(
                item,
                status=result.status,
                work_key=key,
                completed_at=result.fetched_at,
                payload={
                    **attribution,
                    "answer_text": result.answer,
                    "route": result.route,
                    "source_url": result.source_url,
                    "error_type": result.error_type,
                    "error_stage": result.error_stage,
                    "elapsed_ms": result.elapsed_ms,
                    "answer_timeout_ms": result.company.answer_timeout_ms,
                    "source_run_id": context.run.run_id,
                },
                max_attempts=config.max_attempts,
                retry_seconds=config.retry_seconds,
            )

        last_export = monotonic()
        saved_since_export = 0
        try:
            heartbeat.start()
            with processing_clickhouse.get_connection() as client:
                if config.mode == "process":
                    while not stopped.is_set() and store.progress(task_id)["remaining"]:
                        with closing(
                            company_brave_browser.iter_answers(
                                companies(),
                                requests_per_route=config.requests_per_route,
                                on_result=save,
                            )
                        ) as results:
                            for result in results:
                                saved_since_export += 1
                                if (
                                    saved_since_export >= config.export_batch_size
                                    or monotonic() - last_export
                                    >= config.export_interval_seconds
                                ):
                                    try:
                                        publish_results(
                                            store,
                                            client,
                                            task_id,
                                            batch_size=config.export_batch_size,
                                        )
                                    except Exception as error:
                                        context.log.warning(
                                            "Publication deferred (%s); responses are saved in PostgreSQL",
                                            type(error).__name__,
                                        )
                                    last_export = monotonic()
                                    saved_since_export = 0
                                context.log.info(
                                    "Brave task=%s input=%s route=%s status=%s error=%s stage=%s answer_timeout_ms=%s elapsed_ms=%s",
                                    task_id,
                                    result.company.company_id,
                                    result.route,
                                    result.status,
                                    result.error_type,
                                    result.error_stage,
                                    result.company.answer_timeout_ms,
                                    result.elapsed_ms,
                                )
                        if store.progress(task_id)["remaining"]:
                            stopped.wait(1)
                if errors:
                    raise dg.Failure(
                        "Lease maintenance failed; resume the saved task",
                        metadata={"task_id": task_id},
                    )
                publish_results(
                    store, client, task_id, batch_size=config.export_batch_size
                )
        finally:
            stopped.set()
            heartbeat.join()
            store.release(context.run.run_id, max_attempts=config.max_attempts)
        counts = store.progress(task_id)
        metadata = {
            key: value
            for key, value in counts.items()
            if key not in ("task_id", "processor", "status")
        }
        metadata.update(
            task_id=task_id,
            output_tables="corpscout.se_company_brave_domains",
            request_slots=len(ROUTES) * config.requests_per_route,
        )
        if config.mode == "process" and counts["terminal_failed"]:
            raise dg.Failure(
                "Task finished with failed items; responses and progress are saved",
                metadata=metadata,
            )
        return dg.MaterializeResult(metadata=metadata)


company_brave_search_job = dg.define_asset_job(
    name="company_brave_search_job",
    selection=dg.AssetSelection.assets(se_company_brave_domains),
)

defs = dg.Definitions(
    assets=[se_company_brave_domains],
    jobs=[company_brave_search_job],
    resources={
        "company_brave_browser": BraveBrowserResource(
            crawl_proxy1=dg.EnvVar("crawl_proxy1"),
            crawl_proxy2=dg.EnvVar("crawl_proxy2"),
            crawl_proxy3=dg.EnvVar("crawl_proxy3"),
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
