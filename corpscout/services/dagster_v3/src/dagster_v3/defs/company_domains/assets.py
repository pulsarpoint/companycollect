"""Frozen Brave tasks with durable responses and replayable ClickHouse publication."""

import json
import os
import re
from collections.abc import Iterator
from contextlib import closing
from threading import Event, Thread
from time import monotonic
from typing import Literal
from uuid import UUID, uuid4

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore
from dagster_v3.defs.company_domains.browser import (
    ROUTES,
    BraveBrowserResource,
    BraveSearchResult,
    CompanySearchInput,
)

PROCESSOR_VERSION = "brave-v2"
RESULT_TABLE = "company_brave_info"
EXPORT_DESTINATION = "company_brave_info_v1"
EXPORT_COLUMNS = (
    "result_id",
    "task_id",
    "input_id",
    "work_key",
    "attempt",
    "status",
    "export_batch_id",
    "country_code",
    "company_id",
    "company_name",
    "query",
    "query_type",
    "processor_version",
    "answer_text",
    "route",
    "source_url",
    "error_type",
    "source_run_id",
    "completed_at",
)
# A server-owned named collection supplies the read-only PostgreSQL connection.
PUBLISH_SQL = f"""INSERT INTO corpscout.{RESULT_TABLE} ({", ".join(EXPORT_COLUMNS)})
SELECT {", ".join(EXPORT_COLUMNS)}
FROM postgresql(processing_postgres, table='brave_export', schema='processing')
WHERE export_batch_id = %(batch_id)s
"""


class BraveSearchConfig(dg.Config):
    task_id: str | None = None
    mode: Literal["process", "publish"] = "process"
    input_relation: str = "corpscout.se_company_brave_input"
    input_namespace: str = Field(default="se_company", min_length=1)
    query_type: str = Field(default="official_website", min_length=1)
    query_template: str = Field(
        default="Find the official website of {company_name}.", min_length=1
    )
    requests_per_route: int = Field(default=1, ge=1, le=8)
    max_companies: int | None = Field(default=None, ge=1)
    company_ids: list[str] = Field(default_factory=list)
    freshness_days: int = Field(default=30, ge=0)
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_seconds: int = Field(default=60, ge=0, le=3600)
    lease_seconds: int = Field(default=300, ge=30, le=3600)
    export_batch_size: int = Field(default=100, ge=1, le=10_000)
    export_interval_seconds: int = Field(default=30, ge=1, le=3600)

    @field_validator("input_relation")
    @classmethod
    def named_relation(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(
                "input_relation must be a database.table or database.view name"
            )
        return value

    @field_validator("task_id")
    @classmethod
    def stable_task_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None


def snapshot_inputs(
    clickhouse: ClickhouseResource, config: BraveSearchConfig
) -> Iterator[dict]:
    """One streaming SELECT freezes membership and values; no moving keyset cursor."""
    sql = f"SELECT * FROM {config.input_relation}"
    params = {}
    if config.company_ids:
        sql += " WHERE input_id IN %(ids)s"
        params["ids"] = tuple(config.company_ids)
    if config.max_companies is not None:
        sql += " ORDER BY input_id LIMIT %(limit)s"
        params["limit"] = config.max_companies
    with clickhouse.get_connection() as client:
        rows = client.execute_iter(
            sql, params, with_column_types=True, settings={"max_block_size": 1000}
        )
        columns = [name for name, _ in next(rows)]
        if "input_id" not in columns or len(columns) != len(set(columns)):
            raise ValueError(
                "input relation requires uniquely named columns including input_id"
            )
        for row in rows:
            # Dates/decimals from other named input views are frozen as text too.
            yield json.loads(
                json.dumps(dict(zip(columns, row, strict=True)), default=str)
            )


def publish_results(
    store: ProcessingStore, client, task_id: str, *, batch_size: int
) -> int:
    published = 0
    while batch := store.export_batch(
        task_id, limit=batch_size, destination=EXPORT_DESTINATION
    ):
        client.execute(
            PUBLISH_SQL,
            {"batch_id": batch.batch_id},
            settings={
                "max_execution_time": 60,
                "postgresql_connection_pool_size": 2,
                "postgresql_connection_attempt_timeout": 5,
            },
        )
        [(count,)] = client.execute(
            f"SELECT count() FROM corpscout.{RESULT_TABLE} FINAL WHERE task_id=%(task_id)s AND export_batch_id=%(batch_id)s",
            {"batch_id": batch.batch_id, "task_id": task_id},
        )
        if count != batch.result_count:
            raise ValueError(
                "ClickHouse publication count does not match the closed batch"
            )
        store.acknowledge(batch)
        published += batch.result_count
    return published


@dg.asset(
    group_name="company_domains",
    kinds={"python", "browser", "postgres", "clickhouse"},
    pool="company_domains_brave",
    tags={"source": "brave"},
    description="Freeze a named input relation, render the query template, and collect copied Brave "
    "responses with four continuously refilled routes. PostgreSQL holds task progress and responses; "
    "closed batches are imported by ClickHouse SQL. Supply task_id to resume or mode=publish to replay exports.",
)
def company_brave_search_results(
    context: dg.AssetExecutionContext,
    config: BraveSearchConfig,
    clickhouse: ClickhouseResource,
    processing_clickhouse: ClickhouseResource,
    company_brave_browser: BraveBrowserResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    task_id = config.task_id or str(uuid4())
    # Persist identity in the event log before selection so interrupted preparation is traceable.
    context.add_output_metadata({"task_id": task_id})
    context.log.info("Brave task_id=%s mode=%s", task_id, config.mode)
    with processing.get_store() as store:
        task = store.task(task_id)
        if task is None:
            if config.mode == "publish":
                raise ValueError("publish mode requires an existing task_id")
            frozen_config = {
                key: getattr(config, key)
                for key in (
                    "input_relation",
                    "input_namespace",
                    "query_type",
                    "query_template",
                    "company_ids",
                    "max_companies",
                    "freshness_days",
                )
            }
            with closing(snapshot_inputs(clickhouse, config)) as inputs:
                store.freeze(
                    task_id,
                    processor=PROCESSOR_VERSION,
                    config=frozen_config,
                    work_config={
                        key: frozen_config[key]
                        for key in ("input_namespace", "query_type")
                    },
                    inputs=inputs,
                    query_template=config.query_template,
                    freshness_days=config.freshness_days,
                )
        elif task["processor"] != PROCESSOR_VERSION:
            raise ValueError("task belongs to a different processor version")
        elif task["status"] != "ready":
            raise ValueError("task is not ready for processing")
        # New input/template settings only apply to new tasks. Resumes use the stored queries.
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
            while not stopped.is_set():
                item = store.claim(
                    task_id,
                    owner=context.run.run_id,
                    lease_seconds=config.lease_seconds,
                    max_attempts=config.max_attempts,
                )
                if item is None:
                    return
                claims[item.lease_token] = item
                yield CompanySearchInput(
                    item.input_id,
                    str(item.input_data.get("company_name") or item.input_id),
                    item.query,
                    item.lease_token,
                )

        def save(result: BraveSearchResult):
            item = claims.pop(result.company.request_id)
            if result.status == "success" and not result.answer.strip():
                raise ValueError("cannot save an empty successful Brave response")
            store.complete(
                item,
                status=result.status,
                completed_at=result.fetched_at,
                payload={
                    "answer_text": result.answer,
                    "route": result.route,
                    "source_url": result.source_url,
                    "error_type": result.error_type,
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
                                    "Brave task=%s input=%s route=%s status=%s",
                                    task_id,
                                    result.company.company_id,
                                    result.route,
                                    result.status,
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
            store.release(context.run.run_id)
        counts = store.progress(task_id)
        metadata = {
            key: value
            for key, value in counts.items()
            if key not in ("task_id", "processor", "status")
        }
        metadata.update(
            task_id=task_id,
            table=f"corpscout.{RESULT_TABLE}",
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
    selection=dg.AssetSelection.assets(company_brave_search_results),
)

defs = dg.Definitions(
    assets=[company_brave_search_results],
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
