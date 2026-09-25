"""Append source selections to one open draft per scope and crawl type."""

import hashlib
import json
from typing import Literal
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.input import (
    INPUT_TABLES,
    TASK_DOMAINS,
    CrawlInputConfig,
    selected_domains_sql,
    task_processor,
)


class CrawlQueueInputConfig(CrawlInputConfig):
    crawl_type: Literal["full", "jobs", "site_info"]
    queue_scope: str = Field(default="workspace", min_length=1)
    submission_id: str | None = None

    @field_validator("submission_id")
    @classmethod
    def valid_submission(cls, value):
        return str(UUID(value)) if value is not None else None

    @field_validator("queue_scope")
    @classmethod
    def valid_scope(cls, value):
        if not value.strip():
            raise ValueError("queue_scope must not be blank")
        return value


def load_crawl_draft(config, submission_id, store, clickhouse):
    selection = config.model_dump(exclude={"task_id", "submission_id", "queue_scope"})
    for key in ("ids", "excluded_ids", "targets"):
        selection[key] = sorted(set(selection[key]))
    selection["filters"] = {
        key: sorted(set(values)) for key, values in selection["filters"].items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    # Keep bulk manual values out of PostgreSQL receipts.
    selection["targets"] = {
        "count": len(selection["targets"]),
        "sha256": hashlib.sha256(json.dumps(selection["targets"]).encode()).hexdigest(),
    }
    processor = task_processor(config.crawl_type)
    receipt = draft_queue.submission(store, submission_id)
    task_id = str(receipt["task_id"]) if receipt else None
    if receipt:
        task = store.task(task_id)
        if (
            task["processor"] != processor
            or task["queue_scope"] != config.queue_scope
            or (config.task_id is not None and config.task_id != task_id)
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError(
                "submission_id belongs to a different selection, task or scope"
            )
        if receipt["status"] == "completed":
            return {
                "task_id": task_id,
                "submission_id": submission_id,
                "input_count": receipt["input_count"],
                "total": task["total"],
            }
    while True:
        if receipt is None:
            task_id = draft_queue.find_draft(
                store,
                scope=config.queue_scope,
                processor=processor,
                task_id=config.task_id,
            )
        with store.selection_lock(task_id):
            latest = draft_queue.submission(store, submission_id)
            if latest is not None:
                if (
                    str(latest["task_id"]) != task_id
                    or latest["selection_fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "submission_id belongs to another task or selection"
                    )
                if latest["status"] == "completed":
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": latest["input_count"],
                        "total": store.task(task_id)["total"],
                    }
            if (
                store.task(task_id)["status"] != "draft"
                and receipt is None
                and config.task_id is None
            ):
                continue
            receipt = draft_queue.prepare_submission(
                store,
                task_id=task_id,
                submission_id=submission_id,
                source=config.source_relation or "manual",
                selection=selection,
                fingerprint=fingerprint,
            )
            if receipt["status"] == "completed":
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": receipt["input_count"],
                    "total": store.task(task_id)["total"],
                }
            try:
                with clickhouse.get_connection() as client:
                    query_id = "crawl-queue-import:" + submission_id
                    client.execute(
                        "KILL QUERY WHERE query_id=%(id)s SYNC", {"id": query_id}
                    )
                    # A retry replaces only this submission's rows, from the current source.
                    client.execute(
                        f"DELETE FROM {TASK_DOMAINS} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                        settings={"lightweight_deletes_sync": 2},
                    )
                    selected_sql, params = selected_domains_sql(config)
                    params.update(
                        source=config.source_relation or "manual",
                        task=task_id,
                        type=config.crawl_type,
                        submission=submission_id,
                        priority=config.priority,
                    )
                    settings = {"async_insert": 0, "use_query_cache": 0}
                    [(count,)] = client.execute(
                        f"SELECT count() FROM ({selected_sql}) AS selected",
                        params,
                        query_id=query_id,
                        settings=settings,
                    )
                    if count > 1_000_000:
                        raise ValueError(
                            "A crawl submission supports at most one million domains"
                        )
                    if config.targets and count == 0:
                        raise ValueError("No valid HTTP(S) websites in targets")
                    target = INPUT_TABLES[
                        ("full", "jobs", "site_info").index(config.crawl_type)
                    ]
                    priority = ", priority" if config.priority is not None else ""
                    priority_value = (
                        ", %(priority)s" if config.priority is not None else ""
                    )
                    # Persist recurring presets without replacing operator settings.
                    client.execute(
                        f"""INSERT INTO {target} (domain,website_url,source,created_at,updated_at,revision{priority})
                        SELECT s.domain,s.website_url,%(source)s,now64(6),now64(6),1{priority_value}
                        FROM ({selected_sql}) AS s LEFT ANTI JOIN {target}_current AS e ON s.domain=e.domain""",
                        params,
                        query_id=query_id,
                        settings=settings,
                    )
                    # Entries land straight in the task's partition; the first queued URL wins.
                    client.execute(
                        f"""INSERT INTO {TASK_DOMAINS} (task_id,crawl_type,domain,website_url,source_name,submission_id)
                        SELECT %(task)s,%(type)s,s.domain,s.website_url,%(source)s,%(submission)s
                        FROM ({selected_sql}) AS s LEFT ANTI JOIN
                        (SELECT domain FROM {TASK_DOMAINS} WHERE task_id=%(task)s) AS e ON s.domain=e.domain""",
                        params,
                        query_id=query_id,
                        settings=settings,
                    )
                    [(total,)] = client.execute(
                        f"SELECT count() FROM {TASK_DOMAINS} WHERE task_id=%(task)s",
                        {"task": task_id},
                    )
                    if total > 1_000_000:
                        raise ValueError(
                            "A crawl draft supports at most one million domains"
                        )
                    draft_queue.finish_submission(
                        store,
                        submission_id=submission_id,
                        task_id=task_id,
                        count=count,
                        total=total,
                    )
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": count,
                        "total": total,
                    }
            except BaseException:
                draft_queue.fail_submission(store, submission_id)
                raise


@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse", "postgres"},
    pool="website_crawl_input",
    metadata={"dagster/table_name": TASK_DOMAINS},
    description="Append manual URLs or source selections to the open crawl draft. Does not start crawling or skip recent results.",
)
def website_crawl_input(
    context: dg.AssetExecutionContext,
    config: CrawlQueueInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    submission_id = (
        config.submission_id
        or context.run.tags.get("processing/submission_id")
        or context.run.root_run_id
        or context.run.run_id
    )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/submission_id": submission_id}
    )
    with processing.get_store() as store:
        metadata = load_crawl_draft(config, submission_id, store, clickhouse)
    context.instance.add_run_tags(
        context.run.run_id, {"processing/task_id": metadata["task_id"]}
    )
    return dg.MaterializeResult(metadata=metadata)


website_crawl_input_job = dg.define_asset_job(
    "website_crawl_input_job", selection=dg.AssetSelection.assets(website_crawl_input)
)
defs = dg.Definitions(assets=[website_crawl_input], jobs=[website_crawl_input_job])
