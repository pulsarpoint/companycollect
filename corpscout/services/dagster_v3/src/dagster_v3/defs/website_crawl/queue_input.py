"""Append source selections to one open draft per scope and crawl type."""

import hashlib
import json
from datetime import UTC, datetime
from itertools import batched
from typing import Literal
from uuid import UUID

import dagster as dg
from corpscout_identity.registration import (
    WebsiteObservation,
    identify_website,
    register_websites,
)
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
    if config.workspace_domain_filters is None:
        # Keep existing SE/manual submission fingerprints stable.
        selection.pop("workspace_domain_filters")
    # Preserve receipts created before the suffix filter was available.
    if config.se_domain_filters is not None and not config.se_domain_filters.suffix:
        selection["se_domain_filters"].pop("suffix")
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
                    # The source reader has its own connection so parent registration and
                    # inserts can proceed without interrupting its streaming response.
                    with clickhouse.get_connection() as reader:
                        selected = reader.execute_iter(
                            selected_sql,
                            params,
                            query_id=query_id,
                            settings={**settings, "max_block_size": 5000},
                        )
                        for group in batched(selected, 5000):
                            stamp = datetime.now(UTC)
                            identities = [
                                (domain, url, identify_website(url))
                                for domain, url in group
                            ]
                            register_websites(
                                client,
                                [
                                    WebsiteObservation(identity, stamp, None, None)
                                    for _, _, identity in identities
                                ],
                                source="website_crawl_requests",
                                run_id=submission_id,
                            )
                            ids = tuple(
                                {identity.website_id for _, _, identity in identities}
                            )
                            existing = {
                                row[0]
                                for row in client.execute(
                                    f"SELECT website_id FROM {target}_current WHERE website_id IN %(ids)s",
                                    {"ids": ids},
                                    settings=settings,
                                )
                            }
                            presets = [
                                dict(
                                    domain=domain,
                                    website_url=url,
                                    website_id=identity.website_id,
                                    request_identity_version=2,
                                    source=params["source"],
                                    created_at=stamp,
                                    updated_at=stamp,
                                    revision=1,
                                    **(
                                        {"priority": config.priority}
                                        if config.priority is not None
                                        else {}
                                    ),
                                )
                                for domain, url, identity in identities
                                if identity.website_id not in existing
                            ]
                            if presets:
                                client.execute(
                                    f"INSERT INTO {target} ({','.join(presets[0])}) VALUES",
                                    presets,
                                    settings=settings,
                                )
                            queued = {
                                row[0]
                                for row in client.execute(
                                    f"SELECT website_id FROM {TASK_DOMAINS} WHERE task_id=%(task)s AND website_id IN %(ids)s",
                                    {"task": task_id, "ids": ids},
                                    settings=settings,
                                )
                            }
                            entries = [
                                dict(
                                    task_id=task_id,
                                    crawl_type=config.crawl_type,
                                    domain=domain,
                                    website_url=url,
                                    website_id=identity.website_id,
                                    request_identity_version=2,
                                    source_name=params["source"],
                                    submission_id=submission_id,
                                )
                                for domain, url, identity in identities
                                if identity.website_id not in queued
                            ]
                            if entries:
                                client.execute(
                                    f"INSERT INTO {TASK_DOMAINS} ({','.join(entries[0])}) VALUES",
                                    entries,
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
