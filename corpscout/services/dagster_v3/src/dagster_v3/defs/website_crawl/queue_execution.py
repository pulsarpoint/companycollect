"""Freeze crawl drafts, checkpoint durable outcomes and remove completed membership."""

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from itertools import batched
from time import monotonic, sleep
from uuid import uuid4

import dagster as dg
from dlt.sources.helpers.requests import Session
from psycopg2.extras import Json

from dagster_v3.defs.website_crawl.input import TASK_DOMAINS, task_processor
from dagster_v3.defs.website_crawl.results import (
    DEFAULT_CRAWLER_API_URL,
    INPUTS_BY_TYPE,
    RESULTS_BY_TYPE,
    SUBMISSIONS,
    effective_payload,
    read_rows,
    result_record,
    send_crawl,
)


def save_document(objects, key, document):
    body = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    if objects.exists(key):
        if objects.read_bytes(key) != body:
            raise ValueError("Crawl execution snapshot changed")
    else:
        objects.write_bytes(key, body)
    return {"key": key, "sha256": hashlib.sha256(body).hexdigest()}


def read_document(objects, reference):
    body = objects.read_bytes(reference["key"])
    if hashlib.sha256(body).hexdigest() != reference["sha256"]:
        raise ValueError("Crawl execution checksum mismatch")
    return json.loads(body)


# The request identity, computed where the entries live. The Python twin is
# dispatch.crawl_payload(row, crawl_type, execution_id)["request_id"].
REQUEST_ID_SQL = "concat('dagster-crawl-', lower(hex(SHA256(concat(%(exec)s, ':', %(type)s, ':', domain)))))"


def _clickhouse_time(value: str) -> str:
    return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def crawl_parameters(task: dict, crawl_type: str) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "type": crawl_type,
        "exec": execution["execution_id"],
        "cutoff": _clickhouse_time(execution["freshness_cutoff"]),
        "started": _clickhouse_time(execution["started_at"]),
    }


def remaining_crawl_entries(
    client, task: dict, crawl_type: str, *, after: str = "", limit: int = 500
) -> list[dict]:
    """Frozen entries after ``after`` (by domain) without a result of this execution.

    Each row carries the current preset so the caller can build the payload and
    decide skips; the preset columns are named explicitly to avoid clashing with
    the entry's own domain and URL.
    """
    return read_rows(
        client,
        f"""SELECT q.domain AS domain, q.website_url AS selected_url, q.request_id AS request_id,
            p.website_url AS preset_url, p.enabled AS enabled, p.revision AS revision,
            p.page_mode AS page_mode, p.pages AS pages, p.instructions AS instructions,
            p.headless AS headless, p.proxy_route AS proxy_route,
            p.save_artifacts AS save_artifacts, p.preset_version AS preset_version,
            p.config_json AS config_json
        FROM (
            SELECT domain, website_url, {REQUEST_ID_SQL} AS request_id
            FROM {TASK_DOMAINS}
            WHERE task_id=%(task)s AND crawl_type=%(type)s AND domain > %(after)s
        ) AS q
        LEFT JOIN {INPUTS_BY_TYPE[crawl_type]}_current AS p ON q.domain = p.domain
        WHERE q.request_id NOT IN (
            SELECT request_id FROM {RESULTS_BY_TYPE[crawl_type]} WHERE run_id = %(exec)s)
        ORDER BY q.domain LIMIT %(limit)s""",
        {**crawl_parameters(task, crawl_type), "after": after, "limit": limit},
    )


def fresh_work_keys(
    client, task: dict, crawl_type: str, pairs: list[tuple[str, str]]
) -> set:
    """(domain, work_key) pairs with a success inside the frozen window (a later failure never hides it)."""
    if not pairs:
        return set()
    return set(
        client.execute(
            f"""SELECT domain, work_key FROM {RESULTS_BY_TYPE[crawl_type]} FINAL
            WHERE (domain, work_key) IN %(pairs)s
              AND finished_at >= toDateTime64(%(cutoff)s, 6, 'UTC')
              AND finished_at <= toDateTime64(%(started)s, 6, 'UTC')
            GROUP BY domain, work_key
            HAVING max(successful)""",
            {**crawl_parameters(task, crawl_type), "pairs": tuple(pairs)},
        )
    )


def dispatchable_entries(
    client, rows: list[dict], *, task: dict, crawl_type: str, config
) -> list[dict]:
    """Requests to send for a page of remaining entries; disabled and fresh ones are skips."""
    execution = task["config"]["execution"]
    items = []
    for row in rows:
        if not row["revision"]:
            raise ValueError("A queued domain has no crawl preset")
        # Copy so a caller can re-decide the same page (e.g. with a different
        # config) without this call's pops corrupting its rows.
        row = dict(row)
        selected_url, preset_url = row.pop("selected_url"), row.pop("preset_url")
        if not row["enabled"]:
            continue
        row["website_url"] = selected_url or preset_url
        payload, work_key = effective_payload(
            row, crawl_type, execution["execution_id"], config
        )
        if payload["request_id"] != row["request_id"]:
            raise ValueError("Request identity differs between ClickHouse and Python")
        items.append(
            {
                "crawl_type": crawl_type,
                "domain": row["domain"],
                "request_id": row["request_id"],
                "input_revision": row["revision"],
                "work_key": work_key,
                "run_id": execution["execution_id"],
                "request_json": json.dumps(payload, sort_keys=True),
            }
        )
    fresh = (
        set()
        if config.force_refresh
        else fresh_work_keys(
            client,
            task,
            crawl_type,
            [(item["domain"], item["work_key"]) for item in items],
        )
    )
    return [item for item in items if (item["domain"], item["work_key"]) not in fresh]


def count_unresolved(client, task: dict, crawl_type: str, config) -> int:
    """Remaining entries that are neither disabled nor fresh; finish refuses while any exist."""
    unresolved = 0
    after = ""
    while True:
        rows = remaining_crawl_entries(client, task, crawl_type, after=after)
        if not rows:
            return unresolved
        after = rows[-1]["domain"]
        unresolved += len(
            dispatchable_entries(
                client, rows, task=task, crawl_type=crawl_type, config=config
            )
        )


def start_crawl_execution(store, client, task_id, crawl_type, config, run_id):
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != task_processor(crawl_type)
        or task["queue_scope"] is None
    ):
        raise ValueError("Expected a crawl draft of the selected type")
    if config.domains or config.bucket is not None or config.batch_id is not None:
        raise ValueError(
            "Draft processing uses the whole queue; omit domains, bucket and batch_id"
        )
    profile = config.model_dump(
        exclude={
            "task_id",
            "execution_id",
            "domains",
            "bucket",
            "batch_id",
            "max_batches",
            "wait_timeout_seconds",
            "poll_interval_seconds",
        }
    )
    saved = task["config"].get("execution")
    if saved is not None:
        if config.execution_id not in (None, saved["execution_id"]):
            raise ValueError(
                "Resume the saved execution; add a new draft for another crawl"
            )
        if saved["profile"] != profile:
            raise ValueError(
                "Execution settings are frozen; resume with the same profile"
            )
        if task["status"] not in ("selected", "ready", "completed"):
            raise ValueError("Crawl execution is not resumable")
        return task
    if task["status"] != "draft":
        raise ValueError("Queue is not an open draft")
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT count(*) AS pending FROM processing.input_submissions WHERE task_id=%s AND status NOT IN ('completed','cancelled')",
            (task_id,),
        )
        if cursor.fetchone()["pending"]:
            raise ValueError("Finish or retry outstanding imports before Start")
    [(total,)] = client.execute(
        f"SELECT count() FROM {TASK_DOMAINS} WHERE task_id=%(task)s AND crawl_type=%(type)s",
        {"task": task_id, "type": crawl_type},
    )
    if total == 0 or total != task["total"]:
        raise ValueError("Queue is empty or its membership changed")
    now = datetime.now(UTC)
    execution = {
        "execution_id": config.execution_id or str(uuid4()),
        "profile": profile,
        "dagster_run_id": run_id,
        "started_at": now.isoformat(),
        "freshness_cutoff": (
            now - timedelta(days=config.refresh_interval_days)
        ).isoformat(),
    }
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET status='selected',frozen_at=clock_timestamp(),config=%s,
            source_info=%s WHERE task_id=%s RETURNING *""",
            (
                Json({"execution": execution}),
                Json(
                    {"relation": TASK_DOMAINS, "crawl_type": crawl_type, "total": total}
                ),
                task_id,
            ),
        )
        return dict(cursor.fetchone())


def prepare_crawl_execution(store, client, objects, task, crawl_type, config):
    task_id = str(task["task_id"])
    execution = task["config"]["execution"]
    root = f"queue-executions/crawler/{task_id}/{execution['execution_id']}"
    if task["work_config"].get("plan"):
        return read_document(objects, task["work_config"]["plan"])
    objects.ensure_bucket()
    references = []
    after = ""
    total = skipped = 0
    while True:
        rows = read_rows(
            client,
            f"""SELECT q.domain AS domain,q.website_url AS selected_url,p.*
            FROM (SELECT * FROM {TASK_DOMAINS} WHERE task_id=%(task)s AND crawl_type=%(type)s AND domain>%(after)s ORDER BY domain LIMIT 500) AS q
            LEFT JOIN {INPUTS_BY_TYPE[crawl_type]}_current AS p ON q.domain=p.domain ORDER BY q.domain""",
            {"task": task_id, "type": crawl_type, "after": after},
        )
        if not rows:
            break
        key = f"{root}/batch-{len(references):06d}.json"
        if objects.exists(key):
            body = objects.read_bytes(key)
            document = json.loads(body)
            reference = {"key": key, "sha256": hashlib.sha256(body).hexdigest()}
            if (
                document["domains"] != [row["domain"] for row in rows]
                or document["execution"] != execution
            ):
                raise ValueError("Crawl queue membership changed while preparing")
        else:
            if any(not row["revision"] for row in rows):
                raise ValueError("A queued domain has no crawl preset")
            fresh = (
                set(
                    client.execute(
                        f"""SELECT domain,work_key FROM {RESULTS_BY_TYPE[crawl_type]} FINAL
                WHERE successful AND domain IN %(domains)s AND finished_at >= toDateTime64(%(cutoff)s,6,'UTC') AND finished_at <= toDateTime64(%(started)s,6,'UTC')""",
                        {
                            "domains": tuple(row["domain"] for row in rows),
                            "cutoff": datetime.fromisoformat(
                                execution["freshness_cutoff"]
                            ).strftime("%Y-%m-%d %H:%M:%S.%f"),
                            "started": datetime.fromisoformat(
                                execution["started_at"]
                            ).strftime("%Y-%m-%d %H:%M:%S.%f"),
                        },
                    )
                )
                if not config.force_refresh
                else set()
            )
            requests = []
            skips = []
            for row in rows:
                row["website_url"] = row.pop("selected_url") or row["website_url"]
                payload, work_key = effective_payload(
                    row, crawl_type, execution["execution_id"], config
                )
                if not row["enabled"] or (row["domain"], work_key) in fresh:
                    skips.append(
                        {
                            "domain": row["domain"],
                            "reason": "disabled" if not row["enabled"] else "recent",
                        }
                    )
                else:
                    requests.append(
                        {
                            "crawl_type": crawl_type,
                            "domain": row["domain"],
                            "request_id": payload["request_id"],
                            "input_revision": row["revision"],
                            "work_key": work_key,
                            "run_id": execution["dagster_run_id"],
                            "request_json": json.dumps(payload, sort_keys=True),
                        }
                    )
            document = {
                "execution": execution,
                "domains": [row["domain"] for row in rows],
                "requests": requests,
                "skips": skips,
            }
            reference = save_document(objects, key, document)
        references.append(reference)
        total += len(rows)
        skipped += len(document["skips"])
        after = rows[-1]["domain"]
    if total != task["total"]:
        raise ValueError("Frozen crawl membership count changed")
    plan = {
        "execution": execution,
        "total": total,
        "skipped": skipped,
        "batches": references,
    }
    reference = save_document(objects, f"{root}/plan.json", plan)
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET status='ready',ready_at=now(),admitted_count=%s,
            skipped_count=%s,work_config=%s WHERE task_id=%s""",
            (
                total - skipped,
                skipped,
                Json({"plan": reference, "batches": {}}),
                task_id,
            ),
        )
    return plan


def collect_crawl_requests(
    context, client, http, url, table, requests, config, checkpoint
):
    for group in batched(requests, min(config.batch_size, config.max_in_flight)):
        for item in group:
            send_crawl(http, url, json.loads(item["request_json"]), validate=True)
        # Durable receipt before dispatch; a crash can always replay the same identity.
        client.execute(
            f"INSERT INTO {SUBMISSIONS} ({','.join(group[0])}) VALUES",
            list(group),
            settings={"async_insert": 0},
        )
        for item in group:
            send_crawl(http, url, json.loads(item["request_json"]), validate=False)
        waiting = {item["request_id"]: item for item in group}
        deadline = monotonic() + config.wait_timeout_seconds
        while waiting:
            records = []
            for request_id, item in waiting.items():
                response = http.get(
                    f"{url}/v1/crawls/{request_id}",
                    timeout=(10, 30),
                    allow_redirects=False,
                )
                response.raise_for_status()
                job = response.json()
                if job.get("request_id") != request_id:
                    raise ValueError("Crawler returned a different request identity")
                if (
                    job["state"] not in {"completed", "failed", "cancelled"}
                    or job["s3_state"] == "pending"
                ):
                    continue
                response = http.get(
                    f"{url}/v1/crawls/{request_id}/result",
                    timeout=(10, 60),
                    allow_redirects=False,
                )
                response.raise_for_status()
                records.append(result_record(item, job, response.json()))
            if records:
                client.execute(
                    f"INSERT INTO {table} ({','.join(records[0])}) VALUES",
                    records,
                    settings={"async_insert": 1, "wait_for_async_insert": 1},
                )
                checkpoint()
                for record in records:
                    del waiting[record["request_id"]]
                context.log.info(
                    "Saved %s crawl outcomes; %s still running in this group",
                    len(records),
                    len(waiting),
                )
            if waiting:
                if monotonic() >= deadline:
                    raise TimeoutError(
                        "Crawls are pending; resume this task to recover the saved execution"
                    )
                sleep(config.poll_interval_seconds)


def finish_crawl_execution(context, store, client, task_id):
    task = store.task(task_id)
    if (
        task["succeeded_count"] + task["terminal_failed_count"] + task["skipped_count"]
        != task["total"]
    ):
        raise ValueError("Crawl outcomes are incomplete; inputs retained")
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET status='completed',completed_at=coalesce(completed_at,greatest(frozen_at,clock_timestamp())) WHERE task_id=%s",
            (task_id,),
        )
    if task["inputs_purged_at"] is None:
        client.execute(
            f"DELETE FROM {TASK_DOMAINS} WHERE task_id=%(task)s",
            {"task": task_id},
            settings={"lightweight_deletes_sync": 2},
        )
        if client.execute(
            f"SELECT count() FROM {TASK_DOMAINS} WHERE task_id=%(task)s",
            {"task": task_id},
        ) != [(0,)]:
            raise ValueError("Crawl queue cleanup incomplete; resume to finish cleanup")
        with store.transaction() as cursor:
            cursor.execute(
                "UPDATE processing.tasks SET inputs_purged_at=coalesce(inputs_purged_at,greatest(completed_at,clock_timestamp())) WHERE task_id=%s",
                (task_id,),
            )
    outcome = "completed_with_errors" if task["terminal_failed_count"] else "completed"
    context.instance.add_run_tags(
        context.run.run_id,
        {
            "crawler/outcome": outcome,
            "crawler/failed_pages": str(task["terminal_failed_count"]),
            "crawler/succeeded_pages": str(task["succeeded_count"]),
            "crawler/skipped_pages": str(task["skipped_count"]),
        },
    )
    return dg.MaterializeResult(
        metadata={
            "task_id": task_id,
            "execution_id": task["config"]["execution"]["execution_id"],
            "completion_status": outcome,
            "completed": task["succeeded_count"],
            "unsuccessful": task["terminal_failed_count"],
            "skipped": task["skipped_count"],
            "inputs_purged": True,
        }
    )


def process_crawl_draft(
    context, config, clickhouse, processing, objects, crawl_type, task_id
):
    with (
        processing.get_store() as store,
        store.selection_lock(task_id),
        clickhouse.get_connection() as client,
    ):
        task = start_crawl_execution(
            store, client, task_id, crawl_type, config, context.run.run_id
        )
        context.instance.add_run_tags(
            context.run.run_id,
            {
                "processing/task_id": task_id,
                "crawler/execution_id": task["config"]["execution"]["execution_id"],
            },
        )
        if task["status"] == "completed":
            return finish_crawl_execution(context, store, client, task_id)
        plan = prepare_crawl_execution(store, client, objects, task, crawl_type, config)
        table = RESULTS_BY_TYPE[crawl_type]
        url = (
            os.environ.get("CRAWLER_API_URL", "").strip() or DEFAULT_CRAWLER_API_URL
        ).rstrip("/")
        token = os.environ.get("CRAWLER_API_TOKEN", "").strip()
        if not token:
            raise ValueError("Configure CRAWLER_API_TOKEN on the Dagster host")
        with Session(raise_for_status=False) as http:
            http.headers["Authorization"] = f"Bearer {token}"
            for index, reference in enumerate(plan["batches"]):
                work = store.task(task_id)["work_config"]
                if str(index) in work["batches"]:
                    continue
                document = read_document(objects, reference)
                requests = document["requests"]
                identities = tuple(item["request_id"] for item in requests)

                def outcomes():
                    return (
                        dict(
                            client.execute(
                                f"SELECT request_id,argMax(successful,attempt) FROM {table} FINAL WHERE request_id IN %(ids)s GROUP BY request_id",
                                {"ids": identities},
                            )
                        )
                        if identities
                        else {}
                    )

                def checkpoint():
                    current = outcomes()
                    with store.transaction() as cursor:
                        cursor.execute(
                            "UPDATE processing.tasks SET succeeded_count=%s,terminal_failed_count=%s WHERE task_id=%s",
                            (
                                sum(b["succeeded"] for b in work["batches"].values())
                                + sum(current.values()),
                                sum(b["failed"] for b in work["batches"].values())
                                + len(current)
                                - sum(current.values()),
                                task_id,
                            ),
                        )

                saved = outcomes()
                collect_crawl_requests(
                    context,
                    client,
                    http,
                    url,
                    table,
                    [item for item in requests if item["request_id"] not in saved],
                    config,
                    checkpoint,
                )
                saved = outcomes()
                if set(saved) != set(identities):
                    raise ValueError(
                        "Crawl results are not all durable; inputs retained"
                    )
                work["batches"][str(index)] = {
                    "succeeded": sum(saved.values()),
                    "failed": len(saved) - sum(saved.values()),
                }
                with store.transaction() as cursor:
                    cursor.execute(
                        "UPDATE processing.tasks SET work_config=%s,succeeded_count=%s,terminal_failed_count=%s WHERE task_id=%s",
                        (
                            Json(work),
                            sum(b["succeeded"] for b in work["batches"].values()),
                            sum(b["failed"] for b in work["batches"].values()),
                            task_id,
                        ),
                    )
                context.log.info(
                    "Saved outcomes for crawl batch %s/%s",
                    index + 1,
                    len(plan["batches"]),
                )
        return finish_crawl_execution(context, store, client, task_id)
