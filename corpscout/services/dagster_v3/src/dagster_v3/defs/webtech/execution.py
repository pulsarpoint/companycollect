"""Freeze draft membership, then persist replayable per-page execution decisions."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from psycopg2.extras import Json
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.processing import ProcessingStore
from dagster_v3.defs.common.resources import ObjectStoreResource

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.webtech.input import INPUT_RELATION, PROCESSOR_VERSION
from dagster_v3.defs.webtech.models import WEBTECH_DETECTOR_VERSION


def start_execution(
    *,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    task_id: str,
    execution_id: str | None,
    force_rescan: bool,
    recent_days: int,
    batch_size: int,
    run_id: str,
) -> dict:
    """Called under the same session lock used by imports and result processing."""
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != PROCESSOR_VERSION
        or task["queue_scope"] is None
    ):
        raise ValueError("Expected a Webtech draft queue")
    profile = {
        "force_rescan": force_rescan,
        "recent_days": recent_days,
        "batch_size": batch_size,
        "detector_version": WEBTECH_DETECTOR_VERSION,
    }
    saved = task["config"].get("execution")
    identity = execution_id or (saved["execution_id"] if saved else str(uuid4()))
    if saved is not None and identity == saved["execution_id"]:
        if saved["profile"] != profile:
            raise ValueError(
                "Execution settings are frozen; resume with the same profile"
            )
        if task["status"] not in ("selected", "ready", "completed"):
            raise ValueError("Execution is not resumable")
        return task
    if task["inputs_purged_at"] is not None:
        raise ValueError("Inputs were purged; create a new draft")
    if task["status"] != "draft" and not task["work_config"].get("finished"):
        raise ValueError(
            "Finish or resume the existing execution before starting another"
        )
    if task["status"] not in ("draft", "ready", "completed"):
        raise ValueError("Queue cannot be started in this state")
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT count(*) AS pending FROM processing.input_submissions WHERE task_id=%s AND status NOT IN ('completed','cancelled')",
            (task_id,),
        )
        if cursor.fetchone()["pending"]:
            raise ValueError("Finish or retry all outstanding imports before Start")
    snapshot = ClickHouseInputQueue(
        clickhouse, INPUT_RELATION, selection_task_id=task_id
    ).inspect()
    if snapshot["total"] == 0:
        raise ValueError("Cannot start an empty queue")
    now = datetime.now(UTC)
    execution = {
        "execution_id": identity,
        "profile": profile,
        "dagster_run_id": run_id,
        "started_at": now.isoformat(),
        "freshness_cutoff": (now - timedelta(days=recent_days)).isoformat(),
    }
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET status='selected',frozen_at=coalesce(frozen_at,%s),
            completed_at=NULL,source_info=%s,total=%s,config=%s,work_config='{}',ready_at=NULL,
            admitted_count=0,succeeded_count=0,skipped_count=0,terminal_failed_count=0
            WHERE task_id=%s RETURNING *""",
            (
                now,
                Json(snapshot),
                snapshot["total"],
                Json({"execution": execution}),
                task_id,
            ),
        )
        return dict(cursor.fetchone())


def read_plan_object(object_store: ObjectStoreResource, key: str, digest: str) -> dict:
    body = object_store.read_bytes(key)
    if hashlib.sha256(body).hexdigest() != digest:
        raise ValueError("Execution plan checksum mismatch")
    return json.loads(body)


def write_plan_object(
    object_store: ObjectStoreResource, key: str, document: dict
) -> dict:
    body = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(body).hexdigest()
    if object_store.exists(key):
        existing = object_store.read_bytes(key)
        if existing != body:
            raise ValueError("Immutable execution plan changed")
    else:
        object_store.write_bytes(key, body)
    return {"key": key, "sha256": digest}


def prepare_execution(
    *,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    task: dict,
) -> dict:
    task_id = str(task["task_id"])
    execution = task["config"]["execution"]
    root = f"queue-executions/webtech/{task_id}/{execution['execution_id']}"
    plan_key = f"{root}/plan.json"
    # Root commit marker is written last, before any remote scan submission.
    if object_store.exists(plan_key):
        plan = json.loads(object_store.read_bytes(plan_key))
        if plan["execution"] != execution or plan["source_info"] != task["source_info"]:
            raise ValueError("Execution plan belongs to a different snapshot")
    else:
        object_store.ensure_bucket()
        buckets = []
        skipped = 0
        queue = ClickHouseInputQueue(
            clickhouse, INPUT_RELATION, selection_task_id=task_id
        )
        after = None
        bucket = 0
        while True:
            inputs = queue.read(
                task["source_info"],
                after=after,
                limit=execution["profile"]["batch_size"],
            )
            if not inputs:
                break
            rows = [
                (
                    row["input_id"],
                    row["root_domain"],
                    row["website_origin"],
                    row["page_url"],
                )
                for row in inputs
            ]
            after = rows[-1][0]
            key = f"{root}/bucket-{bucket:03d}.json"
            if object_store.exists(key):
                # A retry reuses already prepared decisions, even before root commit.
                body = object_store.read_bytes(key)
                document = json.loads(body)
                reference = {"key": key, "sha256": hashlib.sha256(body).hexdigest()}
            else:
                with clickhouse.get_connection() as client:
                    recent = {}
                    if rows and not execution["profile"]["force_rescan"]:
                        observations = client.execute(
                            """SELECT root_domain,website_origin,page_url,
                            argMax(tuple(scan_id,outcome,scanned_at),tuple(scanned_at,scan_id)) AS latest
                            FROM corpscout.webtech_domain_scan_results FINAL
                            WHERE (root_domain,website_origin,page_url) IN %(pages)s
                            AND detector_version=%(detector)s
                            AND scanned_at >= toDateTime64(%(cutoff)s,6,'UTC')
                            AND scanned_at <= toDateTime64(%(started)s,6,'UTC')
                            GROUP BY root_domain,website_origin,page_url HAVING latest.2='success'""",
                            {
                                "pages": tuple(row[1:4] for row in rows),
                                "detector": WEBTECH_DETECTOR_VERSION,
                                "cutoff": datetime.fromisoformat(
                                    execution["freshness_cutoff"]
                                ).strftime("%Y-%m-%d %H:%M:%S.%f"),
                                "started": datetime.fromisoformat(
                                    execution["started_at"]
                                ).strftime("%Y-%m-%d %H:%M:%S.%f"),
                            },
                        )
                        recent = {
                            tuple(row[:3]): {
                                "scan_id": row[3][0],
                                "scanned_at": row[3][2].isoformat(),
                            }
                            for row in observations
                        }
                document = {
                    "execution": execution,
                    "bucket": bucket,
                    "items": [
                        {
                            "input_id": identity,
                            "root_domain": domain,
                            "page_url": page,
                            "decision": "skip_recent"
                            if (domain, origin, page) in recent
                            else "scan",
                            "previous_result": recent.get((domain, origin, page)),
                        }
                        for identity, domain, origin, page in rows
                    ],
                }
                reference = write_plan_object(object_store, key, document)
            if (
                document["execution"] != execution
                or document["bucket"] != bucket
                or [item["input_id"] for item in document["items"]]
                != [row[0] for row in rows]
            ):
                raise ValueError("Bucket plan identity mismatch")
            count = len(document["items"])
            bucket_skipped = sum(
                item["decision"] == "skip_recent" for item in document["items"]
            )
            skipped += bucket_skipped
            if count:
                buckets.append(
                    {
                        **reference,
                        "bucket": bucket,
                        "total": count,
                        "skipped": bucket_skipped,
                    }
                )
            bucket += 1
        if sum(bucket["total"] for bucket in buckets) != task["total"]:
            raise ValueError("Input membership changed during execution preparation")
        plan = {
            "execution": execution,
            "source_info": task["source_info"],
            "buckets": buckets,
            "skipped": skipped,
        }
        write_plan_object(object_store, plan_key, plan)
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET status='ready',ready_at=coalesce(ready_at,now()),
            skipped_count=%s,admitted_count=%s+succeeded_count+terminal_failed_count,work_config=work_config || %s
            WHERE task_id=%s AND status IN ('selected','ready')""",
            (
                plan["skipped"],
                plan["skipped"],
                Json({"plan_uri": f"s3://{object_store.bucket}/{plan_key}"}),
                task_id,
            ),
        )
    return plan


def record_bucket(
    store: ProcessingStore, task_id: str, bucket: int, result: dict
) -> None:
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT work_config FROM processing.tasks WHERE task_id=%s FOR UPDATE",
            (task_id,),
        )
        work = cursor.fetchone()["work_config"]
        completed = work.setdefault("buckets", {})
        completed[str(bucket)] = result
        cursor.execute(
            "UPDATE processing.tasks SET work_config=%s,succeeded_count=%s,terminal_failed_count=%s,admitted_count=skipped_count+%s WHERE task_id=%s",
            (
                Json(work),
                sum(row["succeeded"] for row in completed.values()),
                sum(row["failed"] for row in completed.values()),
                sum(row["indexed"] for row in completed.values()),
                task_id,
            ),
        )


def finish_execution(store: ProcessingStore, task_id: str) -> dict:
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT * FROM processing.tasks WHERE task_id=%s FOR UPDATE", (task_id,)
        )
        task = dict(cursor.fetchone())
        if (
            task["succeeded_count"]
            + task["skipped_count"]
            + task["terminal_failed_count"]
            != task["total"]
        ):
            raise ValueError("Not every input has a published outcome")
        cursor.execute(
            """UPDATE processing.tasks SET work_config=work_config || '{"finished":true}'::jsonb,
            status='completed', completed_at=coalesce(completed_at,now())
            WHERE task_id=%s RETURNING *""",
            (task_id,),
        )
        return dict(cursor.fetchone())


def purge_completed_inputs(
    store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str
) -> None:
    """Clear fully processed queue membership while holding the task's selection lock.

    Results, manifests and PostgreSQL progress remain as history. Mark cleanup
    only after ClickHouse confirms deletion, so interrupted cleanup can retry.
    """
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != PROCESSOR_VERSION
        or task["queue_scope"] is None
        or task["status"] != "completed"
        or not task["work_config"].get("finished")
        or task["succeeded_count"]
        + task["skipped_count"]
        + task["terminal_failed_count"]
        != task["total"]
    ):
        raise ValueError("Only fully completed Webtech tasks can clear their inputs")
    if task["inputs_purged_at"] is not None:
        return
    with clickhouse.get_connection() as client:
        client.execute(
            f"DELETE FROM {INPUT_RELATION} WHERE task_id=%(task)s",
            {"task": task_id},
            settings={"lightweight_deletes_sync": 2},
        )
        if (
            client.execute(
                f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
                {"task": task_id},
            )[0][0]
            != 0
        ):
            raise RuntimeError("Completed Webtech input cleanup is not yet visible")
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET inputs_purged_at=coalesce(inputs_purged_at,now()) WHERE task_id=%s",
            (task_id,),
        )
