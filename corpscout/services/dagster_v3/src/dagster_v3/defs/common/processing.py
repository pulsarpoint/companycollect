"""Bounded input admission, fenced progress claims and a replayable result outbox."""

import hashlib
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from string import Formatter
from threading import RLock
from uuid import NAMESPACE_URL, uuid4, uuid5

import dagster as dg
import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values


@dataclass(frozen=True)
class ClaimedItem:
    task_id: str
    input_id: str
    attempt: int
    lease_token: str


@dataclass(frozen=True)
class ExportBatch:
    batch_id: str
    task_id: str
    destination: str
    result_count: int


def render_query(template: str, values: dict) -> str:
    for _, field, spec, conversion in Formatter().parse(template):
        if field is None:
            continue
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field) or spec or conversion:
            raise ValueError(
                "query template supports only simple {column_name} placeholders"
            )
        if (
            field not in values
            or values[field] is None
            or not str(values[field]).strip()
        ):
            raise ValueError(f"query template input {field} is missing or empty")
    query = template.format_map(values).strip()
    if not query:
        raise ValueError("query template must produce a nonempty query")
    return query


def work_key(
    processor: str, work_config: dict, template: str, values: dict, query: str
) -> str:
    canonical = json.dumps(
        [processor, work_config, template, values, query],
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class ProcessingStore:
    """One bounded connection per run, shared only during short serialized transactions.

    Separate run processes use independent connections and PostgreSQL row locks.
    No transaction remains open while a browser or ClickHouse request runs.
    """

    def __init__(self, connection):
        self.connection = connection
        self.lock = RLock()

    @contextmanager
    def transaction(self):
        with (
            self.lock,
            self.connection,
            self.connection.cursor(cursor_factory=RealDictCursor) as cursor,
        ):
            cursor.execute("SET LOCAL synchronous_commit = on")
            yield cursor

    def task(self, task_id: str) -> dict | None:
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT * FROM processing.tasks WHERE task_id=%s", (task_id,)
            )
            return cursor.fetchone()

    @contextmanager
    def selection_lock(self, task_id: str):
        # A session lock also fences a retry after its prior process disappeared.
        # Commit before ClickHouse work: no PostgreSQL transaction stays open.
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS acquired",
                ("brave_input:" + task_id,),
            )
            if not cursor.fetchone()["acquired"]:
                raise ValueError("this input selection is already being initialized")
        try:
            yield
        finally:
            with self.transaction() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                    ("brave_input:" + task_id,),
                )

    def prepare_selection(
        self, task_id: str, *, processor: str, fingerprint: str
    ) -> tuple[dict, bool]:
        with self.transaction() as cursor:
            cursor.execute(
                """INSERT INTO processing.tasks(task_id,processor,config,work_config,status)
                VALUES (%s,%s,%s,'{}','preparing') ON CONFLICT (task_id) DO NOTHING RETURNING task_id""",
                (task_id, processor, Json({"selection_fingerprint": fingerprint})),
            )
            created = cursor.fetchone() is not None
            cursor.execute(
                "SELECT * FROM processing.tasks WHERE task_id=%s FOR UPDATE", (task_id,)
            )
            task = dict(cursor.fetchone())
            if (
                task["processor"] != processor
                or task["config"].get("selection_fingerprint") != fingerprint
            ):
                raise ValueError("task_id already belongs to a different selection")
            if task["status"] == "cancelled":
                raise ValueError("input selection was cancelled")
            return task, created

    def finish_selection(self, task_id: str, source_info: dict) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE processing.tasks SET source_info=%s,total=%s,status='selected'
                WHERE task_id=%s AND status='preparing' RETURNING task_id""",
                (Json(source_info), source_info["total"], task_id),
            )
            if cursor.fetchone() is None:
                raise ValueError("input task is no longer being initialized")

    def activate_selection(
        self, task_id: str, *, config: dict, work_config: dict
    ) -> dict:
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT * FROM processing.tasks WHERE task_id=%s FOR UPDATE", (task_id,)
            )
            task = dict(cursor.fetchone())
            if task["status"] == "selected":
                saved = {
                    **task["config"],
                    **config,
                    "input_relation": task["source_info"]["relation"],
                }
                cursor.execute(
                    """UPDATE processing.tasks SET config=%s,work_config=%s,status='ready',ready_at=now()
                    WHERE task_id=%s RETURNING *""",
                    (Json(saved), Json(work_config), task_id),
                )
                return dict(cursor.fetchone())
            if task["status"] != "ready":
                raise ValueError("input initialization must finish before processing")
            return task

    def register(
        self,
        task_id: str,
        *,
        processor: str,
        config: dict,
        work_config: dict,
        source_info: dict,
    ) -> None:
        """Save one task record, never the input selection or its payloads."""
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (task_id,)
            )
            cursor.execute(
                "SELECT * FROM processing.tasks WHERE task_id=%s", (task_id,)
            )
            existing = cursor.fetchone()
            if existing:
                if any(
                    existing[key] != value
                    for key, value in (
                        ("processor", processor),
                        ("config", config),
                        ("work_config", work_config),
                        ("source_info", source_info),
                    )
                ):
                    raise ValueError(
                        "task configuration differs from its fixed selection"
                    )
                return
            cursor.execute(
                """INSERT INTO processing.tasks
                (task_id,processor,config,work_config,status,total,source_info,ready_at)
                VALUES (%s,%s,%s,%s,'ready',%s,%s,now())""",
                (
                    task_id,
                    processor,
                    Json(config),
                    Json(work_config),
                    source_info["total"],
                    Json(source_info),
                ),
            )

    def admit(
        self, task_id: str, *, after: str | None, input_ids: list[str], capacity: int
    ) -> bool:
        """Commit a bounded page of identities and its source cursor atomically.

        A stale reader returns False and reloads the cursor. No cursor advances
        past identities that have not been committed as recoverable progress.
        """
        if not input_ids or len(input_ids) > capacity:
            raise ValueError("admission requires a nonempty bounded page")
        if any(
            not isinstance(value, str) or not value.strip() or "\x00" in value
            for value in input_ids
        ):
            raise ValueError("input_id must be a nonempty stable string without NUL")
        if input_ids != sorted(set(input_ids)) or (
            after is not None and input_ids[0] <= after
        ):
            raise ValueError("input IDs must be unique and ordered after the cursor")
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT * FROM processing.tasks WHERE task_id=%s FOR UPDATE", (task_id,)
            )
            task = cursor.fetchone()
            if task is None or task["status"] != "ready":
                raise ValueError("task is not ready")
            if task["source_cursor"] != after:
                return False
            open_count = task["admitted_count"] - sum(
                task[key]
                for key in (
                    "succeeded_count",
                    "terminal_failed_count",
                    "skipped_count",
                    "cancelled_count",
                )
            )
            if open_count + len(input_ids) > capacity:
                return False
            if (
                task["admitted_count"] + len(input_ids) > task["total"]
                or input_ids[-1] > task["source_info"]["upper_id"]
            ):
                raise ValueError("input queue differs from its fixed selection")
            execute_values(
                cursor,
                "INSERT INTO processing.items (task_id,input_id) VALUES %s",
                [(task_id, value) for value in input_ids],
                page_size=capacity,
            )
            cursor.execute(
                """UPDATE processing.tasks SET source_cursor=%s,admitted_count=admitted_count+%s
                WHERE task_id=%s""",
                (input_ids[-1], len(input_ids), task_id),
            )
            return True

    def skip_if_fresh(
        self, item: ClaimedItem, *, work_key: str, freshness_days: int
    ) -> bool:
        if freshness_days <= 0:
            return False
        with self.transaction() as cursor:
            cursor.execute(
                """SELECT r.result_id FROM processing.results r
                JOIN processing.export_batches b ON b.batch_id=r.export_batch_id
                WHERE r.work_key=%s AND r.status='success' AND b.published_at IS NOT NULL
                  AND r.completed_at >= now()-%s*interval '1 day'
                ORDER BY r.completed_at DESC LIMIT 1""",
                (work_key, freshness_days),
            )
            hit = cursor.fetchone()
            if hit is None:
                return False
            cursor.execute(
                """UPDATE processing.items SET state='skipped',accepted_result_id=%s,
                    lease_owner=NULL,lease_token=NULL
                WHERE task_id=%s AND input_id=%s AND state='running'
                  AND lease_token=%s AND lease_expires_at>now() RETURNING input_id""",
                (hit["result_id"], item.task_id, item.input_id, item.lease_token),
            )
            return cursor.fetchone() is not None

    def claim(
        self, task_id: str, *, owner: str, lease_seconds: int, max_attempts: int
    ) -> ClaimedItem | None:
        with self.transaction() as cursor:
            # Recover expired leases separately. Combining this predicate with the
            # pending selection forces a full queue scan/sort instead of an index seek.
            cursor.execute(
                """UPDATE processing.items SET
                    state=CASE WHEN attempt >= %s THEN 'terminal_failed' ELSE 'queued' END,
                    lease_token=NULL,lease_owner=NULL,next_attempt_at=now()
                WHERE task_id=%s AND state='running' AND lease_expires_at <= now()""",
                (max_attempts, task_id),
            )
            while True:
                cursor.execute(
                    """SELECT i.input_id,i.attempt FROM processing.items i
                    JOIN processing.tasks t USING (task_id)
                    WHERE i.task_id=%s AND t.status='ready'
                      AND i.state IN ('queued','retry_wait') AND i.next_attempt_at <= now()
                    ORDER BY i.next_attempt_at,i.input_id LIMIT 1 FOR UPDATE OF i SKIP LOCKED""",
                    (task_id,),
                )
                candidate = cursor.fetchone()
                if candidate is None:
                    return None
                if candidate["attempt"] >= max_attempts:
                    cursor.execute(
                        "UPDATE processing.items SET state='terminal_failed' WHERE task_id=%s AND input_id=%s",
                        (task_id, candidate["input_id"]),
                    )
                    continue
                cursor.execute(
                    """UPDATE processing.items SET state='running',attempt=attempt+1,
                        lease_owner=%s,lease_token=%s,lease_expires_at=now()+%s*interval '1 second'
                    WHERE task_id=%s AND input_id=%s
                    RETURNING task_id::text,input_id,attempt,lease_token::text""",
                    (
                        owner,
                        str(uuid4()),
                        lease_seconds,
                        task_id,
                        candidate["input_id"],
                    ),
                )
                return ClaimedItem(**cursor.fetchone())

    def retry_failed(self, task_id: str, *, max_attempts: int) -> int:
        """Requeue only failed items with additional budget, preserving attempt history."""
        with self.transaction() as cursor:
            cursor.execute(
                """SELECT task_id FROM processing.tasks
                WHERE task_id=%s AND status='ready' FOR UPDATE""",
                (task_id,),
            )
            if cursor.fetchone() is None:
                raise ValueError("task is not ready for processing")
            cursor.execute(
                """UPDATE processing.items SET state='queued',next_attempt_at=now()
                WHERE task_id=%s AND state='terminal_failed' AND attempt<%s""",
                (task_id, max_attempts),
            )
            return cursor.rowcount

    def heartbeat(self, owner: str, *, lease_seconds: int) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE processing.items SET lease_expires_at=now()+%s*interval '1 second'
                WHERE state='running' AND lease_owner=%s AND lease_expires_at>now()""",
                (lease_seconds, owner),
            )

    def release(self, owner: str, *, max_attempts: int) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE processing.items SET
                state=CASE WHEN attempt >= %s THEN 'terminal_failed' ELSE 'queued' END,
                lease_token=NULL,lease_owner=NULL,
                next_attempt_at=now() WHERE state='running' AND lease_owner=%s""",
                (max_attempts, owner),
            )

    def complete(
        self,
        item: ClaimedItem,
        *,
        status: str,
        work_key: str,
        payload: dict,
        completed_at: datetime,
        max_attempts: int,
        retry_seconds: int,
    ) -> str | None:
        result_id = str(uuid5(NAMESPACE_URL, item.lease_token))
        state = (
            "succeeded"
            if status == "success"
            else ("terminal_failed" if item.attempt >= max_attempts else "retry_wait")
        )
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE processing.items SET state=%s,lease_token=NULL,lease_owner=NULL,
                next_attempt_at=now()+%s*interval '1 second'
                WHERE task_id=%s AND input_id=%s AND state='running' AND lease_token=%s
                  AND lease_expires_at>now() RETURNING input_id""",
                (state, retry_seconds, item.task_id, item.input_id, item.lease_token),
            )
            if cursor.fetchone() is None:
                return None
            cursor.execute(
                """INSERT INTO processing.results
                (result_id,task_id,input_id,work_key,attempt,status,payload,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    result_id,
                    item.task_id,
                    item.input_id,
                    work_key,
                    item.attempt,
                    status,
                    Json(payload),
                    completed_at,
                ),
            )
            cursor.execute(
                "UPDATE processing.items SET accepted_result_id=%s WHERE task_id=%s AND input_id=%s",
                (result_id, item.task_id, item.input_id),
            )
        return result_id

    def progress(self, task_id: str) -> dict:
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT * FROM processing.task_progress WHERE task_id=%s", (task_id,)
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("unknown processing task")
            return dict(row)

    def export_batch(
        self, task_id: str, *, limit: int, destination: str
    ) -> ExportBatch | None:
        with self.transaction() as cursor:
            cursor.execute(
                "SELECT task_id FROM processing.tasks WHERE task_id=%s FOR UPDATE",
                (task_id,),
            )
            cursor.execute(
                """SELECT batch_id::text,task_id::text,destination,result_count FROM processing.export_batches
                WHERE task_id=%s AND published_at IS NULL ORDER BY created_at LIMIT 1""",
                (task_id,),
            )
            row = cursor.fetchone()
            if row:
                if row["destination"] != destination:
                    raise ValueError("export destination changed for an existing batch")
                return ExportBatch(**row)
            cursor.execute(
                """SELECT result_id FROM processing.results
                WHERE task_id=%s AND export_batch_id IS NULL ORDER BY completed_at,result_id LIMIT %s FOR UPDATE""",
                (task_id, limit),
            )
            result_ids = [str(row["result_id"]) for row in cursor.fetchall()]
            if not result_ids:
                return None
            batch = ExportBatch(str(uuid4()), task_id, destination, len(result_ids))
            cursor.execute(
                "INSERT INTO processing.export_batches (batch_id,task_id,destination,result_count) VALUES (%s,%s,%s,%s)",
                (batch.batch_id, task_id, destination, batch.result_count),
            )
            cursor.execute(
                "UPDATE processing.results SET export_batch_id=%s WHERE result_id=ANY(%s::uuid[])",
                (batch.batch_id, result_ids),
            )
            return batch

    def acknowledge(self, batch: ExportBatch) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                "UPDATE processing.export_batches SET published_at=coalesce(published_at,now()) WHERE batch_id=%s",
                (batch.batch_id,),
            )


class ProcessingResource(dg.ConfigurableResource):
    postgres_url: str

    @contextmanager
    def get_store(self):
        connection = psycopg2.connect(
            self.postgres_url, connect_timeout=10, application_name="dagster_processing"
        )
        try:
            yield ProcessingStore(connection)
        finally:
            connection.close()
