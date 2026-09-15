"""Transactional task snapshots, fenced claims and a replayable result outbox."""

import hashlib
import json
import re
from collections.abc import Iterable
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
    input_data: dict
    query: str
    work_key: str
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

    def freeze(
        self,
        task_id: str,
        *,
        processor: str,
        config: dict,
        work_config: dict,
        inputs: Iterable[dict],
        query_template: str,
        freshness_days: int,
    ) -> None:
        with self.transaction() as cursor:
            # Serializes two creators of the same ID without locking unrelated tasks.
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (task_id,)
            )
            cursor.execute(
                "SELECT processor, config, work_config FROM processing.tasks WHERE task_id=%s",
                (task_id,),
            )
            existing = cursor.fetchone()
            if existing:
                if (
                    existing["processor"] != processor
                    or existing["config"] != config
                    or existing["work_config"] != work_config
                ):
                    raise ValueError(
                        "task configuration differs from its frozen snapshot"
                    )
                return
            cursor.execute(
                "INSERT INTO processing.tasks (task_id, processor, config, work_config, status) VALUES (%s,%s,%s,%s,'preparing')",
                (task_id, processor, Json(config), Json(work_config)),
            )
            rows = []
            total = 0
            for values in inputs:
                input_id = values.get("input_id")
                if not isinstance(input_id, str) or not input_id.strip():
                    raise ValueError("input_id must be a nonempty stable string")
                query = render_query(query_template, values)
                canonical = json.dumps(
                    [processor, work_config, query_template, values, query],
                    sort_keys=True,
                    ensure_ascii=False,
                )
                work_key = hashlib.sha256(canonical.encode()).hexdigest()
                rows.append((task_id, input_id, Json(values), work_key, query))
                total += 1
                if len(rows) == 1000:
                    execute_values(
                        cursor,
                        "INSERT INTO processing.items (task_id,input_id,input_data,work_key,query) VALUES %s",
                        rows,
                        page_size=1000,
                    )
                    rows.clear()
            if rows:
                execute_values(
                    cursor,
                    "INSERT INTO processing.items (task_id,input_id,input_data,work_key,query) VALUES %s",
                    rows,
                    page_size=1000,
                )
            if freshness_days > 0:
                # Only reuse already published successes; task-local outboxes stay independent.
                cursor.execute(
                    """
                    UPDATE processing.items i SET state='skipped', accepted_result_id=cached.result_id
                    FROM (
                        SELECT pending.input_id, hit.result_id FROM processing.items pending
                        CROSS JOIN LATERAL (
                            SELECT r.result_id FROM processing.results r
                            JOIN processing.export_batches b ON b.batch_id=r.export_batch_id
                            WHERE r.work_key=pending.work_key AND r.status='success'
                              AND b.published_at IS NOT NULL
                              AND r.completed_at >= now() - %s * interval '1 day'
                            ORDER BY r.completed_at DESC LIMIT 1
                        ) hit WHERE pending.task_id=%s
                    ) cached
                    WHERE i.task_id=%s AND i.input_id=cached.input_id
                """,
                    (freshness_days, task_id, task_id),
                )
            cursor.execute(
                "UPDATE processing.tasks SET total=%s,status='ready',ready_at=now() WHERE task_id=%s",
                (total, task_id),
            )

    def claim(
        self, task_id: str, *, owner: str, lease_seconds: int, max_attempts: int
    ) -> ClaimedItem | None:
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE processing.items SET state='terminal_failed', lease_token=NULL, lease_owner=NULL
                WHERE task_id=%s AND attempt >= %s AND
                  (state IN ('queued','retry_wait') OR (state='running' AND lease_expires_at <= now()))""",
                (task_id, max_attempts),
            )
            cursor.execute(
                """
                WITH candidate AS (
                    SELECT i.task_id,i.input_id FROM processing.items i JOIN processing.tasks t USING (task_id)
                    WHERE i.task_id=%s AND t.status='ready' AND i.attempt < %s
                      AND ((i.state IN ('queued','retry_wait') AND i.next_attempt_at <= now())
                           OR (i.state='running' AND i.lease_expires_at <= now()))
                    ORDER BY i.next_attempt_at,i.input_id LIMIT 1 FOR UPDATE OF i SKIP LOCKED
                )
                UPDATE processing.items i SET state='running',attempt=attempt+1,
                    lease_owner=%s,lease_token=%s,lease_expires_at=now()+%s*interval '1 second'
                FROM candidate c WHERE i.task_id=c.task_id AND i.input_id=c.input_id
                RETURNING i.task_id::text,i.input_id,i.input_data,i.query,i.work_key,i.attempt,i.lease_token::text
            """,
                (task_id, max_attempts, owner, str(uuid4()), lease_seconds),
            )
            row = cursor.fetchone()
            return ClaimedItem(**row) if row else None

    def heartbeat(self, owner: str, *, lease_seconds: int) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE processing.items SET lease_expires_at=now()+%s*interval '1 second'
                WHERE state='running' AND lease_owner=%s AND lease_expires_at>now()""",
                (lease_seconds, owner),
            )

    def release(self, owner: str) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                """UPDATE processing.items SET state='queued',lease_token=NULL,lease_owner=NULL,
                next_attempt_at=now() WHERE state='running' AND lease_owner=%s""",
                (owner,),
            )

    def complete(
        self,
        item: ClaimedItem,
        *,
        status: str,
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
                    item.work_key,
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
