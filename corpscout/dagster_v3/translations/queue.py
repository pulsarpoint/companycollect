from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
from pathlib import Path
from uuid import uuid4

import duckdb

from translations.types import SmokeTranslationResult


QUEUE_STATUS_PENDING = "pending"
QUEUE_STATUS_LEASED = "leased"
QUEUE_STATUS_COMPLETED = "completed"
QUEUE_STATUS_FAILED_RETRYABLE = "failed_retryable"


@dataclass(frozen=True)
class TranslationQueueItem:
    source_duckdb_path: str
    source_table: str
    source_pk: str
    source_field: str
    source_text: str
    target_language: str

    @property
    def source_text_hash(self) -> str:
        return hashlib.sha256(self.source_text.encode("utf-8")).hexdigest()

    @property
    def item_id(self) -> str:
        raw_id = "|".join(
            [
                self.source_duckdb_path,
                self.source_table,
                self.source_pk,
                self.source_field,
                self.source_text_hash,
                self.target_language,
            ]
        )
        return hashlib.sha256(raw_id.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ClaimedTranslationItem:
    item_id: str
    batch_id: str
    source_field: str
    source_text: str
    target_language: str
    attempt_count: int


@dataclass(frozen=True)
class TranslationQueueSummary:
    total_items: int
    pending_items: int
    leased_items: int
    completed_items: int
    failed_retryable_items: int
    result_items: int
    batch_attempts: int
    successful_batches: int
    failed_batches: int


@dataclass(frozen=True)
class CompletedTranslationQueueResult:
    item_id: str
    source_duckdb_path: str
    source_table: str
    source_pk: str
    source_field: str
    source_text_hash: str
    target_language: str
    translated_text: str


class TranslationQueue:
    def __init__(self, duckdb_path: str | Path) -> None:
        self.duckdb_path = Path(duckdb_path)

    def initialize(self) -> None:
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            self.initialize_tables(conn)

    @staticmethod
    def initialize_tables(
        conn: duckdb.DuckDBPyConnection,
        *,
        table_prefix: str = "",
    ) -> None:
        qualified_prefix = f"{table_prefix}." if table_prefix else ""
        conn.execute(
            f"""
            create table if not exists {qualified_prefix}translation_items (
                item_id text primary key,
                source_duckdb_path text not null,
                source_table text not null,
                source_pk text not null,
                source_field text not null,
                source_text text not null,
                source_text_hash text not null,
                target_language text not null,
                status text not null,
                attempt_count integer not null,
                leased_by text,
                leased_at timestamp,
                batch_id text,
                last_error_category text,
                last_error_message text,
                created_at timestamp not null,
                updated_at timestamp not null
            )
            """
        )
        conn.execute(
            f"""
            create table if not exists {qualified_prefix}translation_results (
                item_id text primary key,
                translated_text text not null,
                provider text not null,
                completed_at timestamp not null
            )
            """
        )
        conn.execute(
            f"""
            create table if not exists {qualified_prefix}translation_batch_attempts (
                batch_id text primary key,
                worker_id text not null,
                item_count integer not null,
                status text not null,
                started_at timestamp not null,
                finished_at timestamp not null,
                duration_seconds double not null,
                error_category text,
                error_message text
            )
            """
        )

    def enqueue_items(self, items: list[TranslationQueueItem]) -> int:
        if not items:
            return 0
        now = _now()
        rows = [
            (
                item.item_id,
                item.source_duckdb_path,
                item.source_table,
                item.source_pk,
                item.source_field,
                item.source_text,
                item.source_text_hash,
                item.target_language,
                QUEUE_STATUS_PENDING,
                0,
                now,
                now,
            )
            for item in items
        ]
        with self._connect() as conn:
            conn.executemany(
                """
                insert into translation_items (
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
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict (item_id) do nothing
                """,
                rows,
            )
            return len(rows)

    def claim_batch(self, *, limit: int, worker_id: str) -> list[ClaimedTranslationItem]:
        batch_id = str(uuid4())
        now = _now()
        with self._connect() as conn:
            rows = conn.execute(
                """
                select item_id, source_field, source_text, target_language, attempt_count
                from translation_items
                where status = ?
                order by source_table, source_pk, source_field, item_id
                limit ?
                """,
                [QUEUE_STATUS_PENDING, limit],
            ).fetchall()
            if not rows:
                rows = conn.execute(
                    """
                    select item_id, source_field, source_text, target_language, attempt_count
                    from translation_items
                    where status = ?
                    order by source_table, source_pk, source_field, item_id
                    limit ?
                    """,
                    [QUEUE_STATUS_FAILED_RETRYABLE, limit],
                ).fetchall()
            if not rows:
                return []

            item_ids = [row[0] for row in rows]
            conn.executemany(
                """
                update translation_items
                set
                    status = ?,
                    leased_by = ?,
                    leased_at = ?,
                    batch_id = ?,
                    updated_at = ?
                where item_id = ?
                """,
                [
                    (
                        QUEUE_STATUS_LEASED,
                        worker_id,
                        now,
                        batch_id,
                        now,
                        item_id,
                    )
                    for item_id in item_ids
                ],
            )
            return [
                ClaimedTranslationItem(
                    item_id=row[0],
                    batch_id=batch_id,
                    source_field=row[1],
                    source_text=row[2],
                    target_language=row[3],
                    attempt_count=row[4],
                )
                for row in rows
            ]

    def complete_batch(
        self,
        items: list[ClaimedTranslationItem],
        translations: list[SmokeTranslationResult],
        *,
        provider: str,
        duration_seconds: float,
    ) -> None:
        if not items:
            return
        batch_id = _single_batch_id(items)
        now = _now()
        translation_by_id = {translation.item_id: translation for translation in translations}
        missing_item_ids = {item.item_id for item in items} - set(translation_by_id)
        if missing_item_ids:
            raise ValueError(f"missing translation item ids: {sorted(missing_item_ids)}")

        with self._connect() as conn:
            conn.executemany(
                """
                insert or replace into translation_results (
                    item_id,
                    translated_text,
                    provider,
                    completed_at
                )
                values (?, ?, ?, ?)
                """,
                [
                    (
                        item.item_id,
                        translation_by_id[item.item_id].translated_text,
                        provider,
                        now,
                    )
                    for item in items
                ],
            )
            conn.executemany(
                """
                update translation_items
                set
                    status = ?,
                    leased_by = null,
                    leased_at = null,
                    batch_id = null,
                    last_error_category = null,
                    last_error_message = null,
                    updated_at = ?
                where item_id = ?
                """,
                [(QUEUE_STATUS_COMPLETED, now, item.item_id) for item in items],
            )
            self._insert_batch_attempt(
                conn,
                batch_id=batch_id,
                worker_id="unknown",
                item_count=len(items),
                status="success",
                duration_seconds=duration_seconds,
                error_category=None,
                error_message=None,
            )

    def fail_batch(
        self,
        items: list[ClaimedTranslationItem],
        *,
        error_category: str,
        error_message: str,
        duration_seconds: float,
    ) -> None:
        if not items:
            return
        batch_id = _single_batch_id(items)
        now = _now()
        with self._connect() as conn:
            conn.executemany(
                """
                update translation_items
                set
                    status = ?,
                    attempt_count = attempt_count + 1,
                    leased_by = null,
                    leased_at = null,
                    batch_id = null,
                    last_error_category = ?,
                    last_error_message = ?,
                    updated_at = ?
                where item_id = ?
                """,
                [
                    (
                        QUEUE_STATUS_FAILED_RETRYABLE,
                        error_category,
                        error_message,
                        now,
                        item.item_id,
                    )
                    for item in items
                ],
            )
            self._insert_batch_attempt(
                conn,
                batch_id=batch_id,
                worker_id="unknown",
                item_count=len(items),
                status="failed",
                duration_seconds=duration_seconds,
                error_category=error_category,
                error_message=error_message,
            )

    def summary(self) -> TranslationQueueSummary:
        with self._connect() as conn:
            return TranslationQueueSummary(
                total_items=self._count(conn, "translation_items", None),
                pending_items=self._count(conn, "translation_items", QUEUE_STATUS_PENDING),
                leased_items=self._count(conn, "translation_items", QUEUE_STATUS_LEASED),
                completed_items=self._count(conn, "translation_items", QUEUE_STATUS_COMPLETED),
                failed_retryable_items=self._count(
                    conn, "translation_items", QUEUE_STATUS_FAILED_RETRYABLE
                ),
                result_items=self._count(conn, "translation_results", None),
                batch_attempts=self._count(conn, "translation_batch_attempts", None),
                successful_batches=self._count(conn, "translation_batch_attempts", "success"),
                failed_batches=self._count(conn, "translation_batch_attempts", "failed"),
            )

    def result_count(self) -> int:
        with self._connect() as conn:
            return self._count(conn, "translation_results", None)

    def completed_results(self) -> list[CompletedTranslationQueueResult]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                select
                    i.item_id,
                    i.source_duckdb_path,
                    i.source_table,
                    i.source_pk,
                    i.source_field,
                    i.source_text_hash,
                    i.target_language,
                    r.translated_text
                from translation_items i
                join translation_results r on r.item_id = i.item_id
                where i.status = ?
                order by i.source_table, i.source_pk, i.source_field, i.item_id
                """,
                [QUEUE_STATUS_COMPLETED],
            ).fetchall()
        return [
            CompletedTranslationQueueResult(
                item_id=row[0],
                source_duckdb_path=row[1],
                source_table=row[2],
                source_pk=row[3],
                source_field=row[4],
                source_text_hash=row[5],
                target_language=row[6],
                translated_text=row[7],
            )
            for row in rows
        ]

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.duckdb_path))

    def _insert_batch_attempt(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        batch_id: str,
        worker_id: str,
        item_count: int,
        status: str,
        duration_seconds: float,
        error_category: str | None,
        error_message: str | None,
    ) -> None:
        now = _now()
        conn.execute(
            """
            insert or replace into translation_batch_attempts (
                batch_id,
                worker_id,
                item_count,
                status,
                started_at,
                finished_at,
                duration_seconds,
                error_category,
                error_message
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                batch_id,
                worker_id,
                item_count,
                status,
                now,
                now,
                duration_seconds,
                error_category,
                error_message,
            ],
        )

    @staticmethod
    def _count(
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        status: str | None,
    ) -> int:
        if status is None:
            return int(conn.execute(f"select count(*) from {table_name}").fetchone()[0])
        return int(
            conn.execute(
                f"select count(*) from {table_name} where status = ?",
                [status],
            ).fetchone()[0]
        )


def _single_batch_id(items: list[ClaimedTranslationItem]) -> str:
    batch_ids = {item.batch_id for item in items}
    if len(batch_ids) != 1:
        raise ValueError(f"expected one batch_id, got {sorted(batch_ids)}")
    return next(iter(batch_ids))


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
