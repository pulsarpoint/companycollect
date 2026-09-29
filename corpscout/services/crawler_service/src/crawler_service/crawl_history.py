"""Local SQLite attempt history and durable status notifications."""

import json
import sqlite3
from pathlib import Path


class CrawlHistory:
    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path, uri=True)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript("""
            PRAGMA auto_vacuum=INCREMENTAL;
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS attempts (
                request_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                domain TEXT NOT NULL, state TEXT NOT NULL, source TEXT NOT NULL,
                updated_at TEXT NOT NULL, job TEXT NOT NULL,
                PRIMARY KEY (request_id, attempt)
            );
            CREATE INDEX IF NOT EXISTS attempts_status ON attempts(state, updated_at DESC);
            CREATE INDEX IF NOT EXISTS attempts_domain ON attempts(domain, updated_at DESC);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                state TEXT NOT NULL, created_at TEXT NOT NULL, payload TEXT NOT NULL
            );
        """)

    def save(self, job: dict) -> None:
        # A queued recovery is the next attempt, not a rewrite of the interrupted one.
        if job["state"] == "queued" and job["attempt"] > 0:
            return
        encoded = json.dumps(job, ensure_ascii=False)
        with self.connection:
            previous = self.connection.execute(
                "SELECT job FROM attempts WHERE request_id=? AND attempt=?",
                (job["request_id"], job["attempt"]),
            ).fetchone()
            if previous is not None and previous["job"] == encoded:
                return
            if job["attempt"] > 0:
                self.connection.execute(
                    "DELETE FROM attempts WHERE request_id=? AND attempt=0",
                    (job["request_id"],),
                )
            self.connection.execute(
                """
                INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id, attempt) DO UPDATE SET
                    state=excluded.state, updated_at=excluded.updated_at, job=excluded.job
            """,
                (
                    job["request_id"],
                    job["attempt"],
                    job["domain"],
                    job["state"],
                    job["source"],
                    job["updated_at"],
                    encoded,
                ),
            )
            self.connection.execute(
                "INSERT INTO events(request_id, attempt, state, created_at, payload) VALUES (?, ?, ?, ?, ?)",
                (
                    job["request_id"],
                    job["attempt"],
                    job["state"],
                    job["updated_at"],
                    encoded,
                ),
            )

    def get(self, request_id: str, attempt: int) -> dict | None:
        row = self.connection.execute(
            "SELECT job FROM attempts WHERE request_id=? AND attempt=?",
            (request_id, attempt),
        ).fetchone()
        return json.loads(row["job"]) if row is not None else None

    def list_attempts(
        self,
        *,
        state: str | None,
        domain: str | None,
        source: str | None,
        limit: int,
        offset: int,
    ) -> dict:
        where, params = [], []
        if state is not None:
            where.append("state = ?")
            params.append(state)
        if domain:
            where.append("instr(domain, ?) > 0")
            params.append(domain.casefold())
        if source is not None:
            where.append("source = ?")
            params.append(source)
        clause = " WHERE " + " AND ".join(where) if where else ""
        total = self.connection.execute(
            "SELECT count(*) FROM attempts" + clause, params
        ).fetchone()[0]
        rows = self.connection.execute(
            "SELECT job FROM attempts"
            + clause
            + " ORDER BY updated_at DESC, request_id LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
        return {
            "attempts": [json.loads(row["job"]) for row in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def events(self, after: int, limit: int = 100) -> list[dict]:
        return [
            {"id": row["id"], "job": json.loads(row["payload"])}
            for row in self.connection.execute(
                "SELECT id, payload FROM events WHERE id > ? ORDER BY id LIMIT ?",
                (after, limit),
            )
        ]

    def revision(self) -> int:
        return self.connection.execute(
            "SELECT coalesce(max(id), 0) FROM events"
        ).fetchone()[0]

    def pending_uploads(self) -> list[dict]:
        return [
            json.loads(row["job"])
            for row in self.connection.execute(
                "SELECT job FROM attempts WHERE state IN ('failed', 'completed', 'cancelled') "
                "AND json_extract(job, '$.s3_state') = 'pending' ORDER BY updated_at LIMIT 100"
            )
        ]

    def prune(self, before: str, *, lookup_path: Path, limit: int = 100) -> dict[str, int]:
        """Expire old delivered status events; keep attempt summaries for retries."""
        if limit < 1:
            raise ValueError("Retention limit must be positive")
        # Opening read-only also prevents a typo from creating an empty lookup
        # database. The bound URI safely supports spaces and other path text.
        self.connection.execute(
            "ATTACH DATABASE ? AS lookup_retention",
            (lookup_path.resolve().as_uri() + "?mode=ro",),
        )
        try:
            with self.connection:
                count = self.connection.execute(
                    """DELETE FROM events WHERE id IN (
                        SELECT e.id FROM events e JOIN attempts a
                            ON a.request_id=e.request_id AND a.attempt=e.attempt
                        WHERE e.created_at < ?
                            AND coalesce(json_extract(a.job,'$.finished_at'),a.updated_at) < ?
                            AND a.state IN ('completed','failed','cancelled')
                            AND e.id < (SELECT max(id) FROM events)
                            AND (
                                (coalesce(json_extract(a.job,'$.purpose'),'crawl')='crawl'
                                    AND json_extract(a.job,'$.s3_state')='uploaded')
                                OR (json_extract(a.job,'$.purpose')='company_lookup'
                                    AND EXISTS (
                                        SELECT 1 FROM lookup_retention.publications p
                                        WHERE p.request_id=a.request_id AND p.attempt=a.attempt
                                            AND p.published_at < ?
                                    ))
                            )
                        ORDER BY e.id LIMIT ?
                    )""",
                    (before, before, before, limit),
                ).rowcount
        finally:
            self.connection.execute("DETACH DATABASE lookup_retention")
        if self.connection.execute("PRAGMA auto_vacuum").fetchone()[0] == 2:
            self.connection.execute("PRAGMA incremental_vacuum(128)").fetchall()
        self.connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall()
        page_size = self.connection.execute("PRAGMA page_size").fetchone()[0]
        return {
            "events": count,
            "reusable_bytes": self.connection.execute("PRAGMA freelist_count").fetchone()[0] * page_size,
            "database_bytes": self.connection.execute("PRAGMA page_count").fetchone()[0] * page_size,
        }

    def close(self) -> None:
        self.connection.close()
