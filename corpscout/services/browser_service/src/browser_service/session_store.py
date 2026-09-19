"""Durable assignments; terminal IDs are retained and cannot be reused."""

import sqlite3
from pathlib import Path
from time import time


class SessionStore:
    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                owner TEXT NOT NULL,
                request_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                profile_id TEXT,
                generation TEXT,
                state TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_request_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                ended_at REAL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS active_profile
            ON sessions(profile_id)
            WHERE state = 'ready';
        """)
        path.chmod(0o600)

    def get(self, identifier: str) -> dict | None:
        row = self.connection.execute(
            "SELECT * FROM sessions WHERE id = ?", (identifier,)
        ).fetchone()
        return dict(row) if row is not None else None

    def create(
        self, identifier: str, owner: str, request_id: str, domain: str, timeout: float
    ) -> None:
        now = time()
        with self.connection:
            self.connection.execute(
                """INSERT INTO sessions(id, owner, request_id, domain, state,
                   created_at, last_request_at, expires_at) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)""",
                (identifier, owner, request_id, domain, now, now, now + timeout),
            )

    def assign(self, identifier: str, profile_id: str, generation: str | None) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE sessions SET profile_id=?, generation=?, state='ready' WHERE id=? AND state='queued'",
                (profile_id, generation, identifier),
            )

    def touch(self, identifier: str, timeout: float, generation: str | None) -> None:
        now = time()
        with self.connection:
            self.connection.execute(
                """UPDATE sessions SET last_request_at=?, expires_at=?, generation=COALESCE(?, generation)
                   WHERE id=? AND state IN ('queued', 'ready')""",
                (now, now + timeout, generation, identifier),
            )

    def finish(self, identifier: str, state: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE sessions SET state=?, ended_at=? WHERE id=? AND ended_at IS NULL",
                (state, time() if state != "releasing" else None, identifier),
            )

    def interrupt_previous_process(self) -> None:
        # Browser processes belong to the old process group. Their tabs do not survive
        # service startup; preserve the mapping as history, never silently reassign it.
        with self.connection:
            self.connection.execute(
                "UPDATE sessions SET state='interrupted', ended_at=? WHERE ended_at IS NULL",
                (time(),),
            )

    def recent(self, limit: int = 100) -> list[dict]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM sessions ORDER BY sequence DESC LIMIT ?", (limit,)
            )
        ]

    def close(self) -> None:
        self.connection.close()
