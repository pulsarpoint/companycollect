"""Persistent browser identities and exclusive, capacity-limited executions."""

import json
import sqlite3
from pathlib import Path
from time import time
from uuid import uuid4


class BrowserSessionError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class SessionStore:
    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info(sessions)")
        }
        if "profile_id" in columns:
            # Old lease history is retained separately; its IDs never denoted saved profiles.
            self.connection.execute("ALTER TABLE sessions RENAME TO legacy_sessions")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS settings (
                name TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY, route TEXT NOT NULL, headless INTEGER NOT NULL,
                pinned INTEGER NOT NULL DEFAULT 0, label TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL, last_used_at REAL NOT NULL,
                retained_until REAL NOT NULL, expired_at REAL
            );
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL, request_id TEXT NOT NULL,
                domain TEXT NOT NULL, state TEXT NOT NULL, generation TEXT,
                created_at REAL NOT NULL, last_request_at REAL NOT NULL,
                expires_at REAL NOT NULL, ended_at REAL, error TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS one_live_execution
                ON executions(session_id) WHERE ended_at IS NULL;
            CREATE INDEX IF NOT EXISTS execution_history ON executions(session_id, created_at);
        """)
        path.chmod(0o600)

    def get_setting(self, name: str) -> dict | None:
        row = self.connection.execute(
            "SELECT value FROM settings WHERE name=?", (name,)
        ).fetchone()
        return json.loads(row[0]) if row is not None else None

    def set_setting(self, name: str, value: dict) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO settings VALUES (?, ?, ?) ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (name, json.dumps(value), time()),
            )

    def delete_setting(self, name: str) -> None:
        with self.connection:
            self.connection.execute("DELETE FROM settings WHERE name=?", (name,))

    def session(self, identifier: str) -> dict | None:
        row = self.connection.execute(
            "SELECT * FROM sessions WHERE id=?", (identifier,)
        ).fetchone()
        return dict(row) if row is not None else None

    def ensure_session(
        self,
        identifier: str,
        *,
        route: str | None,
        headless: bool | None,
        retention: float,
        label: str = "",
        pinned: bool = False,
    ) -> dict:
        with self.connection:
            previous = self.session(identifier)
            if previous is not None:
                if previous["expired_at"] is not None or (
                    not previous["pinned"]
                    and previous["retained_until"] <= time()
                    and self.for_profile(identifier) is None
                ):
                    raise BrowserSessionError(
                        410, "Saved session expired; create a new session"
                    )
                if route is not None and route != previous["route"]:
                    raise BrowserSessionError(
                        409, "Saved session belongs to a different proxy route"
                    )
                return previous
            now = time()
            self.connection.execute(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    identifier,
                    route or "direct",
                    True if headless is None else headless,
                    pinned,
                    label,
                    now,
                    now,
                    now + retention,
                ),
            )
            return self.session(identifier)

    def get(self, identifier: str) -> dict | None:
        row = self.connection.execute(
            "SELECT *,session_id AS profile_id FROM executions WHERE session_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (identifier,),
        ).fetchone()
        return dict(row) if row is not None else None

    def for_profile(self, identifier: str) -> dict | None:
        row = self.connection.execute(
            "SELECT *,session_id AS profile_id FROM executions WHERE session_id=? AND ended_at IS NULL",
            (identifier,),
        ).fetchone()
        return dict(row) if row is not None else None

    def claim(
        self,
        identifier: str,
        request_id: str,
        domain: str,
        *,
        max_browsers: int,
        timeout: float,
        retention: float,
        headless: bool | None,
    ) -> dict:
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            existing = self.for_profile(identifier)
            saved = self.session(identifier)
            if saved is None or saved["expired_at"] is not None:
                raise BrowserSessionError(410, "Saved session is unavailable")
            if existing is not None:
                if existing["request_id"] != request_id or existing["domain"] != domain:
                    raise BrowserSessionError(
                        409, "Session is being used by another request"
                    )
                if headless is not None and bool(saved["headless"]) != headless:
                    raise BrowserSessionError(
                        409, "Close this execution before changing browser mode"
                    )
                if existing["state"] == "stopping":
                    raise BrowserSessionError(409, "Session is still closing")
                return existing
            if (
                self.connection.execute(
                    "SELECT count() FROM executions WHERE ended_at IS NULL"
                ).fetchone()[0]
                >= max_browsers
            ):
                raise BrowserSessionError(
                    503, "Browser capacity exhausted; retry with the same session ID"
                )
            now, execution = time(), uuid4().hex
            self.connection.execute(
                "UPDATE sessions SET headless=?,last_used_at=?,retained_until=? WHERE id=?",
                (
                    saved["headless"] if headless is None else headless,
                    now,
                    now + retention,
                    identifier,
                ),
            )
            self.connection.execute(
                """INSERT INTO executions
                (id,session_id,request_id,domain,state,created_at,last_request_at,expires_at)
                VALUES (?,?,?,?,'starting',?,?,?)""",
                (execution, identifier, request_id, domain, now, now, now + timeout),
            )
            return self.for_profile(identifier)

    def ready(self, execution: str, generation: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE executions SET state='ready',generation=? WHERE id=? AND state='starting'",
                (generation, execution),
            )

    def touch(self, execution: str, timeout: float, generation: str | None) -> None:
        now = time()
        with self.connection:
            self.connection.execute(
                "UPDATE executions SET last_request_at=?,expires_at=?,generation=COALESCE(?,generation) WHERE id=? AND ended_at IS NULL AND state IN ('starting','ready')",
                (now, now + timeout, generation, execution),
            )

    def finish(
        self, execution: str, state: str, *, retention: float, error: str | None = None
    ) -> None:
        now = time()
        with self.connection:
            self.connection.execute(
                "UPDATE executions SET state=?,ended_at=?,error=? WHERE id=? AND ended_at IS NULL",
                (state, None if state == "stopping" else now, error, execution),
            )
            self.connection.execute(
                "UPDATE sessions SET last_used_at=?,retained_until=? WHERE id=(SELECT session_id FROM executions WHERE id=?)",
                (now, now + retention, execution),
            )

    def set_pinned(self, identifier: str, pinned: bool, retention: float) -> None:
        saved = self.session(identifier)
        if saved is None:
            raise BrowserSessionError(404, "Unknown saved session")
        if saved["expired_at"] is not None:
            raise BrowserSessionError(410, "Saved session expired")
        with self.connection:
            self.connection.execute(
                "UPDATE sessions SET pinned=?,retained_until=? WHERE id=?",
                (pinned, time() + retention, identifier),
            )

    def expire_profiles(self) -> list[str]:
        with self.connection:
            self.connection.execute(
                """UPDATE sessions SET expired_at=?
                WHERE expired_at IS NULL AND NOT pinned AND retained_until<=?
                AND id NOT IN (SELECT session_id FROM executions WHERE ended_at IS NULL)""",
                (time(), time()),
            )
        return [
            row[0]
            for row in self.connection.execute(
                "SELECT id FROM sessions WHERE expired_at IS NOT NULL"
            )
        ]

    def interrupt_previous_process(self) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE executions SET state='interrupted',ended_at=? WHERE ended_at IS NULL",
                (time(),),
            )

    def saved(self, limit: int = 100) -> list[dict]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM sessions WHERE expired_at IS NULL ORDER BY last_used_at DESC LIMIT ?",
                (limit,),
            )
        ]

    def recent(self, limit: int = 100) -> list[dict]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT *,session_id AS profile_id FROM executions ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        ]

    def close(self) -> None:
        self.connection.close()
