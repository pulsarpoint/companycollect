"""SQLite checkpoints for a bounded Brave batch, independent of browser lifetimes."""

import hashlib
import json
import sqlite3
from pathlib import Path
from time import time

from fastapi import HTTPException

from browser_service.brave_models import BraveBatchRequest


class BraveBatchStore:
    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL, payload TEXT NOT NULL,
                controller_id TEXT NOT NULL, state TEXT NOT NULL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                lease_until REAL NOT NULL, reason TEXT NOT NULL DEFAULT '',
                published INTEGER NOT NULL DEFAULT 0
            );
            CREATE UNIQUE INDEX IF NOT EXISTS unfinished_execution
                ON batches(execution_id) WHERE state != 'completed';
            CREATE TABLE IF NOT EXISTS items (
                batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
                position INTEGER NOT NULL, input_id TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending', request_id TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0, result TEXT, outcome TEXT,
                PRIMARY KEY(batch_id,input_id)
            );
        """)
        path.chmod(0o600)
        with self.connection:
            self.connection.execute(
                "UPDATE items SET state='pending' WHERE state='running'"
            )
            self.connection.execute("""UPDATE batches SET state='paused',lease_until=0,
                reason='Browser service restarted; resume the batch'
                WHERE state != 'completed'""")

    def get(self, batch_id: str) -> dict:
        row = self.connection.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(404, "Unknown Brave batch")
        return dict(row)

    def unfinished(self, execution_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT batch_id FROM batches WHERE execution_id=? AND state!='completed'",
            (execution_id,),
        ).fetchone()
        return self.snapshot(row[0]) if row is not None else None

    def submit(self, payload: BraveBatchRequest) -> None:
        value = payload.model_dump(mode="json")
        identity = {key: item for key, item in value.items() if key != "source_run_id"}
        fingerprint = hashlib.sha256(
            json.dumps(identity, sort_keys=True).encode()
        ).hexdigest()
        batch_id = str(payload.batch_id)
        previous = self.connection.execute(
            "SELECT fingerprint FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if previous is not None:
            if previous[0] != fingerprint:
                raise HTTPException(
                    409, "Batch ID belongs to different inputs or configuration"
                )
            return
        if self.unfinished(str(payload.execution_id)) is not None:
            raise HTTPException(
                409, "Resume the existing unfinished batch before submitting another"
            )
        profile = payload.options.llm.model_dump(exclude={"api_key_encrypted"})
        digest = hashlib.sha256(
            json.dumps(profile, sort_keys=True).encode()
        ).hexdigest()[:16]
        now = time()
        with self.connection:
            self.connection.execute(
                "DELETE FROM batches WHERE state='completed' AND updated_at<?",
                (now - 7 * 86400,),
            )
            self.connection.execute(
                """INSERT INTO batches
                (batch_id,execution_id,fingerprint,payload,controller_id,state,created_at,updated_at,lease_until)
                VALUES (?,?,?,?,?,'paused',?,?,0)""",
                (
                    batch_id,
                    str(payload.execution_id),
                    fingerprint,
                    json.dumps(value),
                    str(payload.source_run_id),
                    now,
                    now,
                ),
            )
            self.connection.executemany(
                """INSERT INTO items
                (batch_id,position,input_id,request_id) VALUES (?,?,?,?)""",
                [
                    (
                        batch_id,
                        index,
                        item.input_id,
                        f"dagster-{item.result_id}-{digest}",
                    )
                    for index, item in enumerate(payload.items)
                ],
            )

    def activate(
        self, batch_id: str, controller_id: str, owner_request_id: str
    ) -> None:
        batch = self.get(batch_id)
        payload = json.loads(batch["payload"])
        payload["source_run_id"] = controller_id
        payload["owner_request_id"] = owner_request_id
        now = time()
        with self.connection:
            self.connection.execute(
                """UPDATE batches SET controller_id=?,payload=?,state='running',
                reason='',updated_at=?,lease_until=? WHERE batch_id=?""",
                (controller_id, json.dumps(payload), now, now + 60, batch_id),
            )
            self.connection.execute(
                "UPDATE items SET state='pending' WHERE batch_id=? AND state='running'",
                (batch_id,),
            )

    def heartbeat(self, batch_id: str, controller_id: str) -> None:
        if self.get(batch_id)["controller_id"] != controller_id:
            raise HTTPException(409, "This batch belongs to another controller")
        with self.connection:
            self.connection.execute(
                "UPDATE batches SET lease_until=? WHERE batch_id=?",
                (time() + 60, batch_id),
            )

    def state(
        self, batch_id: str, state: str, reason: str = "", published: int = 0
    ) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE batches SET state=?,reason=?,published=?,updated_at=? WHERE batch_id=?",
                (state, reason, published, time(), batch_id),
            )
            if state == "paused":
                self.connection.execute(
                    "UPDATE batches SET lease_until=0 WHERE batch_id=?", (batch_id,)
                )

    def claim(self, batch_id: str) -> dict | None:
        with self.connection:
            row = self.connection.execute(
                """SELECT * FROM items WHERE batch_id=? AND state='pending'
                ORDER BY position LIMIT 1""",
                (batch_id,),
            ).fetchone()
            if row is None:
                return None
            self.connection.execute(
                "UPDATE items SET state='running' WHERE batch_id=? AND input_id=?",
                (batch_id, row["input_id"]),
            )
            return dict(row)

    def retry_request(self, batch_id: str, item: dict) -> str:
        attempt = item["attempt"] + 1
        base = item["request_id"].split("-retry-")[0]
        request_id = f"{base}-retry-{attempt}"
        with self.connection:
            self.connection.execute(
                "UPDATE items SET request_id=?,attempt=? WHERE batch_id=? AND input_id=?",
                (request_id, attempt, batch_id, item["input_id"]),
            )
        item.update(request_id=request_id, attempt=attempt)
        return request_id

    def save(self, batch_id: str, input_id: str, result: dict) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE items SET state='done',result=?,outcome=? WHERE batch_id=? AND input_id=?",
                (
                    json.dumps(result, ensure_ascii=False),
                    result["status"],
                    batch_id,
                    input_id,
                ),
            )
            self.connection.execute(
                "UPDATE batches SET updated_at=? WHERE batch_id=?", (time(), batch_id)
            )

    def results(self, batch_id: str) -> list[dict]:
        return [
            json.loads(row[0])
            for row in self.connection.execute(
                "SELECT result FROM items WHERE batch_id=? AND state='done' ORDER BY position",
                (batch_id,),
            )
        ]

    def snapshot(self, batch_id: str) -> dict:
        batch = self.get(batch_id)
        payload = json.loads(batch["payload"])
        rows = self.connection.execute(
            """SELECT state,count(*) AS n FROM items WHERE batch_id=? GROUP BY state""",
            (batch_id,),
        ).fetchall()
        counts = {row["state"]: row["n"] for row in rows}
        outcomes = {
            row[0]: row[1]
            for row in self.connection.execute(
                "SELECT outcome,count(*) FROM items WHERE batch_id=? AND state='done' GROUP BY outcome",
                (batch_id,),
            )
        }
        return {
            "batch_id": batch_id,
            "task_id": payload["task_id"],
            "execution_id": batch["execution_id"],
            "controller_id": batch["controller_id"],
            "state": batch["state"],
            "reason": batch["reason"],
            "total": len(payload["items"]),
            "processed": counts.get("done", 0),
            "running": counts.get("running", 0),
            "pending": counts.get("pending", 0),
            "succeeded": outcomes.get("success", 0),
            "failed": outcomes.get("error", 0),
            "published": batch["published"],
            "created_at": batch["created_at"],
            "updated_at": batch["updated_at"],
        }

    def close(self) -> None:
        self.connection.close()
