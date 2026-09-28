"""Durable lookup batches and publication receipts in the crawler's local SQLite."""

import json
import sqlite3
from pathlib import Path

from crawler_service.storage import utc_now


def batch_settings(payload: dict) -> dict:
    """Saved revisions identify credentials; a fresh encryption nonce is not new work."""
    settings = dict(payload)
    settings.pop("website_id", None)
    settings.pop("website_ids", None)  # Derived identities do not change a frozen batch.
    for key in ("llm", "decision_llm"):
        profile = settings.get(key)
        if profile and profile.get("profile_id") and profile.get("profile_revision"):
            settings[key] = {
                name: value
                for name, value in profile.items()
                if name != "api_key_encrypted"
            }
    if "entries" in settings:
        settings["entries"] = [
            dict(entry, request=batch_settings(entry["request"]))
            for entry in settings["entries"]
        ]
    return settings


class LookupStore:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                created_at TEXT NOT NULL, published_at TEXT,
                cancelled INTEGER NOT NULL DEFAULT 0, error TEXT
            );
            CREATE TABLE IF NOT EXISTS items (
                request_id TEXT NOT NULL, batch_id TEXT NOT NULL, domain TEXT NOT NULL,
                dispatched INTEGER NOT NULL DEFAULT 0, cancelled INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(batch_id, request_id)
            );
            CREATE INDEX IF NOT EXISTS items_batch ON items(batch_id);
            CREATE TABLE IF NOT EXISTS publications (
                request_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                payload TEXT NOT NULL, published_at TEXT, error TEXT,
                PRIMARY KEY(request_id, attempt)
            );
        """)
        # A resumed transport batch may contain unfinished requests from an older
        # batch. Keep both memberships; results remain unique by request/attempt.
        primary_key = [row["name"] for row in self.db.execute("PRAGMA table_info(items)") if row["pk"]]
        if primary_key == ["request_id"]:
            with self.db:
                self.db.execute("BEGIN IMMEDIATE")
                self.db.execute("""CREATE TABLE items_by_batch (
                    request_id TEXT NOT NULL, batch_id TEXT NOT NULL, domain TEXT NOT NULL,
                    dispatched INTEGER NOT NULL DEFAULT 0, cancelled INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(batch_id, request_id))""")
                self.db.execute("INSERT INTO items_by_batch SELECT * FROM items")
                self.db.execute("DROP TABLE items")
                self.db.execute("ALTER TABLE items_by_batch RENAME TO items")
                self.db.execute("CREATE INDEX items_batch ON items(batch_id)")
        self.db.execute("CREATE INDEX IF NOT EXISTS items_request ON items(request_id)")

    def submit(self, batch_id: str, payload: dict, items: list[dict]) -> None:
        encoded = json.dumps(payload, sort_keys=True)
        with self.db:
            previous = self.db.execute(
                "SELECT payload FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if previous is not None:
                if batch_settings(json.loads(previous["payload"])) != batch_settings(
                    payload
                ):
                    raise ValueError(
                        "Batch ID already exists with different inputs or settings"
                    )
                # Re-admission is explicit and validates models before this call.
                self.db.execute(
                    "UPDATE batches SET cancelled=0,published_at=NULL,error=NULL WHERE batch_id=? AND cancelled=1",
                    (batch_id,),
                )
                self.db.execute(
                    "UPDATE items SET cancelled=0 WHERE batch_id=? AND NOT dispatched",
                    (batch_id,),
                )
                return
            incoming = {item["request_id"]: item["domain"] for item in items}
            placeholders = ",".join("?" for _ in incoming)
            for old in self.db.execute(f"""SELECT i.request_id,i.domain,b.payload
                FROM items i JOIN batches b USING(batch_id)
                WHERE i.request_id IN ({placeholders})""", tuple(incoming)):
                comparable = []
                for document in (json.loads(old["payload"]), payload):
                    settings = batch_settings(document)
                    settings.pop("batch_id", None)
                    settings.pop("domains", None)
                    if "entries" in settings:
                        settings["entries"] = [entry for entry in settings["entries"]
                            if entry["request"]["request_id"] == old["request_id"]]
                    comparable.append(settings)
                if incoming[old["request_id"]] != old["domain"] or comparable[0] != comparable[1]:
                    raise ValueError("Request ID already exists with different inputs or settings")
            self.db.execute(
                "INSERT INTO batches(batch_id,payload,created_at) VALUES (?,?,?)",
                (batch_id, encoded, utc_now()),
            )
            self.db.executemany(
                """INSERT INTO items(request_id,batch_id,domain,dispatched)
                SELECT ?,?,?,EXISTS(SELECT 1 FROM items WHERE request_id=? AND dispatched)
                    OR EXISTS(SELECT 1 FROM publications WHERE request_id=?)""",
                [(i["request_id"], batch_id, i["domain"], i["request_id"], i["request_id"]) for i in items],
            )

    def undispatched(self, limit: int) -> list[dict]:
        return [
            dict(row)
            for row in self.db.execute(
                """SELECT * FROM (
                    SELECT i.*, b.payload, b.created_at,
                        row_number() OVER (PARTITION BY i.request_id ORDER BY b.created_at,b.batch_id) AS dispatch_rank
                    FROM items i JOIN batches b USING(batch_id)
                    WHERE NOT i.dispatched AND NOT b.cancelled
                ) WHERE dispatch_rank=1 ORDER BY created_at,request_id LIMIT ?""",
                (limit,),
            )
        ]

    def dispatched(self, request_id: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE items SET dispatched=1 WHERE request_id=? AND NOT cancelled", (request_id,)
            )

    def enqueue(self, request_id: str, attempt: int, payload: dict) -> None:
        # A finished attempt is immutable. Delivery retries replay the same rows.
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO publications(request_id,attempt,payload) VALUES (?,?,?)",
                (request_id, attempt, json.dumps(payload)),
            )

    def ready(self) -> list[tuple[str | None, list[dict]]]:
        groups = []
        singles = self.db.execute("""SELECT p.* FROM publications p LEFT JOIN items i USING(request_id)
            WHERE i.request_id IS NULL AND p.published_at IS NULL LIMIT 100""").fetchall()
        if singles:
            groups.append((None, [dict(row) for row in singles]))
        batches = self.db.execute("""SELECT b.batch_id FROM batches b WHERE published_at IS NULL
            AND NOT EXISTS (SELECT 1 FROM items i WHERE i.batch_id=b.batch_id AND NOT i.cancelled
                AND NOT EXISTS (SELECT 1 FROM publications p WHERE p.request_id=i.request_id))
            ORDER BY b.created_at LIMIT 1""").fetchall()
        for batch in batches:
            rows = self.db.execute(
                """SELECT p.* FROM publications p JOIN items i USING(request_id)
                WHERE i.batch_id=? AND p.published_at IS NULL ORDER BY p.request_id,p.attempt""",
                (batch["batch_id"],),
            ).fetchall()
            groups.append((batch["batch_id"], [dict(row) for row in rows]))
        return groups

    def delivered(
        self, batch_id: str | None, rows: list[dict], error: str | None
    ) -> None:
        timestamp = utc_now() if error is None else None
        with self.db:
            self.db.executemany(
                "UPDATE publications SET published_at=?,error=? WHERE request_id=? AND attempt=?",
                [(timestamp, error, r["request_id"], r["attempt"]) for r in rows],
            )
            if batch_id is not None:
                self.db.execute(
                    "UPDATE batches SET published_at=?,error=? WHERE batch_id=?",
                    (timestamp, error, batch_id),
                )

    def receipt(self, request_id: str, attempt: int) -> dict:
        row = self.db.execute(
            "SELECT published_at,error FROM publications WHERE request_id=? AND attempt=?",
            (request_id, attempt),
        ).fetchone()
        return {
            "state": "published" if row and row["published_at"] else "pending",
            "published_at": row["published_at"] if row else None,
            "error": row["error"] if row else None,
        }

    def snapshot(self, batch_id: str) -> dict | None:
        batch = self.db.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if batch is None:
            return None
        counts = self.db.execute(
            """SELECT count(*) AS total, sum(i.dispatched) AS dispatched,
            sum(i.cancelled AND p.request_id IS NULL) AS skipped, sum(p.request_id IS NOT NULL) AS processed,
            sum(json_extract(p.payload,'$.status')='matched') AS matched,
            sum(json_extract(p.payload,'$.status')='not_found') AS not_found,
            sum(json_extract(p.payload,'$.status')='already_mapped') AS already_mapped,
            sum(json_extract(p.payload,'$.status') IN ('failed','cancelled')) AS failed
            FROM items i LEFT JOIN publications p USING(request_id) WHERE i.batch_id=?""",
            (batch_id,),
        ).fetchone()
        result = {key: counts[key] or 0 for key in counts.keys()}
        settled = result["processed"] + result["skipped"] == result["total"]
        state = (
            ("cancelled" if settled else "cancelling")
            if batch["cancelled"]
            else (
                "published"
                if batch["published_at"]
                else "publishing"
                if settled
                else "running"
            )
        )
        return dict(
            result,
            batch_id=batch_id,
            state=state,
            created_at=batch["created_at"],
            published_at=batch["published_at"],
            publication_error=batch["error"],
        )

    def cancel(self, batch_id: str) -> list[str]:
        with self.db:
            self.db.execute(
                "UPDATE batches SET cancelled=1 WHERE batch_id=? AND published_at IS NULL",
                (batch_id,),
            )
            self.db.execute(
                "UPDATE items SET cancelled=1 WHERE batch_id=? AND NOT dispatched",
                (batch_id,),
            )
        return [
            row[0]
            for row in self.db.execute(
                """SELECT request_id FROM items AS target WHERE batch_id=? AND dispatched
                AND NOT EXISTS (SELECT 1 FROM items AS other JOIN batches b USING(batch_id)
                    WHERE other.request_id=target.request_id AND other.batch_id!=target.batch_id
                        AND NOT b.cancelled)""",
                (batch_id,),
            )
        ]

    def close(self) -> None:
        self.db.close()
