#!/usr/bin/env python3
"""Bounded, restartable migration 436 backfill and UUID-aware cutover.

Create shadows with golang-migrate first. Backfill can be repeated. For cutover,
quiesce indexing and gate readers, then use --writers-held --readers-held.
The durable ledger records UUIDs BEFORE any exchange; interrupted swaps resume.
"""

import argparse
import itertools
import json
import os
from functools import lru_cache
from pathlib import Path

from clickhouse_driver import Client
from dotenv import load_dotenv

from dagster_v3.defs.webtech.pages import page_identity
from dagster_v3.defs.webtech.storage import WEBTECH_RESULT_COLUMNS as CURRENT_SCAN_COLUMNS

# This completed migration predates task-input lineage columns.
WEBTECH_RESULT_COLUMNS = tuple(c for c in CURRENT_SCAN_COLUMNS if c not in {"task_id", "input_id"})
from dagster_v3.defs.webtech.technologies import WEBTECH_TECHNOLOGY_COLUMNS

TABLES = {
    "webtech_domain_technologies": WEBTECH_TECHNOLOGY_COLUMNS,
    "webtech_domain_scan_results": WEBTECH_RESULT_COLUMNS,
}
MIGRATIONS = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
normalize = lru_cache(maxsize=20_000)(page_identity)


def connect() -> Client:
    return Client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="corpscout",
        secure=os.getenv("CLICKHOUSE_SECURE", "").lower() in {"true", "1", "yes"},
        send_receive_timeout=1200,
        settings={
            "max_threads": 4,
            "max_execution_time": 1200,
            "max_memory_usage": 8_000_000_000,
        },
    )


def save(path: Path, value: dict) -> None:
    temp = path.with_suffix(".tmp")
    with temp.open("w") as stream:
        json.dump(value, stream, indent=2, default=str)
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)


def backfill(client: Client) -> dict[str, int]:
    """Stream native blocks, preserving every source column and original timestamp."""
    result = {}
    for table, columns in TABLES.items():
        old = columns[:-2]
        source = connect()
        count = 0
        try:
            rows = source.execute_iter(
                f"SELECT {', '.join(old)} FROM corpscout.{table} FINAL",
                settings={"max_block_size": 10_000},
            )
            while batch := list(itertools.islice(rows, 25_000)):
                output = [
                    (*row, *normalize(row[old.index("requested_url")])) for row in batch
                ]
                client.execute(
                    f"INSERT INTO corpscout.{table}_v2 ({', '.join(columns)}) VALUES",
                    output,
                )
                count += len(batch)
                if count % 250_000 == 0:
                    print(json.dumps({"table": table, "copied": count}), flush=True)
        finally:
            source.disconnect()
        result[table] = count
        print(json.dumps({"table": table, "copied": count, "done": True}), flush=True)
    return result


def reconcile(client: Client) -> dict:
    result = {}
    for table, columns in TABLES.items():
        cols = ", ".join(columns[:-2])
        missing = 0
        cursor = ""
        # Root ranges keep exact full-payload EXCEPT comparisons bounded in memory.
        while roots := client.execute(
            f"SELECT DISTINCT root_domain FROM corpscout.{table} WHERE root_domain > %(cursor)s ORDER BY root_domain LIMIT 10000",
            {"cursor": cursor},
        ):
            end = roots[-1][0]
            where = "WHERE root_domain > %(cursor)s AND root_domain <= %(end)s"
            missing += client.execute(
                f"SELECT count() FROM (SELECT {cols} FROM corpscout.{table} FINAL {where} EXCEPT DISTINCT SELECT {cols} FROM corpscout.{table}_v2 FINAL {where})",
                {"cursor": cursor, "end": end},
            )[0][0]
            cursor = end
        conflicts = client.execute(
            f"SELECT count() FROM (SELECT root_domain, website_origin, page_url, crawl_id, detector_version, scan_id FROM corpscout.{table}_v2 GROUP BY ALL HAVING uniqExact(report_sha256)>1)"
        )[0][0]
        count = client.execute(f"SELECT count() FROM corpscout.{table}_v2 FINAL")[0][0]
        result[table] = {
            "missing_source_rows": missing,
            "conflicting_reports": conflicts,
            "logical_rows": count,
        }
        if missing or conflicts:
            raise RuntimeError(json.dumps(result))
    # All summary markers must own a complete set. Orphan detections stay history.
    mismatch = client.execute("""SELECT count() FROM
        (SELECT root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,technology_count
         FROM corpscout.webtech_domain_scan_results_v2 FINAL) s
        LEFT JOIN (SELECT root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256,count() n
         FROM corpscout.webtech_domain_technologies_v2 FINAL GROUP BY ALL) d
        USING (root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256)
        WHERE s.technology_count != d.n""")[0][0]
    result["incomplete_summaries"] = mismatch
    if mismatch:
        raise RuntimeError(json.dumps(result))
    result["orphan_reports"] = client.execute("""SELECT count() FROM
        (SELECT DISTINCT root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256
         FROM corpscout.webtech_domain_technologies_v2 FINAL
         EXCEPT DISTINCT SELECT root_domain,website_origin,page_url,crawl_id,detector_version,scan_id,report_sha256
         FROM corpscout.webtech_domain_scan_results_v2 FINAL)""")[0][0]
    return result


def table_uuids(client: Client) -> dict[str, str]:
    return dict(
        client.execute(
            "SELECT name,toString(uuid) FROM system.tables WHERE database='corpscout' AND name LIKE 'webtech%'"
        )
    )


def exchange_pair(client: Client, table: str, old_uuid: str, new_uuid: str) -> None:
    current = table_uuids(client)
    if current.get(table) == old_uuid and current.get(table + "_v2") == new_uuid:
        client.execute(f"EXCHANGE TABLES corpscout.{table} AND corpscout.{table}_v2")
    elif current.get(table) != new_uuid or current.get(table + "_v2") != old_uuid:
        raise RuntimeError(f"Unexpected UUIDs; refusing to exchange {table}")


def cutover(client: Client, ledger: Path) -> dict:
    if ledger.exists():
        state = json.loads(ledger.read_text())
    else:
        state = {
            "validation": reconcile(client),
            "uuids": table_uuids(client),
            "completed": [],
            "old_view": client.execute(
                "SHOW CREATE TABLE corpscout.webtech_domain_technologies_current"
            )[0][0],
        }
        save(ledger, state)
    for table in TABLES:
        exchange_pair(
            client, table, state["uuids"][table], state["uuids"][table + "_v2"]
        )
        if table not in state["completed"]:
            state["completed"].append(table)
            save(ledger, state)
    ddl = (MIGRATIONS / "000437_corpscout_webtech_page_current_lookup.up.sql").read_text()
    view = (
        ddl[ddl.index("CREATE OR REPLACE VIEW") :]
        .replace("_v2", "")
    )
    client.execute(view.rstrip().rstrip(";"))
    client.execute(
        "DROP VIEW IF EXISTS corpscout.webtech_domain_technologies_current_v2"
    )
    state["view_recreated"] = True
    state["final_uuids"] = table_uuids(client)
    save(ledger, state)
    return state


def rollback(client: Client, ledger: Path) -> dict:
    state = json.loads(ledger.read_text())
    if not state.get("view_recreated"):
        raise RuntimeError(
            "Complete or repair the interrupted forward cutover before rollback"
        )
    if not state.get("rollback_replayed"):
        # Legacy keys cannot represent multi-page producers. Never silently collapse them.
        multiple = client.execute(
            "SELECT count() FROM (SELECT root_domain,crawl_id,detector_version FROM corpscout.webtech_domain_scan_results FINAL GROUP BY ALL HAVING uniqExact(page_url)>1)"
        )[0][0]
        if multiple:
            raise RuntimeError(
                "Multi-page scans exist; legacy rollback requires an explicit data decision"
            )
        for table, columns in TABLES.items():
            if table_uuids(client)[table] != state["uuids"][table + "_v2"]:
                raise RuntimeError("Unexpected canonical UUID before rollback replay")
            cols = ", ".join(columns[:-2])
            client.execute(
                f"INSERT INTO corpscout.{table}_v2 ({cols}) SELECT {cols} FROM corpscout.{table} FINAL"
            )
        state["rollback_replayed"] = True
        save(ledger, state)
    for table in TABLES:
        exchange_pair(
            client, table, state["uuids"][table + "_v2"], state["uuids"][table]
        )
    client.execute(
        state["old_view"].replace("CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
    )
    state["rolled_back"] = True
    save(ledger, state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=["backfill", "reconcile", "cutover", "rollback"]
    )
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--writers-held", action="store_true")
    parser.add_argument("--readers-held", action="store_true")
    args = parser.parse_args()
    load_dotenv()
    client = connect()
    try:
        if args.action == "backfill":
            result = backfill(client)
        elif args.action == "reconcile":
            result = reconcile(client)
        else:
            if not args.writers_held or not args.readers_held:
                parser.error("cutover requires held writers and readers")
            result = (
                rollback(client, args.ledger)
                if args.action == "rollback"
                else cutover(client, args.ledger)
            )
        if args.action not in {"cutover", "rollback"}:
            save(args.ledger, result)
        print(json.dumps(result, default=str), flush=True)
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
