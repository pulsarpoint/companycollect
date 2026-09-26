"""One-time, replayable import of archived and unpublished Brave outcomes."""

import json
import re
from uuid import UUID

from clickhouse_driver import Client

from dagster_v3.defs.common.processing import ProcessingStore
from dagster_v3.defs.company_domains.results import (
    RESULT_COLUMNS,
    RESULT_TABLE,
    insert_results,
)


def migrate_results(
    store: ProcessingStore, client: Client, *, batch_size: int
) -> dict[str, int]:
    """Run after old workers stop. Verify every PG outcome and retain all legacy data."""
    # Load full archived answers before enriching them from the compact PG records,
    # whose answer_text has already been removed after successful archival.
    tables = client.execute(
        "SELECT name FROM system.tables WHERE database='corpscout' "
        "AND endsWith(name,'_company_brave_search_results_latest_success')"
    )
    for (table,) in tables:
        if re.fullmatch(r"[a-z]{2}_company_brave_search_results_latest_success", table) is None:
            continue
        archive = table.removesuffix("_latest_success") + "_s3_archive"
        for relation in (f"corpscout.{table}", f"corpscout.{archive}"):
            if client.execute(f"EXISTS TABLE {relation}") != [(1,)]:
                continue
            client.execute(
                f"INSERT INTO {RESULT_TABLE} ({','.join(column for column in RESULT_COLUMNS if not column.startswith('search_'))}) "
                "SELECT country_code,company_id,company_name,query_type,query,toUUID(result_id),"
                "status,answer_text,completed_at,error_type,'',route,source_url,0,0,'[]',"
                "toUUID(task_id),toUUID(task_id),source_run_id,input_id,attempt,processor_version "
                f"FROM {relation} WHERE toUUID(result_id) NOT IN (SELECT result_id FROM {RESULT_TABLE})",
                settings={"async_insert": 0, "use_query_condition_cache": 0},
            )
    verified = 0
    after = "00000000-0000-0000-0000-000000000000"
    while True:
        with store.transaction() as cursor:
            cursor.execute(
                "SELECT r.*,t.processor,t.config FROM processing.results r "
                "JOIN processing.tasks t USING(task_id) "
                "WHERE t.processor='brave-v2' AND r.result_id>%s::uuid "
                "ORDER BY r.result_id LIMIT %s",
                (after, batch_size),
            )
            rows = cursor.fetchall()
        if not rows:
            break
        ids = tuple(str(row["result_id"]) for row in rows)
        saved = client.execute(
            f"SELECT {','.join(RESULT_COLUMNS)} FROM {RESULT_TABLE} FINAL "
            "WHERE result_id IN %(ids)s",
            {"ids": ids},
        )
        existing = {
            str(values[5]): dict(zip(RESULT_COLUMNS, values, strict=True))
            for values in saved
        }
        records = []
        for row in rows:
            payload = row["payload"]
            result_id = str(row["result_id"])
            previous = existing.get(result_id, {})
            record = {
                "country_code": payload.get(
                    "country_code", previous.get("country_code", "")
                ),
                "company_id": payload.get("company_id", previous.get("company_id", "")),
                "company_name": payload.get(
                    "company_name", previous.get("company_name", "")
                ),
                "query_type": row["config"].get(
                    "query_type", previous.get("query_type", "official_website")
                ),
                "query": payload.get("query", previous.get("query", "")),
                "result_id": UUID(result_id),
                "status": row["status"],
                "answer_text": payload.get(
                    "answer_text", previous.get("answer_text", "")
                ),
                "completed_at": row["completed_at"],
                "error_type": payload.get("error_type", ""),
                "error_stage": payload.get("error_stage", ""),
                "route": payload.get("route", ""),
                "source_url": payload.get("source_url", ""),
                "elapsed_ms": payload.get("elapsed_ms", 0),
                "answer_timeout_ms": payload.get("answer_timeout_ms", 0),
                "challenge_runs_json": json.dumps(
                    payload.get("challenge_runs", []), ensure_ascii=False
                ),
                "task_id": UUID(str(row["task_id"])),
                "execution_id": UUID(str(row["task_id"])),
                "source_run_id": payload.get("source_run_id", ""),
                "input_id": row["input_id"],
                "attempt": row["attempt"],
                "processor_version": row["processor"],
                "search_id": previous.get("search_id", ""),
                "search_revision": previous.get("search_revision", 0),
                "search_name": previous.get("search_name", ""),
            }
            if not record["country_code"] or not record["company_id"]:
                raise ValueError(
                    f"legacy result {result_id} is missing company attribution"
                )
            if record["status"] == "success" and not record["answer_text"].strip():
                raise ValueError(
                    f"legacy result {result_id} is missing its archived answer"
                )
            records.append(record)
        insert_results(client, records)
        actual = client.execute(
            f"SELECT {','.join(RESULT_COLUMNS)} FROM {RESULT_TABLE} FINAL "
            "WHERE result_id IN %(ids)s",
            {"ids": ids},
        )
        expected = {
            tuple(record[column] for column in RESULT_COLUMNS) for record in records
        }
        if set(actual) != expected:
            raise ValueError(
                "migrated Brave outcomes do not match the Postgres records"
            )
        verified += len(records)
        after = str(rows[-1]["result_id"])
    [(total,)] = client.execute(f"SELECT count() FROM {RESULT_TABLE} FINAL")
    return {"postgres_results_verified": verified, "clickhouse_results": total}
