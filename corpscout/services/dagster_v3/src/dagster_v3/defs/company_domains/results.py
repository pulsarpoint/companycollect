"""Completed Brave attempts, freshness selection, and successful-answer projection."""

import json
from datetime import datetime, timedelta
from uuid import UUID

from clickhouse_driver import Client

from dagster_v3.defs.company_domains.browser import BraveSearchResult

RESULT_TABLE = "corpscout.company_brave_search_results"
LATEST_VIEW = "corpscout.company_brave_search_results_latest"
RESULT_COLUMNS = (
    "country_code",
    "company_id",
    "company_name",
    "query_type",
    "query",
    "result_id",
    "status",
    "answer_text",
    "completed_at",
    "error_type",
    "error_stage",
    "route",
    "source_url",
    "elapsed_ms",
    "answer_timeout_ms",
    "challenge_runs_json",
    "task_id",
    "execution_id",
    "source_run_id",
    "input_id",
    "attempt",
    "processor_version",
)
INSERT_SETTINGS = {"async_insert": 1, "wait_for_async_insert": 1}


def search_is_due(
    completed_at: datetime | None,
    *,
    force: bool,
    rescan_old: bool,
    started_at: datetime,
) -> bool:
    return (
        force
        or completed_at is None
        or (rescan_old and completed_at < started_at - timedelta(days=30))
    )


def page_outcomes(
    client: Client, rows: list[dict], *, query_type: str, execution_id: str
) -> tuple[dict[tuple[str, str], datetime], set[str]]:
    keys = tuple((row["country_code"], row["company_id"]) for row in rows)
    latest = client.execute(
        f"SELECT country_code,company_id,completed_at FROM {LATEST_VIEW} "
        "WHERE query_type=%(query_type)s AND (country_code,company_id) IN %(keys)s",
        {"query_type": query_type, "keys": keys},
    )
    # A later run's outcome must not hide completion in the execution being resumed.
    completed = client.execute(
        f"SELECT DISTINCT input_id FROM {RESULT_TABLE} "
        "WHERE execution_id=%(execution_id)s AND input_id IN %(input_ids)s",
        {
            "execution_id": execution_id,
            "input_ids": tuple(row["input_id"] for row in rows),
        },
    )
    return {(country, company): timestamp for country, company, timestamp in latest}, {
        row[0] for row in completed
    }


def result_record(
    result: BraveSearchResult,
    values: dict,
    *,
    task_id: str,
    execution_id: str,
    query_type: str,
    source_run_id: str,
    processor_version: str,
) -> dict:
    if result.status == "success" and not result.answer.strip():
        raise ValueError("cannot save an empty successful Brave response")
    return {
        "country_code": values["country_code"],
        "company_id": values["company_id"],
        "company_name": values["company_name"],
        "query_type": query_type,
        "query": result.query,
        "result_id": UUID(result.company.request_id),
        "status": result.status,
        "answer_text": result.answer,
        "completed_at": result.fetched_at,
        "error_type": result.error_type,
        "error_stage": result.error_stage,
        "route": result.route,
        "source_url": result.source_url,
        "elapsed_ms": result.elapsed_ms,
        "answer_timeout_ms": result.company.answer_timeout_ms,
        "challenge_runs_json": json.dumps(result.challenge_runs, ensure_ascii=False),
        "task_id": UUID(task_id),
        "execution_id": UUID(execution_id),
        "source_run_id": source_run_id,
        "input_id": values["input_id"],
        "attempt": 1,
        "processor_version": processor_version,
    }


def insert_results(client: Client, records: list[dict]) -> None:
    client.execute(
        f"INSERT INTO {RESULT_TABLE} ({','.join(RESULT_COLUMNS)}) VALUES",
        [tuple(record[column] for column in RESULT_COLUMNS) for record in records],
        settings=INSERT_SETTINGS,
    )


def repair_current_answers(client: Client, *, execution_id: str) -> None:
    """Repair an interrupted materialized-view write without another Brave request."""
    client.execute(
        "INSERT INTO corpscout.se_company_brave_search_results_latest_success "
        "(result_id,task_id,input_id,work_key,attempt,status,export_batch_id,country_code,"
        "company_id,company_name,query,query_type,processor_version,answer_text,route,"
        "source_url,error_type,source_run_id,completed_at,archive_path) "
        f"SELECT toString(result_id),toString(task_id),input_id,'',attempt,toString(status),'',"
        "country_code,company_id,company_name,query,query_type,processor_version,answer_text,"
        "route,source_url,error_type,source_run_id,completed_at,'' "
        f"FROM {RESULT_TABLE} FINAL WHERE execution_id=%(execution_id)s "
        "AND country_code='SE' AND status='success' "
        "AND toString(result_id) NOT IN (SELECT result_id FROM corpscout.se_company_brave_search_results_latest_success FINAL)",
        {"execution_id": execution_id},
    )
