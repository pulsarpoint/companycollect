"""Publish completed local batches to the migration-owned Brave result table."""

import json
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx
from pydantic import Field, field_validator

from browser_service.brave_models import BraveBatchItem, BraveBatchRequest
from browser_service.capture import StrictModel

RESULT_TABLE = "corpscout.company_brave_search_results"
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
    "search_id",
    "search_revision",
    "search_name",
)


class BraveClickHouseSettings(StrictModel):
    url: str
    username: str
    password: str = Field(repr=False)

    @field_validator("url")
    @classmethod
    def endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "ClickHouse requires an HTTP URL without credentials, query or fragment"
            )
        return value


def result_record(batch: BraveBatchRequest, item: BraveBatchItem, result: dict) -> dict:
    if result["query"] != item.query or result["status"] not in {
        "success",
        "error",
        "blocked",
    }:
        raise ValueError("Browser returned a different query or invalid outcome")
    if result["status"] == "success" and not result["answer"].strip():
        raise ValueError("Browser returned an empty successful answer")
    completed = datetime.fromisoformat(result["fetched_at"]).astimezone(UTC)
    return {
        "country_code": item.country_code,
        "company_id": item.company_id,
        "company_name": item.company_name,
        "query_type": batch.query_type,
        "query": item.query,
        "result_id": str(item.result_id),
        "status": "success" if result["status"] == "success" else "error",
        "answer_text": result["answer"] if result["status"] == "success" else "",
        "completed_at": completed.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "error_type": result["error_type"],
        "error_stage": result["error_stage"],
        "route": result["route"],
        "source_url": result["source_url"],
        "elapsed_ms": result["elapsed_ms"],
        "answer_timeout_ms": round(batch.options.answer_timeout_seconds * 1000),
        "challenge_runs_json": json.dumps(
            result.get("challenge_runs", []), ensure_ascii=False
        ),
        "task_id": str(batch.task_id),
        "execution_id": str(batch.execution_id),
        "source_run_id": str(batch.source_run_id),
        "input_id": item.input_id,
        "attempt": 1,
        "processor_version": batch.processor_version,
        "search_id": batch.search_id,
        "search_revision": batch.search_revision,
        "search_name": batch.search_name,
    }


class BraveBatchPublisher:
    def __init__(self, settings: BraveClickHouseSettings):
        self.settings = settings

    async def publish(self, batch: BraveBatchRequest, records: list[dict]) -> None:
        if len(records) != len(batch.items):
            raise ValueError(
                "All batch results must be durable locally before publication"
            )
        async with httpx.AsyncClient(
            base_url=self.settings.url,
            auth=(self.settings.username, self.settings.password),
            timeout=30,
            follow_redirects=False,
        ) as http:
            params = {
                "param_task": str(batch.task_id),
                "param_execution": str(batch.execution_id),
                "param_ids": "["
                + ",".join("'" + str(item.result_id) + "'" for item in batch.items)
                + "]",
                "wait_end_of_query": "1",
                "max_execution_time": "20",
            }
            where = "task_id={task:UUID} AND execution_id={execution:UUID} AND result_id IN {ids:Array(UUID)}"
            query = f"SELECT toString(result_id) FROM {RESULT_TABLE} FINAL WHERE {where} FORMAT TabSeparated"
            response = await http.post("", params=params, content=query)
            response.raise_for_status()
            saved = set(response.text.splitlines())
            missing = [record for record in records if record["result_id"] not in saved]
            if missing:
                content = f"INSERT INTO {RESULT_TABLE} ({','.join(RESULT_COLUMNS)}) FORMAT JSONEachRow\n"
                content += "\n".join(
                    json.dumps(record, ensure_ascii=False) for record in missing
                )
                response = await http.post(
                    "",
                    params={
                        "wait_end_of_query": "1",
                        "async_insert": "0",
                        "date_time_input_format": "best_effort",
                    },
                    content=content.encode(),
                )
                response.raise_for_status()
            # Reconcile a partial insert/MV failure before acknowledging completion.
            projection = (
                """INSERT INTO corpscout.se_company_brave_search_results_latest_success
                (result_id,task_id,input_id,work_key,attempt,status,export_batch_id,country_code,
                 company_id,company_name,query,query_type,processor_version,answer_text,route,
                 source_url,error_type,source_run_id,completed_at,archive_path)
                SELECT toString(result_id),toString(task_id),input_id,'',attempt,toString(status),'',
                    country_code,company_id,company_name,query,query_type,processor_version,answer_text,
                    route,source_url,error_type,source_run_id,completed_at,''
                FROM corpscout.company_brave_search_results FINAL WHERE """
                + where
                + """
                AND country_code='SE' AND status='success' AND toString(result_id) NOT IN (
                    SELECT result_id FROM corpscout.se_company_brave_search_results_latest_success FINAL
                    WHERE result_id IN arrayMap(id -> toString(id),{ids:Array(UUID)}))"""
            )
            response = await http.post("", params=params, content=projection)
            response.raise_for_status()
            response = await http.post("", params=params, content=query)
            response.raise_for_status()
            if set(response.text.splitlines()) != {
                str(item.result_id) for item in batch.items
            }:
                raise RuntimeError(
                    "ClickHouse has not confirmed every result in this batch"
                )
