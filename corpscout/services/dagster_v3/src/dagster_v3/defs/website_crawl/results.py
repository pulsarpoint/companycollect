"""Crawl processing with recoverable submissions and per-type result storage.

A run with ``task_id`` processes a crawl draft (see ``queue_execution``). Without a
task it processes a bounded batch of explicit or due inputs from the request tables,
recovering pending receipts first.
"""

import hashlib
import json
import os
import re
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

import dagster as dg
from corpscout_identity.observations import register_crawl_results
from corpscout_identity.urls import website_reference
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers.requests import Session
from pydantic import ConfigDict, Field, field_validator, model_validator

from dagster_v3.defs.common.encrypted_llm import EncryptedLLMConfig
from dagster_v3.defs.common.llm_control import finish_external_request
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.dispatch import (
    INPUTS_BY_TYPE,
    crawl_payload,
    reject_crawl_credentials,
    send_crawl,
    verify_crawl_llm,
)
from dagster_v3.defs.website_crawl.matching_batches import process_matching_batch
from dagster_v3.defs.website_crawl.outcome_logging import log_crawl_outcomes

RESULTS_BY_TYPE = {
    "full": "corpscout.website_full_crawl_results",
    "jobs": "corpscout.website_jobs_crawl_results",
    "site_info": "corpscout.website_site_info_results",
}
SUBMISSIONS = "corpscout.website_crawl_submissions"
EXECUTION_TAG = "website_crawl/execution"
# Content and skip-policy settings are fixed for an execution, like Brave's query.
# Operational settings (batch sizes, in-flight limit, CAPTCHA budget, timeouts) may change.
FIXED_ON_RESUME = (
    "api",
    "model",
    "llm",
    "max_pages",
    "max_model_calls",
    "page_selection",
    "instructions",
    "crawler_config",
    "force_refresh",
    "full_crawl_all",
    "match_company",
    "company_country",
    "skip_company_matching_if_mapped",
    "refresh_interval_days",
)


class CrawlResultsConfig(dg.Config):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    task_id: str | None = Field(
        default=None,
        description="Crawl draft to process (queue task created by website_crawl_input). Omit to process explicit domains or due inputs.",
    )
    execution_id: str | None = Field(
        default=None,
        description="Original Dagster run ID to resume. Its content settings and freshness cutoff stay fixed.",
    )
    domains: list[str] = Field(default_factory=list, max_length=100)
    batch_id: str | None = Field(
        default=None, description="Reuse this UUID to recover a manual batch."
    )
    batch_size: int = Field(default=25, ge=1, le=100)
    max_batches: int = Field(default=1, ge=1, le=100)
    max_in_flight: int = Field(default=3, ge=1, le=20)
    bucket: int | None = Field(default=None, ge=0, le=255)
    refresh_interval_days: int = Field(default=30, ge=1, le=3650)
    force_refresh: bool = False
    full_crawl_all: bool = Field(
        default=False,
        strict=True,
        description="Override the company-only gate for full crawls, including shops and content sites.",
    )
    match_company: bool | None = Field(
        default=None,
        description="Override the saved request's matching flag. None uses the request table.",
    )
    company_country: Literal["SE"] = "SE"
    skip_company_matching_if_mapped: bool = True
    matching_batch_size: int = Field(default=200, ge=1, le=1000)
    challenge_agent_model: str = Field(
        pattern=r"^(deepseek-flash|z-ai/glm-5[.]3-flash)$"
    )
    challenge_agent_max_runs: int = Field(ge=3, le=1000)
    api: str = Field(pattern=r"^(deepseek|openrouter)$")
    model: str = Field(min_length=1, max_length=200)
    llm: EncryptedLLMConfig | None = Field(
        default=None,
        description="Selected LLM profile with an encrypted API key. Omit only for the crawler's legacy environment configuration.",
    )
    max_pages: int = Field(ge=1, le=500)
    max_model_calls: int = Field(ge=1, le=1000)
    page_selection: Literal["saved", "instructions", "basic_info"]
    instructions: str | None = Field(default=None, max_length=20000)
    crawler_config: dict = Field(
        default_factory=dict,
        description="Additional ResearchConfig settings. Required model/limits are separate fields. No credentials.",
    )

    @model_validator(mode="after")
    def validate_options(self):
        if not self.model.strip():
            raise ValueError("model must not be blank")
        if self.llm is not None:
            if self.llm.model != self.model:
                raise ValueError("llm.model must match model")
            if self.api != (
                "deepseek"
                if self.llm.provider == "deepseek"
                or urlsplit(self.llm.base_url).hostname == "api.deepseek.com"
                else "openrouter"
            ):
                raise ValueError("api must match the selected LLM provider")
        reject_crawl_credentials(self.crawler_config)
        if self.page_selection == "instructions":
            if self.instructions is None or not self.instructions.strip():
                raise ValueError("custom page selection requires instructions")
        elif self.instructions is not None:
            raise ValueError("instructions require page_selection=instructions")
        if self.page_selection == "basic_info" and self.max_pages != 1:
            raise ValueError("basic info uses max_pages=1")
        if {"model", "max_pages", "max_model_calls"}.intersection(self.crawler_config):
            raise ValueError(
                "set required model and limits using their explicit config fields"
            )
        if self.task_id is not None and self.domains:
            raise ValueError("task_id processes the whole draft; omit domains")
        return self

    def with_frozen_llm(self, settings: dict) -> "CrawlResultsConfig":
        """Re-use the original ciphertext so retries send an identical request body."""
        llm = settings.get("llm")
        return self.model_copy(
            update={"llm": EncryptedLLMConfig(**llm) if llm is not None else None}
        )

    wait_timeout_seconds: float = Field(default=1800, gt=0, le=86400)
    poll_interval_seconds: float = Field(default=2, gt=0, le=30)

    @field_validator("batch_id", "task_id", "execution_id")
    @classmethod
    def valid_batch_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("domains")
    @classmethod
    def valid_domains(cls, values: list[str]) -> list[str]:
        if any(
            len(value) > 253 or not re.fullmatch(r"[a-z0-9][a-z0-9.-]*[a-z0-9]", value)
            for value in values
        ):
            raise ValueError("domains must be saved lowercase hostnames")
        return sorted(set(values))


# crawler-service API on the crawler VM, reached by its Tailscale MagicDNS name.
DEFAULT_CRAWLER_API_URL = "http://crawler:8080"


def effective_payload(
    row: dict, crawl_type: str, batch_id: str, config: CrawlResultsConfig
) -> tuple[dict, str]:
    if (crawl_type == "site_info") != (config.page_selection == "basic_info"):
        raise ValueError(
            "basic_info page selection is required only for the basic-info results asset"
        )
    if config.full_crawl_all and crawl_type != "full":
        raise ValueError("full_crawl_all is only supported for full crawls")
    payload = crawl_payload(row, crawl_type, batch_id)
    if crawl_type == "full":
        payload["full_crawl_all"] = config.full_crawl_all
        # Explicit saved pages must not bypass the full-crawl eligibility gate.
        if "pages" in payload:
            payload["site_info"] = True
    for name in ("challenge_agent_model", "challenge_agent_max_runs", "api"):
        payload[name] = getattr(config, name)
    payload["config"] = {
        **payload.get("config", {}),
        **config.crawler_config,
        "model": config.model,
        "max_pages": config.max_pages,
        "max_model_calls": config.max_model_calls,
    }
    if config.llm is not None:
        payload["llm"] = config.llm.model_dump(exclude_none=True)
    if config.page_selection == "instructions":
        payload.pop("crawl", None)
        payload["instructions"] = config.instructions
    matching = (
        bool(row.get("match_company", False))
        if config.match_company is None
        else config.match_company
    )
    if matching:
        if crawl_type not in {"full", "site_info"} or config.llm is None:
            raise ValueError(
                "Company matching requires a basic/full crawl with a selected LLM"
            )
        country = (
            row.get("company_country", "")
            if config.match_company is None
            else config.company_country
        )
        if country != "SE":
            raise ValueError("Company matching currently supports SE only")
        payload.update(
            site_info=True,
            company_lookup={
                "country": country,
                "skip_if_mapped": bool(row.get("skip_company_matching_if_mapped", True))
                if config.match_company is None
                else config.skip_company_matching_if_mapped,
            },
        )
    # Operational settings do not invalidate content. Every content/model setting does.
    semantic = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "request_id",
            "website_id",
            "interactive",
            "challenge_agent_model",
            "challenge_agent_max_runs",
        }
    }
    if config.llm is not None:
        # Credential rotation/re-encryption does not alter requested content.
        semantic["llm"] = config.llm.model_dump(
            exclude={"api_key_encrypted"}, exclude_none=True
        )
    work_key = hashlib.sha256(
        json.dumps([crawl_type, semantic], sort_keys=True).encode()
    ).hexdigest()
    return payload, work_key


def read_rows(client, sql: str, params: dict) -> list[dict]:
    values, columns = client.execute(sql, params, with_column_types=True)
    names = [column[0] for column in columns]
    return [dict(zip(names, value, strict=True)) for value in values]


def fresh_crawl_results(
    client,
    crawl_type: str,
    websites: tuple[str, ...],
    *,
    cutoff: datetime,
    started: datetime,
) -> set[tuple[str, str]]:
    """Only the latest attempt per website can satisfy the frozen freshness window."""
    if not websites:
        return set()
    return set(
        client.execute(
            f"""SELECT website_id, work_key FROM (
                SELECT website_id, work_key, successful{", company_matching_status, request_id, attempt" if crawl_type != "jobs" else ""}
                FROM {RESULTS_BY_TYPE[crawl_type]} FINAL
                WHERE website_id IN %(websites)s
                  AND finished_at >= toDateTime64(%(cutoff)s, 6, 'UTC')
                  AND finished_at <= toDateTime64(%(started)s, 6, 'UTC')
                ORDER BY finished_at DESC, request_id DESC, attempt DESC
                LIMIT 1 BY website_id
            ) WHERE successful
                {"AND (company_matching_status = '' OR (company_matching_status IN ('matched', 'not_found', 'already_mapped') AND (request_id, attempt, website_id) IN (SELECT request_id, attempt, website_id FROM corpscout.website_company_lookup_results)))" if crawl_type != "jobs" else ""}""",
            {
                "websites": websites,
                "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "started": started.strftime("%Y-%m-%d %H:%M:%S.%f"),
            },
        )
    )


def result_record(submission: dict, job: dict, result: dict) -> dict:
    if job["request_id"] != submission["request_id"]:
        raise ValueError("Crawler returned a different request identity")
    request = json.loads(submission["request_json"])
    website_id = website_reference(request["url"], submission["website_id"])
    for supplied in (
        request.get("website_id"),
        job.get("website_id"),
        result.get("website_id"),
    ):
        if supplied is not None:
            website_reference(request["url"], supplied)
    crawl = result.get("crawl", result)
    if not isinstance(crawl, dict):
        raise ValueError("Crawler result has no crawl object")
    receipt = (job.get("s3_event") or {}).get("result")
    info = crawl.get("site_info")
    state = job["state"]
    status = crawl.get("status") or job.get("crawl_status") or ""
    error = job.get("error") or ""
    successful = (
        state == "completed" and status in {"finished", "skip_crawling"} and not error
    )
    if submission["crawl_type"] == "site_info":
        successful = successful and isinstance(info, dict) and bool(info)
    observations = [
        document["input"]["observations"]
        for document in result.get("documents", [])
        if "observations" in document.get("input", {})
    ]
    return {
        "domain": submission["domain"],
        "website_url": request["url"],
        "website_id": website_id,
        "request_id": submission["request_id"],
        "attempt": job["attempt"],
        "input_revision": submission["input_revision"],
        "work_key": submission["work_key"],
        "run_id": submission["run_id"],
        "state": state,
        "crawl_status": status,
        "successful": successful,
        "started_at": datetime.fromisoformat(job["started_at"])
        if job.get("started_at")
        else None,
        "finished_at": datetime.fromisoformat(job["finished_at"]),
        "site_info": json.dumps(info, ensure_ascii=False) if info is not None else None,
        "page_observations": json.dumps(observations, ensure_ascii=False),
        "pages": json.dumps(crawl.get("pages", []), ensure_ascii=False),
        "model_usage": json.dumps(crawl.get("usage", {})),
        "error": error,
        "s3_path": f"{receipt['bucket']}/{receipt['key']}" if receipt else "",
        "s3_state": job["s3_state"],
    }


def resolve_execution(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    crawl_type: str,
) -> dict:
    """Keep one execution's content settings and freshness cutoff fixed across retries."""
    execution_id = config.execution_id or context.run.root_run_id or context.run.run_id
    original = context.instance.get_run_by_id(execution_id)
    if original is None:
        raise ValueError("execution_id must identify the original Dagster run")
    settings = config.model_dump(include=set(FIXED_ON_RESUME))
    if EXECUTION_TAG in original.tags:
        execution = json.loads(original.tags[EXECUTION_TAG])
        if execution["crawl_type"] != crawl_type:
            raise ValueError("execution_id belongs to a different crawl type")
        if execution.get("task_id") is not None:
            raise ValueError(
                "execution_id belongs to a retired crawl task; add its domains to a draft instead"
            )
        # Only ciphertext may differ. Keep the original envelope for every resumed
        # request, while still refusing changes to the provider, endpoint or model.
        saved_llm = execution["settings"].get("llm")
        if settings["llm"] is not None and saved_llm is not None:
            settings["llm"]["api_key_encrypted"] = saved_llm["api_key_encrypted"]
        changed = [
            name
            for name in FIXED_ON_RESUME
            if settings[name]
            != execution["settings"].get(
                name,
                {
                    "full_crawl_all": False,
                    "company_country": "SE",
                    "skip_company_matching_if_mapped": True,
                }.get(name),
            )
        ]
        if changed:
            raise ValueError(
                f"resume must keep {', '.join(changed)} unchanged; start a new execution instead"
            )
        context.instance.add_run_tags(
            context.run.run_id, {EXECUTION_TAG: original.tags[EXECUTION_TAG]}
        )
        return execution
    if config.execution_id is not None:
        raise ValueError("the original run has no saved crawl execution to resume")
    execution = {
        "execution_id": execution_id,
        "crawl_type": crawl_type,
        "started_at": datetime.now(UTC).isoformat(),
        "settings": settings,
    }
    tags = {EXECUTION_TAG: json.dumps(execution, sort_keys=True)}
    context.instance.add_run_tags(execution_id, tags)
    if execution_id != context.run.run_id:
        context.instance.add_run_tags(context.run.run_id, tags)
    return execution


def process_crawls(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    crawl_type: Literal["full", "jobs", "site_info"],
) -> dg.MaterializeResult:
    if (crawl_type == "site_info") != (config.page_selection == "basic_info"):
        raise ValueError(
            "basic_info page selection is required only for the basic-info results asset"
        )
    if config.task_id is not None:
        with processing.get_store() as store:
            task = store.task(config.task_id)
        if task is None or task["queue_scope"] is None:
            raise ValueError(
                f"unknown crawl task for {crawl_type}: add domains with website_crawl_input first"
            )
        from dagster_v3.defs.website_crawl.queue_execution import process_crawl_draft

        return process_crawl_draft(
            context, config, clickhouse, processing, crawl_type, config.task_id
        )
    table = RESULTS_BY_TYPE[crawl_type]
    input_table = INPUTS_BY_TYPE[crawl_type] + "_current"
    execution = resolve_execution(context, config, crawl_type)
    config = config.with_frozen_llm(execution["settings"])
    batch_id = config.batch_id or execution["execution_id"]
    # Freshness is judged against the execution's start, so a resume sees the same cutoff.
    cutoff = datetime.fromisoformat(execution["started_at"]) - timedelta(
        days=config.refresh_interval_days
    )
    metadata = {
        "input_table": input_table,
        "result_table": table,
        "execution_id": execution["execution_id"],
        "batch_id": batch_id,
        "completed": 0,
        "unsuccessful": 0,
        "fresh_skipped": 0,
        "recovered": 0,
    }
    url = os.environ.get("CRAWLER_API_URL", "").strip() or DEFAULT_CRAWLER_API_URL
    token = os.environ.get("CRAWLER_API_TOKEN", "").strip()
    if not token:
        raise ValueError("Configure CRAWLER_API_TOKEN on the Dagster host")
    with (
        processing.get_store() as store,
        clickhouse.get_connection() as client,
        Session(raise_for_status=False) as http,
    ):
        # The direct PostgreSQL session fences all runs of this crawl type, including
        # retries. CH stores immutable receipts/results, never mutable ownership.
        lock_name = "website_crawl:" + crawl_type
        with store.transaction() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS acquired",
                (lock_name,),
            )
            if not cursor.fetchone()["acquired"]:
                raise ValueError(
                    "A processor for this crawl type is already running; retry after it finishes"
                )
        try:
            for relation in (
                table,
                table + "_latest_success",
                input_table,
                SUBMISSIONS,
            ):
                if client.execute(f"EXISTS TABLE {relation}") != [(1,)]:
                    raise ValueError(
                        f"Apply ClickHouse migration 000430 before processing: {relation}"
                    )
            http.headers["Authorization"] = f"Bearer {token}"
            verified_llms: set[str] = set()
            if config.llm is not None:
                verify_crawl_llm(http, url, config.llm.model_dump(exclude_none=True))
                verified_llms.add(
                    json.dumps(config.llm.model_dump(exclude_none=True), sort_keys=True)
                )
            params = {
                "type": crawl_type,
                "limit": config.batch_size * config.max_batches,
            }
            selected = ""
            if config.domains:
                params["domains"] = tuple(config.domains)
                selected = " AND domain IN %(domains)s"
                present = client.execute(
                    f"SELECT domain FROM {input_table} WHERE domain IN %(domains)s",
                    params,
                )
                if len(present) != len(config.domains):
                    raise ValueError(
                        "Some selected inputs no longer exist; refresh the list"
                    )
            if config.bucket is not None:
                params["bucket"] = config.bucket
                selected += " AND toUInt16(cityHash64(domain) % 256) = %(bucket)s"
            pending_sql = f"""SELECT * FROM {SUBMISSIONS} FINAL
                WHERE crawl_type=%(type)s AND request_id NOT IN (SELECT request_id FROM {table} FINAL
                {"WHERE company_matching_status = '' OR (request_id, attempt, website_id) IN (SELECT request_id, attempt, website_id FROM corpscout.website_company_lookup_results)" if crawl_type != "jobs" else ""})"""
            pending = read_rows(
                client,
                pending_sql
                + selected
                + " ORDER BY submitted_at, domain LIMIT %(limit)s",
                params,
            )
            for item in pending:
                pending_llm = json.loads(item["request_json"]).get("llm")
                if pending_llm is None:
                    continue
                identity = json.dumps(pending_llm, sort_keys=True)
                if identity not in verified_llms:
                    # Recovery can adopt receipts from an earlier execution with
                    # another model/key. Verify the envelope that will be re-sent.
                    verify_crawl_llm(http, url, pending_llm)
                    verified_llms.add(identity)
            metadata["recovered"] = len(pending)
            # Pending receipts are resumed before admitting new work. Their payload is
            # immutable even if the input row or this run's overrides have changed.
            processed = 0

            def collect(submissions: list[dict]) -> None:
                nonlocal processed
                matching = [
                    item
                    for item in submissions
                    if json.loads(item["request_json"]).get("company_lookup")
                ]
                submissions = [item for item in submissions if item not in matching]
                for start in range(0, len(matching), config.matching_batch_size):
                    group = matching[start : start + config.matching_batch_size]
                    processed += process_matching_batch(
                        context, client, http, url, group
                    )
                    [(ok, failed)] = client.execute(
                        f"SELECT countIf(successful AND company_matching_status NOT IN ('failed', 'cancelled')), countIf(NOT successful OR company_matching_status IN ('failed', 'cancelled')) FROM {table} FINAL WHERE request_id IN %(ids)s",
                        {"ids": tuple(item["request_id"] for item in group)},
                    )
                    metadata["completed"] += ok
                    metadata["unsuccessful"] += failed
                for start in range(0, len(submissions), config.max_in_flight):
                    group = submissions[start : start + config.max_in_flight]
                    for item in group:
                        send_crawl(
                            http, url, json.loads(item["request_json"]), validate=False
                        )
                    waiting = {item["request_id"]: item for item in group}
                    deadline = monotonic() + config.wait_timeout_seconds
                    while waiting:
                        records = []
                        for request_id, item in list(waiting.items()):
                            response = http.get(
                                f"{url.rstrip('/')}/v1/crawls/{request_id}",
                                timeout=(10, 30),
                                allow_redirects=False,
                            )
                            response.raise_for_status()
                            job = response.json()
                            if job.get("request_id") != request_id:
                                raise ValueError(
                                    "Crawler returned a different request identity"
                                )
                            if job["state"] in {
                                "completed",
                                "failed",
                                "cancelled",
                            } and json.loads(item["request_json"]).get("llm", {}).get(
                                "profile_id"
                            ):
                                finish_external_request(
                                    "crawler", request_id, job["state"]
                                )
                            if (
                                job["state"] not in {"completed", "failed", "cancelled"}
                                or job["s3_state"] == "pending"
                            ):
                                continue
                            response = http.get(
                                f"{url.rstrip('/')}/v1/crawls/{request_id}/result",
                                timeout=(10, 60),
                                allow_redirects=False,
                            )
                            response.raise_for_status()
                            record = result_record(item, job, response.json())
                            records.append(record)
                        if records:
                            register_crawl_results(
                                client,
                                records,
                                source=table.split(".")[-1],
                                run_id=context.run.run_id,
                            )
                            client.execute(
                                f"INSERT INTO {table} ({', '.join(records[0])}) VALUES",
                                records,
                                settings={
                                    "async_insert": 1,
                                    "wait_for_async_insert": 1,
                                },
                            )
                            for record in records:
                                metadata["completed"] += int(record["successful"])
                                metadata["unsuccessful"] += int(
                                    not record["successful"]
                                )
                                processed += 1
                                del waiting[record["request_id"]]
                            log_crawl_outcomes(context, records, crawl_type)
                            context.log.info(
                                "Stored %s crawl outcomes for type=%s",
                                len(records),
                                crawl_type,
                            )
                        if waiting:
                            if monotonic() >= deadline:
                                raise TimeoutError(
                                    "Crawls are still pending; materialize this results asset again to recover existing requests"
                                )
                            sleep(config.poll_interval_seconds)

            recovered_websites = {item["website_id"] for item in pending}
            collect(pending)
            remaining = params["limit"] - processed
            cursor_filter = ""
            while remaining > 0:
                rows = read_rows(
                    client,
                    f"""SELECT * FROM {input_table}
                    WHERE enabled {selected} {cursor_filter}
                    AND website_id NOT IN (SELECT website_id FROM ({pending_sql}))
                    ORDER BY priority DESC, domain ASC, website_id ASC LIMIT 500""",
                    params,
                )
                if not rows:
                    break
                domains = tuple(row["domain"] for row in rows)
                fresh = fresh_crawl_results(
                    client,
                    crawl_type,
                    tuple(row["website_id"] for row in rows),
                    cutoff=cutoff,
                    started=datetime.fromisoformat(execution["started_at"]),
                )
                # Do not repeat a domain in a retried/manual batch, including failed results.
                existing = set(
                    row[0]
                    for row in client.execute(
                        f"SELECT request_id FROM {SUBMISSIONS} FINAL WHERE crawl_type=%(type)s AND domain IN %(domains)s",
                        {"type": crawl_type, "domains": domains},
                    )
                )
                admitted = []
                for row in rows:
                    last = row
                    if row["website_id"] in recovered_websites:
                        continue
                    payload, key = effective_payload(row, crawl_type, batch_id, config)
                    if payload["request_id"] in existing:
                        continue
                    if not config.force_refresh and (row["website_id"], key) in fresh:
                        metadata["fresh_skipped"] += 1
                        continue
                    admitted.append(
                        {
                            "crawl_type": crawl_type,
                            "domain": row["domain"],
                            "website_id": row["website_id"],
                            "request_id": payload["request_id"],
                            "input_revision": row["revision"],
                            "work_key": key,
                            "run_id": context.run.run_id,
                            "request_json": json.dumps(payload, sort_keys=True),
                        }
                    )
                    if len(admitted) == min(config.batch_size, remaining):
                        break
                if admitted:
                    for item in admitted:
                        send_crawl(
                            http, url, json.loads(item["request_json"]), validate=True
                        )
                    client.execute(
                        f"INSERT INTO {SUBMISSIONS} ({', '.join(admitted[0])}) VALUES",
                        admitted,
                        settings={"async_insert": 0},
                    )
                    collect(admitted)
                    remaining -= len(admitted)
                # Resume after the last inspected input, including trailing skips.
                # Advancing only past the last admission counts those skips again.
                params.update(
                    after_priority=last["priority"],
                    after_domain=last["domain"],
                    after_website=last["website_id"],
                )
                cursor_filter = " AND (priority < %(after_priority)s OR (priority = %(after_priority)s AND (domain,website_id) > (%(after_domain)s,%(after_website)s)))"
        finally:
            with store.transaction() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (lock_name,)
                )
    return dg.MaterializeResult(metadata=metadata)
