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
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers.requests import Session
from pydantic import ConfigDict, Field, field_validator, model_validator

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.dispatch import (
    INPUTS_BY_TYPE,
    crawl_payload,
    reject_crawl_credentials,
    send_crawl,
    verify_crawl_llm,
)

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
    "refresh_interval_days",
)


class CrawlLLMConfig(dg.Config):
    """Opaque credentials supplied by Backoffice; only the crawler can decrypt them."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    provider: str = Field(min_length=1, max_length=100)
    base_url: str = Field(min_length=1, max_length=2048)
    model: str = Field(min_length=1, max_length=200)
    api_key_encrypted: str = Field(
        pattern=r"^v1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{23,}$",
        max_length=16384,
        repr=False,
    )

    @model_validator(mode="after")
    def validate_profile(self):
        for value in (self.provider, self.model, self.base_url):
            if value != value.strip() or any(
                ord(char) < 32 or ord(char) == 127 for char in value
            ):
                raise ValueError(
                    "LLM profile fields cannot contain control characters or whitespace padding"
                )
        try:
            endpoint = urlsplit(self.base_url)
            endpoint.port  # urlsplit defers invalid-port validation until access.
        except ValueError:
            raise ValueError("LLM base_url must have a valid URL and port") from None
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or any(char.isspace() for char in self.base_url)
            or endpoint.username is not None
            or endpoint.password is not None
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError(
                "LLM base_url must be an HTTP(S) endpoint without credentials, query or fragment"
            )
        return self


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
    challenge_agent_model: str = Field(
        pattern=r"^(deepseek-flash|z-ai/glm-5[.]3-flash)$"
    )
    challenge_agent_max_runs: int = Field(ge=3, le=1000)
    api: str = Field(pattern=r"^(deepseek|openrouter)$")
    model: str = Field(min_length=1, max_length=200)
    llm: CrawlLLMConfig | None = Field(
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
            update={"llm": CrawlLLMConfig(**llm) if llm is not None else None}
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
    payload = crawl_payload(row, crawl_type, batch_id)
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
        payload["llm"] = config.llm.model_dump()
    if config.page_selection == "instructions":
        payload.pop("crawl", None)
        payload["instructions"] = config.instructions
    # Operational settings do not invalidate content. Every content/model setting does.
    semantic = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "request_id",
            "interactive",
            "challenge_agent_model",
            "challenge_agent_max_runs",
        }
    }
    if config.llm is not None:
        # Credential rotation/re-encryption does not alter requested content.
        semantic["llm"] = config.llm.model_dump(exclude={"api_key_encrypted"})
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
    domains: tuple[str, ...],
    *,
    cutoff: datetime,
    started: datetime,
) -> set[tuple[str, str]]:
    """Only the latest attempt per domain can satisfy the frozen freshness window."""
    if not domains:
        return set()
    return set(
        client.execute(
            f"""SELECT domain, work_key FROM (
                SELECT domain, work_key, successful
                FROM {RESULTS_BY_TYPE[crawl_type]} FINAL
                WHERE domain IN %(domains)s
                  AND finished_at >= toDateTime64(%(cutoff)s, 6, 'UTC')
                  AND finished_at <= toDateTime64(%(started)s, 6, 'UTC')
                ORDER BY finished_at DESC, request_id DESC, attempt DESC
                LIMIT 1 BY domain
            ) WHERE successful""",
            {
                "domains": domains,
                "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "started": started.strftime("%Y-%m-%d %H:%M:%S.%f"),
            },
        )
    )


def result_record(submission: dict, job: dict, result: dict) -> dict:
    if job["request_id"] != submission["request_id"]:
        raise ValueError("Crawler returned a different request identity")
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
        "website_url": json.loads(submission["request_json"])["url"],
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
            if settings[name] != execution["settings"].get(name)
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
                verify_crawl_llm(http, url, config.llm.model_dump())
                verified_llms.add(json.dumps(config.llm.model_dump(), sort_keys=True))
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
                WHERE crawl_type=%(type)s AND request_id NOT IN (SELECT request_id FROM {table} FINAL)"""
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
                for start in range(0, len(submissions), config.max_in_flight):
                    group = submissions[start : start + config.max_in_flight]
                    for item in group:
                        send_crawl(
                            http, url, json.loads(item["request_json"]), validate=False
                        )
                    waiting = {item["request_id"]: item for item in group}
                    deadline = monotonic() + config.wait_timeout_seconds
                    while waiting:
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
                            # Async inserts are acknowledged only after flush. An ambiguous
                            # retry replaces the same request/attempt instead of double counting.
                            client.execute(
                                f"INSERT INTO {table} ({', '.join(record)}) VALUES",
                                [record],
                                settings={
                                    "async_insert": 1,
                                    "wait_for_async_insert": 1,
                                },
                            )
                            metadata["completed"] += int(record["successful"])
                            metadata["unsuccessful"] += int(not record["successful"])
                            processed += 1
                            context.log.info(
                                "Stored crawl domain=%s type=%s request=%s state=%s successful=%s",
                                item["domain"],
                                crawl_type,
                                request_id,
                                job["state"],
                                record["successful"],
                            )
                            del waiting[request_id]
                        if waiting:
                            if monotonic() >= deadline:
                                raise TimeoutError(
                                    "Crawls are still pending; materialize this results asset again to recover existing requests"
                                )
                            sleep(config.poll_interval_seconds)

            recovered_domains = {item["domain"] for item in pending}
            collect(pending)
            remaining = params["limit"] - processed
            cursor_filter = ""
            while remaining > 0:
                rows = read_rows(
                    client,
                    f"""SELECT * FROM {input_table}
                    WHERE enabled {selected} {cursor_filter}
                    AND domain NOT IN (SELECT domain FROM ({pending_sql}))
                    ORDER BY priority DESC, domain ASC LIMIT 500""",
                    params,
                )
                if not rows:
                    break
                domains = tuple(row["domain"] for row in rows)
                fresh = fresh_crawl_results(
                    client,
                    crawl_type,
                    domains,
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
                    if row["domain"] in recovered_domains:
                        continue
                    payload, key = effective_payload(row, crawl_type, batch_id, config)
                    if payload["request_id"] in existing:
                        continue
                    if not config.force_refresh and (row["domain"], key) in fresh:
                        metadata["fresh_skipped"] += 1
                        continue
                    admitted.append(
                        {
                            "crawl_type": crawl_type,
                            "domain": row["domain"],
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
                    after_priority=last["priority"], after_domain=last["domain"]
                )
                cursor_filter = " AND (priority < %(after_priority)s OR (priority = %(after_priority)s AND domain > %(after_domain)s))"
        finally:
            with store.transaction() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (lock_name,)
                )
    return dg.MaterializeResult(metadata=metadata)
