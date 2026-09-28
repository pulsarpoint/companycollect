"""Local durable crawl jobs submitted through REST or the manual console."""

import asyncio
import fcntl
import hashlib
import json
import logging
import traceback
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, TextIO
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from corpscout_identity.urls import website_reference
from pydantic import Field, field_serializer, field_validator, model_validator

from crawler_service.brave_browser import BraveSearch
from crawler_service.browser_client import BrowserLeaseClient
from crawler_service.company_lookup import (
    CompanyLookupBatchRequest,
    CompanyLookupOptions,
    basic_info_result,
    find_company,
)
from crawler_service.company_lookup_results import publish as publish_lookup_results
from crawler_service.company_lookup_store import LookupStore
from crawler_service.crawl import crawl_company
from crawler_service.crawl_history import CrawlHistory
from crawler_service.debug_trace import (
    CURRENT_TRACE,
    SECRET_FIELD,
    CrawlTrace,
    trace_event,
    trace_http_hooks,
)
from crawler_service.discovery import crawlable_url, normalize_url
from crawler_service.human_control import HumanSession
from crawler_service.identity_registration import register_results
from crawler_service.jev import JevClient
from crawler_service.llm import ModelClient
from crawler_service.llm_profile import EncryptedLLMProfile, LLMProfileError
from crawler_service.models import ResearchConfig, StrictModel
from crawler_service.profiles import site_information
from crawler_service.storage import utc_now, write_json

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from crawler_service.service_results import S3Results

TERMINAL_STATES = {"completed", "failed", "cancelled"}
type AgentRunBudget = Annotated[int, Field(ge=3, le=1000, strict=True)]
type AgentModel = Literal["deepseek-flash", "z-ai/glm-5.3-flash"]


class CrawlRequest(StrictModel):
    request_id: str = Field(
        default_factory=lambda: uuid4().hex,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$",
    )
    url: str = Field(max_length=8192)
    website_id: str = Field(default="", pattern=r"^(?:[a-f0-9]{64})?$")
    pages: list[str] | None = Field(default=None, min_length=1, max_length=1000)
    instructions: str | None = Field(default=None, max_length=20000)
    site_info: bool = False
    full_crawl_all: bool = Field(default=False, strict=True)
    save_artifacts: bool = True
    debug: bool = Field(default=False, strict=True)
    crawl: bool | Literal["full"] | None = None
    api: Literal["deepseek", "openrouter"] = "deepseek"
    config: ResearchConfig | None = None
    llm: EncryptedLLMProfile | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    decision_llm: EncryptedLLMProfile | None = Field(default=None, exclude_if=lambda value: value is None)
    decision_tasks: list[Literal["site_eligibility", "link_selection", "company_match"]] = Field(default_factory=list, exclude_if=lambda value: not value)
    company_lookup: CompanyLookupOptions | None = Field(default=None, exclude_if=lambda value: value is None)
    challenge_agent_max_runs: AgentRunBudget | None = None
    challenge_agent_model: AgentModel = "deepseek-flash"
    interactive: bool = False
    session_id: str | None = Field(default=None, pattern=r"^[a-f0-9]{32}$")

    @field_serializer("config")
    def configuration_overrides(self, value: ResearchConfig | None) -> dict | None:
        # Preserve which fields the caller supplied; the crawler selects API defaults.
        return value.model_dump(exclude_unset=True) if value is not None else None

    @field_validator("url")
    @classmethod
    def website_url(cls, value: str) -> str:
        return normalize_url(value)

    @field_validator("instructions")
    @classmethod
    def nonempty_instructions(cls, value: str | None) -> str | None:
        if value is not None:
            value = value.strip()
            if not value:
                raise ValueError("Selection instructions must not be empty")
        return value

    @model_validator(mode="after")
    def crawl_options(self):
        self.website_id = website_reference(self.url, self.website_id or None)
        if self.company_lookup is not None:
            if self.llm is None:
                raise ValueError("Company matching requires a processing model")
            self.site_info = True
            self.debug = True
        elif "company_match" in self.decision_tasks:
            raise ValueError("Company candidate ranking requires company lookup mode")
        if self.llm is not None and self.llm.is_decision_model:
            raise ValueError("Jev is a typed decision model; choose a text-generation model for this crawl")
        if bool(self.decision_tasks) != (self.decision_llm is not None):
            raise ValueError("Select a Jev model and at least one decision step together")
        if len(set(self.decision_tasks)) != len(self.decision_tasks):
            raise ValueError("Decision steps must not be repeated")
        if self.decision_llm is not None:
            if not self.decision_llm.is_decision_model or self.decision_llm.base_url.rstrip("/") != "https://openrouter.ai/api/v1" or self.decision_llm.reasoning_effort is not None:
                raise ValueError("Decision steps require a Jev model on OpenRouter without reasoning effort")
            if self.llm is None:
                raise ValueError("Select a processing LLM alongside Jev")
            if "site_eligibility" in self.decision_tasks and self.pages is not None and not self.site_info:
                raise ValueError("Site eligibility requires first-page classification")
            if "link_selection" in self.decision_tasks and (self.crawl is False or self.site_info and self.crawl is None and self.pages is None and self.instructions is None or self.pages is not None and self.instructions is None):
                raise ValueError("Link selection is not used for this crawl mode")
        if self.full_crawl_all and (self.crawl is False or (self.site_info and self.crawl is None and self.pages is None and self.instructions is None)):
            raise ValueError("full_crawl_all requires deeper collection, not site information alone")
        if self.crawl == "full" and (
            self.pages is not None or self.instructions is not None
        ):
            raise ValueError(
                'crawl="full" cannot be combined with pages or instructions; omit crawl for targeted collection'
            )
        if self.crawl is False and (
            not self.site_info
            or self.pages is not None
            or self.instructions is not None
        ):
            raise ValueError("crawl=false requires site_info alone")
        if self.pages is not None:
            self.pages = list(
                dict.fromkeys(normalize_url(p, self.url) for p in self.pages)
            )
            if any(not crawlable_url(p) for p in self.pages):
                raise ValueError("pages must contain crawlable HTML URLs")
            limits = self.config if self.config is not None else ResearchConfig()
            required = set(self.pages)
            if self.site_info:
                required.add(self.url)
            if self.instructions is None and len(required) > limits.max_pages:
                raise ValueError("max_pages is smaller than the requested page count")
            if (
                self.instructions is not None
                and len(self.pages) > limits.max_candidates
            ):
                raise ValueError(
                    "max_candidates is smaller than the supplied page list"
                )
        return self


class CrawlBatchItem(StrictModel):
    request: CrawlRequest
    crawl_type: Literal["site_info", "full"]
    input_revision: int = Field(ge=1)
    work_key: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def matching_crawl(self):
        if self.request.company_lookup is None or self.request.interactive:
            raise ValueError("Durable batches require unattended company matching crawls")
        if (self.crawl_type == "site_info") != (self.request.crawl is False):
            raise ValueError("Crawl type must match the requested collection mode")
        return self


class CrawlBatchRequest(StrictModel):
    batch_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    input_id: str = Field(default="", max_length=128)
    run_id: str = Field(default="", max_length=128)
    entries: list[CrawlBatchItem] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_requests(self):
        ids = [entry.request.request_id for entry in self.entries]
        if len(set(ids)) != len(ids):
            raise ValueError("Batch request IDs must be unique")
        return self


class CrawlJob(StrictModel):
    schema_version: Literal["company-crawl-job/1.0"] = "company-crawl-job/1.0"
    request_id: str
    purpose: Literal["crawl", "company_lookup"] = "crawl"
    lookup_result_version: int = 0
    state: Literal[
        "queued",
        "running",
        "blocked",
        "captcha",
        "awaiting_human",
        "completed",
        "failed",
        "cancelled",
    ]
    # "jetstream" stays valid only so history from the retired NATS input still loads.
    source: Literal["rest", "jetstream", "manual"]
    submitted_at: str
    url: str = ""
    domain: str = ""
    website_id: str = ""  # Historical job metadata can precede central registration.
    updated_at: str = Field(default_factory=utc_now)
    current_url: str | None = None
    reason: str | None = None
    blocked_reason: str | None = None
    assistance_deadline: str | None = None
    browser_available: bool = False
    verification_available: bool = False
    browser_session_id: str | None = None
    browser_lease_id: str | None = None
    browser_execution_id: str | None = None
    challenge_agent_running: bool = False
    challenge_agent_result: dict | None = None
    challenge_agent_results: list[dict] = Field(default_factory=list)
    challenge_agent_max_runs: int = 0
    challenge_agent_model: str = "deepseek-flash"
    challenge_agent_budget_exhausted: bool = False
    interactive: bool | None = None
    collected_pages: int = 0
    debug_enabled: bool = False
    retry_of: str | None = None
    retry_of_attempt: int | None = None
    s3_state: Literal["not_configured", "pending", "uploaded"] = "not_configured"
    s3_event: dict | None = None
    s3_error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    attempt: int = 0
    crawl_status: str | None = None
    result_file: str | None = None
    error: str | None = None


class RequestConflict(ValueError):
    pass


class ServiceUnavailable(RuntimeError):
    pass


class CrawlService:
    """Own one output directory, bounded workers, idempotency and crash recovery.

    A filesystem lock intentionally restricts each local store to one process.
    Both network inputs may run in that process. Incomplete attempts are retained
    when a restart needs to repeat a crawl; completed results are never repeated.
    """

    def __init__(
        self,
        output_dir: Path,
        environment: dict[str, str],
        *,
        concurrency: int,
        max_pending: int,
    ):
        if concurrency < 1 or max_pending < concurrency:
            raise ValueError("Require 1 <= concurrency <= max_pending")
        self.root = output_dir.resolve()
        self.environment = environment
        self.concurrency = concurrency
        self.max_pending = max_pending
        self.jobs: dict[str, CrawlJob] = {}
        self.finished: dict[str, asyncio.Event] = {}
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.manual_queue: asyncio.Queue[str] = asyncio.Queue()
        self.lookup_queue: asyncio.Queue[str] = asyncio.Queue()
        self.lookup_store: LookupStore | None = None
        self.lookup_dispatch_task: asyncio.Task | None = None
        self.lookup_delivery_task: asyncio.Task | None = None
        self.lookup_concurrency = int(environment.get("CRAWL_LOOKUP_CONCURRENCY", "4"))
        self.lookup_timeout = int(environment.get("CRAWL_LOOKUP_TIMEOUT_SECONDS", "900"))
        if self.lookup_timeout < 1:
            raise ValueError("CRAWL_LOOKUP_TIMEOUT_SECONDS must be positive")
        if not 1 <= self.lookup_concurrency <= 20:
            raise ValueError("CRAWL_LOOKUP_CONCURRENCY must be between 1 and 20")
        self.workers: list[asyncio.Task] = []
        self.executions: dict[str, asyncio.Task] = {}
        self.human_sessions: dict[str, HumanSession] = {}
        self.search = BraveSearch(self.root / ".brave-search")
        self.browser_url = environment.get("BROWSER_API_URL")
        self.browser_token = environment.get("BROWSER_API_TOKEN")
        self.manual_concurrency = int(environment.get("CRAWL_MANUAL_CONCURRENCY", "2"))
        if self.manual_concurrency < 1:
            raise ValueError("CRAWL_MANUAL_CONCURRENCY must be positive")
        self.history: CrawlHistory | None = None
        self.results: S3Results | None = None
        self.delivery_task: asyncio.Task | None = None
        self.human_enabled = (
            environment.get("CRAWL_HUMAN_ENABLED", "false").lower() == "true"
        )
        self.challenge_agent_enabled = (
            environment.get("CRAWL_CHALLENGE_AGENT_ENABLED", "false").lower() == "true"
        )
        self.challenge_agent_max_runs = int(
            environment.get("CRAWL_CHALLENGE_AGENT_MAX_RUNS", "3")
        )
        if not 3 <= self.challenge_agent_max_runs <= 1000:
            raise ValueError(
                "CRAWL_CHALLENGE_AGENT_MAX_RUNS must be between 3 and 1000"
            )
        if self.challenge_agent_enabled and not self.human_enabled:
            raise ValueError(
                "Automatic CAPTCHA assistance requires CRAWL_HUMAN_ENABLED"
            )
        self.lock: TextIO | None = None
        self.accepting = False

    async def start(self) -> None:
        if self.lock is not None:
            raise ServiceUnavailable("This service is already running")
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / ".service.lock").open("a", encoding="utf-8")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.lock.close()
            self.lock = None
            raise ServiceUnavailable(
                "Output directory is owned by another service"
            ) from error
        try:
            self.jobs.clear()
            self.finished.clear()
            self.queue = asyncio.Queue()
            self.manual_queue = asyncio.Queue()
            self.history = CrawlHistory(self.root / "crawl-history.sqlite3")
            self.lookup_queue = asyncio.Queue()
            self.lookup_store = LookupStore(self.root / "company-lookups.sqlite3")
            for path in sorted((self.root / "jobs").glob("*/request.json")):
                request = CrawlRequest.model_validate_json(
                    path.read_text(encoding="utf-8")
                )
                if request.request_id != path.parent.name:
                    raise ValueError("Stored request ID does not match its directory")
                status_file = path.parent / "job.json"
                job = (
                    CrawlJob.model_validate_json(
                        status_file.read_text(encoding="utf-8")
                    )
                    if status_file.exists()
                    else CrawlJob(
                        request_id=request.request_id,
                        state="queued",
                        source="rest",
                        submitted_at=utc_now(),
                    )
                )
                if job.request_id != request.request_id:
                    raise ValueError("Stored job and request IDs do not match")
                result = path.parent / "attempts" / f"{job.attempt:04}" / "result.json"
                job.purpose = "company_lookup" if request.company_lookup is not None else "crawl"
                if not status_file.exists() and request.company_lookup is not None:
                    job.lookup_result_version = 1
                job.url = request.url
                website_reference(request.url, job.website_id or None)
                job.website_id = request.website_id
                job.domain = (urlsplit(request.url).hostname or "").removeprefix("www.")
                job.browser_available = False
                job.verification_available = False
                job.browser_session_id = None
                job.assistance_deadline = None
                if job.state not in TERMINAL_STATES | {"queued"} and result.exists():
                    document = json.loads(result.read_text(encoding="utf-8"))
                    outcome = document.get("crawl", document)
                    job.state = "failed" if outcome["status"] in {"failed", "cancelled", "running"} else "completed"
                    job.crawl_status = outcome["status"]
                    job.result_file = str(result.relative_to(self.root))
                    job.finished_at = outcome["finished_at"]
                elif job.state not in TERMINAL_STATES | {"queued"}:
                    job.state = "failed"
                    job.error = (
                        "Interrupted before completion; retry this attempt manually"
                    )
                    job.finished_at = utc_now()
                    job.result_file = str(
                        (result.parent / "error.json").relative_to(self.root)
                    )
                    write_json(
                        self.root / job.result_file,
                        {
                            "request_id": job.request_id,
                            "website_id": request.website_id,
                            "attempt": job.attempt,
                            "error": job.error,
                            "finished_at": job.finished_at,
                        },
                    )
                self.jobs[job.request_id] = job
                self.finished[job.request_id] = asyncio.Event()
                self.persist(job)
                if job.purpose == "company_lookup":
                    # Recover the disk-write -> SQLite handoff gap without republishing old tests.
                    for saved in path.parent.glob("attempts/*/result.json"):
                        document = json.loads(saved.read_text())
                        if document.get("schema_version") == "website-company-lookup/1.1":
                            recovered = job.model_copy(update={"attempt": int(saved.parent.name)})
                            self.queue_lookup_publication(request, recovered, saved.parent)
                    if job.state in TERMINAL_STATES and job.attempt and job.lookup_result_version == 1:
                        self.queue_lookup_publication(request, job, result.parent)
                if job.state in TERMINAL_STATES:
                    self.finished[job.request_id].set()
                else:
                    (
                        self.lookup_queue if job.purpose == "company_lookup" else self.manual_queue if job.source == "manual" else self.queue
                    ).put_nowait(job.request_id)
            self.workers = [
                asyncio.create_task(self.work(self.queue))
                for _ in range(self.concurrency)
            ] + [
                asyncio.create_task(self.work(self.manual_queue))
                for _ in range(self.manual_concurrency)
            ]
            self.workers += [asyncio.create_task(self.work(self.lookup_queue)) for _ in range(self.lookup_concurrency)]
            self.lookup_dispatch_task = asyncio.create_task(self.dispatch_lookup_batches())
            self.lookup_delivery_task = asyncio.create_task(self.deliver_lookup_results())
            if self.results is not None:
                self.delivery_task = asyncio.create_task(self.deliver_results())
            self.accepting = True
        except BaseException:
            await self.close()
            raise

    def healthy(self) -> bool:
        return self.accepting and all(not worker.done() for worker in self.workers) and all(task is None or not task.done() for task in (self.lookup_dispatch_task, self.lookup_delivery_task))

    def persist(self, job: CrawlJob) -> None:
        job.updated_at = utc_now()
        if (
            job.state in TERMINAL_STATES
            and self.results is not None
            and job.s3_state == "not_configured"
            and job.purpose == "crawl"
        ):
            job.s3_state = "pending"
        write_json(self.root / "jobs" / job.request_id / "job.json", job.model_dump())
        if self.history is not None:
            self.history.save(job.model_dump())

    def submit(
        self,
        request: CrawlRequest,
        *,
        source: Literal["rest", "manual"],
        retry_of: str | None = None,
        retry_of_attempt: int | None = None,
    ) -> CrawlJob:
        if not self.healthy():
            raise ServiceUnavailable("Crawl workers are unavailable")
        folder = self.root / "jobs" / request.request_id
        if request.request_id in self.jobs:
            previous = CrawlRequest.model_validate_json(
                (folder / "request.json").read_text(encoding="utf-8")
            ).model_dump()
            if previous != request.model_dump():
                raise RequestConflict(
                    "request_id already belongs to a different request"
                )
            return self.jobs[request.request_id].model_copy(deep=True)
        if (
            sum(
                j.state not in TERMINAL_STATES
                and (j.source == "manual") == (source == "manual")
                for j in self.jobs.values()
            )
            >= self.max_pending
        ):
            raise ServiceUnavailable("Crawl queue is full")
        needs_model = (
            request.pages is None
            or request.instructions is not None
            or request.site_info
        )
        if request.llm is not None:
            request.llm.decrypt_api_key(self.environment)
        else:
            key_name = "DEEPSEEK" if request.api == "deepseek" else "OPENROUTER_API_KEY"
            if needs_model and not self.environment.get(key_name):
                raise ServiceUnavailable(f"Configure {key_name} on the service")
        if request.decision_llm is not None:
            request.decision_llm.decrypt_api_key(self.environment)
        if request.company_lookup is not None and not self.environment.get("CLICKHOUSE_URL"):
            raise ServiceUnavailable("Configure CLICKHOUSE_URL on the crawler for company lookup tests")
        job = CrawlJob(
            request_id=request.request_id,
            purpose="company_lookup" if request.company_lookup is not None else "crawl",
            lookup_result_version=1 if request.company_lookup is not None else 0,
            state="queued",
            source=source,
            submitted_at=utc_now(),
            url=request.url,
            website_id=request.website_id,
            domain=(urlsplit(request.url).hostname or "").removeprefix("www."),
            retry_of=retry_of,
            retry_of_attempt=retry_of_attempt,
            interactive=request.interactive,
            debug_enabled=request.debug,
            challenge_agent_max_runs=(
                request.challenge_agent_max_runs or self.challenge_agent_max_runs
            )
            if self.challenge_agent_enabled
            else 0,
            challenge_agent_model=request.challenge_agent_model,
            s3_state="pending" if self.results is not None and request.company_lookup is None else "not_configured",
        )
        write_json(folder / "request.json", request.model_dump())
        self.persist(job)
        self.jobs[request.request_id] = job
        self.finished[request.request_id] = asyncio.Event()
        (self.lookup_queue if request.company_lookup is not None else self.manual_queue if source == "manual" else self.queue).put_nowait(
            request.request_id
        )
        return job.model_copy(deep=True)

    async def wait(self, request_id: str) -> CrawlJob:
        await self.finished[request_id].wait()
        job = self.jobs[request_id]
        if job.state not in TERMINAL_STATES or job.result_file is None:
            raise ServiceUnavailable("Job did not publish a terminal result")
        return job.model_copy(deep=True)

    async def work(self, queue: asyncio.Queue[str]) -> None:
        while True:
            request_id = await queue.get()
            job = self.jobs[request_id]
            try:
                if job.state not in TERMINAL_STATES:
                    await self.execute(job)
            finally:
                self.finished[request_id].set()
                queue.task_done()

    def progress(self, request_id: str, state: str, values: dict) -> None:
        job = self.jobs[request_id]
        if job.state in TERMINAL_STATES:
            return
        trace_event("progress", values.get("reason") or state, details={"state": state, **values})
        updated = CrawlJob.model_validate(job.model_dump() | values | {"state": state})
        self.jobs[request_id] = updated
        self.persist(updated)

    def retry(
        self,
        request_id: str,
        attempt: int,
        new_id: str,
        *,
        interactive: bool = True,
        challenge_agent_max_runs: AgentRunBudget | None = None,
        challenge_agent_model: AgentModel | None = None,
    ) -> CrawlJob:
        if not self.human_enabled:
            raise ServiceUnavailable(
                "Enable CRAWL_HUMAN_ENABLED on the crawler server for interactive retries"
            )
        assert self.history is not None
        previous = self.history.get(request_id, attempt)
        if previous is None or previous["state"] != "failed":
            raise RequestConflict("Only a saved failed attempt can be retried")
        if any(
            j.retry_of == request_id
            and j.retry_of_attempt == attempt
            and j.request_id != new_id
            and j.state not in TERMINAL_STATES
            for j in self.jobs.values()
        ):
            raise RequestConflict("This failed attempt already has an active retry")
        request = CrawlRequest.model_validate_json(
            (self.root / "jobs" / request_id / "request.json").read_text(
                encoding="utf-8"
            )
        )
        request.request_id = new_id
        request.save_artifacts = True
        request.interactive = interactive
        request.session_id = previous.get("browser_lease_id") or request.session_id
        budget = max(
            3,
            previous.get("challenge_agent_max_runs", 0)
            or request.challenge_agent_max_runs
            or self.challenge_agent_max_runs,
        )
        exhausted = previous.get("challenge_agent_budget_exhausted", False)
        # Older attempts stored exhaustion only in the immutable crawl result.
        if (
            not exhausted
            and not previous.get("challenge_agent_max_runs")
            and previous.get("result_file")
        ):
            saved = json.loads(
                (self.root / previous["result_file"]).read_text(encoding="utf-8")
            )
            failure = saved.get("crawl", saved).get("human_assistance") or {}
            exhausted = "agent budget exhausted" in failure.get("agent_reason", "")
        request.challenge_agent_max_runs = (
            challenge_agent_max_runs
            if challenge_agent_max_runs is not None
            else min(budget * 2, 1000)
            if exhausted
            else budget
        )
        if challenge_agent_model is not None:
            request.challenge_agent_model = challenge_agent_model
        if new_id in self.jobs:
            existing = self.jobs[new_id]
            if existing.retry_of != request_id or existing.retry_of_attempt != attempt:
                raise RequestConflict("The retry request ID is already in use")
        return self.submit(
            CrawlRequest.model_validate(request.model_dump()),
            source="manual",
            retry_of=request_id,
            retry_of_attempt=attempt,
        )

    def activate_verification(self, request_id: str) -> None:
        session = self.human_sessions.get(request_id)
        if session is None or not session.waiting or not session.verification_available:
            raise RequestConflict("No Brave verification is waiting for activation")
        session.verification_requested.set()

    def resume(self, request_id: str) -> None:
        job = self.jobs.get(request_id)
        if job is not None and job.challenge_agent_running:
            raise RequestConflict(
                "The CAPTCHA agent is running; wait for its access check"
            )
        session = self.human_sessions.get(request_id)
        if session is None or not session.waiting:
            raise RequestConflict("This crawl is not waiting for human assistance")
        if session.verification_available:
            raise RequestConflict("Start verification before resuming Brave search")
        if not session.interactive and not session.verifying_search:
            raise RequestConflict(
                "Wait for this attempt to fail, then retry interactively"
            )
        session.resume_requested.set()

    def cancel(self, request_id: str) -> None:
        job = self.jobs[request_id]
        if job.state in TERMINAL_STATES:
            raise RequestConflict("This attempt has already finished")
        job.state = "cancelled"
        if request_id in self.executions:
            self.executions[request_id].cancel()
        else:
            self.finish_cancelled(job)
            self.finished[request_id].set()

    def finish_cancelled(self, job: CrawlJob) -> None:
        if job.purpose == "company_lookup":
            job.attempt = max(1, job.attempt)
        job.state, job.error = "cancelled", "Cancelled by operator"
        job.challenge_agent_running = False
        job.finished_at = utc_now()
        job.browser_available = False
        job.verification_available = False
        job.assistance_deadline = None
        job.result_file = f"jobs/{job.request_id}/attempts/{job.attempt:04}/error.json"
        write_json(
            self.root / job.result_file,
            {
                "request_id": job.request_id,
                "website_id": job.website_id,
                "attempt": job.attempt,
                "error": job.error,
                "finished_at": job.finished_at,
            },
        )
        if job.purpose == "company_lookup":
            request = CrawlRequest.model_validate_json((self.root / "jobs" / job.request_id / "request.json").read_text())
            self.queue_lookup_publication(request, job, (self.root / job.result_file).parent)
        self.persist(job)

    def submit_lookup_batch(self, request: CompanyLookupBatchRequest) -> dict:
        if not self.accepting or self.lookup_store is None:
            raise ServiceUnavailable("Crawler is not accepting batches")
        if not self.environment.get("CLICKHOUSE_URL") or not self.environment.get("CLICKHOUSE_RESULTS_URL"):
            raise ServiceUnavailable("Configure lookup registry and result ClickHouse connections first")
        request.llm.decrypt_api_key(self.environment)
        if request.decision_llm is not None:
            request.decision_llm.decrypt_api_key(self.environment)
        items = [{"domain": domain, "request_id": "lookup-" + hashlib.sha256(f"{request.batch_id}:{domain}".encode()).hexdigest()} for domain in request.domains]
        try:
            self.lookup_store.submit(request.batch_id, request.model_dump() | {"website_ids": {domain: website_reference(f"https://{domain}/") for domain in request.domains}}, items)
        except ValueError as error:
            raise RequestConflict(str(error)) from error
        return self.lookup_store.snapshot(request.batch_id)

    def submit_crawl_batch(self, request: CrawlBatchRequest) -> dict:
        if not self.accepting or self.lookup_store is None:
            raise ServiceUnavailable("Crawler is not accepting batches")
        if not self.environment.get("CLICKHOUSE_URL") or not self.environment.get("CLICKHOUSE_RESULTS_URL"):
            raise ServiceUnavailable("Configure registry and result ClickHouse connections first")
        items = []
        for entry in request.entries:
            assert entry.request.llm is not None
            entry.request.llm.decrypt_api_key(self.environment)
            if entry.request.decision_llm is not None:
                entry.request.decision_llm.decrypt_api_key(self.environment)
            items.append({"request_id": entry.request.request_id, "domain": urlsplit(entry.request.url).hostname})
        try:
            self.lookup_store.submit(request.batch_id, request.model_dump(), items)
        except ValueError as error:
            raise RequestConflict(str(error)) from error
        return self.lookup_store.snapshot(request.batch_id)

    async def dispatch_lookup_batches(self) -> None:
        assert self.lookup_store is not None
        while True:
            active = sum(job.purpose == "company_lookup" and job.state not in TERMINAL_STATES for job in self.jobs.values())
            for item in self.lookup_store.undispatched(max(0, self.lookup_concurrency - active)):
                payload = json.loads(item["payload"])
                if "entries" in payload:
                    entry = next(entry for entry in payload["entries"] if entry["request"]["request_id"] == item["request_id"])
                    request = CrawlRequest.model_validate(entry["request"])
                else:
                    website_id = payload.get("website_ids", {}).get(item["domain"], "")
                    options = CompanyLookupOptions(country=payload["country"], skip_if_mapped=payload.get("skip_if_mapped", False))
                    payload = {key: value for key, value in payload.items() if key not in {"batch_id", "domains", "input_id", "run_id", "country", "skip_if_mapped", "max_pages", "website_ids"}}
                    request = CrawlRequest(request_id=item["request_id"], url=f"https://{item['domain']}/", website_id=website_id,
                        company_lookup=options, debug=True, site_info=True, **payload)
                try:
                    self.submit(request, source="rest")
                except ServiceUnavailable:
                    break
                self.lookup_store.dispatched(item["request_id"])
            await asyncio.sleep(0.25)

    def queue_lookup_publication(self, request: CrawlRequest, job: CrawlJob, attempt: Path) -> None:
        assert self.lookup_store is not None and request.company_lookup is not None
        saved = attempt / "result.json"
        now = datetime.now(UTC).isoformat(timespec="microseconds")
        saved_result = json.loads(saved.read_text()) if saved.exists() else None
        result = saved_result if saved_result and "crawl" not in saved_result else {
            "schema_version": "website-company-lookup/1.1", "status": "cancelled" if job.state == "cancelled" else "failed", "found": False,
            "started_at": job.started_at or now, "finished_at": now,
            "stop_reason": "lookup_failed", "reasons": [job.error or "Lookup interrupted before completion"],
            "company_id": None, "confidence": None, "company": None, "site_type": "unknown",
            "candidates": [], "searches": [], "identity": [], "pages": [],
        }
        if saved_result and "crawl" in saved_result:
            result["crawl_result"] = saved_result
            result["site_info_result"] = basic_info_result(saved_result)
        if "site_info_result" not in result:
            basic_path = attempt / "matching" / "basic" / "result.json"
            result["site_info_result"] = json.loads(basic_path.read_text()) if basic_path.exists() else {
                "schema_version": "company-crawl-result/1.2", "documents": [], "crawl": {
                    "status": "failed", "site_info": site_information(None, request.url),
                    "started_at": job.started_at or now, "finished_at": now, "stop_reason": "initial_page_unavailable",
                    "pages": [], "usage": {},
                }}
        settings = request.model_dump()
        for key in ("llm", "decision_llm"):
            if settings.get(key):
                settings[key].pop("api_key_encrypted", None)
        for key in ("request_id", "url", "session_id", "website_id"):
            settings.pop(key, None)
        result.update(country=request.company_lookup.country, domain=job.domain, website_url=request.url, website_id=request.website_id,
            request_id=job.request_id, attempt=job.attempt, result_path=str(saved.relative_to(self.root)),
            work_key=hashlib.sha256(json.dumps(settings, sort_keys=True).encode()).hexdigest())
        batch = self.lookup_store.db.execute("SELECT b.batch_id,b.payload FROM batches b JOIN items i USING(batch_id) WHERE i.request_id=?", (job.request_id,)).fetchone()
        if batch:
            metadata = json.loads(batch["payload"])
            result.update(batch_id=batch["batch_id"], input_id=metadata["input_id"], run_id=metadata["run_id"])
            if "entries" in metadata:
                entry = next(entry for entry in metadata["entries"] if entry["request"]["request_id"] == job.request_id)
                result.update({key: entry[key] for key in ("crawl_type", "input_revision", "work_key")})
        result.setdefault("crawl_type", "site_info" if request.crawl is False or request.crawl is None and not request.pages and not request.instructions else "full")
        result["site_info"] = result["site_info_result"]["crawl"]["site_info"]
        result["persisted_to_database"] = False
        # Keep the full portable basic capture locally, not duplicated in SQLite.
        for capture in (result["site_info_result"], result.get("crawl_result", {})):
            if capture:
                capture["website_id"] = request.website_id
        write_json(saved, result)
        result = json.loads(json.dumps(result))
        for capture in (result["site_info_result"], result.get("crawl_result", {})):
            for document in capture.get("documents", []):
                document.pop("html", None)
                document.pop("rendered_html", None)
        self.lookup_store.enqueue(job.request_id, job.attempt, result)
        trace_event("company_publication", "Findings saved to SQLite for ClickHouse delivery", details={"request_id": job.request_id, "attempt": job.attempt, "batch_id": result.get("batch_id")})

    async def deliver_lookup_results(self) -> None:
        assert self.lookup_store is not None
        while True:
            if self.environment.get("CLICKHOUSE_RESULTS_URL"):
                async with httpx.AsyncClient(base_url=self.environment["CLICKHOUSE_RESULTS_URL"].rstrip("/") + "/",
                    auth=httpx.BasicAuth(self.environment.get("CLICKHOUSE_RESULTS_USER", "default"), self.environment.get("CLICKHOUSE_RESULTS_PASSWORD", "")), timeout=60) as http:
                    for batch_id, rows in self.lookup_store.ready():
                        error = None
                        try:
                            results = [json.loads(row["payload"]) for row in rows]
                            await asyncio.to_thread(register_results, self.environment, results)
                            await publish_lookup_results(http, results)
                        except Exception as failure:
                            # Provider URLs/credentials must never enter receipts or logs.
                            error = type(failure).__name__ + (f" (HTTP {failure.response.status_code})" if isinstance(failure, httpx.HTTPStatusError) else "")
                            LOGGER.warning("Lookup publication pending: batch=%s attempts=%s error=%s", batch_id, len(rows), error)
                        self.lookup_store.delivered(batch_id, rows, error)
                        if error is None:
                            LOGGER.info("Lookup findings published: batch=%s attempts=%s", batch_id, len(rows))
            await asyncio.sleep(5)

    async def deliver_results(self) -> None:
        assert self.history is not None and self.results is not None
        while True:
            for document in self.history.pending_uploads():
                job = CrawlJob.model_validate(document)
                if job.purpose == "company_lookup":
                    continue
                try:
                    job.s3_event = await asyncio.to_thread(
                        self.results.prepare_event, self.root, job
                    )
                    job.s3_state, job.s3_error = "uploaded", None
                except Exception as error:
                    job.s3_error = type(error).__name__
                    LOGGER.warning(
                        "S3 upload pending for %s attempt %s (%s)",
                        job.request_id,
                        job.attempt,
                        type(error).__name__,
                    )
                current = self.jobs.get(job.request_id)
                if (
                    current is not None
                    and current.attempt == job.attempt
                    and current.state in TERMINAL_STATES
                ):
                    self.jobs[job.request_id] = job
                    self.persist(job)
                else:
                    self.history.save(job.model_dump())
            await asyncio.sleep(5)

    async def run_scan(
        self,
        request: CrawlRequest,
        job: CrawlJob,
        attempt: Path,
        human: HumanSession | None,
    ) -> dict:
        # Authenticate again before leasing a browser, including recovered/retried jobs.
        api_key = (
            request.llm.decrypt_api_key(self.environment)
            if request.llm is not None
            else self.environment.get(
                "DEEPSEEK" if request.api == "deepseek" else "OPENROUTER_API_KEY"
            )
        )
        decision_api_key = request.decision_llm.decrypt_api_key(self.environment) if request.decision_llm is not None else None
        trace = CURRENT_TRACE.get()
        if trace is not None:
            trace.secrets.update(key for key in (api_key, decision_api_key) if key)
        async with AsyncExitStack() as stack:
            search = self.search
            browser_client = None
            if not self.browser_url:
                raise ValueError(
                    "Configure BROWSER_API_URL for the external browser service"
                )
            job.browser_lease_id = (
                request.session_id or job.browser_lease_id or uuid4().hex
            )
            self.persist(job)
            self.progress(
                job.request_id,
                "running",
                {
                    "reason": "Waiting for browser capacity",
                    "blocked_reason": None,
                },
            )

            def browser_ready(snapshot: dict) -> None:
                self.jobs[job.request_id].browser_execution_id = snapshot.get(
                    "executionId"
                )
                if (
                    human is not None
                    and not human.verification_available
                    and snapshot["generation"] is not None
                ):
                    human.session_id = snapshot["generation"]
                    human.headed = not snapshot.get("headless", False)
                    human.browser_available = snapshot.get(
                        "desktopAvailable", human.headed
                    )

            http = await stack.enter_async_context(
                httpx.AsyncClient(
                    base_url=self.browser_url,
                    event_hooks=trace_http_hooks("browser_http"),
                    headers={"Authorization": f"Bearer {self.browser_token}"}
                    if self.browser_token
                    else {},
                    timeout=330,
                )
            )
            browser_client = await stack.enter_async_context(
                BrowserLeaseClient(http, on_ready=browser_ready).lease(
                    identifier=job.browser_lease_id,
                    request_id=job.request_id,
                    domain=job.domain,
                    headless=not request.interactive,
                )
            )
            search = BraveSearch(
                self.search.root,
                browser_client=browser_client,
                lock=self.search.lock,
            )
            stack.push_async_callback(search.close)
            self.progress(
                job.request_id,
                "running",
                {
                    "reason": f"Scanning with session {browser_client.profile_id}",
                    "browser_lease_id": browser_client.id,
                    "browser_execution_id": browser_client.execution_id,
                },
            )
            crawl_result = None
            if request.company_lookup is None or request.crawl is not None or request.pages is not None or request.instructions is not None:
                manifest = await crawl_company(
                    request.url,
                    search=search,
                    output_dir=attempt,
                    pages=request.pages,
                    instructions=request.instructions,
                    site_info=request.site_info,
                    full_crawl_all=request.full_crawl_all,
                    save_artifacts=request.save_artifacts or human is not None,
                    crawl=request.crawl,
                    api=request.llm.api if request.llm is not None else request.api,
                    config=request.llm.crawl_config(request.config)
                    if request.llm is not None
                    else request.config,
                    api_key=api_key,
                    base_url=request.llm.base_url if request.llm is not None else None,
                    **({"decision_model": request.decision_llm.model, "decision_api_key": decision_api_key,
                        "decision_tasks": request.decision_tasks} if request.decision_llm is not None else {}),
                    **({"human": human} if human is not None else {}),
                    **(
                        {"browser_client": browser_client}
                        if browser_client is not None
                        else {}
                    ),
                )
                if request.company_lookup is None:
                    return manifest
                crawl_result = json.loads((attempt / "result.json").read_text())
            if request.company_lookup is not None:
                assert request.llm is not None and api_key is not None
                model_http = await stack.enter_async_context(httpx.AsyncClient(
                    base_url=request.llm.base_url.rstrip("/") + "/",
                    event_hooks=trace_http_hooks("model_http"), timeout=180))
                matching_config = request.llm.crawl_config(request.config).model_copy(update={"max_pages": request.company_lookup.max_pages if crawl_result is not None else (request.config.max_pages if request.config is not None else 4)})
                model = ModelClient(model_http, api_key, matching_config, attempt / "matching", api=request.llm.api)
                if crawl_result is not None:
                    model.calls.extend(crawl_result["crawl"].get("usage", {}).get("by_call", []))
                decisions = None
                if request.decision_llm is not None:
                    assert decision_api_key is not None
                    decision_http = await stack.enter_async_context(httpx.AsyncClient(event_hooks=trace_http_hooks("decision_http")))
                    decisions = JevClient(decision_http, decision_api_key, request.decision_llm.model, request.decision_tasks, model)
                database_http = await stack.enter_async_context(httpx.AsyncClient(
                    base_url=self.environment["CLICKHOUSE_URL"].rstrip("/") + "/", timeout=20,
                    auth=httpx.BasicAuth(self.environment.get("CLICKHOUSE_USER", "default"), self.environment.get("CLICKHOUSE_PASSWORD", ""))))
                try:
                    return await asyncio.wait_for(find_company(url=request.url, options=request.company_lookup, llm=model,
                        decisions=decisions, browser_client=browser_client, human=human,
                        output_dir=attempt / "matching", database_http=database_http, crawl_result=crawl_result), self.lookup_timeout)
                finally:
                    matching_path = attempt / "matching" / "result.json"
                    if matching_path.exists():
                        write_json(attempt / "result.json", json.loads(matching_path.read_text()))
                    # Save before browser cleanup; it must not hold back durable results.
                    self.queue_lookup_publication(request, job, attempt)

    async def execute(self, job: CrawlJob) -> None:
        folder = self.root / "jobs" / job.request_id
        request = CrawlRequest.model_validate_json(
            (folder / "request.json").read_text(encoding="utf-8")
        )
        job.attempt += 1
        job.state, job.started_at, job.error = "running", utc_now(), None
        job.challenge_agent_running = False
        job.challenge_agent_result = None
        job.challenge_agent_results = []
        job.challenge_agent_budget_exhausted = False
        job.challenge_agent_max_runs = (
            (request.challenge_agent_max_runs or self.challenge_agent_max_runs)
            if self.challenge_agent_enabled
            else 0
        )
        job.challenge_agent_model = request.challenge_agent_model
        job.interactive = request.interactive
        job.collected_pages = 0
        self.persist(job)
        attempt = folder / "attempts" / f"{job.attempt:04}"
        human = None
        if self.human_enabled:
            human = HumanSession(
                request_id=job.request_id,
                headed=request.interactive,
                interactive=request.interactive,
                challenge_agent_max_runs=job.challenge_agent_max_runs,
                challenge_agent_model=request.challenge_agent_model,
                timeout=900 if request.interactive else 10,
                notify=lambda state, values: self.progress(
                    job.request_id, state, values
                ),
            )
            self.human_sessions[job.request_id] = human
        trace = CrawlTrace(attempt, [value for name, value in self.environment.items() if SECRET_FIELD.search(name)]) if request.debug else None
        trace_token = CURRENT_TRACE.set(trace)
        if trace is not None:
            logging.getLogger().addHandler(trace)
            trace.event("crawl", "Crawl started", details={"request": request.model_dump(), "attempt": job.attempt,
                        "queued_at": job.submitted_at, "started_at": job.started_at})
        try:
            execution = asyncio.create_task(self.run_scan(request, job, attempt, human))
            self.executions[job.request_id] = execution
            manifest = await asyncio.wait_for(execution, self.lookup_timeout) if job.purpose == "company_lookup" and request.crawl is None and request.pages is None and request.instructions is None else await execution
            job = self.jobs[job.request_id]
            job.crawl_status = manifest["status"]
            job.collected_pages = sum(
                page["fetch_status"] == "fetched" for page in manifest.get("pages", [])
            )
            job.result_file = str((attempt / "result.json").relative_to(self.root))
            job.state = (
                "failed"
                if manifest["status"] == "failed"
                or manifest["stop_reason"] == "human_assistance_timeout"
                else "completed"
            )
            if job.purpose == "company_lookup" and job.state == "completed":
                job.reason = (
                    f"Company match proposed: {manifest['company_id']}"
                    if manifest["found"]
                    else "Company matching skipped: domain already mapped" if manifest["status"] == "already_mapped" else f"No company match: {manifest['site_type']} ({manifest['stop_reason']})"
                )
            if job.state == "failed":
                job.error = manifest["stop_reason"]
                job.reason = "Crawl failed; available for interactive retry"
                failure = manifest.get("human_assistance")
                if failure is not None:
                    job.reason = (
                        f"Saved {job.collected_pages} pages; stopped at {failure['reason']} on {failure['url']}. "
                        f"{failure.get('agent_reason', 'Human assistance timed out.')} "
                        "Available for interactive retry."
                    )
        except asyncio.CancelledError:
            trace_event("crawl", "Crawl interrupted or cancelled", level="warning")
            job = self.jobs[job.request_id]
            if job.state == "cancelled":
                self.finish_cancelled(job)
                return
            interrupted = job.model_copy(deep=True)
            interrupted.state = "failed"
            interrupted.challenge_agent_running = False
            interrupted.error = "Interrupted before completion"
            interrupted.reason = "The service stopped during this attempt"
            interrupted.finished_at = utc_now()
            interrupted.browser_available = False
            interrupted.verification_available = False
            interrupted.assistance_deadline = None
            interrupted.result_file = str(
                (attempt / "error.json").relative_to(self.root)
            )
            interrupted.s3_state = (
                "pending" if self.results is not None and job.purpose == "crawl" else "not_configured"
            )
            write_json(
                self.root / interrupted.result_file,
                {
                    "request_id": job.request_id,
                    "error": interrupted.error,
                    "finished_at": interrupted.finished_at,
                },
            )
            assert self.history is not None
            self.history.save(interrupted.model_dump())
            if job.source == "manual" or job.purpose == "company_lookup":
                self.jobs[job.request_id] = interrupted
                self.persist(interrupted)
                raise
            job.state = "queued"
            job.error = "Interrupted; a new attempt will run on restart"
            self.persist(job)
            raise
        except Exception as error:
            trace_event("crawl", f"Crawl failed ({type(error).__name__})", level="error", details={"error": str(error), "traceback": traceback.format_exc()})
            job = self.jobs[job.request_id]
            LOGGER.error(
                "Crawl job %s failed (%s)", job.request_id, type(error).__name__
            )
            job.state = "failed"
            job.error = (
                str(error)
                if isinstance(error, LLMProfileError)
                else f"Crawl execution failed ({type(error).__name__})"
            )
            error_file = attempt / "error.json"
            write_json(
                error_file,
                {
                    "schema_version": "company-crawl-error/1.0",
                    "request_id": job.request_id,
                    "url": request.url,
                    "finished_at": utc_now(),
                    "error": job.error,
                },
            )
            job.result_file = str(error_file.relative_to(self.root))
            if job.purpose == "company_lookup" and (attempt / "result.json").is_file():
                job.result_file = str((attempt / "result.json").relative_to(self.root))
        finally:
            if job.result_file is not None:
                saved_path = self.root / job.result_file
                saved_result = json.loads(saved_path.read_text(encoding="utf-8"))
                saved_result.update(website_id=request.website_id, request_id=job.request_id, attempt=job.attempt)
                write_json(saved_path, saved_result)
            if job.purpose == "company_lookup":
                self.queue_lookup_publication(request, job, attempt)
            self.jobs[job.request_id].challenge_agent_running = False
            self.executions.pop(job.request_id, None)
            self.human_sessions.pop(job.request_id, None)
            if trace is not None:
                trace.event("crawl", "Crawl execution ended", level="error" if self.jobs[job.request_id].state == "failed" else "info",
                            details=self.jobs[job.request_id].model_dump())
                logging.getLogger().removeHandler(trace)
                trace.close()
            CURRENT_TRACE.reset(trace_token)
        job.finished_at = utc_now()
        job.browser_available = False
        job.verification_available = False
        job.assistance_deadline = None
        self.persist(job)

    async def close(self) -> None:
        self.accepting = False
        for worker in self.workers:
            worker.cancel()
        outcomes = await asyncio.gather(*self.workers, return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, Exception):
                LOGGER.error("Crawl worker stopped (%s)", type(outcome).__name__)
        self.workers.clear()
        await self.search.close()
        for task in (self.lookup_dispatch_task, self.lookup_delivery_task):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if self.lookup_store is not None:
            self.lookup_store.close()
            self.lookup_store = None
        if self.delivery_task is not None:
            self.delivery_task.cancel()
            await asyncio.gather(self.delivery_task, return_exceptions=True)
            self.delivery_task = None
        for event in self.finished.values():
            event.set()
        if self.lock is not None:
            fcntl.flock(self.lock, fcntl.LOCK_UN)
            self.lock.close()
            self.lock = None
        if self.history is not None:
            self.history.close()
            self.history = None
