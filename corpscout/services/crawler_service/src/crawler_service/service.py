"""Local durable crawl jobs submitted through REST or the manual console."""

import asyncio
import fcntl
import json
import logging
from contextlib import AsyncExitStack
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, TextIO
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from pydantic import Field, field_serializer, field_validator, model_validator

from crawler_service.brave_browser import BraveSearch
from crawler_service.browser_client import BrowserLeaseClient
from crawler_service.crawl import crawl_company
from crawler_service.crawl_history import CrawlHistory
from crawler_service.discovery import crawlable_url, normalize_url
from crawler_service.human_control import HumanSession
from crawler_service.models import ResearchConfig, StrictModel
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
    pages: list[str] | None = Field(default=None, min_length=1, max_length=1000)
    instructions: str | None = Field(default=None, max_length=20000)
    site_info: bool = False
    save_artifacts: bool = True
    crawl: bool | Literal["full"] | None = None
    api: Literal["deepseek", "openrouter"] = "deepseek"
    config: ResearchConfig | None = None
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


class CrawlJob(StrictModel):
    schema_version: Literal["company-crawl-job/1.0"] = "company-crawl-job/1.0"
    request_id: str
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
                job.url = request.url
                job.domain = (urlsplit(request.url).hostname or "").removeprefix("www.")
                job.browser_available = False
                job.verification_available = False
                job.browser_session_id = None
                job.assistance_deadline = None
                if job.state not in TERMINAL_STATES | {"queued"} and result.exists():
                    document = json.loads(result.read_text(encoding="utf-8"))
                    job.state = (
                        "failed"
                        if document["crawl"]["status"] == "failed"
                        else "completed"
                    )
                    job.crawl_status = document["crawl"]["status"]
                    job.result_file = str(result.relative_to(self.root))
                    job.finished_at = document["crawl"]["finished_at"]
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
                            "error": job.error,
                            "finished_at": job.finished_at,
                        },
                    )
                self.jobs[job.request_id] = job
                self.finished[job.request_id] = asyncio.Event()
                self.persist(job)
                if job.state in TERMINAL_STATES:
                    self.finished[job.request_id].set()
                else:
                    (
                        self.manual_queue if job.source == "manual" else self.queue
                    ).put_nowait(job.request_id)
            self.workers = [
                asyncio.create_task(self.work(self.queue))
                for _ in range(self.concurrency)
            ] + [
                asyncio.create_task(self.work(self.manual_queue))
                for _ in range(self.manual_concurrency)
            ]
            if self.results is not None:
                self.delivery_task = asyncio.create_task(self.deliver_results())
            self.accepting = True
        except BaseException:
            await self.close()
            raise

    def healthy(self) -> bool:
        return self.accepting and all(not worker.done() for worker in self.workers)

    def persist(self, job: CrawlJob) -> None:
        job.updated_at = utc_now()
        if (
            job.state in TERMINAL_STATES
            and self.results is not None
            and job.s3_state == "not_configured"
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
        key_name = "DEEPSEEK" if request.api == "deepseek" else "OPENROUTER_API_KEY"
        if needs_model and not self.environment.get(key_name):
            raise ServiceUnavailable(f"Configure {key_name} on the service")
        job = CrawlJob(
            request_id=request.request_id,
            state="queued",
            source=source,
            submitted_at=utc_now(),
            url=request.url,
            domain=(urlsplit(request.url).hostname or "").removeprefix("www."),
            retry_of=retry_of,
            retry_of_attempt=retry_of_attempt,
            interactive=request.interactive,
            challenge_agent_max_runs=(
                request.challenge_agent_max_runs or self.challenge_agent_max_runs
            )
            if self.challenge_agent_enabled
            else 0,
            challenge_agent_model=request.challenge_agent_model,
            s3_state="pending" if self.results is not None else "not_configured",
        )
        write_json(folder / "request.json", request.model_dump())
        self.persist(job)
        self.jobs[request.request_id] = job
        self.finished[request.request_id] = asyncio.Event()
        (self.manual_queue if source == "manual" else self.queue).put_nowait(
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
                "error": job.error,
                "finished_at": job.finished_at,
            },
        )
        self.persist(job)

    async def deliver_results(self) -> None:
        assert self.history is not None and self.results is not None
        while True:
            for document in self.history.pending_uploads():
                job = CrawlJob.model_validate(document)
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
            return await crawl_company(
                request.url,
                search=search,
                output_dir=attempt,
                pages=request.pages,
                instructions=request.instructions,
                site_info=request.site_info,
                save_artifacts=request.save_artifacts or human is not None,
                crawl=request.crawl,
                api=request.api,
                config=request.config,
                api_key=self.environment.get(
                    "DEEPSEEK" if request.api == "deepseek" else "OPENROUTER_API_KEY"
                ),
                **({"human": human} if human is not None else {}),
                **(
                    {"browser_client": browser_client}
                    if browser_client is not None
                    else {}
                ),
            )

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
        try:
            execution = asyncio.create_task(self.run_scan(request, job, attempt, human))
            self.executions[job.request_id] = execution
            manifest = await execution
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
                "pending" if self.results is not None else "not_configured"
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
            if job.source == "manual":
                self.jobs[job.request_id] = interrupted
                self.persist(interrupted)
                raise
            job.state = "queued"
            job.error = "Interrupted; a new attempt will run on restart"
            self.persist(job)
            raise
        except Exception as error:
            job = self.jobs[job.request_id]
            LOGGER.error(
                "Crawl job %s failed (%s)", job.request_id, type(error).__name__
            )
            job.state = "failed"
            job.error = f"Crawl execution failed ({type(error).__name__})"
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
        finally:
            self.jobs[job.request_id].challenge_agent_running = False
            self.executions.pop(job.request_id, None)
            self.human_sessions.pop(job.request_id, None)
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
