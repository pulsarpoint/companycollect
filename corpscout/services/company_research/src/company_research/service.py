"""Local durable crawl jobs shared by REST and JetStream inputs."""

import asyncio
import fcntl
import json
import logging
from pathlib import Path
from typing import Literal, TextIO
from uuid import uuid4

from pydantic import Field, field_serializer, field_validator, model_validator

from company_research.crawl import crawl_company
from company_research.discovery import crawlable_url, normalize_url
from company_research.models import ResearchConfig, StrictModel
from company_research.storage import utc_now, write_json

LOGGER = logging.getLogger(__name__)


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
    state: Literal["queued", "running", "completed", "failed"]
    source: Literal["rest", "jetstream"]
    submitted_at: str
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
        self.workers: list[asyncio.Task] = []
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
                if job.state == "running" and result.exists():
                    document = json.loads(result.read_text(encoding="utf-8"))
                    job.state = "completed"
                    job.crawl_status = document["crawl"]["status"]
                    job.result_file = str(result.relative_to(self.root))
                    job.finished_at = document["crawl"]["finished_at"]
                elif job.state == "running":
                    job.state = "queued"
                self.jobs[job.request_id] = job
                self.finished[job.request_id] = asyncio.Event()
                self.persist(job)
                if job.state in {"completed", "failed"}:
                    self.finished[job.request_id].set()
                else:
                    self.queue.put_nowait(job.request_id)
            self.workers = [
                asyncio.create_task(self.work()) for _ in range(self.concurrency)
            ]
            self.accepting = True
        except BaseException:
            await self.close()
            raise

    def healthy(self) -> bool:
        return self.accepting and all(not worker.done() for worker in self.workers)

    def persist(self, job: CrawlJob) -> None:
        write_json(self.root / "jobs" / job.request_id / "job.json", job.model_dump())

    def submit(
        self, request: CrawlRequest, *, source: Literal["rest", "jetstream"]
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
            sum(j.state in {"queued", "running"} for j in self.jobs.values())
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
        )
        write_json(folder / "request.json", request.model_dump())
        self.persist(job)
        self.jobs[request.request_id] = job
        self.finished[request.request_id] = asyncio.Event()
        self.queue.put_nowait(request.request_id)
        return job.model_copy(deep=True)

    async def wait(self, request_id: str) -> CrawlJob:
        await self.finished[request_id].wait()
        job = self.jobs[request_id]
        if job.state not in {"completed", "failed"} or job.result_file is None:
            raise ServiceUnavailable("Job did not publish a terminal result")
        return job.model_copy(deep=True)

    async def work(self) -> None:
        while True:
            request_id = await self.queue.get()
            job = self.jobs[request_id]
            try:
                await self.execute(job)
            finally:
                self.finished[request_id].set()
                self.queue.task_done()

    async def execute(self, job: CrawlJob) -> None:
        folder = self.root / "jobs" / job.request_id
        request = CrawlRequest.model_validate_json(
            (folder / "request.json").read_text(encoding="utf-8")
        )
        job.attempt += 1
        job.state, job.started_at, job.error = "running", utc_now(), None
        self.persist(job)
        attempt = folder / "attempts" / f"{job.attempt:04}"
        try:
            manifest = await crawl_company(
                request.url,
                output_dir=attempt,
                pages=request.pages,
                instructions=request.instructions,
                site_info=request.site_info,
                save_artifacts=request.save_artifacts,
                crawl=request.crawl,
                api=request.api,
                config=request.config,
                api_key=self.environment.get(
                    "DEEPSEEK" if request.api == "deepseek" else "OPENROUTER_API_KEY"
                ),
            )
            job.crawl_status = manifest["status"]
            job.result_file = str((attempt / "result.json").relative_to(self.root))
            job.state = "completed"
        except asyncio.CancelledError:
            job.state = "queued"
            job.error = "Interrupted; a new attempt will run on restart"
            self.persist(job)
            raise
        except Exception as error:
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
        job.finished_at = utc_now()
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
        for event in self.finished.values():
            event.set()
        if self.lock is not None:
            fcntl.flock(self.lock, fcntl.LOCK_UN)
            self.lock.close()
            self.lock = None
