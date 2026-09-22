"""Concurrent Brave Ask requests; browser execution belongs to browser-service."""

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from queue import Full, Queue
from threading import Event, Lock
from time import monotonic, sleep
from typing import Literal
from urllib.parse import urlsplit
from uuid import uuid4

import dagster as dg
from dlt.sources.helpers.requests import Session
from pydantic import Field
from requests.exceptions import RequestException

BRAVE_ORIGIN = "https://search.brave.com"
ROUTES = ("direct", "crawl_proxy1", "crawl_proxy2", "crawl_proxy3")


@dataclass(frozen=True)
class CompanySearchInput:
    company_id: str
    company_name: str
    query: str
    request_id: str
    answer_timeout_ms: int

    def __post_init__(self) -> None:
        if not self.company_id.strip() or not self.company_name.strip():
            raise ValueError("Brave requires a company ID and a nonempty company name")
        if self.answer_timeout_ms <= 0:
            raise ValueError("Brave requires a positive answer timeout")


@dataclass(frozen=True)
class BraveSearchResult:
    company: CompanySearchInput
    query: str
    route: str
    source_url: str
    fetched_at: datetime
    status: Literal["success", "error"]
    answer: str = ""
    error_type: str = ""
    error_stage: str = ""
    elapsed_ms: int = 0
    challenge_runs: list[dict] = field(default_factory=list)


class BraveBrowserResource(dg.ConfigurableResource):
    """Bounded HTTP workers drawing from one lazy company iterator."""

    api_url: str
    api_token: str = Field(repr=False)
    page_timeout_ms: int = Field(default=60_000, gt=0, le=300_000)
    request_timeout_seconds: int = Field(default=900, gt=0, le=1800)
    capacity_timeout_seconds: int = Field(default=120, gt=0)
    challenge_agent_max_runs: int | None = Field(default=None, ge=0, le=1000)
    challenge_agent_model: str | None = None

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        parsed = urlsplit(self.api_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            raise ValueError(
                "Brave requires an HTTP browser-service URL without credentials"
            )
        if not self.api_token.strip():
            raise ValueError("Brave requires a browser-service API token")
        if self.challenge_agent_model not in {
            None,
            "deepseek-flash",
            "z-ai/glm-5.3-flash",
        }:
            raise ValueError("Unsupported Brave CAPTCHA agent model")

    def ask(
        self,
        http: Session,
        company: CompanySearchInput,
        route: str,
        *,
        session_id: str | None = None,
    ) -> BraveSearchResult:
        """Reuse a request ID after transport failures; never repeat a finished query."""
        started = monotonic()
        request_id = f"dagster-{company.request_id}"
        payload = {
            "request_id": request_id,
            "query": company.query,
            "route": route,
            "page_timeout_seconds": self.page_timeout_ms / 1000,
            "answer_timeout_seconds": company.answer_timeout_ms / 1000,
            "timeout_seconds": self.request_timeout_seconds,
        }
        if session_id is not None:
            payload["session_id"] = session_id
        if self.challenge_agent_max_runs is not None:
            payload["challenge_agent_max_runs"] = self.challenge_agent_max_runs
        if self.challenge_agent_model is not None:
            payload["challenge_agent_model"] = self.challenge_agent_model
        deadline = started + self.capacity_timeout_seconds
        submitted = False
        poll = False
        try:
            while monotonic() < deadline:
                try:
                    timeout = (10, min(30, max(0.1, deadline - monotonic())))
                    if poll:
                        response = http.get(
                            f"{self.api_url.rstrip('/')}/v1/brave/requests/{request_id}",
                            timeout=timeout,
                        )
                    else:
                        response = http.post(
                            f"{self.api_url.rstrip('/')}/v1/brave/ask",
                            json=payload,
                            timeout=timeout,
                        )
                except RequestException:
                    # The POST may already be executing. Recover through its status endpoint.
                    poll = True
                    if not submitted:
                        submitted = True
                        deadline = monotonic() + self.request_timeout_seconds + 30
                    sleep(1)
                    continue
                if response.status_code == 404 and poll:
                    poll = False
                elif response.status_code == 503 or (
                    response.status_code == 409 and response.headers.get("Retry-After")
                ):
                    poll = response.status_code == 409
                else:
                    response.raise_for_status()
                    data = response.json()
                    if data["status"] == "running":
                        poll = True
                        if not submitted:
                            submitted = True
                            deadline = monotonic() + self.request_timeout_seconds + 30
                    elif data["status"] == "interrupted":
                        raise RuntimeError("Browser service interrupted the request")
                    else:
                        if (
                            data["request_id"] != request_id
                            or data["query"] != company.query
                            or data["route"] != route
                        ):
                            raise ValueError(
                                "Browser service returned a different request"
                            )
                        success = data["status"] == "success"
                        if success and (
                            not isinstance(data["answer"], str)
                            or not data["answer"].strip()
                        ):
                            raise ValueError("Browser service returned an empty answer")
                        return BraveSearchResult(
                            company,
                            company.query,
                            route,
                            data["source_url"],
                            datetime.fromisoformat(data["fetched_at"]),
                            "success" if success else "error",
                            answer=data["answer"] if success else "",
                            error_type=data["error_type"],
                            error_stage=data["error_stage"],
                            elapsed_ms=data["elapsed_ms"],
                            challenge_runs=data.get("challenge_runs", []),
                        )
                sleep(1)
            raise TimeoutError("Browser service request timed out")
        except Exception as error:  # noqa: BLE001 - isolate and sanitize each company's HTTP failure
            # Never retain HTTP exception messages: URLs/headers can contain secrets.
            return BraveSearchResult(
                company,
                company.query,
                route,
                BRAVE_ORIGIN,
                datetime.now(UTC),
                "error",
                error_type=type(error).__name__,
                error_stage="browser_service",
                elapsed_ms=round((monotonic() - started) * 1000),
            )

    def iter_answers(
        self,
        companies: Iterator[CompanySearchInput],
        *,
        requests_per_route: int,
        on_result: Callable[[BraveSearchResult], None],
    ) -> Iterator[BraveSearchResult]:
        """Persist each result before claiming the next company on an idle route."""
        if requests_per_route < 1:
            raise ValueError("requests_per_route must be positive")
        worker_count = len(ROUTES) * requests_per_route
        events: Queue[BraveSearchResult | BaseException | None] = Queue(
            maxsize=worker_count
        )
        stopped = Event()
        input_lock = Lock()

        def next_company() -> CompanySearchInput | None:
            with input_lock:
                return None if stopped.is_set() else next(companies, None)

        def send(event: BraveSearchResult | BaseException | None) -> None:
            while not stopped.is_set():
                try:
                    events.put(event, timeout=0.1)
                    return
                except Full:
                    continue

        def worker(route: str) -> None:
            session_id = uuid4().hex
            try:
                company = next_company()
                if company is None:
                    return
                with Session(raise_for_status=False) as http:
                    http.headers["Authorization"] = f"Bearer {self.api_token}"
                    while company is not None and not stopped.is_set():
                        result = self.ask(http, company, route, session_id=session_id)
                        on_result(result)
                        send(result)
                        company = next_company()
            except BaseException as error:  # noqa: BLE001 - always notify the result consumer when a worker exits
                send(
                    RuntimeError(f"Brave route {route} failed ({type(error).__name__})")
                )
            finally:
                send(None)

        with ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="brave_http"
        ) as executor:
            futures = [
                executor.submit(worker, route)
                for _ in range(requests_per_route)
                for route in ROUTES
            ]
            finished = 0
            try:
                while finished < worker_count:
                    event = events.get()
                    if event is None:
                        finished += 1
                    elif isinstance(event, BaseException):
                        raise event
                    else:
                        yield event
                for future in futures:
                    future.result()
            finally:
                stopped.set()
