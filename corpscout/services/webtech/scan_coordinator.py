import asyncio
import hashlib
import json
import logging
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from urllib.parse import urlsplit

from config import WebtechServiceSettings
from models import WebtechCandidate, WebtechDomainResult
from s3_store import RustfsStore, S3Location
from scanner import scan_webtech_candidates
from service_models import (
    ScanPollResponse,
    ScanProgressEvent,
    ScanRequest,
    ScanSnapshot,
    ScanStatus,
    StoredDomainResultDocument,
    StoredResultReference,
)

LOGGER = logging.getLogger("uvicorn.error")
SCAN_HEARTBEAT_SECONDS = 60.0
SCAN_STALLED_AFTER_SECONDS = 120.0
SCAN_STALL_FAIL_AFTER_SECONDS = 600.0

type ScanFunction = Callable[..., Awaitable[tuple[WebtechDomainResult, ...]]]


class ScanBusyError(RuntimeError):
    """Raised when a different scan already owns the workstation."""


class ScanNotFoundError(KeyError):
    """Raised when a scan ID is unknown to this service process."""


class ScanJob:
    """In-memory coordination for one durable, per-domain RustFS scan."""

    def __init__(
        self,
        *,
        scan_id: str,
        request: ScanRequest,
        progress_batch_size: int,
        recovered_results: dict[str, StoredResultReference] | None = None,
    ) -> None:
        self.scan_id = scan_id
        self.request = request
        self.progress_batch_size = progress_batch_size
        self.results = recovered_results or {}
        self.status: ScanStatus = "pending"
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None
        self.error_message = ""
        self.events: list[ScanProgressEvent] = []
        self.task: asyncio.Task[None] | None = None
        self.stall_reason: str | None = None
        self._pending_event_results: list[StoredResultReference] = []
        self._condition = asyncio.Condition()
        self._started_monotonic: float | None = None
        self._last_progress_at: datetime | None = None
        self._last_progress_monotonic: float | None = None

    async def mark_running(self) -> None:
        async with self._condition:
            self.status = "running"
            self.started_at = datetime.now(UTC)
            self.finished_at = None
            self.error_message = ""
            self._started_monotonic = time.monotonic()
            self._last_progress_at = self.started_at
            self._last_progress_monotonic = self._started_monotonic
            self._condition.notify_all()

    async def record_result(self, result: StoredResultReference) -> None:
        async with self._condition:
            self.results[result.input_id] = result
            self._pending_event_results.append(result)
            self._last_progress_at = datetime.now(UTC)
            self._last_progress_monotonic = time.monotonic()
            if len(self._pending_event_results) >= self.progress_batch_size:
                self._publish_progress_event()
            self._condition.notify_all()

    async def flush_progress_event(self) -> None:
        async with self._condition:
            if self._pending_event_results:
                self._publish_progress_event()
            self._condition.notify_all()

    async def publish_recovered(self) -> None:
        """Report results reused from storage, so a new envelope still publishes them."""
        async with self._condition:
            if self.results and not self.events:
                self._pending_event_results.extend(self.results.values())
                self._publish_progress_event()
                self._condition.notify_all()

    async def mark_completed(self, finished_at: datetime) -> None:
        async with self._condition:
            self.status = "completed"
            self.finished_at = finished_at
            self._condition.notify_all()

    async def mark_failed(self, error_message: str) -> None:
        async with self._condition:
            if self._pending_event_results:
                self._publish_progress_event()
            self.status = "failed"
            self.finished_at = datetime.now(UTC)
            self.error_message = error_message[:2_000]
            self._condition.notify_all()

    async def mark_cancelled(self) -> None:
        async with self._condition:
            if self._pending_event_results:
                self._publish_progress_event()
            self.status = "cancelled"
            self.finished_at = datetime.now(UTC)
            self._condition.notify_all()

    async def poll(self, *, after_event: int, wait_seconds: float) -> ScanPollResponse:
        async with self._condition:
            if (
                self._latest_event_sequence <= after_event
                and self.status in {"pending", "running"}
                and wait_seconds > 0
            ):
                try:
                    async with asyncio.timeout(wait_seconds):
                        await self._condition.wait_for(
                            lambda: (
                                self._latest_event_sequence > after_event
                                or self.status not in {"pending", "running"}
                            )
                        )
                except TimeoutError:
                    pass
            return ScanPollResponse(
                scan=self.snapshot(),
                events=[event for event in self.events if event.sequence > after_event],
            )

    def snapshot(self) -> ScanSnapshot:
        elapsed_seconds = self._elapsed_seconds()
        completed_count = len(self.results)
        outcomes = Counter(result.outcome for result in self.results.values())
        return ScanSnapshot(
            scan_id=self.scan_id,
            status=self.status,
            crawl_id=self.request.crawl_id,
            detector_version=self.request.detector_version,
            total_count=len(self.request.candidates),
            completed_count=completed_count,
            outcome_counts=dict(sorted(outcomes.items())),
            technology_count=sum(
                result.technology_count for result in self.results.values()
            ),
            started_at=self.started_at,
            finished_at=self.finished_at,
            last_progress_at=self._last_progress_at,
            elapsed_seconds=round(elapsed_seconds, 3),
            progress_age_seconds=round(self._progress_age_seconds(), 3),
            domains_per_minute=round(
                completed_count / elapsed_seconds * 60 if elapsed_seconds > 0 else 0.0,
                2,
            ),
            latest_event_sequence=self._latest_event_sequence,
            error_message=self.error_message,
        )

    @property
    def _latest_event_sequence(self) -> int:
        return self.events[-1].sequence if self.events else 0

    def _publish_progress_event(self) -> None:
        window = tuple(self._pending_event_results)
        self._pending_event_results.clear()
        elapsed_seconds = self._elapsed_seconds()
        completed_count = len(self.results)
        outcomes = Counter(result.outcome for result in window)
        event = ScanProgressEvent(
            sequence=self._latest_event_sequence + 1,
            completed_count=completed_count,
            total_count=len(self.request.candidates),
            window_count=len(window),
            window_outcome_counts=dict(sorted(outcomes.items())),
            window_technology_count=sum(result.technology_count for result in window),
            elapsed_seconds=round(elapsed_seconds, 3),
            domains_per_minute=round(
                completed_count / elapsed_seconds * 60 if elapsed_seconds > 0 else 0.0,
                2,
            ),
            results=list(window),
        )
        self.events.append(event)
        LOGGER.info(
            "Webtech scan progress scan_id=%s crawl_id=%s completed=%s/%s "
            "batch=%s outcomes=%s technologies=%s elapsed_seconds=%.1f "
            "rate_per_minute=%.2f sequence=%s",
            self.scan_id,
            self.request.crawl_id,
            event.completed_count,
            event.total_count,
            event.window_count,
            event.window_outcome_counts,
            event.window_technology_count,
            event.elapsed_seconds,
            event.domains_per_minute,
            event.sequence,
        )

    def _elapsed_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        if self.finished_at is not None:
            return max(0.0, (self.finished_at - self.started_at).total_seconds())
        if self._started_monotonic is not None:
            return max(0.0, time.monotonic() - self._started_monotonic)
        return max(0.0, (datetime.now(UTC) - self.started_at).total_seconds())

    def _progress_age_seconds(self) -> float:
        if self.status not in {"pending", "running"}:
            return 0.0
        if self._last_progress_monotonic is not None:
            return max(0.0, time.monotonic() - self._last_progress_monotonic)
        if self._last_progress_at is not None:
            return max(
                0.0,
                (datetime.now(UTC) - self._last_progress_at).total_seconds(),
            )
        return 0.0


class ScanCoordinator:
    """Own the workstation's single active CloakBrowser scan."""

    def __init__(
        self,
        *,
        settings: WebtechServiceSettings,
        store: RustfsStore,
        scan_function: ScanFunction = scan_webtech_candidates,
    ) -> None:
        self.settings = settings
        self.store = store
        self.scan_function = scan_function
        self.jobs: dict[str, ScanJob] = {}
        self.active_scan_id: str | None = None
        self._lock = asyncio.Lock()

    async def submit(self, request: ScanRequest) -> ScanSnapshot:
        """Start or reattach a scan; a new envelope of the same execution supersedes the old."""
        while True:
            async with self._lock:
                accepted = await self._submit_locked(request)
            if isinstance(accepted, ScanSnapshot):
                return accepted
            superseded = accepted
            # Cancel outside the lock: the cancelled run clears the active slot under it.
            LOGGER.warning(
                "Webtech scan superseded old=%s new=%s crawl_id=%s",
                superseded.scan_id,
                self._scan_id(request),
                request.crawl_id,
            )
            await self.cancel(superseded.scan_id)

    async def _submit_locked(self, request: ScanRequest) -> ScanSnapshot | ScanJob:
        """Return the accepted snapshot, or the active job of the same execution to cancel first."""
        if len(request.candidates) > self.settings.max_candidates:
            raise ValueError(
                f"scan request exceeds limit {self.settings.max_candidates}"
            )
        scan_id = self._scan_id(request)
        existing = self.jobs.get(scan_id)
        if existing is not None and existing.status in {"pending", "running"}:
            snapshot = existing.snapshot()
            LOGGER.info(
                "Webtech scan reattached scan_id=%s crawl_id=%s "
                "status=%s completed=%s/%s progress_age_seconds=%.1f",
                scan_id,
                request.crawl_id,
                snapshot.status,
                snapshot.completed_count,
                snapshot.total_count,
                snapshot.progress_age_seconds,
            )
            return snapshot
        if existing is not None and existing.status == "completed":
            snapshot = existing.snapshot()
            LOGGER.info(
                "Webtech scan reused scan_id=%s crawl_id=%s completed=%s/%s",
                scan_id,
                request.crawl_id,
                snapshot.completed_count,
                snapshot.total_count,
            )
            return snapshot
        if self.active_scan_id is not None:
            active = self.jobs.get(self.active_scan_id)
            if active is not None and active.status in {"pending", "running"}:
                if active.request.crawl_id == request.crawl_id:
                    return active
                raise ScanBusyError(f"scan {self.active_scan_id} is already running")

        recovered = await asyncio.to_thread(self._load_recovered_results, request)
        job = ScanJob(
            scan_id=scan_id,
            request=request,
            progress_batch_size=self.settings.progress_batch_size,
            recovered_results=recovered,
        )
        self.jobs[scan_id] = job
        self.active_scan_id = scan_id
        LOGGER.info(
            "Webtech scan accepted scan_id=%s crawl_id=%s total=%s recovered=%s",
            scan_id,
            request.crawl_id,
            len(request.candidates),
            len(recovered),
        )
        job.task = asyncio.create_task(
            self._run(job),
            name=f"webtech-scan-{scan_id}",
        )
        return job.snapshot()

    async def poll(
        self,
        scan_id: str,
        *,
        after_event: int,
        wait_seconds: float,
    ) -> ScanPollResponse:
        job = self.jobs.get(scan_id)
        if job is None:
            raise ScanNotFoundError(scan_id)
        return await job.poll(after_event=after_event, wait_seconds=wait_seconds)

    async def cancel(self, scan_id: str) -> ScanSnapshot:
        job = self.jobs.get(scan_id)
        if job is None:
            raise ScanNotFoundError(scan_id)
        if job.task is not None and not job.task.done():
            job.task.cancel()
            try:
                await job.task
            except asyncio.CancelledError:
                pass
        return job.snapshot()

    async def shutdown(self) -> None:
        tasks = [
            job.task
            for job in self.jobs.values()
            if job.task is not None and not job.task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self, job: ScanJob) -> None:
        await job.mark_running()
        await job.publish_recovered()
        remaining_candidates = tuple(
            WebtechCandidate(
                root_domain=candidate.root_domain,
                harmonic_rank=candidate.harmonic_rank,
                task_id=candidate.task_id,
                input_id=candidate.input_id,
                page_url=candidate.page_url,
            )
            for candidate in job.request.candidates
            if candidate.input_id not in job.results
        )
        LOGGER.info(
            "Webtech scan started scan_id=%s crawl_id=%s total=%s recovered=%s "
            "remaining=%s browsers=%s pages_per_browser=%s "
            "domain_timeout_seconds=%s domains_per_context=%s",
            job.scan_id,
            job.request.crawl_id,
            len(job.request.candidates),
            len(job.results),
            len(remaining_candidates),
            self.settings.browser_count,
            self.settings.pages_per_browser,
            self.settings.domain_timeout_seconds,
            self.settings.domains_per_context,
        )
        heartbeat_task = asyncio.create_task(
            self._log_progress_heartbeats(job),
            name=f"webtech-heartbeat-{job.scan_id}",
        )

        async def persist_result(result: WebtechDomainResult) -> None:
            document = _domain_result_document(job, result)
            location = self._execution_page_location(
                job.request, result.candidate.input_id
            )
            stored = await asyncio.to_thread(
                self.store.write_json,
                location,
                document,
            )
            reference = StoredResultReference(
                root_domain=result.candidate.root_domain,
                harmonic_rank=result.candidate.harmonic_rank,
                input_id=result.candidate.input_id,
                outcome=result.outcome,
                timeout_stage=result.timeout_stage,
                technology_count=(
                    len(result.report.technologies) if result.report is not None else 0
                ),
                duration_ms=result.duration_ms,
                object_key=stored.location.key,
                sha256=stored.sha256,
                size_bytes=stored.size_bytes,
            )
            await job.record_result(reference)

        try:
            if remaining_candidates:
                await self.scan_function(
                    remaining_candidates,
                    settings=self.settings.scanner_settings(),
                    progress_callback=persist_result,
                )
            if len(job.results) != len(job.request.candidates):
                raise RuntimeError(
                    "scanner finished without storing every candidate: "
                    f"stored={len(job.results)} total={len(job.request.candidates)}"
                )
            await job.flush_progress_event()
            await job.mark_completed(datetime.now(UTC))
            snapshot = job.snapshot()
            LOGGER.info(
                "Webtech scan completed scan_id=%s crawl_id=%s completed=%s/%s "
                "outcomes=%s technologies=%s elapsed_seconds=%.1f "
                "rate_per_minute=%.2f",
                job.scan_id,
                job.request.crawl_id,
                snapshot.completed_count,
                snapshot.total_count,
                snapshot.outcome_counts,
                snapshot.technology_count,
                snapshot.elapsed_seconds,
                snapshot.domains_per_minute,
            )
        except asyncio.CancelledError:
            if job.stall_reason is not None:
                await job.mark_failed(job.stall_reason)
                LOGGER.error(
                    "Webtech scan failed scan_id=%s crawl_id=%s completed=%s/%s "
                    "error=%s",
                    job.scan_id,
                    job.request.crawl_id,
                    len(job.results),
                    len(job.request.candidates),
                    job.stall_reason,
                )
                return
            await job.mark_cancelled()
            LOGGER.warning(
                "Webtech scan cancelled scan_id=%s crawl_id=%s completed=%s/%s",
                job.scan_id,
                job.request.crawl_id,
                len(job.results),
                len(job.request.candidates),
            )
            raise
        except Exception as error:
            LOGGER.exception(
                "Webtech scan failed scan_id=%s crawl_id=%s completed=%s/%s",
                job.scan_id,
                job.request.crawl_id,
                len(job.results),
                len(job.request.candidates),
            )
            await job.mark_failed(str(error) or type(error).__name__)
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            async with self._lock:
                if self.active_scan_id == job.scan_id:
                    self.active_scan_id = None

    async def _log_progress_heartbeats(self, job: ScanJob) -> None:
        while job.status in {"pending", "running"}:
            await asyncio.sleep(SCAN_HEARTBEAT_SECONDS)
            snapshot = job.snapshot()
            if snapshot.status not in {"pending", "running"}:
                return
            if snapshot.progress_age_seconds >= SCAN_STALL_FAIL_AFTER_SECONDS:
                self._fail_stalled_scan(job, snapshot)
                return
            if snapshot.progress_age_seconds >= SCAN_STALLED_AFTER_SECONDS:
                LOGGER.warning(
                    "Webtech scan stalled scan_id=%s crawl_id=%s "
                    "completed=%s/%s progress_age_seconds=%.1f "
                    "elapsed_seconds=%.1f rate_per_minute=%.2f",
                    snapshot.scan_id,
                    snapshot.crawl_id,
                    snapshot.completed_count,
                    snapshot.total_count,
                    snapshot.progress_age_seconds,
                    snapshot.elapsed_seconds,
                    snapshot.domains_per_minute,
                )
                continue
            LOGGER.info(
                "Webtech scan heartbeat scan_id=%s crawl_id=%s "
                "completed=%s/%s progress_age_seconds=%.1f "
                "elapsed_seconds=%.1f rate_per_minute=%.2f",
                snapshot.scan_id,
                snapshot.crawl_id,
                snapshot.completed_count,
                snapshot.total_count,
                snapshot.progress_age_seconds,
                snapshot.elapsed_seconds,
                snapshot.domains_per_minute,
            )

    def _fail_stalled_scan(self, job: ScanJob, snapshot: ScanSnapshot) -> None:
        """Give up on a scan that has made no progress for too long.

        Cancelling the scan task lets ``_run`` record the failure and free the
        single scan slot; a resubmission then recovers the stored results from
        RustFS and scans only the domains still missing.
        """
        remaining = snapshot.total_count - snapshot.completed_count
        job.stall_reason = (
            f"scan stalled: no progress for {snapshot.progress_age_seconds:.0f}s "
            f"with {remaining} domains remaining"
        )
        LOGGER.error(
            "Webtech scan stalled beyond limit; failing scan_id=%s crawl_id=%s "
            "completed=%s/%s progress_age_seconds=%.1f limit_seconds=%.0f",
            snapshot.scan_id,
            snapshot.crawl_id,
            snapshot.completed_count,
            snapshot.total_count,
            snapshot.progress_age_seconds,
            SCAN_STALL_FAIL_AFTER_SECONDS,
        )
        if job.task is not None and not job.task.done():
            job.task.cancel()

    def _load_recovered_results(
        self, request: ScanRequest
    ) -> dict[str, StoredResultReference]:
        """Recover pages this execution already stored, whichever envelope scanned them."""
        recovered: dict[str, StoredResultReference] = {}
        for candidate in request.candidates:
            location = self._execution_page_location(request, candidate.input_id)
            if not self.store.exists(location):
                continue
            body = self.store.read_bytes(location)
            document = StoredDomainResultDocument.model_validate_json(body)
            if (
                document.crawl_id != request.crawl_id
                or document.detector_version != request.detector_version
                or document.candidate != candidate
            ):
                raise ValueError(f"stored page identity mismatch: {location.key}")
            recovered[candidate.input_id] = _stored_reference(
                document, location.key, body
            )
        return recovered

    def _scan_id(self, request: ScanRequest) -> str:
        identity = {
            "crawl_id": request.crawl_id,
            "detector_version": request.detector_version,
            "input_ids": sorted(candidate.input_id for candidate in request.candidates),
            "scanner_settings": self.settings.public_scanner_settings(),
        }
        body = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]

    def _execution_page_location(
        self, request: ScanRequest, input_id: str
    ) -> S3Location:
        # One result per page per execution, shared by every envelope of that execution.
        return self.store.child(
            "scans",
            f"detector_version={request.detector_version}",
            f"crawl_id={request.crawl_id}",
            "pages",
            f"input_id={input_id}",
            "report.json",
        )


def _domain_result_document(
    job: ScanJob,
    result: WebtechDomainResult,
) -> StoredDomainResultDocument:
    return StoredDomainResultDocument(
        schema_version=1,
        scan_id=job.scan_id,
        crawl_id=job.request.crawl_id,
        # Envelopes are not stored, so pages carry no envelope label.
        partition_key="",
        detector_version=job.request.detector_version,
        candidate={
            "root_domain": result.candidate.root_domain,
            "harmonic_rank": result.candidate.harmonic_rank,
            "task_id": result.candidate.task_id,
            "input_id": result.candidate.input_id,
            "page_url": result.candidate.page_url,
        },
        outcome=result.outcome,
        requested_url=result.requested_url,
        final_url=result.final_url,
        final_hostname=urlsplit(result.final_url).hostname or "",
        http_fallback_used=result.http_fallback_used,
        scanned_at=result.scanned_at,
        duration_ms=result.duration_ms,
        error_message=result.error_message,
        timeout_stage=result.timeout_stage,
        report=result.report,
    )


def _stored_reference(
    document: StoredDomainResultDocument, key: str, body: bytes
) -> StoredResultReference:
    return StoredResultReference(
        root_domain=document.candidate.root_domain,
        harmonic_rank=document.candidate.harmonic_rank,
        input_id=document.candidate.input_id,
        outcome=document.outcome,
        timeout_stage=document.timeout_stage,
        technology_count=len(document.report.technologies)
        if document.report is not None
        else 0,
        duration_ms=document.duration_ms,
        object_key=key,
        sha256=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
    )
