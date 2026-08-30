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
    CandidateManifest,
    FinalScanManifest,
    ScanPollResponse,
    ScanProgressEvent,
    ScanRequest,
    ScanSnapshot,
    ScanStatus,
    StoredDomainResultDocument,
    StoredResultReference,
)

LOGGER = logging.getLogger(__name__)

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
        manifest: CandidateManifest,
        result_prefix: S3Location,
        final_manifest_location: S3Location,
        progress_batch_size: int,
        recovered_results: dict[str, StoredResultReference] | None = None,
    ) -> None:
        self.scan_id = scan_id
        self.request = request
        self.manifest = manifest
        self.result_prefix = result_prefix
        self.final_manifest_location = final_manifest_location
        self.progress_batch_size = progress_batch_size
        self.results = recovered_results or {}
        self.status: ScanStatus = "pending"
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None
        self.error_message = ""
        self.events: list[ScanProgressEvent] = []
        self.task: asyncio.Task[None] | None = None
        self._pending_event_results: list[StoredResultReference] = []
        self._condition = asyncio.Condition()
        self._started_monotonic: float | None = None

    @classmethod
    def from_final_manifest(
        cls,
        manifest: FinalScanManifest,
        *,
        request: ScanRequest,
        candidate_manifest: CandidateManifest,
        result_prefix: S3Location,
        final_manifest_location: S3Location,
        progress_batch_size: int,
    ) -> "ScanJob":
        job = cls(
            scan_id=manifest.scan_id,
            request=request,
            manifest=candidate_manifest,
            result_prefix=result_prefix,
            final_manifest_location=final_manifest_location,
            progress_batch_size=progress_batch_size,
            recovered_results={
                result.root_domain: result for result in manifest.results
            },
        )
        job.status = "completed"
        job.started_at = manifest.started_at
        job.finished_at = manifest.finished_at
        return job

    async def mark_running(self) -> None:
        async with self._condition:
            self.status = "running"
            self.started_at = datetime.now(UTC)
            self.finished_at = None
            self.error_message = ""
            self._started_monotonic = time.monotonic()
            self._condition.notify_all()

    async def record_result(self, result: StoredResultReference) -> None:
        async with self._condition:
            self.results[result.root_domain] = result
            self._pending_event_results.append(result)
            if len(self._pending_event_results) >= self.progress_batch_size:
                self._publish_progress_event()
            self._condition.notify_all()

    async def flush_progress_event(self) -> None:
        async with self._condition:
            if self._pending_event_results:
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
                            lambda: self._latest_event_sequence > after_event
                            or self.status not in {"pending", "running"}
                        )
                except TimeoutError:
                    pass
            return ScanPollResponse(
                scan=self.snapshot(),
                events=[
                    event
                    for event in self.events
                    if event.sequence > after_event
                ],
            )

    def snapshot(self) -> ScanSnapshot:
        elapsed_seconds = self._elapsed_seconds()
        completed_count = len(self.results)
        outcomes = Counter(result.outcome for result in self.results.values())
        return ScanSnapshot(
            scan_id=self.scan_id,
            status=self.status,
            crawl_id=self.request.crawl_id,
            partition_key=self.request.partition_key,
            detector_version=self.request.detector_version,
            candidate_manifest_uri=self.request.candidate_manifest_uri,
            result_prefix_uri=self.result_prefix.uri,
            final_manifest_uri=self.final_manifest_location.uri,
            total_count=len(self.manifest.candidates),
            completed_count=completed_count,
            outcome_counts=dict(sorted(outcomes.items())),
            technology_count=sum(
                result.technology_count for result in self.results.values()
            ),
            started_at=self.started_at,
            finished_at=self.finished_at,
            elapsed_seconds=round(elapsed_seconds, 3),
            domains_per_minute=round(
                completed_count / elapsed_seconds * 60
                if elapsed_seconds > 0
                else 0.0,
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
        self.events.append(
            ScanProgressEvent(
                sequence=self._latest_event_sequence + 1,
                completed_count=completed_count,
                total_count=len(self.manifest.candidates),
                window_count=len(window),
                window_outcome_counts=dict(sorted(outcomes.items())),
                window_technology_count=sum(
                    result.technology_count for result in window
                ),
                elapsed_seconds=round(elapsed_seconds, 3),
                domains_per_minute=round(
                    completed_count / elapsed_seconds * 60
                    if elapsed_seconds > 0
                    else 0.0,
                    2,
                ),
            )
        )

    def _elapsed_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        if self.finished_at is not None:
            return max(0.0, (self.finished_at - self.started_at).total_seconds())
        if self._started_monotonic is not None:
            return max(0.0, time.monotonic() - self._started_monotonic)
        return max(0.0, (datetime.now(UTC) - self.started_at).total_seconds())


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
        async with self._lock:
            manifest = await asyncio.to_thread(self._load_manifest, request)
            scan_id = self._scan_id(request)
            existing = self.jobs.get(scan_id)
            if existing is not None and existing.status in {"pending", "running"}:
                return existing.snapshot()
            if existing is not None and existing.status == "completed":
                return existing.snapshot()
            if self.active_scan_id is not None:
                active = self.jobs.get(self.active_scan_id)
                if active is not None and active.status in {"pending", "running"}:
                    raise ScanBusyError(
                        f"scan {self.active_scan_id} is already running"
                    )

            result_prefix, final_location = self._scan_locations(
                request=request,
                scan_id=scan_id,
            )
            final_manifest = await asyncio.to_thread(
                self._load_final_manifest,
                final_location,
            )
            if final_manifest is not None:
                if (
                    final_manifest.scan_id != scan_id
                    or final_manifest.crawl_id != request.crawl_id
                    or final_manifest.partition_key != request.partition_key
                    or final_manifest.detector_version != request.detector_version
                    or final_manifest.candidate_manifest_uri
                    != request.candidate_manifest_uri
                    or final_manifest.candidate_manifest_sha256
                    != request.candidate_manifest_sha256
                ):
                    raise ValueError("final manifest identity does not match the request")
                job = ScanJob.from_final_manifest(
                    final_manifest,
                    request=request,
                    candidate_manifest=manifest,
                    result_prefix=result_prefix,
                    final_manifest_location=final_location,
                    progress_batch_size=self.settings.progress_batch_size,
                )
                self.jobs[scan_id] = job
                return job.snapshot()

            recovered = await asyncio.to_thread(
                self._load_recovered_results,
                result_prefix,
                scan_id,
                manifest,
            )
            job = ScanJob(
                scan_id=scan_id,
                request=request,
                manifest=manifest,
                result_prefix=result_prefix,
                final_manifest_location=final_location,
                progress_batch_size=self.settings.progress_batch_size,
                recovered_results=recovered,
            )
            self.jobs[scan_id] = job
            self.active_scan_id = scan_id
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
        remaining_candidates = tuple(
            WebtechCandidate(
                root_domain=candidate.root_domain,
                harmonic_rank=candidate.harmonic_rank,
            )
            for candidate in job.manifest.candidates
            if candidate.root_domain not in job.results
        )

        async def persist_result(result: WebtechDomainResult) -> None:
            document = _domain_result_document(job, result)
            location = S3Location(
                bucket=job.result_prefix.bucket,
                key=(
                    f"{job.result_prefix.key}/"
                    f"root_domain={result.candidate.root_domain}/report.json"
                ),
            )
            stored = await asyncio.to_thread(
                self.store.write_json,
                location,
                document,
            )
            reference = StoredResultReference(
                root_domain=result.candidate.root_domain,
                harmonic_rank=result.candidate.harmonic_rank,
                outcome=result.outcome,
                timeout_stage=result.timeout_stage,
                technology_count=(
                    len(result.report.technologies)
                    if result.report is not None
                    else 0
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
            if len(job.results) != len(job.manifest.candidates):
                raise RuntimeError(
                    "scanner finished without storing every candidate: "
                    f"stored={len(job.results)} total={len(job.manifest.candidates)}"
                )
            await job.flush_progress_event()
            finished_at = datetime.now(UTC)
            final_manifest = _final_manifest(
                job,
                settings=self.settings,
                finished_at=finished_at,
            )
            await asyncio.to_thread(
                self.store.write_json,
                job.final_manifest_location,
                final_manifest,
            )
            await job.mark_completed(finished_at)
        except asyncio.CancelledError:
            await job.mark_cancelled()
            raise
        except Exception as error:
            LOGGER.exception("Webtech scan %s failed", job.scan_id)
            await job.mark_failed(str(error) or type(error).__name__)
        finally:
            async with self._lock:
                if self.active_scan_id == job.scan_id:
                    self.active_scan_id = None

    def _load_manifest(self, request: ScanRequest) -> CandidateManifest:
        location = self.store.parse_allowed_uri(request.candidate_manifest_uri)
        body = self.store.read_bytes(location)
        digest = hashlib.sha256(body).hexdigest()
        if digest != request.candidate_manifest_sha256:
            raise ValueError("candidate manifest SHA-256 does not match the request")
        manifest = CandidateManifest.model_validate_json(body)
        if (
            manifest.crawl_id != request.crawl_id
            or manifest.partition_key != request.partition_key
            or manifest.detector_version != request.detector_version
        ):
            raise ValueError("candidate manifest identity does not match the request")
        if len(manifest.candidates) > self.settings.max_candidates:
            raise ValueError(
                f"candidate manifest exceeds limit {self.settings.max_candidates}"
            )
        return manifest

    def _load_final_manifest(
        self,
        location: S3Location,
    ) -> FinalScanManifest | None:
        if not self.store.exists(location):
            return None
        return FinalScanManifest.model_validate_json(self.store.read_bytes(location))

    def _load_recovered_results(
        self,
        result_prefix: S3Location,
        scan_id: str,
        manifest: CandidateManifest,
    ) -> dict[str, StoredResultReference]:
        candidates_by_domain = {
            candidate.root_domain: candidate for candidate in manifest.candidates
        }
        recovered: dict[str, StoredResultReference] = {}
        listing_prefix = S3Location(
            bucket=result_prefix.bucket,
            key=f"{result_prefix.key}/",
        )
        for key in self.store.list_keys(listing_prefix):
            if not key.endswith("/report.json"):
                continue
            location = S3Location(bucket=result_prefix.bucket, key=key)
            body = self.store.read_bytes(location)
            document = StoredDomainResultDocument.model_validate_json(body)
            candidate = candidates_by_domain.get(document.candidate.root_domain)
            if (
                document.scan_id != scan_id
                or candidate is None
                or candidate.harmonic_rank != document.candidate.harmonic_rank
            ):
                raise ValueError(f"stored result identity mismatch: {key}")
            recovered[document.candidate.root_domain] = StoredResultReference(
                root_domain=document.candidate.root_domain,
                harmonic_rank=document.candidate.harmonic_rank,
                outcome=document.outcome,
                timeout_stage=document.timeout_stage,
                technology_count=(
                    len(document.report.technologies)
                    if document.report is not None
                    else 0
                ),
                duration_ms=document.duration_ms,
                object_key=key,
                sha256=hashlib.sha256(body).hexdigest(),
                size_bytes=len(body),
            )
        return recovered

    def _scan_id(self, request: ScanRequest) -> str:
        identity = {
            "candidate_manifest_sha256": request.candidate_manifest_sha256,
            "crawl_id": request.crawl_id,
            "detector_version": request.detector_version,
            "partition_key": request.partition_key,
            "scanner_settings": self.settings.public_scanner_settings(),
        }
        body = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]

    def _scan_locations(
        self,
        *,
        request: ScanRequest,
        scan_id: str,
    ) -> tuple[S3Location, S3Location]:
        scan_prefix = self.store.child(
            "scans",
            f"detector_version={request.detector_version}",
            f"crawl_id={request.crawl_id}",
            f"partition_key={request.partition_key}",
            f"scan_id={scan_id}",
        )
        result_prefix = S3Location(
            bucket=scan_prefix.bucket,
            key=f"{scan_prefix.key}/results",
        )
        final_location = S3Location(
            bucket=scan_prefix.bucket,
            key=f"{scan_prefix.key}/final-manifest.json",
        )
        return result_prefix, final_location


def _domain_result_document(
    job: ScanJob,
    result: WebtechDomainResult,
) -> StoredDomainResultDocument:
    return StoredDomainResultDocument(
        schema_version=1,
        scan_id=job.scan_id,
        crawl_id=job.request.crawl_id,
        partition_key=job.request.partition_key,
        detector_version=job.request.detector_version,
        candidate={
            "root_domain": result.candidate.root_domain,
            "harmonic_rank": result.candidate.harmonic_rank,
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


def _final_manifest(
    job: ScanJob,
    *,
    settings: WebtechServiceSettings,
    finished_at: datetime,
) -> FinalScanManifest:
    if job.started_at is None:
        raise RuntimeError("cannot finalize a scan that never started")
    ordered_results = sorted(
        job.results.values(),
        key=lambda result: (result.harmonic_rank, result.root_domain),
    )
    outcomes = Counter(result.outcome for result in ordered_results)
    return FinalScanManifest(
        schema_version=1,
        scan_id=job.scan_id,
        crawl_id=job.request.crawl_id,
        partition_key=job.request.partition_key,
        detector_version=job.request.detector_version,
        candidate_manifest_uri=job.request.candidate_manifest_uri,
        candidate_manifest_sha256=job.request.candidate_manifest_sha256,
        started_at=job.started_at,
        finished_at=finished_at,
        elapsed_seconds=max(
            0.0,
            (finished_at - job.started_at).total_seconds(),
        ),
        outcome_counts=dict(sorted(outcomes.items())),
        technology_count=sum(result.technology_count for result in ordered_results),
        scanner_settings=settings.public_scanner_settings(),
        results=ordered_results,
    )
