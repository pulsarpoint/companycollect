import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic

from temporalio import activity
from temporalio.exceptions import ApplicationError

from crawler_ratsit.constants import (
    CRAWL_AND_UPLOAD_ACTIVITY,
    RECORD_RESULT_ACTIVITY,
)
from crawler_ratsit.crawler import RatsitRateLimitedError
from crawler_ratsit.models import (
    CrawlActivityInput,
    CrawlResult,
    FetchedPage,
    identity_kind,
    identity_sha256,
    response_envelope,
    result_from_response_envelope,
)
from crawler_ratsit.object_store import RatsitObjectStore
from crawler_ratsit.result_store import RatsitResultStore

type CrawlPage = Callable[[str], Awaitable[FetchedPage]]

RATE_LIMIT_RETRY_DELAY = timedelta(minutes=10)
LOGGER = logging.getLogger(__name__)


@dataclass
class CrawlBrowser:
    browser_id: str
    crawl_page: CrawlPage
    next_crawl_at: float = 0.0

    def __post_init__(self) -> None:
        if not self.browser_id:
            raise ValueError("browser_id must not be blank")


class RatsitCrawlActivities:
    def __init__(
        self,
        *,
        browsers: tuple[CrawlBrowser, ...],
        per_browser_activities_per_second: float,
        object_store: RatsitObjectStore,
    ) -> None:
        if not browsers:
            raise ValueError("at least one crawl browser is required")
        if per_browser_activities_per_second <= 0:
            raise ValueError("per-browser activity rate must be positive")
        browser_ids = {browser.browser_id for browser in browsers}
        if len(browser_ids) != len(browsers):
            raise ValueError("crawl browser IDs must be unique")

        self._object_store = object_store
        self._browser_interval_seconds = 1 / per_browser_activities_per_second
        self._available_browsers: asyncio.Queue[CrawlBrowser] = asyncio.Queue(
            maxsize=len(browsers)
        )
        for browser in browsers:
            self._available_browsers.put_nowait(browser)

    @activity.defn(name=CRAWL_AND_UPLOAD_ACTIVITY)
    async def crawl_and_upload_company(
        self,
        activity_input: CrawlActivityInput,
    ) -> CrawlResult:
        return await self.capture_company(
            activity_input,
            attempt_number=activity.info().attempt,
        )

    async def capture_company(
        self,
        activity_input: CrawlActivityInput,
        *,
        attempt_number: int,
    ) -> CrawlResult:
        object_key = self._object_store.response_key(
            company_id=activity_input.company_id,
            batch_id=activity_input.batch_id,
        )
        existing = await asyncio.to_thread(
            self._object_store.read_json_if_exists,
            object_key,
        )
        if existing is not None:
            result = result_from_response_envelope(
                existing,
                expected_company_id=activity_input.company_id,
                expected_batch_id=activity_input.batch_id,
            )
            _log_reused_response(
                result,
                browser_id=existing.get("browser_id", "unknown"),
                attempt_number=attempt_number,
            )
            return result

        browser = await self._available_browsers.get()
        try:
            wait_seconds = browser.next_crawl_at - monotonic()
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            browser.next_crawl_at = monotonic() + self._browser_interval_seconds

            attempted_at = datetime.now(UTC)
            started = monotonic()
            LOGGER.info(
                "event=ratsit_crawl_started browser_id=%s identity_sha256=%s "
                "identity_kind=%s batch_id=%s attempt=%d run_id=%s",
                browser.browser_id,
                identity_sha256(activity_input.company_id),
                identity_kind(activity_input.company_id),
                activity_input.batch_id,
                attempt_number,
                activity_input.temporal_run_id,
            )
            try:
                page = await browser.crawl_page(activity_input.company_id)
            except RatsitRateLimitedError as error:
                LOGGER.warning(
                    "event=ratsit_crawl_rate_limited browser_id=%s "
                    "identity_sha256=%s identity_kind=%s batch_id=%s "
                    "http_status=429 attempt=%d run_id=%s",
                    browser.browser_id,
                    identity_sha256(activity_input.company_id),
                    identity_kind(activity_input.company_id),
                    activity_input.batch_id,
                    attempt_number,
                    activity_input.temporal_run_id,
                )
                raise ApplicationError(
                    str(error),
                    type="ratsit_rate_limited",
                    next_retry_delay=RATE_LIMIT_RETRY_DELAY,
                ) from error
            completed_at = datetime.now(UTC)
            duration_ms = max(0, int((monotonic() - started) * 1000))
            browser_id = browser.browser_id
        finally:
            self._available_browsers.put_nowait(browser)

        result = CrawlResult(
            company_id=activity_input.company_id,
            batch_id=activity_input.batch_id,
            outcome=page.outcome,
            selected_at=activity_input.selected_at,
            attempted_at=attempted_at.isoformat(),
            completed_at=completed_at.isoformat(),
            http_status=page.http_status,
            source_url=page.requested_url,
            source_bucket=(
                self._object_store.bucket if page.outcome == "success" else ""
            ),
            source_object_key=object_key if page.outcome == "success" else "",
            content_size_bytes=(
                len(page.content.encode("utf-8")) if page.outcome == "success" else 0
            ),
            duration_ms=duration_ms,
            attempt_count=attempt_number,
            error_type=page.error_type,
            error_message=page.error_message,
            temporal_workflow_id=activity_input.temporal_workflow_id,
            temporal_run_id=activity_input.temporal_run_id,
        )
        if page.outcome != "success":
            _log_completed_result(result, browser_id=browser_id)
            return result

        created = await asyncio.to_thread(
            self._object_store.write_json_if_absent,
            object_key,
            response_envelope(
                result,
                browser_id=browser_id,
                final_url=page.final_url,
                content=page.content,
            ),
        )
        if created:
            _log_completed_result(result, browser_id=browser_id)
            return result

        existing = await asyncio.to_thread(
            self._object_store.read_json_if_exists,
            object_key,
        )
        if existing is None:
            raise RuntimeError(
                f"S3 object {object_key} disappeared after a conditional-write conflict"
            )
        result = result_from_response_envelope(
            existing,
            expected_company_id=activity_input.company_id,
            expected_batch_id=activity_input.batch_id,
        )
        _log_reused_response(
            result,
            browser_id=existing.get("browser_id", "unknown"),
            attempt_number=attempt_number,
        )
        return result


class RatsitResultActivities:
    def __init__(self, *, result_store: RatsitResultStore) -> None:
        self._result_store = result_store

    @activity.defn(name=RECORD_RESULT_ACTIVITY)
    async def record_crawl_result(self, result: CrawlResult) -> None:
        await asyncio.to_thread(self._result_store.record, result)
        LOGGER.info(
            "event=ratsit_result_recorded identity_sha256=%s identity_kind=%s "
            "batch_id=%s outcome=%s http_status=%s content_bytes=%d attempt=%d "
            "s3_stored=%s s3_bucket=%s s3_key=%s run_id=%s",
            identity_sha256(result.company_id),
            identity_kind(result.company_id),
            result.batch_id,
            result.outcome,
            result.http_status,
            result.content_size_bytes,
            result.attempt_count,
            bool(result.source_object_key),
            result.source_bucket,
            result.source_object_key,
            result.temporal_run_id,
        )


def _log_completed_result(result: CrawlResult, *, browser_id: str) -> None:
    LOGGER.info(
        "event=ratsit_crawl_completed browser_id=%s identity_sha256=%s "
        "identity_kind=%s batch_id=%s outcome=%s http_status=%s "
        "content_bytes=%d duration_ms=%d attempt=%d s3_stored=%s "
        "s3_bucket=%s s3_key=%s run_id=%s",
        browser_id,
        identity_sha256(result.company_id),
        identity_kind(result.company_id),
        result.batch_id,
        result.outcome,
        result.http_status,
        result.content_size_bytes,
        result.duration_ms,
        result.attempt_count,
        bool(result.source_object_key),
        result.source_bucket,
        result.source_object_key,
        result.temporal_run_id,
    )


def _log_reused_response(
    result: CrawlResult,
    *,
    browser_id: object,
    attempt_number: int,
) -> None:
    LOGGER.info(
        "event=ratsit_s3_response_reused browser_id=%s identity_sha256=%s "
        "identity_kind=%s batch_id=%s outcome=%s http_status=%s "
        "content_bytes=%d attempt=%d s3_bucket=%s s3_key=%s run_id=%s",
        browser_id,
        identity_sha256(result.company_id),
        identity_kind(result.company_id),
        result.batch_id,
        result.outcome,
        result.http_status,
        result.content_size_bytes,
        attempt_number,
        result.source_bucket,
        result.source_object_key,
        result.temporal_run_id,
    )
