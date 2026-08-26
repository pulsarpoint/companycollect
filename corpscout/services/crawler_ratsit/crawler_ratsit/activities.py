import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from time import monotonic

from temporalio import activity

from crawler_ratsit.constants import (
    CRAWL_AND_UPLOAD_ACTIVITY,
    RECORD_RESULT_ACTIVITY,
)
from crawler_ratsit.models import (
    CrawlActivityInput,
    CrawlResult,
    FetchedPage,
    response_envelope,
    result_from_response_envelope,
)
from crawler_ratsit.object_store import RatsitObjectStore
from crawler_ratsit.result_store import RatsitResultStore


type CrawlPage = Callable[[str], Awaitable[FetchedPage]]


class RatsitActivities:
    def __init__(
        self,
        *,
        crawl_page: CrawlPage,
        object_store: RatsitObjectStore,
        result_store: RatsitResultStore,
    ) -> None:
        self._crawl_page = crawl_page
        self._object_store = object_store
        self._result_store = result_store

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
            return result_from_response_envelope(
                existing,
                expected_company_id=activity_input.company_id,
                expected_batch_id=activity_input.batch_id,
            )

        attempted_at = datetime.now(UTC)
        started = monotonic()
        page = await self._crawl_page(activity_input.company_id)
        completed_at = datetime.now(UTC)
        result = CrawlResult(
            company_id=activity_input.company_id,
            batch_id=activity_input.batch_id,
            outcome=page.outcome,
            selected_at=activity_input.selected_at,
            attempted_at=attempted_at.isoformat(),
            completed_at=completed_at.isoformat(),
            http_status=page.http_status,
            source_url=page.requested_url,
            source_bucket=self._object_store.bucket,
            source_object_key=object_key,
            content_size_bytes=len(page.content.encode("utf-8")),
            duration_ms=max(0, int((monotonic() - started) * 1000)),
            attempt_count=attempt_number,
            error_type=page.error_type,
            error_message=page.error_message,
            temporal_workflow_id=activity_input.temporal_workflow_id,
            temporal_run_id=activity_input.temporal_run_id,
        )
        created = await asyncio.to_thread(
            self._object_store.write_json_if_absent,
            object_key,
            response_envelope(
                result,
                final_url=page.final_url,
                content=page.content,
            ),
        )
        if created:
            return result

        existing = await asyncio.to_thread(
            self._object_store.read_json_if_exists,
            object_key,
        )
        if existing is None:
            raise RuntimeError(
                f"S3 object {object_key} disappeared after a conditional-write conflict"
            )
        return result_from_response_envelope(
            existing,
            expected_company_id=activity_input.company_id,
            expected_batch_id=activity_input.batch_id,
        )

    @activity.defn(name=RECORD_RESULT_ACTIVITY)
    async def record_crawl_result(self, result: CrawlResult) -> None:
        await asyncio.to_thread(self._result_store.record, result)
