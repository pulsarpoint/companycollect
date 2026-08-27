import asyncio
import io
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError
from temporalio import activity
from temporalio.exceptions import ApplicationError

from crawler_ratsit.activities import RATE_LIMIT_RETRY_DELAY, RatsitActivities
from crawler_ratsit.crawler import RatsitRateLimitedError
from crawler_ratsit.models import CrawlActivityInput, FetchedPage
from crawler_ratsit.object_store import RatsitObjectStore
from crawler_ratsit.result_store import RatsitResultStore


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        object_identity = (Bucket, Key)
        if object_identity not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "GetObject",
            )
        return {"Body": io.BytesIO(self.objects[object_identity])}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        IfNoneMatch: str,
    ) -> None:
        assert ContentType == "application/json"
        assert IfNoneMatch == "*"
        object_identity = (Bucket, Key)
        if object_identity in self.objects:
            raise ClientError(
                {
                    "Error": {
                        "Code": "PreconditionFailed",
                        "Message": "already exists",
                    }
                },
                "PutObject",
            )
        self.objects[object_identity] = Body


class UnusedClickHouseClient:
    def insert(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("ClickHouse is not used by the capture activity")


def test_capture_activity_reuses_existing_s3_response() -> None:
    async def run_test() -> None:
        crawl_calls = 0

        async def crawl_page(company_id: str) -> FetchedPage:
            nonlocal crawl_calls
            crawl_calls += 1
            return FetchedPage(
                outcome="success",
                requested_url=f"https://www.ratsit.se/{company_id}",
                final_url=f"https://www.ratsit.se/{company_id}",
                http_status=200,
                content="<h1>Räksmörgås AB</h1>",
                error_type="",
                error_message="",
            )

        s3_client = FakeS3Client()
        activities = RatsitActivities(
            crawl_page=crawl_page,
            object_store=RatsitObjectStore(
                s3_client,
                bucket="source-sweden-ratsit",
                prefix="raw",
            ),
            result_store=RatsitResultStore(
                UnusedClickHouseClient(),
                database="corpscout",
            ),
        )
        activity_input = CrawlActivityInput(
            company_id="5562434182",
            batch_id=str(uuid4()),
            selected_at=datetime(2020, 1, 1, tzinfo=UTC).isoformat(),
            temporal_workflow_id="ratsit/test",
            temporal_run_id="run-id",
        )

        first = await activities.capture_company(activity_input, attempt_number=1)
        second = await activities.capture_company(activity_input, attempt_number=2)

        assert crawl_calls == 1
        assert second == first
        assert first.content_size_bytes == len("<h1>Räksmörgås AB</h1>".encode())
        stored_body = next(iter(s3_client.objects.values()))
        assert json.loads(stored_body)["result"]["company_id"] == "5562434182"

    asyncio.run(run_test())


def test_rate_limited_activity_requests_a_ten_minute_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def rate_limited_crawl(_company_id: str) -> FetchedPage:
        raise RatsitRateLimitedError("Ratsit returned retryable HTTP status 429")

    activities = RatsitActivities(
        crawl_page=rate_limited_crawl,
        object_store=RatsitObjectStore(
            FakeS3Client(),
            bucket="source-sweden-ratsit",
            prefix="raw",
        ),
        result_store=RatsitResultStore(
            UnusedClickHouseClient(),
            database="corpscout",
        ),
    )
    activity_input = CrawlActivityInput(
        company_id="5562434182",
        batch_id=str(uuid4()),
        selected_at=datetime(2020, 1, 1, tzinfo=UTC).isoformat(),
        temporal_workflow_id="ratsit/test",
        temporal_run_id="run-id",
    )
    monkeypatch.setattr(activity, "info", lambda: SimpleNamespace(attempt=1))

    with pytest.raises(ApplicationError) as captured:
        asyncio.run(activities.crawl_and_upload_company(activity_input))

    assert captured.value.type == "ratsit_rate_limited"
    assert captured.value.next_retry_delay == timedelta(minutes=10)
    assert RATE_LIMIT_RETRY_DELAY == timedelta(minutes=10)
