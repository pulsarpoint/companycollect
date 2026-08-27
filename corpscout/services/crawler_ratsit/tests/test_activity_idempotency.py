import asyncio
import io
import json
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError
from temporalio import activity
from temporalio.exceptions import ApplicationError

import crawler_ratsit.activities as activities_module
from crawler_ratsit.activities import (
    RATE_LIMIT_RETRY_DELAY,
    CrawlBrowser,
    RatsitCrawlActivities,
)
from crawler_ratsit.crawler import RatsitRateLimitedError
from crawler_ratsit.models import CrawlActivityInput, FetchedPage, identity_sha256
from crawler_ratsit.object_store import RatsitObjectStore


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


def test_capture_activity_reuses_existing_s3_response(
    caplog: pytest.LogCaptureFixture,
) -> None:
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
        activities = RatsitCrawlActivities(
            browsers=(CrawlBrowser(browser_id="direct", crawl_page=crawl_page),),
            per_browser_activities_per_second=0.2,
            object_store=RatsitObjectStore(
                s3_client,
                bucket="source-sweden-ratsit",
                prefix="raw",
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
        stored_response = json.loads(stored_body)
        assert stored_response["browser_id"] == "direct"
        assert stored_response["result"]["company_id"] == "5562434182"

    caplog.set_level(logging.INFO, logger="crawler_ratsit.activities")
    asyncio.run(run_test())

    completion_log = next(
        record.message
        for record in caplog.records
        if "event=ratsit_crawl_completed" in record.message
    )
    assert f"identity_sha256={identity_sha256('5562434182')}" in completion_log
    assert "identity_kind=company" in completion_log
    assert "company_id=" not in completion_log
    assert "workflow_id=" not in completion_log
    assert "http_status=200" in completion_log
    assert "s3_stored=True" in completion_log

    reuse_log = next(
        record.message
        for record in caplog.records
        if "event=ratsit_s3_response_reused" in record.message
    )
    assert "browser_id=direct" in reuse_log
    assert "attempt=2" in reuse_log


def test_capture_activity_keeps_not_found_result_out_of_s3(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def run_test() -> None:
        async def crawl_page(company_id: str) -> FetchedPage:
            return FetchedPage(
                outcome="not_found",
                requested_url=f"https://www.ratsit.se/{company_id[-10:]}",
                final_url="https://www.ratsit.se/foretag?saknas",
                http_status=200,
                content="",
                error_type="ratsit_missing",
                error_message="Ratsit redirected to /foretag?saknas",
                diagnostic_content="<html><body>Missing</body></html>",
            )

        s3_client = FakeS3Client()
        activities = RatsitCrawlActivities(
            browsers=(CrawlBrowser(browser_id="direct", crawl_page=crawl_page),),
            per_browser_activities_per_second=0.2,
            object_store=RatsitObjectStore(
                s3_client,
                bucket="source-sweden-ratsit",
                prefix="raw",
            ),
        )
        result = await activities.capture_company(
            CrawlActivityInput(
                company_id="195562434182",
                batch_id=str(uuid4()),
                selected_at=datetime(2020, 1, 1, tzinfo=UTC).isoformat(),
                temporal_workflow_id="ratsit/test",
                temporal_run_id="run-id",
            ),
            attempt_number=1,
        )

        assert result.outcome == "not_found"
        assert result.source_bucket == ""
        assert result.source_object_key == ""
        assert result.content_size_bytes == 0
        assert result.error_type == "ratsit_missing"
        assert not s3_client.objects

    caplog.set_level(logging.INFO, logger="crawler_ratsit.activities")
    asyncio.run(run_test())

    completion_log = next(
        record.message
        for record in caplog.records
        if "event=ratsit_crawl_completed" in record.message
    )
    assert "identity_kind=individual_owner" in completion_log
    assert "outcome=not_found" in completion_log
    assert "content_bytes=0" in completion_log
    assert "s3_stored=False" in completion_log


def test_rate_limited_activity_requests_a_ten_minute_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def rate_limited_crawl(_company_id: str) -> FetchedPage:
        raise RatsitRateLimitedError("Ratsit returned retryable HTTP status 429")

    activities = RatsitCrawlActivities(
        browsers=(CrawlBrowser(browser_id="proxy1", crawl_page=rate_limited_crawl),),
        per_browser_activities_per_second=0.2,
        object_store=RatsitObjectStore(
            FakeS3Client(),
            bucket="source-sweden-ratsit",
            prefix="raw",
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


def test_browser_pool_leases_each_browser_exclusively() -> None:
    async def run_test() -> None:
        active_browsers: set[str] = set()
        used_browsers: list[str] = []
        maximum_concurrency = 0

        def crawl_page_for(browser_id: str):
            async def crawl_page(company_id: str) -> FetchedPage:
                nonlocal maximum_concurrency
                assert browser_id not in active_browsers
                active_browsers.add(browser_id)
                used_browsers.append(browser_id)
                maximum_concurrency = max(maximum_concurrency, len(active_browsers))
                await asyncio.sleep(0.01)
                active_browsers.remove(browser_id)
                return FetchedPage(
                    outcome="success",
                    requested_url=f"https://www.ratsit.se/{company_id}",
                    final_url=f"https://www.ratsit.se/{company_id}",
                    http_status=200,
                    content=f"<h1>{company_id}</h1>",
                    error_type="",
                    error_message="",
                )

            return crawl_page

        activities = RatsitCrawlActivities(
            browsers=(
                CrawlBrowser(
                    browser_id="direct",
                    crawl_page=crawl_page_for("direct"),
                ),
                CrawlBrowser(
                    browser_id="proxy1",
                    crawl_page=crawl_page_for("proxy1"),
                ),
            ),
            per_browser_activities_per_second=1000,
            object_store=RatsitObjectStore(
                FakeS3Client(),
                bucket="source-sweden-ratsit",
                prefix="raw",
            ),
        )
        company_ids = ("5562434182", "5562434183", "5562434184", "5562434185")
        await asyncio.gather(
            *(
                activities.capture_company(
                    CrawlActivityInput(
                        company_id=company_id,
                        batch_id=str(uuid4()),
                        selected_at=datetime(2020, 1, 1, tzinfo=UTC).isoformat(),
                        temporal_workflow_id=f"ratsit/{company_id}",
                        temporal_run_id=f"run-{company_id}",
                    ),
                    attempt_number=1,
                )
                for company_id in company_ids
            )
        )

        assert maximum_concurrency == 2
        assert set(used_browsers) == {"direct", "proxy1"}

    asyncio.run(run_test())


def test_browser_pool_enforces_the_per_browser_start_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_time = 0.0
    sleep_delays: list[float] = []

    def fake_monotonic() -> float:
        return current_time

    async def fake_sleep(delay: float) -> None:
        nonlocal current_time
        sleep_delays.append(delay)
        current_time += delay

    async def run_test() -> None:
        async def crawl_page(company_id: str) -> FetchedPage:
            return FetchedPage(
                outcome="success",
                requested_url=f"https://www.ratsit.se/{company_id}",
                final_url=f"https://www.ratsit.se/{company_id}",
                http_status=200,
                content=f"<h1>{company_id}</h1>",
                error_type="",
                error_message="",
            )

        activities = RatsitCrawlActivities(
            browsers=(CrawlBrowser(browser_id="direct", crawl_page=crawl_page),),
            per_browser_activities_per_second=0.2,
            object_store=RatsitObjectStore(
                FakeS3Client(),
                bucket="source-sweden-ratsit",
                prefix="raw",
            ),
        )
        for company_id in ("5562434182", "5562434183"):
            await activities.capture_company(
                CrawlActivityInput(
                    company_id=company_id,
                    batch_id=str(uuid4()),
                    selected_at=datetime(2020, 1, 1, tzinfo=UTC).isoformat(),
                    temporal_workflow_id=f"ratsit/{company_id}",
                    temporal_run_id=f"run-{company_id}",
                ),
                attempt_number=1,
            )

    monkeypatch.setattr(activities_module, "monotonic", fake_monotonic)
    monkeypatch.setattr(activities_module.asyncio, "sleep", fake_sleep)
    asyncio.run(run_test())

    assert sleep_delays == [5.0]
