from datetime import UTC, datetime
from uuid import uuid4

import pytest

from crawler_ratsit.models import (
    CrawlCompanyInput,
    CrawlResult,
    ratsit_url,
    response_envelope,
    result_from_response_envelope,
)


def test_company_input_accepts_ten_or_twelve_ascii_digits_and_requires_uuid() -> None:
    batch_id = str(uuid4())

    assert CrawlCompanyInput(company_id="5562434182", batch_id=batch_id) == (
        CrawlCompanyInput(company_id="5562434182", batch_id=batch_id)
    )
    assert CrawlCompanyInput(company_id="195562434182", batch_id=batch_id) == (
        CrawlCompanyInput(company_id="195562434182", batch_id=batch_id)
    )
    assert ratsit_url("5562434182") == "https://www.ratsit.se/5562434182"
    assert ratsit_url("195562434182") == "https://www.ratsit.se/5562434182"
    with pytest.raises(ValueError, match="ten or twelve ASCII digits"):
        CrawlCompanyInput(company_id="556-2434182", batch_id=batch_id)
    with pytest.raises(ValueError, match="batch_id must be a UUID"):
        CrawlCompanyInput(company_id="5562434182", batch_id="batch-1")


def test_response_envelope_round_trips_result_and_checks_content_size() -> None:
    content = "räks"
    result = _success_result(content_size_bytes=len(content.encode("utf-8")))
    envelope = response_envelope(
        result,
        final_url="https://www.ratsit.se/5562434182",
        content=content,
    )

    assert (
        result_from_response_envelope(
            envelope,
            expected_company_id=result.company_id,
            expected_batch_id=result.batch_id,
        )
        == result
    )

    envelope["content"] = "truncated"
    with pytest.raises(ValueError, match="content size"):
        result_from_response_envelope(
            envelope,
            expected_company_id=result.company_id,
            expected_batch_id=result.batch_id,
        )


def _success_result(*, content_size_bytes: int) -> CrawlResult:
    timestamp = datetime(2026, 8, 26, 10, 0, tzinfo=UTC).isoformat()
    return CrawlResult(
        company_id="5562434182",
        batch_id=str(uuid4()),
        outcome="success",
        selected_at=timestamp,
        attempted_at=timestamp,
        completed_at=timestamp,
        http_status=200,
        source_url="https://www.ratsit.se/5562434182",
        source_bucket="source-sweden-ratsit",
        source_object_key="raw/response.json",
        content_size_bytes=content_size_bytes,
        duration_ms=100,
        attempt_count=1,
        error_type="",
        error_message="",
        temporal_workflow_id="ratsit/test",
        temporal_run_id="run-id",
    )
