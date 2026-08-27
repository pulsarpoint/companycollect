from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from crawler_ratsit.models import CrawlResult

RESULT_COLUMNS = (
    "company_id",
    "batch_id",
    "outcome",
    "selected_at",
    "attempted_at",
    "completed_at",
    "http_status",
    "source_url",
    "source_bucket",
    "source_object_key",
    "content_size_bytes",
    "duration_ms",
    "attempt_count",
    "error_type",
    "error_message",
    "temporal_workflow_id",
    "temporal_run_id",
    "recorded_at",
)


class RatsitResultStore:
    def __init__(self, client: Any, *, database: str) -> None:
        self._client = client
        self._database = database

    def record(self, result: CrawlResult) -> None:
        selected_at = datetime.fromisoformat(result.selected_at)
        attempted_at = datetime.fromisoformat(result.attempted_at)
        completed_at = datetime.fromisoformat(result.completed_at)
        recorded_at = max(datetime.now(UTC), completed_at)
        self._client.insert(
            "se_company_ratsit_crawl_results",
            [
                [
                    result.company_id,
                    UUID(result.batch_id),
                    result.outcome,
                    selected_at,
                    attempted_at,
                    completed_at,
                    result.http_status,
                    result.source_url,
                    result.source_bucket,
                    result.source_object_key,
                    result.content_size_bytes,
                    result.duration_ms,
                    result.attempt_count,
                    result.error_type,
                    result.error_message,
                    result.temporal_workflow_id,
                    result.temporal_run_id,
                    recorded_at,
                ]
            ],
            column_names=RESULT_COLUMNS,
            database=self._database,
        )
