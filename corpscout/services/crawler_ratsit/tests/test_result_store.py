from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from crawler_ratsit.models import CrawlResult
from crawler_ratsit.result_store import RESULT_COLUMNS, RatsitResultStore


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def insert(
        self,
        table: str,
        data: list[list[Any]],
        *,
        column_names: tuple[str, ...],
        database: str,
    ) -> None:
        self.calls.append(
            {
                "table": table,
                "data": data,
                "column_names": column_names,
                "database": database,
            }
        )


def test_result_store_inserts_the_migration_column_contract() -> None:
    timestamp = datetime(2026, 8, 26, 10, 0, tzinfo=UTC).isoformat()
    result = CrawlResult(
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
        content_size_bytes=100,
        duration_ms=250,
        attempt_count=1,
        error_type="",
        error_message="",
        temporal_workflow_id="ratsit/test",
        temporal_run_id="run-id",
    )
    client = FakeClickHouseClient()

    RatsitResultStore(client, database="corpscout").record(result)

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["table"] == "se_company_ratsit_crawl_results"
    assert call["database"] == "corpscout"
    assert call["column_names"] == RESULT_COLUMNS
    row = call["data"][0]
    assert row[0] == result.company_id
    assert row[1] == UUID(result.batch_id)
    assert row[10] == result.content_size_bytes
    assert row[17] >= row[5]
