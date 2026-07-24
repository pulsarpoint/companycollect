import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from dagster_v3.defs.sweden_uhm_procurement.clickhouse import (
    uhm_awards_insert_sql,
)
from dagster_v3.defs.sweden_uhm_procurement.resources import sync_uhm_snapshot


class _Response:
    def __init__(self, body: bytes, *, content_length: int | None = None) -> None:
        self.body = body
        self.headers = {
            "Content-Type": "text/csv",
            "Last-Modified": "Tue, 28 Apr 2026 14:58:13 GMT",
            "ETag": '"test-etag"',
        }
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [
            self.body[offset : offset + chunk_size]
            for offset in range(0, len(self.body), chunk_size)
        ]


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls = 0

    def get(self, url: str, *, timeout: int, stream: bool) -> _Response:
        response = self.responses[self.calls]
        self.calls += 1
        return response


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        pass

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return key in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        self.objects[key] = Path(source_path).read_bytes()

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        self.objects[key] = body.encode()


def test_sync_snapshot_is_content_addressed_and_reusable() -> None:
    body = (
        "\ufeffÅr;Upphandlings-ID;Organisationsnummer för leverantör\n"
        "2024;PROC-1;5565338133\n"
    ).encode()
    store = _ObjectStore()
    retrieved_at = datetime(2026, 7, 23, 14, 25, 23, tzinfo=UTC)

    first = sync_uhm_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="run-1",
        retrieved_at=retrieved_at,
        session=_Session([_Response(body, content_length=len(body))]),  # type: ignore[arg-type]
        minimum_size_bytes=1,
        minimum_data_rows=1,
        download_attempts=1,
    )
    second = sync_uhm_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="run-2",
        retrieved_at=retrieved_at,
        session=_Session([_Response(body, content_length=len(body))]),  # type: ignore[arg-type]
        minimum_size_bytes=1,
        minimum_data_rows=1,
        download_attempts=1,
    )

    assert first.object_key == second.object_key
    assert "retrieved_date=2026-07-23" in first.object_key
    assert f"sha256={first.sha256}" in first.object_key
    assert first.downloaded is True
    assert second.downloaded is False
    assert store.objects[first.object_key] == body

    manifest = json.loads(store.objects[first.manifest_key])
    assert manifest["source_run_id"] == "run-1"
    assert manifest["sha256"] == first.sha256
    assert manifest["size_bytes"] == len(body)
    assert manifest["etag"] == '"test-etag"'


def test_sync_snapshot_refuses_short_content_length() -> None:
    body = b"header\nrow\n"
    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        sync_uhm_snapshot(
            object_store=_ObjectStore(),  # type: ignore[arg-type]
            run_id="run",
            retrieved_at=datetime(2026, 7, 23, tzinfo=UTC),
            session=_Session([_Response(body, content_length=len(body) + 10)]),  # type: ignore[arg-type]
            minimum_size_bytes=1,
            minimum_data_rows=1,
            download_attempts=1,
        )


def test_clickhouse_publish_retains_unmatched_supplier_observations() -> None:
    sql = uhm_awards_insert_sql(
        candidate_table="corpscout.candidates",
        awards_stage="corpscout.awards_stage",
    )

    assert "LEFT ANY JOIN corpscout.se_companies" in sql
    assert "'exact'" in sql
    assert "'unmatched_company'" in sql
    assert "u.match_eligibility" in sql
    assert "WHERE u.match_eligibility = 'eligible'" not in sql
