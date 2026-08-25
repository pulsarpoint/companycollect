import json
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.serbia_apr_companies import tables
from dagster_v3.defs.serbia_apr_companies.resources import (
    APR_INTERMEDIATE_CA_PATH,
    apr_companies_http_session,
    sync_apr_companies_snapshot,
)


class _Response:
    def __init__(self, body: bytes, *, content_length: int | None = None) -> None:
        self.body = body
        self.headers = {"Content-Type": "application/json"}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> list[bytes]:
        midpoint = max(len(self.body) // 2, 1)
        return [self.body[:midpoint], self.body[midpoint:]]


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls = 0

    def get(self, url: str, *, timeout: int, stream: bool) -> _Response:
        assert url == tables.SOURCE_URL
        assert stream is True
        response = self.responses[self.calls]
        self.calls += 1
        return response


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_keys: list[str] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket == tables.S3_BUCKET
        return key in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket == tables.S3_BUCKET
        self.uploaded_keys.append(key)
        self.objects[key] = Path(source_path).read_bytes()

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        assert bucket == tables.S3_BUCKET
        self.objects[key] = body.encode("utf-8")


def _snapshot_body(snapshot_date: str = "2026-07-31") -> bytes:
    return json.dumps(
        {
            "DatumPreseka": snapshot_date,
            "Podaci": {
                "00003506": {
                    "PoslovnoIme": "PRVO DRUŠTVO",
                    "SifraOpstine": "70017",
                },
                "21141666": {
                    "PoslovnoIme": "DRUGO DRUŠTVO",
                    "SifraOpstine": "70670",
                },
            },
        },
        ensure_ascii=False,
    ).encode("utf-8")


def test_snapshot_is_validated_content_addressed_and_reusable() -> None:
    body = _snapshot_body()
    store = _ObjectStore()
    retrieved_at = datetime(2026, 8, 25, 8, 30, tzinfo=UTC)

    first = sync_apr_companies_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="run-1",
        retrieved_at=retrieved_at,
        session=_Session([_Response(body)]),  # type: ignore[arg-type]
        minimum_size_bytes=1,
        minimum_record_count=2,
        download_attempts=1,
    )
    second = sync_apr_companies_snapshot(
        object_store=store,  # type: ignore[arg-type]
        run_id="run-2",
        retrieved_at=retrieved_at,
        session=_Session([_Response(body)]),  # type: ignore[arg-type]
        minimum_size_bytes=1,
        minimum_record_count=2,
        download_attempts=1,
    )

    assert store.created_buckets == [tables.S3_BUCKET, tables.S3_BUCKET]
    assert first.object_key == second.object_key
    assert "snapshot_date=2026-07-31" in first.object_key
    assert f"sha256={first.sha256}" in first.object_key
    assert first.record_count == 2
    assert first.downloaded is True
    assert second.downloaded is False
    assert store.uploaded_keys == [first.object_key]
    assert store.objects[first.object_key] == body

    manifest = json.loads(store.objects[first.manifest_key])
    assert manifest == {
        "bucket": tables.S3_BUCKET,
        "content_type": "application/json",
        "downloaded": True,
        "object_key": first.object_key,
        "record_count": 2,
        "retrieved_at": "2026-08-25T08:30:00+00:00",
        "sha256": first.sha256,
        "size_bytes": len(body),
        "snapshot_date": "2026-07-31",
        "source_license": "sodl",
        "source_run_id": "run-1",
        "source_slug": "apr_companies",
        "source_url": tables.SOURCE_URL,
    }


def test_snapshot_retries_when_the_first_json_response_is_truncated() -> None:
    body = _snapshot_body()
    session = _Session([_Response(body[:-10]), _Response(body)])

    snapshot = sync_apr_companies_snapshot(
        object_store=_ObjectStore(),  # type: ignore[arg-type]
        run_id="retry-run",
        retrieved_at=datetime(2026, 8, 25, tzinfo=UTC),
        session=session,  # type: ignore[arg-type]
        minimum_size_bytes=1,
        minimum_record_count=2,
        download_attempts=2,
        retry_base_seconds=0,
    )

    assert session.calls == 2
    assert snapshot.record_count == 2


def test_snapshot_refuses_an_implausibly_small_company_population() -> None:
    body = _snapshot_body()

    with pytest.raises(ValueError, match="too few company records"):
        sync_apr_companies_snapshot(
            object_store=_ObjectStore(),  # type: ignore[arg-type]
            run_id="small-run",
            retrieved_at=datetime(2026, 8, 25, tzinfo=UTC),
            session=_Session([_Response(body)]),  # type: ignore[arg-type]
            minimum_size_bytes=1,
            minimum_record_count=3,
            download_attempts=1,
        )


def test_http_session_pins_apr_intermediate_without_disabling_tls() -> None:
    session = apr_companies_http_session()
    try:
        assert session.verify == str(APR_INTERMEDIATE_CA_PATH)
        assert APR_INTERMEDIATE_CA_PATH.read_text(encoding="ascii").startswith(
            "-----BEGIN CERTIFICATE-----"
        )
    finally:
        session.close()


def test_raw_snapshot_asset_is_registered_as_the_open_data_source_boundary() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    node = repository.asset_graph.get(
        dg.AssetKey("serbia_apr_companies_raw_snapshot_s3")
    )

    assert node.group_name == tables.GROUP_NAME
    assert node.parent_keys == set()
    assert {"python", "json", "s3", "apr"} <= node.kinds
    assert node.tags["country"] == "serbia"
    assert node.tags["source"] == "apr_companies"
    assert node.tags["layer"] == "s3"


def test_source_design_records_the_raw_and_duckdb_contracts() -> None:
    design_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "serbia_apr_companies"
        / "docs"
        / "serbia-apr-companies-design.md"
    )
    text = design_path.read_text(encoding="utf-8")

    assert "57,673,691" in text
    assert "content-addressed" in text
    assert "DuckDB" in text
    assert "serbia_apr_companies_duckdb_load" in text
    assert "commit or roll back together" in text
