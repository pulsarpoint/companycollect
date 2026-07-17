from __future__ import annotations

import json
import threading
import time
from typing import Any

import pytest

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.constants import (
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    financial_bootstrap_response_partition_prefix,
    financial_response_checkpoint_object_key,
    financial_response_object_key,
    financial_response_success_object_key,
)
from dagster_v3.defs.norway_brreg_financial.response_pipeline import (
    RESPONSE_DOWNLOAD_BATCH_SIZE,
    RESPONSE_DOWNLOAD_WORKERS,
    RESPONSE_VERIFY_WORKERS,
    materialize_response_json_partition,
    verified_response_index_frame,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket == NORWAY_BRREG_FINANCIAL_BUCKET

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return (str(bucket), key) in self.objects

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        return self.objects[(str(bucket), key)]

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        self.objects[(str(bucket), key)] = body

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        return [
            key
            for object_bucket, key in self.objects
            if object_bucket == str(bucket) and key.startswith(prefix)
        ]


class StorageProxy:
    def __init__(self, inner: NorwayBrregFinancialParquetStorageResource) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class FakeResponse:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


def test_response_index_schema_contains_only_metadata() -> None:
    columns = financial_fetches.financial_fetches_parquet_schema()

    assert "raw_response" not in columns
    assert "source_object_key" in columns
    assert "source_payload_hash" in columns
    assert "capture_method" in columns
    assert "original_http_bytes_preserved" in columns
    assert RESPONSE_DOWNLOAD_BATCH_SIZE == 250
    assert RESPONSE_DOWNLOAD_WORKERS == 8
    assert RESPONSE_VERIFY_WORKERS == 32


def test_download_hashes_exact_http_bytes_and_uses_bounded_concurrency() -> None:
    lock = threading.Lock()
    active = 0
    maximum_active = 0
    payload = b'[ {"id": 1} ]\n'

    class Client:
        def get(self, _url: str, timeout: int) -> FakeResponse:
            nonlocal active, maximum_active
            assert timeout == financial_fetches.DEFAULT_TIMEOUT_SECONDS
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.005)
            with lock:
                active -= 1
            return FakeResponse(200, payload)

    rows = financial_fetches.download_financial_responses_for_orgs(
        orgs=[_candidate(f"81111{index:04d}") for index in range(24)],
        source_run_id="run-1",
        client=Client(),
        max_workers=8,
        fetched_at="2026-07-17T00:00:00.000Z",
    )

    assert len(rows) == 24
    assert 1 < maximum_active <= 8
    assert all(row["_response_body"] == payload for row in rows)
    assert all(
        row["source_payload_hash"] == financial_fetches.sha256_hex(payload)
        for row in rows
    )


def test_json_partition_retries_only_retryable_orgs_and_reuses_one_client() -> None:
    storage, _object_store = _storage()
    prefix = financial_bootstrap_response_partition_prefix("bucket_00")
    candidates = [_candidate("811111111"), _candidate("822222222")]
    calls: list[list[str]] = []
    clients: list[object] = []

    def client_factory() -> object:
        client = object()
        clients.append(client)
        return client

    def first_download(**kwargs: Any) -> list[dict[str, Any]]:
        assert kwargs["client"] is clients[0]
        calls.append([candidate["org_number"] for candidate in kwargs["orgs"]])
        return [
            _download_success(kwargs["orgs"][0], source_run_id="run-1"),
            _download_failure(kwargs["orgs"][1], source_run_id="run-1"),
        ]

    with pytest.raises(RuntimeError, match="retryable outcomes"):
        materialize_response_json_partition(
            candidates=candidates,
            partition_prefix=prefix,
            source_run_id="run-1",
            storage=storage,
            client_factory=client_factory,
            downloader=first_download,
        )

    def retry_download(**kwargs: Any) -> list[dict[str, Any]]:
        assert kwargs["client"] is clients[1]
        calls.append([candidate["org_number"] for candidate in kwargs["orgs"]])
        return [_download_success(kwargs["orgs"][0], source_run_id="run-2")]

    metadata = materialize_response_json_partition(
        candidates=candidates,
        partition_prefix=prefix,
        source_run_id="run-2",
        storage=storage,
        client_factory=client_factory,
        downloader=retry_download,
    )

    assert calls == [["811111111", "822222222"], ["822222222"]]
    assert len(clients) == 2
    assert metadata["downloaded_count"] == 1
    assert metadata["reused_count"] == 1
    assert metadata["status_counts"] == {"success": 2}
    frame, _ = verified_response_index_frame(
        partition_prefix=prefix,
        storage=storage,
    )
    assert frame.height == 2
    assert "raw_response" not in frame.columns


def test_terminal_not_found_is_not_downloaded_again() -> None:
    storage, _object_store = _storage()
    prefix = financial_bootstrap_response_partition_prefix("bucket_01")
    candidate = _candidate("811111111")
    record = financial_fetches.response_record(
        org=candidate,
        source_url="https://example.test/811111111",
        source_run_id="migration",
        source_line_number=1,
        fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_NOT_FOUND,
        http_status=404,
        error_type="HTTPStatusError",
        error_message="HTTP 404",
        attempt_count=1,
        fetched_at="2026-07-16T00:00:00.000Z",
        original_http_bytes_preserved=False,
    )
    _write_checkpoint(storage, prefix, [record])

    metadata = materialize_response_json_partition(
        candidates=[candidate],
        partition_prefix=prefix,
        source_run_id="run-1",
        storage=storage,
        client_factory=lambda: pytest.fail("terminal outcome must not create a client"),
        downloader=lambda **_kwargs: pytest.fail("terminal outcome must not download"),
    )

    assert metadata["reused_count"] == 1
    assert metadata["downloaded_count"] == 0
    assert metadata["status_counts"] == {"not_found": 1}


def test_completed_partition_is_verified_and_never_extended() -> None:
    storage, _object_store = _storage()
    prefix = financial_bootstrap_response_partition_prefix("bucket_05")
    completed_candidate = _candidate("811111111")
    body = b'[{"id":1}]'
    response_key = financial_response_object_key(prefix, "811111111")
    storage.write_response(response_key, body)
    record = financial_fetches.response_record(
        org=completed_candidate,
        source_url="https://example.test/811111111",
        source_run_id="run-1",
        source_line_number=1,
        fetch_status="success",
        http_status=200,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at="2026-07-17T00:00:00.000Z",
        source_object_key=response_key,
        source_payload_hash=financial_fetches.sha256_hex(body),
    )
    _write_checkpoint(storage, prefix, [record])
    _write_success(storage, prefix, ["811111111"], {"success": 1})

    metadata = materialize_response_json_partition(
        candidates=[completed_candidate, _candidate("822222222")],
        partition_prefix=prefix,
        source_run_id="run-2",
        storage=storage,
        client_factory=lambda: pytest.fail("completed partition must not create a client"),
        downloader=lambda **_kwargs: pytest.fail(
            "completed partition must not download"
        ),
    )

    assert metadata["candidate_count"] == 1
    assert metadata["reused_count"] == 1
    assert metadata["downloaded_count"] == 0


def test_imported_terminal_outcomes_remain_in_bootstrap_candidate_archive() -> None:
    storage, _object_store = _storage()
    prefix = financial_bootstrap_response_partition_prefix("bucket_06")
    imported_candidate = _candidate("811111111")
    imported_body = b'[{"id":"historical"}]'
    imported_key = financial_response_object_key(prefix, "811111111")
    storage.write_response(imported_key, imported_body)
    imported_record = financial_fetches.response_record(
        org=imported_candidate,
        source_url="https://example.test/811111111",
        source_run_id="migration",
        source_line_number=1,
        fetch_status="success",
        http_status=200,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at="2026-07-16T00:00:00.000Z",
        source_object_key=imported_key,
        source_payload_hash=financial_fetches.sha256_hex(imported_body),
        capture_method="parquet_import",
        original_http_bytes_preserved=False,
    )
    _write_checkpoint(storage, prefix, [imported_record])
    current_candidate = _candidate("822222222")

    metadata = materialize_response_json_partition(
        candidates=[current_candidate],
        partition_prefix=prefix,
        source_run_id="run-1",
        storage=storage,
        client_factory=object,
        downloader=lambda **kwargs: [
            _download_success(kwargs["orgs"][0], source_run_id="run-1")
        ],
    )

    assert metadata["candidate_count"] == 2
    assert metadata["reused_count"] == 1
    assert metadata["downloaded_count"] == 1
    frame, _index_metadata = verified_response_index_frame(
        partition_prefix=prefix,
        storage=storage,
    )
    assert frame.get_column("org_number").to_list() == ["811111111", "822222222"]


def test_uncheckpointed_json_is_recovered_after_interruption() -> None:
    storage, _object_store = _storage()
    prefix = financial_bootstrap_response_partition_prefix("bucket_02")
    candidate = _candidate("811111111")
    response_key = financial_response_object_key(prefix, "811111111")
    storage.write_response(response_key, b'[{"id":1}]')

    metadata = materialize_response_json_partition(
        candidates=[candidate],
        partition_prefix=prefix,
        source_run_id="recovery-run",
        storage=storage,
        client_factory=lambda: pytest.fail("existing JSON must be recovered"),
        downloader=lambda **_kwargs: pytest.fail("existing JSON must be recovered"),
    )

    assert metadata["reused_count"] == 1
    [record] = financial_fetches.latest_response_records(
        storage.read_response_records(prefix)
    )
    assert record["capture_method"] == "recovered_existing_json"
    assert record["original_http_bytes_preserved"] is False


def test_imported_json_hash_is_verified_without_claiming_original_http_bytes() -> None:
    storage, _object_store = _storage()
    prefix = financial_bootstrap_response_partition_prefix("bucket_03")
    candidate = _candidate("811111111")
    body = json.dumps([{"id": 1}], separators=(",", ":")).encode()
    response_key = financial_response_object_key(prefix, "811111111")
    storage.write_response(response_key, body)
    record = financial_fetches.response_record(
        org=candidate,
        source_url="https://example.test/811111111",
        source_run_id="parquet-migration",
        source_line_number=1,
        fetch_status="success",
        http_status=200,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at="2026-07-16T00:00:00.000Z",
        source_object_key=response_key,
        source_payload_hash=financial_fetches.sha256_hex(body),
        capture_method="parquet_import",
        original_http_bytes_preserved=False,
    )
    _write_checkpoint(storage, prefix, [record])
    _write_success(storage, prefix, ["811111111"], {"success": 1})

    frame, metadata = verified_response_index_frame(
        partition_prefix=prefix,
        storage=storage,
    )

    [row] = frame.to_dicts()
    assert row["source_payload_hash"] == financial_fetches.sha256_hex(body)
    assert row["capture_method"] == "parquet_import"
    assert row["original_http_bytes_preserved"] is False
    assert metadata["status_counts"] == {"success": 1}


@pytest.mark.parametrize("damage", ["missing", "corrupt", "hash_mismatch"])
def test_response_index_rejects_missing_invalid_or_hash_mismatched_json(
    damage: str,
) -> None:
    storage, object_store = _storage()
    prefix = financial_bootstrap_response_partition_prefix("bucket_04")
    candidate = _candidate("811111111")
    body = b'[{"id":1}]'
    response_key = financial_response_object_key(prefix, "811111111")
    storage.write_response(response_key, body)
    record = financial_fetches.response_record(
        org=candidate,
        source_url="https://example.test/811111111",
        source_run_id="run-1",
        source_line_number=1,
        fetch_status="success",
        http_status=200,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at="2026-07-16T00:00:00.000Z",
        source_object_key=response_key,
        source_payload_hash=financial_fetches.sha256_hex(body),
    )
    _write_checkpoint(storage, prefix, [record])
    _write_success(storage, prefix, ["811111111"], {"success": 1})
    object_key = (NORWAY_BRREG_FINANCIAL_BUCKET, response_key)
    if damage == "missing":
        del object_store.objects[object_key]
    elif damage == "corrupt":
        corrupt = b"not-json"
        object_store.objects[object_key] = corrupt
        record["source_payload_hash"] = financial_fetches.sha256_hex(corrupt)
        _write_checkpoint(storage, prefix, [record], run_id="run-2")
    else:
        object_store.objects[object_key] = b'[{"id":2}]'

    with pytest.raises(RuntimeError):
        verified_response_index_frame(partition_prefix=prefix, storage=storage)


def _storage() -> tuple[StorageProxy, FakeObjectStore]:
    object_store = FakeObjectStore()
    resource = NorwayBrregFinancialParquetStorageResource(object_store=object_store)
    return StorageProxy(resource), object_store


def _candidate(org_number: str) -> dict[str, Any]:
    return {
        "org_number": org_number,
        "legal_name": f"{org_number} AS",
        "website": "",
        "last_submitted_accounts_year": "2024",
    }


def _download_success(
    candidate: dict[str, Any],
    *,
    source_run_id: str,
) -> dict[str, Any]:
    body = json.dumps([{"id": candidate["org_number"]}], separators=(",", ":")).encode()
    record = financial_fetches.response_record(
        org=candidate,
        source_url=f"https://example.test/{candidate['org_number']}",
        source_run_id=source_run_id,
        source_line_number=1,
        fetch_status="success",
        http_status=200,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at=(
            "2026-07-17T00:00:00.000Z"
            if source_run_id == "run-1"
            else "2026-07-17T01:00:00.000Z"
        ),
        source_payload_hash=financial_fetches.sha256_hex(body),
    )
    record["_response_body"] = body
    return record


def _download_failure(
    candidate: dict[str, Any],
    *,
    source_run_id: str,
) -> dict[str, Any]:
    record = financial_fetches.response_record(
        org=candidate,
        source_url=f"https://example.test/{candidate['org_number']}",
        source_run_id=source_run_id,
        source_line_number=1,
        fetch_status="server_error",
        http_status=500,
        error_type="HTTPStatusError",
        error_message="HTTP 500",
        attempt_count=1,
        fetched_at="2026-07-17T00:00:00.000Z",
    )
    record["_response_body"] = None
    return record


def _write_checkpoint(
    storage: StorageProxy,
    prefix: str,
    records: list[dict[str, Any]],
    *,
    run_id: str = "migration",
) -> None:
    storage.write_json_object(
        financial_response_checkpoint_object_key(prefix, run_id, 0),
        {"records": records},
    )


def _write_success(
    storage: StorageProxy,
    prefix: str,
    org_numbers: list[str],
    counts: dict[str, int],
) -> None:
    storage.write_json_object(
        financial_response_success_object_key(prefix),
        {
            "candidate_org_numbers": org_numbers,
            "candidate_count": len(org_numbers),
            "status_counts": counts,
        },
    )
