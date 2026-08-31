import json
from datetime import UTC, datetime
from typing import Any

import pytest

from dagster_v3.defs.common.ats_source import (
    BoardPayload,
    read_snapshot_manifest,
    sync_board_snapshots,
)
from dagster_v3.defs.sweden_greenhouse.source import BOARDS as GREENHOUSE_BOARDS


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self, bucket: str) -> None:
        assert bucket == "test-bucket"

    def exists(self, key: str, bucket: str) -> bool:
        return key in self.objects

    def list_keys(self, prefix: str, bucket: str) -> list[str]:
        assert bucket == "test-bucket"
        return [key for key in self.objects if key.startswith(prefix)]

    def read_bytes(self, key: str, bucket: str) -> bytes:
        assert bucket == "test-bucket"
        return self.objects[key]

    def write_bytes(self, key: str, body: bytes, bucket: str) -> None:
        self.objects[key] = body

    def write_json(self, key: str, body: str, bucket: str) -> None:
        self.objects[key] = body.encode()


def test_provider_snapshot_failure_never_commits_a_partial_manifest() -> None:
    store = MemoryObjectStore()
    board = GREENHOUSE_BOARDS[0]
    calls = 0

    def fetch(_board: object) -> BoardPayload:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("second board failed")
        return BoardPayload(
            payload={"jobs": [{"id": 1}]},
            source_url="https://example.test/api",
            job_count=1,
        )

    with pytest.raises(RuntimeError, match="second board failed"):
        sync_board_snapshots(
            object_store=store,  # type: ignore[arg-type]
            bucket="test-bucket",
            provider="greenhouse",
            boards=(board, board),
            fetch_board=fetch,
            run_id="failed-run",
            retrieved_at=datetime(2026, 8, 31, tzinfo=UTC),
        )

    assert any(key.startswith("raw/") for key in store.objects)
    assert all(not key.startswith("manifests/") for key in store.objects)


def test_snapshot_manifest_prefers_the_current_run() -> None:
    store = MemoryObjectStore()
    store.objects.update(
        {
            "manifests/ashby/run_id=current/manifest.json": _manifest_bytes(
                run_id="current", retrieved_at="2026-08-30T10:00:00+00:00"
            ),
            "manifests/ashby/run_id=newer/manifest.json": _manifest_bytes(
                run_id="newer", retrieved_at="2026-08-31T10:00:00+00:00"
            ),
        }
    )

    manifest = read_snapshot_manifest(
        object_store=store,  # type: ignore[arg-type]
        bucket="test-bucket",
        provider="ashby",
        run_id="current",
    )

    assert manifest["source_run_id"] == "current"


def test_snapshot_manifest_falls_back_to_the_latest_successful_run() -> None:
    store = MemoryObjectStore()
    store.objects.update(
        {
            "manifests/ashby/run_id=older/manifest.json": _manifest_bytes(
                run_id="older", retrieved_at="2026-08-30T10:00:00+00:00"
            ),
            "manifests/ashby/run_id=newer/manifest.json": _manifest_bytes(
                run_id="newer", retrieved_at="2026-08-31T10:00:00+00:00"
            ),
        }
    )

    manifest = read_snapshot_manifest(
        object_store=store,  # type: ignore[arg-type]
        bucket="test-bucket",
        provider="ashby",
        run_id="downstream-only",
    )

    assert manifest["source_run_id"] == "newer"


def _manifest_bytes(*, run_id: str, retrieved_at: str) -> bytes:
    return json.dumps(
        {
            "provider": "ashby",
            "source_run_id": run_id,
            "retrieved_at": retrieved_at,
            "boards": [],
        }
    ).encode()


def test_lever_fetches_every_page(monkeypatch: pytest.MonkeyPatch) -> None:
    from dagster_v3.defs.sweden_lever import source

    skips: list[int] = []

    def get_json(_url: str, *, params: dict[str, Any]) -> list[dict[str, int]]:
        skip = int(params["skip"])
        skips.append(skip)
        if skip == 0:
            return [{"id": index} for index in range(source.PAGE_SIZE)]
        return [{"id": source.PAGE_SIZE}]

    monkeypatch.setattr(source, "get_json", get_json)

    result = source.fetch_board(source.BOARDS[0])

    assert result.job_count == source.PAGE_SIZE + 1
    assert skips == [0, source.PAGE_SIZE]


def test_smartrecruiters_requires_every_posting_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.sweden_smartrecruiters import source

    requested_urls: list[str] = []

    def get_json(url: str, *, params: dict[str, Any] | None = None) -> object:
        requested_urls.append(url)
        if params is not None:
            return {"totalFound": 2, "content": [{"id": "one"}, {"id": "two"}]}
        if url.endswith("/two"):
            raise RuntimeError("detail failed")
        return {"id": "one"}

    monkeypatch.setattr(source, "get_json", get_json)

    with pytest.raises(RuntimeError, match="detail failed"):
        source.fetch_board(source.BOARDS[0])

    assert requested_urls[-2:] == [
        "https://api.smartrecruiters.com/v1/companies/HMGroup/postings/one",
        "https://api.smartrecruiters.com/v1/companies/HMGroup/postings/two",
    ]
