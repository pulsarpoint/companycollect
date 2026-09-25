import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import scan_coordinator
from config import WebtechServiceSettings
from models import (
    WEBTECH_DETECTOR_VERSION,
    ExtensionReport,
    WebtechDomainResult,
)
from s3_store import S3Location, StoredObject, parse_s3_uri
from service import create_app
from service_models import ScanRequest

API_TOKEN = "test-webtech-token-with-safe-length"
BASE_URI = "s3://webtech/webtech"
TASK_ID = str(uuid4())
EXECUTION_ID = str(uuid4())


class InMemoryRustfsStore:
    """Exercise the service's real S3 key and serialization contract in memory."""

    def __init__(self) -> None:
        self.base_location = parse_s3_uri(BASE_URI)
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self) -> None:
        return

    def child(self, *parts: str) -> S3Location:
        key = "/".join(
            part.strip("/")
            for part in (self.base_location.key, *parts)
            if part.strip("/")
        )
        return S3Location(bucket=self.base_location.bucket, key=key)

    def read_bytes(self, location: S3Location) -> bytes:
        return self.objects[location.key]

    def write_json(self, location: S3Location, document: Any) -> StoredObject:
        value = (
            document.model_dump(mode="json")
            if hasattr(document, "model_dump")
            else document
        )
        body = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.objects[location.key] = body
        return StoredObject(
            location=location,
            sha256=hashlib.sha256(body).hexdigest(),
            size_bytes=len(body),
        )

    def exists(self, location: S3Location) -> bool:
        return location.key in self.objects


def service_settings() -> WebtechServiceSettings:
    return WebtechServiceSettings(
        WEBTECH_API_TOKEN=API_TOKEN,
        WEBTECH_S3_PATH=BASE_URI,
        CORPSCOUT_S3_ENDPOINT="http://rustfs.test:9000",
        CORPSCOUT_S3_ACCESS_KEY="test-access",
        CORPSCOUT_S3_SECRET_KEY="test-secret",
        WEBTECH_BROWSER_COUNT=2,
        WEBTECH_PAGES_PER_BROWSER=1,
        WEBTECH_DOMAINS_PER_CONTEXT=1,
        WEBTECH_CONTEXT_LAUNCH_INTERVAL_SECONDS=0,
        WEBTECH_PROGRESS_BATCH_SIZE=2,
    )


def page(index: int, path: str = "", *, task_id: str = TASK_ID) -> dict[str, object]:
    return {
        "root_domain": "novelic.com",
        "harmonic_rank": 0,
        "task_id": task_id,
        "input_id": str(index) * 64,
        "page_url": f"https://novelic.com/{path}",
    }


def scan_request(
    pages: list[tuple[int, str]] | None = None,
    *,
    execution_id: str = EXECUTION_ID,
) -> dict[str, object]:
    """One inline envelope; the default has three pages."""
    return {
        "schema_version": 2,
        "crawl_id": f"webtech-{execution_id}",
        "detector_version": WEBTECH_DETECTOR_VERSION,
        "candidates": [
            page(index, path)
            for index, path in (pages or [(1, ""), (2, "a"), (3, "b")])
        ],
    }


def completed_result(candidate) -> WebtechDomainResult:
    report = ExtensionReport(
        schema_version=3,
        analysis_complete=True,
        analysis_status="complete",
        extension_version="1.4.1",
        page_token=uuid4(),
        url=candidate.page_url,
        technologies=[],
        failure_stage=None,
        error_message="",
        stage_timings_ms={},
    )
    return WebtechDomainResult.success(
        candidate=candidate,
        requested_url=candidate.page_url,
        final_url=report.url,
        report=report,
        scanned_at=datetime.now(UTC),
        duration_ms=125,
    )


def test_scan_stores_only_per_page_reports() -> None:
    store = InMemoryRustfsStore()
    scan_calls = 0

    async def fake_scan(candidates, *, settings, progress_callback):
        nonlocal scan_calls
        scan_calls += 1
        results = []
        for candidate in candidates:
            result = completed_result(candidate)
            callback_result = progress_callback(result)
            if callback_result is not None:
                await callback_result
            results.append(result)
            await asyncio.sleep(0)
        return tuple(results)

    app = create_app(
        settings=service_settings(),
        store=store,
        scan_function=fake_scan,
    )
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    with TestClient(app) as client:
        unauthorized = client.post("/v1/scans", json=scan_request())
        assert unauthorized.status_code == 401

        submitted = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        )
        assert submitted.status_code == 202
        scan_id = submitted.json()["scan_id"]

        cursor = 0
        events = []
        while True:
            response = client.get(
                f"/v1/scans/{scan_id}",
                params={"after_event": cursor, "wait_seconds": 1},
                headers=headers,
            )
            assert response.status_code == 200
            payload = response.json()
            events.extend(payload["events"])
            if payload["events"]:
                cursor = payload["events"][-1]["sequence"]
            if payload["scan"]["status"] == "completed":
                break

        assert payload["scan"]["completed_count"] == 3
        assert payload["scan"]["outcome_counts"] == {"success": 3}
        assert [event["window_count"] for event in events] == [2, 1]
        # Nothing but the per-page reports is written.
        assert len(store.objects) == 3
        assert all(key.endswith("/report.json") for key in store.objects)

        repeated = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        )
        assert repeated.json()["status"] == "completed"
        assert repeated.json()["scan_id"] == scan_id
    assert scan_calls == 1


def test_scan_logs_lifecycle_progress_and_completion(caplog) -> None:
    caplog.set_level(logging.INFO, logger="uvicorn.error")
    store = InMemoryRustfsStore()

    async def fake_scan(candidates, *, settings, progress_callback):
        del settings
        results = []
        for candidate in candidates:
            result = completed_result(candidate)
            callback_result = progress_callback(result)
            if callback_result is not None:
                await callback_result
            results.append(result)
        return tuple(results)

    app = create_app(
        settings=service_settings(),
        store=store,
        scan_function=fake_scan,
    )
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    with TestClient(app) as client:
        submitted = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        ).json()
        completed = _wait_for_terminal(client, submitted["scan_id"], headers)

    assert completed["status"] == "completed"
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Webtech scan accepted" in message and "total=3" in message
        for message in messages
    )
    assert any(
        "Webtech scan started" in message and "recovered=0" in message
        for message in messages
    )
    assert any(
        "Webtech scan progress" in message
        and "completed=2/3" in message
        and "batch=2" in message
        for message in messages
    )
    assert any(
        "Webtech scan completed" in message and "completed=3/3" in message
        for message in messages
    )


def test_scan_logs_stalled_progress_warning(caplog, monkeypatch) -> None:
    caplog.set_level(logging.INFO, logger="uvicorn.error")
    monkeypatch.setattr(scan_coordinator, "SCAN_HEARTBEAT_SECONDS", 0.01)
    monkeypatch.setattr(scan_coordinator, "SCAN_STALLED_AFTER_SECONDS", 0.02)
    store = InMemoryRustfsStore()

    async def slow_scan(candidates, *, settings, progress_callback):
        del settings
        await asyncio.sleep(0.05)
        results = []
        for candidate in candidates:
            result = completed_result(candidate)
            callback_result = progress_callback(result)
            if callback_result is not None:
                await callback_result
            results.append(result)
        return tuple(results)

    app = create_app(
        settings=service_settings(),
        store=store,
        scan_function=slow_scan,
    )
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    with TestClient(app) as client:
        submitted = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        ).json()
        health = client.get("/healthz").json()
        assert health["active_scan"] is True
        assert health["active_scan_id"] == submitted["scan_id"]
        assert health["completed_count"] == 0
        assert health["total_count"] == 3
        assert health["progress_age_seconds"] >= 0
        completed = _wait_for_terminal(client, submitted["scan_id"], headers)

    assert completed["status"] == "completed"
    assert any(
        record.levelno == logging.WARNING
        and "Webtech scan stalled" in record.getMessage()
        and "completed=0/3" in record.getMessage()
        for record in caplog.records
    )


def test_resubmit_recovers_stored_domains_after_a_failed_scan() -> None:
    store = InMemoryRustfsStore()
    attempted_domains: list[tuple[str, ...]] = []

    async def fail_once_then_complete(candidates, *, settings, progress_callback):
        del settings
        attempted_domains.append(tuple(candidate.page_url for candidate in candidates))
        results = []
        for position, candidate in enumerate(candidates):
            result = completed_result(candidate)
            callback_result = progress_callback(result)
            if callback_result is not None:
                await callback_result
            results.append(result)
            if len(attempted_domains) == 1 and position == 0:
                raise RuntimeError("simulated workstation restart")
        return tuple(results)

    app = create_app(
        settings=service_settings(),
        store=store,
        scan_function=fail_once_then_complete,
    )
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    with TestClient(app) as client:
        first = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        ).json()
        scan_id = first["scan_id"]
        failed = _wait_for_terminal(client, scan_id, headers)
        assert failed["status"] == "failed"
        assert failed["completed_count"] == 1

        resumed = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        ).json()
        assert resumed["scan_id"] == scan_id
        completed = _wait_for_terminal(client, scan_id, headers)

    assert completed["status"] == "completed"
    assert completed["completed_count"] == 3
    assert attempted_domains == [
        ("https://novelic.com/", "https://novelic.com/a", "https://novelic.com/b"),
        ("https://novelic.com/a", "https://novelic.com/b"),
    ]


def test_scan_stalled_beyond_limit_is_failed_and_releases_the_slot(
    caplog, monkeypatch
) -> None:
    caplog.set_level(logging.INFO, logger="uvicorn.error")
    monkeypatch.setattr(scan_coordinator, "SCAN_HEARTBEAT_SECONDS", 0.01)
    monkeypatch.setattr(scan_coordinator, "SCAN_STALLED_AFTER_SECONDS", 0.02)
    monkeypatch.setattr(scan_coordinator, "SCAN_STALL_FAIL_AFTER_SECONDS", 0.05)
    store = InMemoryRustfsStore()
    attempts: list[int] = []

    async def stuck_then_complete(candidates, *, settings, progress_callback):
        del settings
        attempts.append(len(candidates))
        if len(attempts) == 1:
            await asyncio.sleep(3600)
        results = []
        for candidate in candidates:
            result = completed_result(candidate)
            callback_result = progress_callback(result)
            if callback_result is not None:
                await callback_result
            results.append(result)
        return tuple(results)

    app = create_app(
        settings=service_settings(),
        store=store,
        scan_function=stuck_then_complete,
    )
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    with TestClient(app) as client:
        submitted = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        ).json()
        scan_id = submitted["scan_id"]
        failed = _wait_for_terminal(client, scan_id, headers)
        assert failed["status"] == "failed"
        assert "stalled" in failed["error_message"]
        assert "3 domains remaining" in failed["error_message"]
        assert client.get("/healthz").json()["active_scan"] is False

        resumed = client.post(
            "/v1/scans",
            json=scan_request(),
            headers=headers,
        ).json()
        assert resumed["scan_id"] == scan_id
        completed = _wait_for_terminal(client, scan_id, headers)

    assert completed["status"] == "completed"
    assert attempts == [3, 3]
    assert any(
        record.levelno == logging.ERROR
        and "Webtech scan stalled beyond limit" in record.getMessage()
        and "completed=0/3" in record.getMessage()
        for record in caplog.records
    )


def _wait_for_terminal(
    client: TestClient,
    scan_id: str,
    headers: dict[str, str],
) -> dict[str, object]:
    cursor = 0
    while True:
        response = client.get(
            f"/v1/scans/{scan_id}",
            params={"after_event": cursor, "wait_seconds": 1},
            headers=headers,
        )
        assert response.status_code == 200
        payload = response.json()
        if payload["events"]:
            cursor = payload["events"][-1]["sequence"]
        if payload["scan"]["status"] not in {"pending", "running"}:
            return payload["scan"]


def test_resubmit_after_restart_recovers_every_stored_page_without_scanning():
    store = InMemoryRustfsStore()
    first_events = _run_scan(store, scan_request(), [])

    async def must_not_scan(candidates, *, settings, progress_callback):
        raise AssertionError("every page is stored; nothing should be scanned")

    # A fresh coordinator (scanner restart) over the same store rescans nothing.
    events = _run_scan(store, scan_request(), [], scan_function=must_not_scan)
    assert len(events) == 1
    assert sorted(item["input_id"] for item in events[0]["results"]) == [
        "1" * 64,
        "2" * 64,
        "3" * 64,
    ]
    assert {item["object_key"] for item in events[0]["results"]} == {
        item["object_key"] for event in first_events for item in event["results"]
    }
    assert len(store.objects) == 3


def _invalid_request(invalid: str) -> dict[str, object]:
    request = scan_request([(1, "")])
    candidate = request["candidates"][0]
    if invalid == "mixed_tasks":
        request["candidates"].append(page(2, "a", task_id=str(uuid4())))
    elif invalid == "duplicate_input":
        request["candidates"].append({**candidate, "page_url": "https://novelic.com/a"})
    elif invalid == "bad_task":
        candidate["task_id"] = "not-a-uuid"
    elif invalid == "bad_input":
        candidate["input_id"] = "invalid"
    elif invalid == "outside_domain":
        candidate["page_url"] = "https://other.com/"
    elif invalid == "credentials":
        candidate["page_url"] = "https://user:secret@novelic.com/"
    elif invalid == "domain_only":
        request["candidates"] = [{"root_domain": "novelic.com", "harmonic_rank": 1}]
    elif invalid == "no_candidates":
        request["candidates"] = []
    elif invalid == "common_crawl_id":
        request["crawl_id"] = "CC-MAIN-2026-30"
    elif invalid == "non_canonical_execution":
        request["crawl_id"] = "webtech-" + EXECUTION_ID.upper()
    return request


@pytest.mark.parametrize(
    "invalid",
    [
        "mixed_tasks",
        "duplicate_input",
        "bad_task",
        "bad_input",
        "outside_domain",
        "credentials",
        "domain_only",
        "no_candidates",
        "common_crawl_id",
        "non_canonical_execution",
    ],
)
def test_scan_request_rejects_invalid_identities(invalid):
    with pytest.raises(ValidationError):
        ScanRequest.model_validate(_invalid_request(invalid))


def test_submit_rejects_an_envelope_over_the_candidate_limit():
    store = InMemoryRustfsStore()
    settings = service_settings().model_copy(update={"max_candidates": 2})
    app = create_app(settings=settings, store=store, scan_function=_complete_all([]))
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    with TestClient(app) as client:
        response = client.post("/v1/scans", json=scan_request(), headers=headers)
    assert response.status_code == 422
    assert "exceeds limit 2" in response.text
    assert not store.objects


def test_scan_id_depends_on_the_pages_not_their_order():
    coordinator = scan_coordinator.ScanCoordinator(
        settings=service_settings(), store=InMemoryRustfsStore()
    )

    def scan_id(pages, **kwargs):
        return coordinator._scan_id(
            ScanRequest.model_validate(scan_request(pages, **kwargs))
        )

    assert scan_id([(1, ""), (2, "a")]) == scan_id([(2, "a"), (1, "")])
    assert scan_id([(1, "")]) != scan_id([(2, "a")])
    assert scan_id([(1, "")]) != scan_id([(1, "")], execution_id=str(uuid4()))


def _complete_all(scanned):
    async def fake_scan(candidates, *, settings, progress_callback):
        del settings
        scanned.extend(candidate.page_url for candidate in candidates)
        for candidate in candidates:
            await progress_callback(completed_result(candidate))
        return ()

    return fake_scan


def _run_scan(store, request, scanned, *, scan_function=None):
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    app = create_app(
        settings=service_settings(),
        store=store,
        scan_function=scan_function or _complete_all(scanned),
    )
    with TestClient(app) as client:
        response = client.post("/v1/scans", json=request, headers=headers)
        assert response.status_code == 202, response.text
        scan_id = response.json()["scan_id"]
        events, cursor = [], 0
        while True:
            payload = client.get(
                f"/v1/scans/{scan_id}",
                params={"after_event": cursor, "wait_seconds": 1},
                headers=headers,
            ).json()
            events.extend(payload["events"])
            if payload["events"]:
                cursor = payload["events"][-1]["sequence"]
            if payload["scan"]["status"] not in {"pending", "running"}:
                assert payload["scan"]["status"] == "completed", payload["scan"]
                return events


def test_events_carry_every_stored_result_reference():
    store = InMemoryRustfsStore()
    events = _run_scan(store, scan_request(), [])
    references = [item for event in events for item in event["results"]]
    assert sorted(item["input_id"] for item in references) == [
        "1" * 64,
        "2" * 64,
        "3" * 64,
    ]
    for item in references:
        assert (
            f"/crawl_id=webtech-{EXECUTION_ID}/pages/input_id={item['input_id']}/report.json"
            in item["object_key"]
        )
        assert item["object_key"] in store.objects


def test_pages_done_in_one_envelope_are_reused_by_another():
    store = InMemoryRustfsStore()
    _run_scan(store, scan_request([(1, ""), (2, "a")]), [])
    # A different envelope of the same execution: page 2 is done, page 3 is new.
    scanned = []
    events = _run_scan(store, scan_request([(2, "a"), (3, "b")]), scanned)
    assert scanned == ["https://novelic.com/b"]
    assert [item["input_id"] for item in events[0]["results"]] == ["2" * 64]
    assert sorted(
        item["input_id"] for event in events for item in event["results"]
    ) == ["2" * 64, "3" * 64]


def test_another_execution_does_not_reuse_pages():
    store = InMemoryRustfsStore()
    _run_scan(store, scan_request([(1, "")], execution_id=str(uuid4())), [])
    scanned = []
    _run_scan(store, scan_request([(1, "")], execution_id=str(uuid4())), scanned)
    assert scanned == ["https://novelic.com/"]


def _envelope(pages, *, execution_id=EXECUTION_ID) -> ScanRequest:
    return ScanRequest.model_validate(scan_request(pages, execution_id=execution_id))


def test_new_envelope_of_the_same_execution_supersedes_the_running_scan(caplog):
    store = InMemoryRustfsStore()
    first = _envelope([(1, ""), (2, "a"), (3, "b")])
    # Page 1 was published from the first envelope, so the next envelope omits it.
    second = _envelope([(2, "a"), (3, "b")])
    scanned: list[list[str]] = []
    blocked = asyncio.Event()

    async def scan(candidates, *, settings, progress_callback):
        del settings
        scanned.append([candidate.page_url for candidate in candidates])
        if len(scanned) == 1:
            for candidate in candidates[:2]:
                await progress_callback(completed_result(candidate))
            blocked.set()
            await asyncio.Event().wait()  # a scan that would run for hours
        for candidate in candidates:
            await progress_callback(completed_result(candidate))
        return ()

    async def scenario():
        coordinator = scan_coordinator.ScanCoordinator(
            settings=service_settings(), store=store, scan_function=scan
        )
        old = await coordinator.submit(first)
        await blocked.wait()
        new = await coordinator.submit(second)
        assert new.scan_id != old.scan_id
        await coordinator.jobs[new.scan_id].task
        return coordinator, old.scan_id, new.scan_id

    with caplog.at_level(logging.INFO, logger="uvicorn.error"):
        coordinator, old_id, new_id = asyncio.run(scenario())

    assert coordinator.jobs[old_id].status == "cancelled"
    new_job = coordinator.jobs[new_id]
    assert new_job.status == "completed"
    assert coordinator.active_scan_id is None
    # Page 2, stored by the superseded scan, is recovered and reported first; only page 3 is scanned.
    assert scanned == [
        ["https://novelic.com/", "https://novelic.com/a", "https://novelic.com/b"],
        ["https://novelic.com/b"],
    ]
    assert [item.input_id for item in new_job.events[0].results] == ["2" * 64]
    assert sorted(new_job.results) == ["2" * 64, "3" * 64]
    assert (
        f"Webtech scan superseded old={old_id} new={new_id} "
        f"crawl_id=webtech-{EXECUTION_ID}" in caplog.text
    )


def test_another_execution_still_gets_busy_while_a_scan_runs():
    store = InMemoryRustfsStore()
    first = _envelope([(1, "")], execution_id=str(uuid4()))
    other = _envelope([(2, "a")], execution_id=str(uuid4()))
    started = asyncio.Event()

    async def scan(candidates, *, settings, progress_callback):
        del candidates, settings, progress_callback
        started.set()
        await asyncio.Event().wait()

    async def scenario():
        coordinator = scan_coordinator.ScanCoordinator(
            settings=service_settings(), store=store, scan_function=scan
        )
        running = await coordinator.submit(first)
        await started.wait()
        with pytest.raises(scan_coordinator.ScanBusyError):
            await coordinator.submit(other)
        assert coordinator.jobs[running.scan_id].status == "running"
        await coordinator.shutdown()

    asyncio.run(scenario())
