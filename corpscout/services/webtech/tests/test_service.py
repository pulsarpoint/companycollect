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
from service_models import CandidateManifest

API_TOKEN = "test-webtech-token-with-safe-length"
BASE_URI = "s3://webtech/webtech"
MANIFEST_URI = f"{BASE_URI}/candidates/test-manifest.json"


class InMemoryRustfsStore:
    """Exercise the service's real S3 key and serialization contract in memory."""

    def __init__(self) -> None:
        self.base_location = parse_s3_uri(BASE_URI)
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self) -> None:
        return

    def parse_allowed_uri(self, uri: str) -> S3Location:
        location = parse_s3_uri(uri)
        if location.bucket != self.base_location.bucket:
            raise ValueError("wrong bucket")
        if not location.key.startswith(f"{self.base_location.key}/"):
            raise ValueError("outside prefix")
        return location

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

    def list_keys(self, prefix: S3Location) -> tuple[str, ...]:
        return tuple(key for key in sorted(self.objects) if key.startswith(prefix.key))


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


def candidate_manifest() -> bytes:
    return json.dumps(
        {
            "schema_version": 2,
            "crawl_id": "CC-MAIN-2026-30",
            "partition_key": "harmonic_top_1000",
            "detector_version": WEBTECH_DETECTOR_VERSION,
            "dagster_run_id": "dagster-test-run",
            "generated_at": "2026-08-30T10:00:00Z",
            "candidates": [
                {"root_domain": "example.com", "harmonic_rank": 1},
                {"root_domain": "example.org", "harmonic_rank": 2},
                {"root_domain": "example.net", "harmonic_rank": 3},
            ],
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def scan_request(body: bytes) -> dict[str, object]:
    return {
        "schema_version": 1,
        "crawl_id": "CC-MAIN-2026-30",
        "partition_key": "harmonic_top_1000",
        "candidate_manifest_uri": MANIFEST_URI,
        "candidate_manifest_sha256": hashlib.sha256(body).hexdigest(),
        "detector_version": WEBTECH_DETECTOR_VERSION,
    }


def completed_result(candidate) -> WebtechDomainResult:
    report = ExtensionReport(
        schema_version=3,
        analysis_complete=True,
        analysis_status="complete",
        extension_version="1.4.1",
        page_token=uuid4(),
        url=f"https://{candidate.root_domain}/",
        technologies=[],
        failure_stage=None,
        error_message="",
        stage_timings_ms={},
    )
    return WebtechDomainResult.success(
        candidate=candidate,
        requested_url=f"https://{candidate.root_domain}",
        final_url=report.url,
        report=report,
        scanned_at=datetime.now(UTC),
        duration_ms=125,
    )


def test_scan_stores_each_domain_and_final_manifest() -> None:
    store = InMemoryRustfsStore()
    body = candidate_manifest()
    store.objects[parse_s3_uri(MANIFEST_URI).key] = body
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
        unauthorized = client.post("/v1/scans", json=scan_request(body))
        assert unauthorized.status_code == 401

        submitted = client.post(
            "/v1/scans",
            json=scan_request(body),
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
        assert sum(key.endswith("/report.json") for key in store.objects) == 3
        assert sum(key.endswith("/final-manifest.json") for key in store.objects) == 1

        repeated = client.post(
            "/v1/scans",
            json=scan_request(body),
            headers=headers,
        )
        assert repeated.json()["status"] == "completed"
        assert repeated.json()["scan_id"] == scan_id
    assert scan_calls == 1


def test_scan_logs_lifecycle_progress_and_completion(caplog) -> None:
    caplog.set_level(logging.INFO, logger="uvicorn.error")
    store = InMemoryRustfsStore()
    body = candidate_manifest()
    store.objects[parse_s3_uri(MANIFEST_URI).key] = body

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
            json=scan_request(body),
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
    body = candidate_manifest()
    store.objects[parse_s3_uri(MANIFEST_URI).key] = body

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
            json=scan_request(body),
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
    body = candidate_manifest()
    store.objects[parse_s3_uri(MANIFEST_URI).key] = body
    attempted_domains: list[tuple[str, ...]] = []

    async def fail_once_then_complete(candidates, *, settings, progress_callback):
        del settings
        attempted_domains.append(
            tuple(candidate.root_domain for candidate in candidates)
        )
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
            json=scan_request(body),
            headers=headers,
        ).json()
        scan_id = first["scan_id"]
        failed = _wait_for_terminal(client, scan_id, headers)
        assert failed["status"] == "failed"
        assert failed["completed_count"] == 1

        resumed = client.post(
            "/v1/scans",
            json=scan_request(body),
            headers=headers,
        ).json()
        assert resumed["scan_id"] == scan_id
        completed = _wait_for_terminal(client, scan_id, headers)

    assert completed["status"] == "completed"
    assert completed["completed_count"] == 3
    assert attempted_domains == [
        ("example.com", "example.org", "example.net"),
        ("example.org", "example.net"),
    ]


def test_scan_stalled_beyond_limit_is_failed_and_releases_the_slot(
    caplog, monkeypatch
) -> None:
    caplog.set_level(logging.INFO, logger="uvicorn.error")
    monkeypatch.setattr(scan_coordinator, "SCAN_HEARTBEAT_SECONDS", 0.01)
    monkeypatch.setattr(scan_coordinator, "SCAN_STALLED_AFTER_SECONDS", 0.02)
    monkeypatch.setattr(scan_coordinator, "SCAN_STALL_FAIL_AFTER_SECONDS", 0.05)
    store = InMemoryRustfsStore()
    body = candidate_manifest()
    store.objects[parse_s3_uri(MANIFEST_URI).key] = body
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
            json=scan_request(body),
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
            json=scan_request(body),
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


@pytest.mark.parametrize("separate_execution", [False, True])
def test_task_pages_are_separate_and_resume_without_rescanning(separate_execution):
    store = InMemoryRustfsStore()
    document = json.loads(candidate_manifest())
    task_id = str(uuid4())
    execution_id = str(uuid4()) if separate_execution else task_id
    document.update(
        schema_version=3,
        crawl_id=f"webtech-{execution_id}",
        dagster_run_id=execution_id,
    )
    document["candidates"] = [
        {
            "root_domain": "novelic.com",
            "harmonic_rank": 0,
            "task_id": task_id,
            "input_id": str(index) * 64,
            "page_url": f"https://novelic.com/{page}",
        }
        for index, page in [(1, ""), (2, "contact")]
    ]
    body = json.dumps(document).encode()
    store.objects[parse_s3_uri(MANIFEST_URI).key] = body
    calls = []

    async def fake_scan(candidates, *, settings, progress_callback):
        calls.extend(candidate.page_url for candidate in candidates)
        for candidate in candidates:
            await progress_callback(
                WebtechDomainResult.failure(
                    candidate=candidate,
                    outcome="navigation_error",
                    requested_url=candidate.page_url,
                    final_url="",
                    scanned_at=datetime.now(UTC),
                    duration_ms=1,
                    http_fallback_used=False,
                    error_message="fixture",
                )
            )
        return ()

    request = {**scan_request(body), "crawl_id": document["crawl_id"]}
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    for _ in range(2):
        app = create_app(
            settings=service_settings(), store=store, scan_function=fake_scan
        )
        with TestClient(app) as client:
            response = client.post("/v1/scans", json=request, headers=headers)
            assert response.status_code == 202, response.text
            scan_id = response.json()["scan_id"]
            for attempt in range(10):
                snapshot = client.get(
                    f"/v1/scans/{scan_id}", params={"wait_seconds": 1}, headers=headers
                ).json()["scan"]
                if snapshot["status"] == "completed":
                    break
            assert snapshot["status"] == "completed"
            assert snapshot["completed_count"] == 2
    assert calls == ["https://novelic.com/", "https://novelic.com/contact"]
    reports = [
        json.loads(body)
        for key, body in store.objects.items()
        if key.endswith("/report.json")
    ]
    assert len(reports) == 2
    assert {report["candidate"]["input_id"] for report in reports} == {
        "1" * 64,
        "2" * 64,
    }


@pytest.mark.parametrize(
    "invalid",
    ["mixed_tasks", "bad_task", "bad_input", "outside_domain", "wrong_execution"],
)
def test_execution_manifest_rejects_invalid_identities(invalid):
    task_id, execution_id = str(uuid4()), str(uuid4())
    document = json.loads(candidate_manifest())
    document.update(
        schema_version=3,
        crawl_id=f"webtech-{execution_id}",
        dagster_run_id=execution_id,
    )
    candidate = {
        "root_domain": "example.com",
        "task_id": task_id,
        "input_id": "a" * 64,
        "page_url": "https://example.com/",
    }
    document["candidates"] = [candidate]
    if invalid == "mixed_tasks":
        document["candidates"].append(
            {**candidate, "task_id": str(uuid4()), "input_id": "b" * 64}
        )
    elif invalid == "bad_task":
        candidate["task_id"] = "not-a-uuid"
    elif invalid == "bad_input":
        candidate["input_id"] = "invalid"
    elif invalid == "outside_domain":
        candidate["page_url"] = "https://other.com/"
    else:
        document["crawl_id"] = f"webtech-{uuid4()}"
    with pytest.raises(ValidationError):
        CandidateManifest.model_validate(document)


def _execution_manifest(execution_id, task_id, pages):
    document = json.loads(candidate_manifest())
    document.update(
        schema_version=3,
        crawl_id=f"webtech-{execution_id}",
        dagster_run_id=execution_id,
    )
    document["candidates"] = [
        {
            "root_domain": "novelic.com",
            "harmonic_rank": 0,
            "task_id": task_id,
            "input_id": str(index) * 64,
            "page_url": f"https://novelic.com/{page}",
        }
        for index, page in pages
    ]
    return document


def _run_scan(store, document, uri, scanned):
    async def fake_scan(candidates, *, settings, progress_callback):
        scanned.extend(candidate.page_url for candidate in candidates)
        for candidate in candidates:
            await progress_callback(completed_result(candidate))
        return ()

    body = json.dumps(document).encode()
    store.objects[parse_s3_uri(uri).key] = body
    request = {
        **scan_request(body),
        "crawl_id": document["crawl_id"],
        "partition_key": document["partition_key"],
        "candidate_manifest_uri": uri,
    }
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    app = create_app(settings=service_settings(), store=store, scan_function=fake_scan)
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
    execution_id, task_id = str(uuid4()), str(uuid4())
    document = _execution_manifest(execution_id, task_id, [(1, ""), (2, "a"), (3, "b")])
    events = _run_scan(store, document, f"{BASE_URI}/candidates/one.json", [])
    references = [item for event in events for item in event["results"]]
    assert sorted(item["input_id"] for item in references) == [
        "1" * 64,
        "2" * 64,
        "3" * 64,
    ]
    for item in references:
        assert (
            f"/crawl_id=webtech-{execution_id}/pages/input_id={item['input_id']}/report.json"
            in item["object_key"]
        )
        assert item["object_key"] in store.objects


def test_pages_done_in_one_envelope_are_reused_by_another():
    store = InMemoryRustfsStore()
    execution_id, task_id = str(uuid4()), str(uuid4())
    first = _execution_manifest(execution_id, task_id, [(1, ""), (2, "a")])
    first["partition_key"] = "envelope-first"
    _run_scan(store, first, f"{BASE_URI}/candidates/first.json", [])
    # A different envelope of the same execution: page 2 is done, page 3 is new.
    second = _execution_manifest(execution_id, task_id, [(2, "a"), (3, "b")])
    second["partition_key"] = "envelope-second"
    scanned = []
    events = _run_scan(store, second, f"{BASE_URI}/candidates/second.json", scanned)
    assert scanned == ["https://novelic.com/b"]
    assert [item["input_id"] for item in events[0]["results"]] == ["2" * 64]
    assert sorted(
        item["input_id"] for event in events for item in event["results"]
    ) == ["2" * 64, "3" * 64]


def test_another_execution_does_not_reuse_pages():
    store = InMemoryRustfsStore()
    task_id = str(uuid4())
    _run_scan(
        store,
        _execution_manifest(str(uuid4()), task_id, [(1, "")]),
        f"{BASE_URI}/candidates/a.json",
        [],
    )
    scanned = []
    _run_scan(
        store,
        _execution_manifest(str(uuid4()), task_id, [(1, "")]),
        f"{BASE_URI}/candidates/b.json",
        scanned,
    )
    assert scanned == ["https://novelic.com/"]


def _envelope_request(store, document, uri):
    from service_models import ScanRequest

    body = json.dumps(document).encode()
    store.objects[parse_s3_uri(uri).key] = body
    return ScanRequest.model_validate(
        {
            **scan_request(body),
            "crawl_id": document["crawl_id"],
            "partition_key": document["partition_key"],
            "candidate_manifest_uri": uri,
        }
    )


def test_new_envelope_of_the_same_execution_supersedes_the_running_scan(caplog):
    store = InMemoryRustfsStore()
    execution_id, task_id = str(uuid4()), str(uuid4())
    first = _execution_manifest(execution_id, task_id, [(1, ""), (2, "a"), (3, "b")])
    first["partition_key"] = "envelope-first"
    # Page 1 was published from the first envelope, so the next envelope omits it.
    second = _execution_manifest(execution_id, task_id, [(2, "a"), (3, "b")])
    second["partition_key"] = "envelope-second"
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
        old = await coordinator.submit(
            _envelope_request(store, first, f"{BASE_URI}/candidates/first.json")
        )
        await blocked.wait()
        new = await coordinator.submit(
            _envelope_request(store, second, f"{BASE_URI}/candidates/second.json")
        )
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
        f"crawl_id=webtech-{execution_id}" in caplog.text
    )


def test_another_execution_still_gets_busy_while_a_scan_runs():
    store = InMemoryRustfsStore()
    task_id = str(uuid4())
    first = _execution_manifest(str(uuid4()), task_id, [(1, "")])
    other = _execution_manifest(str(uuid4()), task_id, [(2, "a")])
    started = asyncio.Event()

    async def scan(candidates, *, settings, progress_callback):
        del candidates, settings, progress_callback
        started.set()
        await asyncio.Event().wait()

    async def scenario():
        coordinator = scan_coordinator.ScanCoordinator(
            settings=service_settings(), store=store, scan_function=scan
        )
        running = await coordinator.submit(
            _envelope_request(store, first, f"{BASE_URI}/candidates/first.json")
        )
        await started.wait()
        with pytest.raises(scan_coordinator.ScanBusyError):
            await coordinator.submit(
                _envelope_request(store, other, f"{BASE_URI}/candidates/other.json")
            )
        assert coordinator.jobs[running.scan_id].status == "running"
        await coordinator.shutdown()

    asyncio.run(scenario())
