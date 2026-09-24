# Webtech Queue Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the webtech scan queue onto the shared processing queue contract: one stored copy of each entry, no persisted batches, results published as they arrive, partition-based cleanup.

**Architecture:** The scanner keys each page result by execution (`crawl_id`) and `input_id` instead of by scan, so any envelope of pages reuses work already done, and it attaches stored result references to its progress events. Dagster derives remaining work from ClickHouse (frozen entries minus this execution's results minus fresh successes), sends envelopes of remaining entries, indexes result references in acknowledged micro-batches, and drops the task's partition when done. The S3 submission manifest, the S3 execution plan, bucket numbering and bucket checkpoints are removed.

**Tech Stack:** Python 3.14, Dagster, ClickHouse (clickhouse-driver), PostgreSQL (psycopg2), FastAPI scanner service, RustFS (S3), pytest against disposable ClickHouse/PostgreSQL containers.

**Spec:** `services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md`

## Global Constraints

- ClickHouse holds entry lists, PostgreSQL holds coordination (task status, receipts, locks, frozen execution settings).
- No persisted batches: a batch is only a transport envelope; nothing about it is stored, numbered or checkpointed.
- Done means a result exists. Progress and completion are derived from the append-only results table.
- One stored copy of each entry: no S3 submission manifest, no S3 execution plan.
- Entry table: `ENGINE = MergeTree`, `ORDER BY (task_id, input_id)`, `PARTITION BY task_id`; cleanup with `ALTER TABLE ... DROP PARTITION`; never `UPDATE`/`DELETE` individual rows (the only exception is the rare submission-retry delete of that submission's rows while the draft is open).
- Results are written in acknowledged micro-batches (every few seconds or few hundred rows), never one insert per result.
- Freshness skips are a query bounded by the execution's frozen start time.
- Every request carries a stable identity derived from `execution_id` (`crawl_id = webtech-<execution_id>`) and `input_id`.
- Destructive migrations carry an inline `throwIf` gate (memory: destructive migrations need inline gates); migration comments must not contain `;` (tests split migration files on `;`).
- Deploy order: Dagster accepts the new event field before the scanner sends it. Never deploy the scanner during an active scan. Do not restart `corpscout-dagster-dev`; code goes out by `light_sync`.
- Commit by explicit path, never `git add -A` (dagster_v3/CLAUDE.md).

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `services/dagster_v3/src/dagster_v3/defs/webtech/models.py` | modify | `RemoteScanProgressEvent.results` |
| `services/webtech/service_models.py` | modify | `ScanProgressEvent.results` |
| `services/webtech/scan_coordinator.py` | modify | execution-scoped page results, recovery across envelopes, references in events |
| `services/webtech/tests/test_service.py` | modify | scanner tests |
| `services/webtech/README.md` | modify | API contract |
| `services/dagster_v3/src/dagster_v3/defs/webtech/storage.py` | modify | `index_result_references` |
| `services/dagster_v3/src/dagster_v3/defs/common/result_buffer.py` | create | acknowledged micro-batch writer buffer |
| `clickhouse/migrations/000446_corpscout_webtech_queue_contract.{up,down}.sql` | create | partitioned `webtech_scan_input` with `submission_id` |
| `services/dagster_v3/src/dagster_v3/defs/webtech/input.py` | modify | no S3 manifest, `submission_id`, retry by reselect |
| `services/dagster_v3/src/dagster_v3/defs/webtech/execution.py` | rewrite | remaining query, completion from results, partition cleanup |
| `services/dagster_v3/src/dagster_v3/defs/webtech/assets.py` | modify | `monitor_webtech_scan(on_results=...)` |
| `services/dagster_v3/src/dagster_v3/defs/webtech/task_assets.py` | rewrite | envelope loop |
| `services/dagster_v3/tests/test_webtech_queue_contract.py` | create | model, identity, buffer, envelope tests |
| `services/dagster_v3/tests/test_webtech_input.py` | modify | fixture + retry test |
| `services/dagster_v3/tests/test_webtech_draft_execution.py` | modify | replace plan/bucket tests |
| `services/dagster_v3/tests/test_clickhouse_migrations.py` | modify | `EXPECTED_MIGRATIONS` |

Test commands (run from `services/dagster_v3` unless noted):
- `uv run --frozen --no-sync pytest <files> -q -p no:cacheprovider`
- scanner: `cd services/webtech && uv run --frozen pytest tests/test_service.py -q -p no:cacheprovider`

---

### Task 1: Dagster accepts result references in scanner progress events

The Dagster model uses `extra="forbid"`; if the scanner sends `results` first, every poll fails. This task ships first.

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/webtech/models.py` (class `RemoteScanProgressEvent`)
- Create: `services/dagster_v3/tests/test_webtech_queue_contract.py`

**Interfaces:**
- Produces: `RemoteScanProgressEvent.results: list[StoredResultReference]` (default empty).

- [ ] **Step 1: Write the failing test**

Create `services/dagster_v3/tests/test_webtech_queue_contract.py`:

```python
"""Queue contract pieces that need no database: models, identities, buffering."""

from dagster_v3.defs.webtech.models import RemoteScanProgressEvent

EVENT = {
    "sequence": 1,
    "completed_count": 1,
    "total_count": 2,
    "window_count": 1,
    "window_outcome_counts": {"success": 1},
    "window_technology_count": 0,
    "elapsed_seconds": 1.0,
    "domains_per_minute": 60.0,
}
REFERENCE = {
    "root_domain": "novelic.com",
    "harmonic_rank": 0,
    "input_id": "a" * 64,
    "outcome": "success",
    "timeout_stage": None,
    "technology_count": 0,
    "duration_ms": 10,
    "object_key": "webtech/scans/x/pages/input_id=" + "a" * 64 + "/report.json",
    "sha256": "0" * 64,
    "size_bytes": 1,
}


def test_progress_event_accepts_result_references():
    event = RemoteScanProgressEvent.model_validate({**EVENT, "results": [REFERENCE]})
    assert [item.input_id for item in event.results] == ["a" * 64]


def test_progress_event_without_results_still_parses():
    assert RemoteScanProgressEvent.model_validate(EVENT).results == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py -q -p no:cacheprovider`
Expected: FAIL — `extra_forbidden` validation error for `results`.

- [ ] **Step 3: Add the field**

In `models.py`, class `RemoteScanProgressEvent`, after `domains_per_minute: float`:

```python
    # Stored page results in this window. Empty from scanners that predate it.
    results: list[StoredResultReference] = Field(default_factory=list)
```

(`StoredResultReference` is defined earlier in the same module; `Field` is already imported.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py -q -p no:cacheprovider`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/webtech/models.py services/dagster_v3/tests/test_webtech_queue_contract.py
git commit -m "feat(webtech): accept stored result references in scanner progress events"
```

---

### Task 2: Scanner keys page results by execution and reports them in events

**Files:**
- Modify: `services/webtech/service_models.py` (class `ScanProgressEvent`)
- Modify: `services/webtech/scan_coordinator.py` (`ScanJob`, `_run.persist_result`, `_load_recovered_results`, new `_execution_page_location`)
- Modify: `services/webtech/tests/test_service.py`
- Modify: `services/webtech/README.md`

**Interfaces:**
- Produces: page result object key `…/scans/detector_version=<v>/crawl_id=<crawl_id>/pages/input_id=<input_id>/report.json` for candidates with an `input_id`; progress events carry `results: list[StoredResultReference]`; the first event of a scan that recovered results carries all recovered references.
- Unchanged: candidates without `input_id` (Common Crawl scans) keep per-scan keys.

- [ ] **Step 1: Write the failing tests**

Append to `services/webtech/tests/test_service.py`:

```python
def _execution_manifest(execution_id, task_id, pages):
    document = json.loads(candidate_manifest())
    document.update(schema_version=3, crawl_id=f"webtech-{execution_id}", dagster_run_id=execution_id)
    document["candidates"] = [
        {"root_domain": "novelic.com", "harmonic_rank": 0, "task_id": task_id,
         "input_id": str(index) * 64, "page_url": f"https://novelic.com/{page}"}
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
    request = {**scan_request(body), "crawl_id": document["crawl_id"],
               "partition_key": document["partition_key"], "candidate_manifest_uri": uri}
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    app = create_app(settings=service_settings(), store=store, scan_function=fake_scan)
    with TestClient(app) as client:
        response = client.post("/v1/scans", json=request, headers=headers)
        assert response.status_code == 202, response.text
        scan_id = response.json()["scan_id"]
        events, cursor = [], 0
        while True:
            payload = client.get(f"/v1/scans/{scan_id}", params={"after_event": cursor, "wait_seconds": 1},
                                 headers=headers).json()
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
    assert sorted(item["input_id"] for item in references) == ["1" * 64, "2" * 64, "3" * 64]
    for item in references:
        assert f"/crawl_id=webtech-{execution_id}/pages/input_id={item['input_id']}/report.json" in item["object_key"]
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
    assert sorted(item["input_id"] for event in events for item in event["results"]) == ["2" * 64, "3" * 64]


def test_another_execution_does_not_reuse_pages():
    store = InMemoryRustfsStore()
    task_id = str(uuid4())
    _run_scan(store, _execution_manifest(str(uuid4()), task_id, [(1, "")]), f"{BASE_URI}/candidates/a.json", [])
    scanned = []
    _run_scan(store, _execution_manifest(str(uuid4()), task_id, [(1, "")]), f"{BASE_URI}/candidates/b.json", scanned)
    assert scanned == ["https://novelic.com/"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd services/webtech && uv run --frozen pytest tests/test_service.py -q -p no:cacheprovider -k "carry_every or reused_by_another or does_not_reuse"`
Expected: FAIL — events have no `results` field (KeyError), and the second envelope rescans page 2.

- [ ] **Step 3: Add `results` to the scanner event model**

In `service_models.py`, class `ScanProgressEvent`, after `domains_per_minute`:

```python
    results: list[StoredResultReference] = Field(default_factory=list)
```

- [ ] **Step 4: Put the window's references into each event**

In `scan_coordinator.py`, `ScanJob._publish_progress_event`, add `results=list(window),` to the `ScanProgressEvent(...)` construction (next to `window_count=`).

Add this method to `ScanJob` (after `flush_progress_event`):

```python
    async def publish_recovered(self) -> None:
        """Report results reused from storage, so a new envelope still publishes them."""
        async with self._condition:
            if self.results and not self.events:
                self._pending_event_results.extend(self.results.values())
                self._publish_progress_event()
                self._condition.notify_all()
```

In `ScanCoordinator._run`, directly after `await job.mark_running()`:

```python
        await job.publish_recovered()
```

- [ ] **Step 5: Key execution pages by `crawl_id` and `input_id`**

Add to `ScanCoordinator` (next to `_scan_locations`):

```python
    def _execution_page_location(self, request: ScanRequest, input_id: str) -> S3Location:
        # One result per page per execution, shared by every envelope of that execution.
        return self.store.child(
            "scans",
            f"detector_version={request.detector_version}",
            f"crawl_id={request.crawl_id}",
            "pages",
            f"input_id={input_id}",
            "report.json",
        )
```

In `_run.persist_result`, replace the `location = S3Location(...)` block with:

```python
            if result.candidate.input_id:
                location = self._execution_page_location(job.request, result.candidate.input_id)
            else:
                location = S3Location(
                    bucket=job.result_prefix.bucket,
                    key=f"{job.result_prefix.key}/root_domain={result.candidate.root_domain}/report.json",
                )
```

- [ ] **Step 6: Recover execution pages across envelopes**

Change `_load_recovered_results` to take the request and look up execution pages. Replace the method with:

```python
    def _load_recovered_results(
        self,
        result_prefix: S3Location,
        scan_id: str,
        manifest: CandidateManifest,
        request: ScanRequest,
    ) -> dict[str, StoredResultReference]:
        candidates = {
            (candidate.input_id or candidate.root_domain): candidate
            for candidate in manifest.candidates
        }
        recovered: dict[str, StoredResultReference] = {}
        # Common Crawl scans keep their per-scan result objects.
        listing_prefix = S3Location(bucket=result_prefix.bucket, key=f"{result_prefix.key}/")
        for key in self.store.list_keys(listing_prefix):
            if not key.endswith("/report.json"):
                continue
            body = self.store.read_bytes(S3Location(bucket=result_prefix.bucket, key=key))
            document = StoredDomainResultDocument.model_validate_json(body)
            identity = document.candidate.input_id or document.candidate.root_domain
            if document.scan_id != scan_id or candidates.get(identity) != document.candidate:
                raise ValueError(f"stored result identity mismatch: {key}")
            recovered[identity] = _stored_reference(document, key, body)
        # Queue pages belong to the execution, whichever envelope scanned them.
        for identity, candidate in candidates.items():
            if not candidate.input_id or identity in recovered:
                continue
            location = self._execution_page_location(request, candidate.input_id)
            if not self.store.exists(location):
                continue
            body = self.store.read_bytes(location)
            document = StoredDomainResultDocument.model_validate_json(body)
            if (
                document.crawl_id != request.crawl_id
                or document.detector_version != request.detector_version
                or document.candidate != candidate
            ):
                raise ValueError(f"stored page identity mismatch: {location.key}")
            recovered[identity] = _stored_reference(document, location.key, body)
        return recovered
```

Add this module-level helper (next to `_domain_result_document`), moving the reference construction out of the old loop:

```python
def _stored_reference(
    document: StoredDomainResultDocument, key: str, body: bytes
) -> StoredResultReference:
    return StoredResultReference(
        root_domain=document.candidate.root_domain,
        harmonic_rank=document.candidate.harmonic_rank,
        input_id=document.candidate.input_id,
        outcome=document.outcome,
        timeout_stage=document.timeout_stage,
        technology_count=len(document.report.technologies) if document.report is not None else 0,
        duration_ms=document.duration_ms,
        object_key=key,
        sha256=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
    )
```

Update the call site in `submit`:

```python
            recovered = await asyncio.to_thread(
                self._load_recovered_results,
                result_prefix,
                scan_id,
                manifest,
                request,
            )
```

- [ ] **Step 7: Run the whole scanner suite**

Run: `cd services/webtech && uv run --frozen pytest -q -p no:cacheprovider`
Expected: all pass, including the existing `test_task_pages_are_separate_and_resume_without_rescanning` and `test_resubmit_recovers_stored_domains_after_a_failed_scan`.

- [ ] **Step 8: Document the contract**

In `services/webtech/README.md`, replace the sentence starting "The service derives an idempotent scan ID" through "writes `final-manifest.json` last." with:

```markdown
The service derives an idempotent scan ID from that content and its scanner settings.
Queue pages (candidates with an `input_id`) are stored once per execution at
`scans/detector_version=…/crawl_id=…/pages/input_id=…/report.json`, so a later
envelope of the same execution reuses every page already done. Common Crawl
candidates keep per-scan result objects. Each progress event (one per 20 stored
results) carries the stored result references of its window; the first event of a
scan that reused stored pages carries those references. `final-manifest.json` is
still written last and lists every result of the scan.
```

- [ ] **Step 9: Commit**

```bash
git add services/webtech/service_models.py services/webtech/scan_coordinator.py services/webtech/tests/test_service.py services/webtech/README.md
git commit -m "feat(webtech-scanner): execution-scoped page results and result references in events"
```

---

### Task 3: Index stored result references with execution identity

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/webtech/storage.py`
- Modify: `services/dagster_v3/tests/test_webtech_queue_contract.py`

**Interfaces:**
- Produces:
  - `index_result_references(*, clickhouse: ClickhouseResource, object_store: ObjectStoreResource, destination: WebtechS3Destination, crawl_id: str, detector_version: str, references: Sequence[StoredResultReference], dagster_run_id: str) -> int`
  - `_validate_execution_result_identity(document: StoredDomainResultDocument, *, reference: StoredResultReference, crawl_id: str, detector_version: str) -> None`
- Unchanged: `index_final_results` (Common Crawl path).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_webtech_queue_contract.py`:

```python
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.webtech.models import (
    StoredDomainResultDocument,
    StoredResultReference,
    WEBTECH_DETECTOR_VERSION,
)
from dagster_v3.defs.webtech.storage import _validate_execution_result_identity


def document(scan_id="first-scan", crawl_id="webtech-exec"):
    return StoredDomainResultDocument(
        schema_version=1, scan_id=scan_id, crawl_id=crawl_id, partition_key="envelope-a",
        detector_version=WEBTECH_DETECTOR_VERSION,
        candidate={"root_domain": "novelic.com", "harmonic_rank": 0, "task_id": "t",
                   "input_id": "a" * 64, "page_url": "https://novelic.com/"},
        outcome="success", requested_url="https://novelic.com", final_url="https://novelic.com/",
        final_hostname="novelic.com", http_fallback_used=False, scanned_at=datetime.now(UTC),
        duration_ms=10, error_message="", timeout_stage=None, report=None,
    )


def test_page_from_an_earlier_envelope_of_the_execution_is_accepted():
    _validate_execution_result_identity(
        document(scan_id="an-earlier-scan"),
        reference=StoredResultReference.model_validate(REFERENCE),
        crawl_id="webtech-exec", detector_version=WEBTECH_DETECTOR_VERSION,
    )


@pytest.mark.parametrize("changed", [{"crawl_id": "webtech-other"}, {"input_id": "b" * 64}])
def test_page_from_another_execution_or_input_is_rejected(changed):
    reference = StoredResultReference.model_validate({**REFERENCE, **({"input_id": changed["input_id"]} if "input_id" in changed else {})})
    with pytest.raises(ValueError, match="identity mismatch"):
        _validate_execution_result_identity(
            document(crawl_id=changed.get("crawl_id", "webtech-exec")),
            reference=reference, crawl_id="webtech-exec", detector_version=WEBTECH_DETECTOR_VERSION,
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name '_validate_execution_result_identity'`.

- [ ] **Step 3: Implement**

In `storage.py`, add `from collections.abc import Sequence` to the imports, then add after `index_final_results`:

```python
def index_result_references(
    *,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
    destination: WebtechS3Destination,
    crawl_id: str,
    detector_version: str,
    references: Sequence[StoredResultReference],
    dagster_run_id: str,
) -> int:
    """Publish stored page results of one execution, from any of its envelopes."""
    if not references:
        return 0
    assert_clickhouse_tables_exist(
        clickhouse,
        database=WEBTECH_CLICKHOUSE_DATABASE,
        tables=[WEBTECH_RESULT_TABLE, WEBTECH_TECHNOLOGY_TABLE, "technology_catalog", "technology_aliases"],
    )
    with clickhouse.get_connection() as client:
        catalog = load_technology_catalog(client)
    recorded_at = datetime.now(UTC)
    rows, detections, seen = [], [], set()
    for reference in references:
        if reference.input_id in seen:
            continue
        seen.add(reference.input_id)
        body = object_store.read_bytes(reference.object_key, bucket=destination.bucket)
        _validate_result_body(reference, body)
        stored = StoredDomainResultDocument.model_validate_json(body)
        _validate_execution_result_identity(
            stored, reference=reference, crawl_id=crawl_id, detector_version=detector_version
        )
        detections.extend(
            technology_rows(stored, reference, catalog, bucket=destination.bucket,
                            run_id=dagster_run_id, recorded_at=recorded_at)
        )
        rows.append(
            _clickhouse_row(stored, result_reference=reference, dagster_run_id=dagster_run_id,
                            recorded_at=recorded_at, result_bucket=destination.bucket)
        )
    with clickhouse.get_connection() as client:
        from dagster_v3.defs.webtech.writes import publish_scan_rows

        publish_scan_rows(client, rows, detections)
    return len(rows)


def _validate_execution_result_identity(
    document: StoredDomainResultDocument,
    *,
    reference: StoredResultReference,
    crawl_id: str,
    detector_version: str,
) -> None:
    # The scan and envelope may differ: pages are reused across envelopes of an execution.
    if (
        document.crawl_id != crawl_id
        or document.detector_version != detector_version
        or not reference.input_id
        or document.candidate.input_id != reference.input_id
        or document.candidate.root_domain != reference.root_domain
        or document.outcome != reference.outcome
        or _technology_count(document.report) != reference.technology_count
    ):
        raise ValueError(f"result identity mismatch: {reference.object_key}")
```

Re-indexing the same page is safe: `publish_scan_rows` keys rows by page, crawl, detector, the stored document's `scan_id` and report hash, and rejects only a *conflicting* report.

- [ ] **Step 4: Run tests**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py -q -p no:cacheprovider`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/webtech/storage.py services/dagster_v3/tests/test_webtech_queue_contract.py
git commit -m "feat(webtech): index stored result references by execution identity"
```

---

### Task 4: Acknowledged micro-batch buffer

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/common/result_buffer.py`
- Modify: `services/dagster_v3/tests/test_webtech_queue_contract.py`

**Interfaces:**
- Produces: `ResultBuffer(flush: Callable[[list[T]], object], *, max_items: int = 500, max_seconds: float = 5.0, clock: Callable[[], float] = time.monotonic)` with `add(items: Iterable[T]) -> None`, `flush_if_due() -> None`, `flush() -> int`, `len(buffer)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_webtech_queue_contract.py`:

```python
from dagster_v3.defs.common.result_buffer import ResultBuffer


def test_buffer_flushes_by_size_and_by_age():
    now = [0.0]
    written = []
    buffer = ResultBuffer(written.append, max_items=3, max_seconds=5, clock=lambda: now[0])
    buffer.add([1, 2])
    assert written == []
    buffer.add([3])
    assert written == [[1, 2, 3]]
    buffer.add([4])
    now[0] = 4.9
    buffer.flush_if_due()
    assert written == [[1, 2, 3]]
    now[0] = 5.0
    buffer.flush_if_due()
    assert written == [[1, 2, 3], [4]]
    assert buffer.flush() == 0


def test_failed_flush_keeps_items_for_the_next_attempt():
    calls = []

    def flaky(batch):
        calls.append(list(batch))
        if len(calls) == 1:
            raise RuntimeError("insert not acknowledged")

    buffer = ResultBuffer(flaky, max_items=10)
    buffer.add(["a", "b"])
    with pytest.raises(RuntimeError, match="acknowledged"):
        buffer.flush()
    assert len(buffer) == 2
    assert buffer.flush() == 2
    assert calls == [["a", "b"], ["a", "b"]]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py -q -p no:cacheprovider`
Expected: FAIL — `ModuleNotFoundError: dagster_v3.defs.common.result_buffer`.

- [ ] **Step 3: Implement**

Create `src/dagster_v3/defs/common/result_buffer.py`:

```python
"""Acknowledged micro-batches for queue results.

ClickHouse creates one part per insert, and async inserts do not coalesce a single
writer that waits for each acknowledgement. Buffer results and insert them every few
seconds or every few hundred rows. Items leave the buffer only after the flush
callback returns, so a failed insert is retried with the same rows.
"""

import time
from collections.abc import Callable, Iterable
from typing import Generic, TypeVar

T = TypeVar("T")


class ResultBuffer(Generic[T]):
    def __init__(
        self,
        flush: Callable[[list[T]], object],
        *,
        max_items: int = 500,
        max_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._flush = flush
        self._max_items = max_items
        self._max_seconds = max_seconds
        self._clock = clock
        self._items: list[T] = []
        self._since: float | None = None

    def __len__(self) -> int:
        return len(self._items)

    def add(self, items: Iterable[T]) -> None:
        for item in items:
            if self._since is None:
                self._since = self._clock()
            self._items.append(item)
        if len(self._items) >= self._max_items:
            self.flush()
        else:
            self.flush_if_due()

    def flush_if_due(self) -> None:
        if self._since is not None and self._clock() - self._since >= self._max_seconds:
            self.flush()

    def flush(self) -> int:
        if not self._items:
            self._since = None
            return 0
        batch = list(self._items)
        self._flush(batch)
        self._items = []
        self._since = None
        return len(batch)
```

- [ ] **Step 4: Run tests**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py -q -p no:cacheprovider`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/common/result_buffer.py services/dagster_v3/tests/test_webtech_queue_contract.py
git commit -m "feat(dagster): acknowledged micro-batch buffer for queue results"
```

---

### Task 5: Partitioned webtech entry table with `submission_id`

**Files:**
- Create: `clickhouse/migrations/000446_corpscout_webtech_queue_contract.up.sql`
- Create: `clickhouse/migrations/000446_corpscout_webtech_queue_contract.down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`)
- Modify: `services/dagster_v3/tests/test_webtech_input.py` (`database` fixture)

**Interfaces:**
- Produces: `corpscout.webtech_scan_input` with `submission_id String`, `PARTITION BY task_id`, `ORDER BY (task_id, input_id)`; results table in tests gains `crawl_id`.

- [ ] **Step 1: Confirm the migration number is free**

Run: `ls clickhouse/migrations | tail -2` and on prod `ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT max(version) FROM corpscout.schema_migrations"'`
Expected: highest local and prod version is 445. If another workstream took 446, use the next free number everywhere in this task (memory: renumber before merge).

- [ ] **Step 2: Write the migrations**

`000446_corpscout_webtech_queue_contract.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Shared queue contract: one partition per task so cleanup is DROP PARTITION,
-- sorted by task for reads, and a submission_id so a retried import replaces only
-- its own rows. Queue inputs are purged after completion, so the table is rebuilt
-- only while empty.
SELECT throwIf(count() > 0, 'webtech_scan_input must be empty before its layout changes')
FROM corpscout.webtech_scan_input;

DROP TABLE IF EXISTS corpscout.webtech_scan_input;

CREATE TABLE corpscout.webtech_scan_input
(
    task_id String,
    input_id String,
    root_domain String,
    website_origin String,
    page_url String,
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submission_id String,
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND length(input_id) = 64,
    CONSTRAINT valid_target CHECK notEmpty(root_domain)
        AND protocol(page_url) IN ('http', 'https') AND notEmpty(domain(page_url)),
    CONSTRAINT valid_source CHECK notEmpty(source_name) AND notEmpty(submission_id)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, input_id);
```

`000446_corpscout_webtech_queue_contract.down.sql`:

```sql
SELECT throwIf(count() > 0, 'webtech_scan_input must be empty before its layout changes')
FROM corpscout.webtech_scan_input;

DROP TABLE IF EXISTS corpscout.webtech_scan_input;

CREATE TABLE corpscout.webtech_scan_input
(
    task_id String,
    input_id String,
    root_domain String,
    website_origin String,
    page_url String,
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(root_domain) % 128),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    INDEX task_id_index task_id TYPE set(0) GRANULARITY 1,
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND length(input_id) = 64,
    CONSTRAINT valid_target CHECK notEmpty(root_domain)
        AND protocol(page_url) IN ('http', 'https') AND notEmpty(domain(page_url)),
    CONSTRAINT valid_source CHECK notEmpty(source_name)
)
ENGINE = MergeTree
ORDER BY (input_id, task_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
```

- [ ] **Step 3: Register the migration and update the test fixture**

In `tests/test_clickhouse_migrations.py`, append `"000446_corpscout_webtech_queue_contract",` as the last entry of `EXPECTED_MIGRATIONS`.

In `tests/test_webtech_input.py`, `database` fixture: add `crawl_id String DEFAULT ''` to the `CREATE TABLE corpscout.webtech_domain_scan_results (...)` column list, and after the 438 migration loop apply 446:

```python
    for name in ("000446_corpscout_webtech_queue_contract.up.sql",):
        path = Path(__file__).parents[3] / "clickhouse/migrations" / name
        for statement in path.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
```

Add this test to `tests/test_webtech_input.py`:

```python
def test_entry_table_follows_the_queue_contract(database):
    client, _ = database
    assert client.execute(
        "SELECT partition_key, sorting_key FROM system.tables WHERE database='corpscout' AND name='webtech_scan_input'"
    ) == [("task_id", "task_id, input_id")]
```

- [ ] **Step 4: Run the migration and input tests**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_webtech_input.py::test_entry_table_follows_the_queue_contract -q -p no:cacheprovider`
Expected: PASS. (Other `test_webtech_input.py` tests fail until Task 6 adds `submission_id` to inserts — that is expected here.)

- [ ] **Step 5: Commit**

```bash
git add clickhouse/migrations/000446_corpscout_webtech_queue_contract.up.sql clickhouse/migrations/000446_corpscout_webtech_queue_contract.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py services/dagster_v3/tests/test_webtech_input.py
git commit -m "feat(clickhouse): partition webtech_scan_input by task and add submission_id"
```

---

### Task 6: Submissions without the S3 manifest

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/webtech/input.py`
- Modify: `services/dagster_v3/tests/test_webtech_input.py`

**Interfaces:**
- Produces: `INPUT_COLUMNS` ends with `"submission_id"`; `load_draft(*, config, submission_id, run_id, clickhouse, store) -> dict` (no `object_store`); asset `webtech_scan_input` no longer requires `webtech_object_store`.
- Keeps: `normalized_target`, `source_query`, `insert_input_batch`, the 1,000,000 cap, receipts and fingerprints (`draft_queue` unchanged; crawl still uses `save_manifest`).

Normalization stays in Python for now: `root_domain()` needs the public-suffix list. Entries stream ClickHouse → Python → ClickHouse with no file and no S3 copy.

- [ ] **Step 1: Replace the manifest-replay test**

In `tests/test_webtech_input.py`:
- change the `add` helper to call `load_draft(...)` without `object_store=objects` (keep the `objects` parameter so call sites stay unchanged);
- delete `test_retry_after_committed_write_uses_manifest_not_changed_source`;
- add:

```python
def test_retry_replaces_only_its_own_rows_from_the_current_source(
    database, store, objects, monkeypatch
):
    from dagster_v3.defs.webtech import input as module

    client, resource = database
    processing, _ = store
    client.execute("DROP TABLE IF EXISTS corpscout.webtech_test_source")
    client.execute(
        "CREATE TABLE corpscout.webtech_test_source (id String,url String,country String) ENGINE=MergeTree ORDER BY id"
    )
    client.execute(
        "INSERT INTO corpscout.webtech_test_source VALUES ('1','novelic.com','SE'),('2','example.org','DE')"
    )
    other = add(resource, processing, objects, targets=["other.se"], source_name="manual")
    submission_id = str(uuid4())
    config = dict(
        submission_id=submission_id,
        source_relation="corpscout.webtech_test_source",
        target_column="url",
        source_record_id_column="id",
        filters={"country": ["SE"]},
    )
    original = module.insert_input_batch

    def crash_after_insert(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("lost acknowledgement")

    with monkeypatch.context() as patch:
        patch.setattr(module, "insert_input_batch", crash_after_insert)
        with pytest.raises(RuntimeError, match="acknowledgement"):
            add(resource, processing, objects, **config)
    client.execute("INSERT INTO corpscout.webtech_test_source VALUES ('3','example.com','SE')")
    result = add(resource, processing, objects, **config)
    # The retry reselects the source: its own rows are replaced, other submissions kept.
    assert result["input_count"] == 2
    assert sorted(client.execute(
        "SELECT root_domain, submission_id != '' FROM corpscout.webtech_scan_input"
    )) == [("example.com", True), ("novelic.com", True), ("other.se", True)]
    assert result["task_id"] == other["task_id"]
    assert not [key for (_, key) in objects.client().objects if key.startswith("queue-inputs/")]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_input.py -q -p no:cacheprovider`
Expected: FAIL — `load_draft()` still requires `object_store`, and inserts lack `submission_id`.

- [ ] **Step 3: Implement**

In `input.py`:

1. Remove imports no longer used: `from pathlib import Path`, `from tempfile import TemporaryDirectory`, `from urllib.parse import urlsplit`, `from dagster_v3.defs.common.resources import ObjectStoreResource`.
2. Append `"submission_id",` as the last entry of `INPUT_COLUMNS`.
3. Replace `load_draft` with:

```python
def load_draft(
    *,
    config: WebtechInputConfig,
    submission_id: str,
    run_id: str,
    clickhouse: ClickhouseResource,
    store: ProcessingStore,
) -> dict:
    selection = config.model_dump(
        exclude={"task_id", "submission_id", "queue_scope", "targets", "se_domain_filters", "excluded_targets"}
    )
    # Omit new empty options so receipts created before these fields keep their fingerprint.
    if config.se_domain_filters is not None:
        selection["se_domain_filters"] = config.se_domain_filters.model_dump()
    if config.excluded_targets:
        selection["excluded_targets"] = sorted(set(config.excluded_targets))
    selection["filters"] = {key: sorted(set(value)) for key, value in config.filters.items()}
    selection["manual_count"] = len(set(config.targets))
    selection["manual_sha256"] = hashlib.sha256(json.dumps(sorted(set(config.targets))).encode()).hexdigest()
    fingerprint = hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()
    receipt = draft_queue.submission(store, submission_id)
    if receipt is not None:
        task_id = str(receipt["task_id"])
        task = store.task(task_id)
        if (
            task["queue_scope"] != config.queue_scope
            or task["processor"] != PROCESSOR_VERSION
            or (config.task_id is not None and config.task_id != task_id)
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError("submission_id already belongs to a different selection, task or scope")
        if receipt["status"] == "completed":
            return {"task_id": task_id, "submission_id": submission_id,
                    "input_count": receipt["input_count"], "total": task["total"]}
    while True:
        if receipt is None:
            task_id = draft_queue.find_draft(
                store, scope=config.queue_scope, processor=PROCESSOR_VERSION, task_id=config.task_id
            )
        with store.selection_lock(task_id):
            if store.task(task_id)["status"] != "draft" and receipt is None and config.task_id is None:
                continue  # Start won the race. Re-resolve to the new default draft.
            latest = draft_queue.submission(store, submission_id)
            if latest is not None and (
                str(latest["task_id"]) != task_id or latest["selection_fingerprint"] != fingerprint
            ):
                raise ValueError("submission_id already belongs to a different selection or task")
            if latest is not None and latest["status"] == "completed":
                return {"task_id": task_id, "submission_id": submission_id,
                        "input_count": latest["input_count"], "total": store.task(task_id)["total"]}
            receipt = draft_queue.prepare_submission(
                store, task_id=task_id, submission_id=submission_id,
                source=config.source_name or config.source_relation or "manual",
                selection=selection, fingerprint=fingerprint,
            )
            try:
                with clickhouse.get_connection() as client:
                    query_id = "webtech-submission:" + submission_id
                    client.execute("KILL QUERY WHERE query_id=%(id)s SYNC", {"id": query_id})
                    # A retry replaces this submission's rows from the current source.
                    client.execute(
                        f"DELETE FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                        settings={"lightweight_deletes_sync": 2},
                    )
                    source = (
                        client.execute_iter(*source_query(config), settings={"max_block_size": 5000})
                        if config.source_relation
                        else ((value, value) for value in sorted(set(config.targets))[: config.max_rows])
                    )
                    source_name = config.source_name or config.source_relation or "manual"
                    seen: set[str] = set()
                    batch = []
                    for value, record_id in source:
                        identity, domain, origin, page = normalized_target(value)
                        if identity in seen:
                            continue
                        seen.add(identity)
                        if len(seen) > 1_000_000:
                            raise ValueError("A submission supports at most one million distinct pages")
                        batch.append((task_id, identity, domain, origin, page, source_name,
                                      record_id, run_id, submission_id))
                        if len(batch) == 5000:
                            insert_input_batch(client, batch, task_id=task_id, query_id=query_id)
                            batch = []
                    if batch:
                        insert_input_batch(client, batch, task_id=task_id, query_id=query_id)
                total = ClickHouseInputQueue(clickhouse, INPUT_RELATION, selection_task_id=task_id).inspect()["total"]
                draft_queue.finish_submission(
                    store, submission_id=submission_id, task_id=task_id, count=len(seen), total=total
                )
                return {"task_id": task_id, "submission_id": submission_id,
                        "input_count": len(seen), "total": total}
            except BaseException:
                # The retry fences its stable ClickHouse query before replacing rows.
                draft_queue.fail_submission(store, submission_id)
                raise
```

4. In the `webtech_scan_input` asset: change `kinds={"clickhouse", "postgres", "s3"}` to `kinds={"clickhouse", "postgres"}`, remove the `webtech_object_store: ObjectStoreResource` parameter, and drop `object_store=webtech_object_store` from the `load_draft(...)` call.

`insert_input_batch` still skips identities already present in the task (another submission may have added the same page) and still rejects conflicting payloads. Its row indexes `[2:5]` are unchanged because `submission_id` is appended last.

- [ ] **Step 4: Run the input tests**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_input.py -q -p no:cacheprovider`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/webtech/input.py services/dagster_v3/tests/test_webtech_input.py
git commit -m "feat(webtech): submissions stream into the queue without an S3 manifest"
```

---

### Task 7: Execution derives remaining work and completion from results

**Files:**
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/webtech/execution.py`
- Modify: `services/dagster_v3/tests/test_webtech_draft_execution.py`

**Interfaces:**
- Keeps: `start_execution(*, store, clickhouse, task_id, execution_id, force_rescan, recent_days, batch_size, run_id) -> dict` (unchanged).
- Produces:
  - `execution_crawl_id(execution: dict) -> str` → `"webtech-<execution_id>"`
  - `remaining_inputs(client, task: dict, *, limit: int, input_ids: Sequence[str] | None = None) -> list[tuple[str, str, str, str]]` → `(input_id, root_domain, website_origin, page_url)` ordered by `input_id`; `input_ids` restricts the answer to those entries
  - `finish_execution(store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str) -> dict`
  - `purge_completed_inputs(store, clickhouse, task_id) -> None` (DROP PARTITION)
- Removes: `prepare_execution`, `read_plan_object`, `write_plan_object`, `record_bucket`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_webtech_draft_execution.py`:
- update the import block to import `start_execution, execution_crawl_id, remaining_inputs, finish_execution, purge_completed_inputs` from `dagster_v3.defs.webtech.execution`;
- delete the `decisions` helper and these tests, which exercise the removed plan/bucket API or DELETE-based cleanup: `test_freshness_is_prepared_per_page_and_reused_on_retry`, `test_incomplete_results_retain_inputs_but_published_errors_complete`, `test_preparation_retry_keeps_already_saved_decisions`, `test_cleanup_is_scoped_and_retries_lost_delete_ack` (the results-asset test is rewritten in Task 8);
- add:

```python
def publish(client, task, rows, *, outcome="success", scanned_at=None):
    execution = task["config"]["execution"]
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (crawl_id,root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome,task_id,input_id) VALUES",
        [
            (execution_crawl_id(execution), root, origin, page, WEBTECH_DETECTOR_VERSION,
             scanned_at or datetime.now(UTC), "scan-1", outcome, str(task["task_id"]), identity)
            for identity, root, origin, page in rows
        ],
    )


def test_remaining_excludes_published_and_fresh_pages(database, store, objects):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome) VALUES",
        [
            ("fresh.com", "https://fresh.com", "https://fresh.com/", WEBTECH_DETECTOR_VERSION, datetime.now(UTC) - timedelta(days=1), "old", "success"),
            ("failed.com", "https://failed.com", "https://failed.com/", WEBTECH_DETECTOR_VERSION, datetime.now(UTC) - timedelta(days=1), "old", "navigation_error"),
        ],
    )
    task_id = add(resource, processing, objects, targets=["fresh.com", "failed.com", "a.com", "b.com"])["task_id"]
    task = start(processing, resource, task_id)
    with resource.get_connection() as connection:
        first = remaining_inputs(connection, task, limit=10)
    # Fresh successes are skipped; an earlier failure is retried.
    assert sorted(row[1] for row in first) == ["a.com", "b.com", "failed.com"]
    publish(client, task, first[:1])
    with resource.get_connection() as connection:
        assert remaining_inputs(connection, task, limit=10) == first[1:]
        # The same answer on resume: results of this execution are after its start.
        assert remaining_inputs(connection, task, limit=1) == first[1:2]
        # Restricted to chosen entries, e.g. one envelope.
        assert remaining_inputs(connection, task, limit=10, input_ids=[first[2][0]]) == first[2:3]


def test_force_rescan_includes_fresh_pages(database, store, objects):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome) VALUES",
        [("fresh.com", "https://fresh.com", "https://fresh.com/", WEBTECH_DETECTOR_VERSION, datetime.now(UTC) - timedelta(days=1), "old", "success")],
    )
    task_id = add(resource, processing, objects, targets=["fresh.com"])["task_id"]
    task = start(processing, resource, task_id, force_rescan=True)
    with resource.get_connection() as connection:
        assert [row[1] for row in remaining_inputs(connection, task, limit=10)] == ["fresh.com"]


def test_finish_counts_results_and_skips_then_purge_drops_the_partition(database, store, objects):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome) VALUES",
        [("fresh.com", "https://fresh.com", "https://fresh.com/", WEBTECH_DETECTOR_VERSION, datetime.now(UTC) - timedelta(days=1), "old", "success")],
    )
    other = add(resource, processing, objects, queue_scope="other", targets=["kept.com"])["task_id"]
    task_id = add(resource, processing, objects, targets=["fresh.com", "a.com", "b.com"])["task_id"]
    task = start(processing, resource, task_id)
    with resource.get_connection() as connection:
        rows = remaining_inputs(connection, task, limit=10)
    with processing.selection_lock(task_id), pytest.raises(ValueError, match="published outcome"):
        finish_execution(processing, resource, task_id)
    publish(client, task, rows[:1])
    publish(client, task, rows[1:], outcome="navigation_error")
    with processing.selection_lock(task_id):
        finished = finish_execution(processing, resource, task_id)
        assert (finished["succeeded_count"], finished["terminal_failed_count"], finished["skipped_count"]) == (1, 1, 1)
        purge_completed_inputs(processing, resource, task_id)
        purge_completed_inputs(processing, resource, task_id)  # idempotent
    assert client.execute("SELECT DISTINCT task_id FROM corpscout.webtech_scan_input") == [(other,)]
    assert processing.task(task_id)["inputs_purged_at"] is not None
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_draft_execution.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'remaining_inputs'`.

- [ ] **Step 3: Rewrite `execution.py`**

Keep `start_execution` exactly as it is today. Replace the rest of the module (everything after `start_execution`, and the imports) so the file reads:

```python
"""Freeze draft membership; derive remaining work and completion from results."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from psycopg2.extras import Json
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingStore
from dagster_v3.defs.webtech.input import INPUT_RELATION, PROCESSOR_VERSION
from dagster_v3.defs.webtech.models import WEBTECH_DETECTOR_VERSION

RESULT_RELATION = "corpscout.webtech_domain_scan_results"


# (start_execution: unchanged, keep the existing function here)


def execution_crawl_id(execution: dict) -> str:
    return f"webtech-{execution['execution_id']}"


def _clickhouse_time(value: str) -> str:
    return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def _parameters(task: dict) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "crawl": execution_crawl_id(execution),
        "force": int(execution["profile"]["force_rescan"]),
        "detector": WEBTECH_DETECTOR_VERSION,
        "cutoff": _clickhouse_time(execution["freshness_cutoff"]),
        "started": _clickhouse_time(execution["started_at"]),
    }


# Frozen entries without a result in this execution, minus pages whose latest result
# inside the frozen freshness window is a success. The window ends at the execution's
# start, so results written by this execution never change the answer on resume.
_REMAINING = f"""
FROM {INPUT_RELATION}
WHERE task_id = %(task)s
  AND input_id NOT IN (
      SELECT input_id FROM {RESULT_RELATION}
      WHERE task_id = %(task)s AND crawl_id = %(crawl)s)
  AND (%(force)s = 1 OR (root_domain, website_origin, page_url) NOT IN (
      SELECT root_domain, website_origin, page_url
      FROM {RESULT_RELATION} FINAL
      WHERE detector_version = %(detector)s
        AND scanned_at >= toDateTime64(%(cutoff)s, 6, 'UTC')
        AND scanned_at <= toDateTime64(%(started)s, 6, 'UTC')
        AND root_domain IN (SELECT root_domain FROM {INPUT_RELATION} WHERE task_id = %(task)s)
      GROUP BY root_domain, website_origin, page_url
      HAVING argMax(outcome, tuple(scanned_at, scan_id)) = 'success'))
"""


def remaining_inputs(
    client, task: dict, *, limit: int, input_ids: Sequence[str] | None = None
) -> list[tuple[str, str, str, str]]:
    only = " AND input_id IN %(ids)s " if input_ids else " "
    return [
        tuple(row)
        for row in client.execute(
            "SELECT input_id, root_domain, website_origin, page_url"
            + _REMAINING
            + only
            + "ORDER BY input_id LIMIT %(limit)s",
            {**_parameters(task), "limit": limit, "ids": tuple(input_ids or ())},
        )
    ]


def finish_execution(store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str) -> dict:
    """Completion is derived from results; skipped pages are the fresh ones."""
    task = store.task(task_id)
    parameters = _parameters(task)
    with clickhouse.get_connection() as client:
        [(remaining,)] = client.execute("SELECT count()" + _REMAINING, parameters)
        if remaining:
            raise ValueError(f"Not every input has a published outcome ({remaining} remaining)")
        [(succeeded, failed)] = client.execute(
            f"""SELECT countIf(outcome = 'success'), countIf(outcome != 'success') FROM (
                SELECT input_id, argMax(outcome, tuple(scanned_at, scan_id)) AS outcome
                FROM {RESULT_RELATION}
                WHERE task_id = %(task)s AND crawl_id = %(crawl)s
                GROUP BY input_id)""",
            parameters,
        )
    skipped = task["total"] - succeeded - failed
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET succeeded_count=%s, terminal_failed_count=%s,
            skipped_count=%s, admitted_count=%s, status='completed',
            work_config=work_config || '{"finished":true}'::jsonb,
            completed_at=coalesce(completed_at, now())
            WHERE task_id=%s RETURNING *""",
            (succeeded, failed, skipped, task["total"], task_id),
        )
        return dict(cursor.fetchone())


def purge_completed_inputs(store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str) -> None:
    """Drop the completed task's partition. Results and task history remain."""
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != PROCESSOR_VERSION
        or task["queue_scope"] is None
        or task["status"] != "completed"
        or not task["work_config"].get("finished")
    ):
        raise ValueError("Only fully completed Webtech tasks can clear their inputs")
    if task["inputs_purged_at"] is not None:
        return
    with clickhouse.get_connection() as client:
        client.execute(f"ALTER TABLE {INPUT_RELATION} DROP PARTITION %(task)s", {"task": task_id})
        [(left,)] = client.execute(
            f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s", {"task": task_id}
        )
        if left:
            raise RuntimeError("Completed Webtech input cleanup is not yet visible")
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET inputs_purged_at=coalesce(inputs_purged_at, now()) WHERE task_id=%s",
            (task_id,),
        )
```

(`timedelta`, `uuid4`, `Json` and `ClickHouseInputQueue` stay imported because `start_execution` uses them.)

- [ ] **Step 4: Run the execution tests**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_draft_execution.py -q -p no:cacheprovider -k "not results_asset"`
Expected: PASS (the results-asset test is rewritten in Task 8).

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/webtech/execution.py services/dagster_v3/tests/test_webtech_draft_execution.py
git commit -m "feat(webtech): remaining work and completion come from results, cleanup drops the partition"
```

---

### Task 8: Envelope loop with continuous, micro-batched publishing

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/webtech/assets.py` (`monitor_webtech_scan`)
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/webtech/task_assets.py`
- Modify: `services/dagster_v3/tests/test_webtech_draft_execution.py`
- Modify: `services/dagster_v3/tests/test_webtech_queue_contract.py`

**Interfaces:**
- Consumes: `index_result_references` (Task 3), `ResultBuffer` (Task 4), `remaining_inputs`, `finish_execution`, `purge_completed_inputs`, `execution_crawl_id`, `start_execution` (Task 7), `RemoteScanProgressEvent.results` (Task 1).
- Produces: `monitor_webtech_scan(..., on_results: Callable[[list[StoredResultReference]], None] | None = None, on_poll: Callable[[], None] | None = None)`; `envelope_partition_key(input_ids: Sequence[str]) -> str`.

- [ ] **Step 1: Precondition — no legacy (non-draft) webtech tasks are open**

Run on the processing PostgreSQL:

```bash
ssh companycollect "sudo docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"SELECT count(*) FROM processing.tasks WHERE processor='webtech-input-v1' AND queue_scope IS NULL AND status NOT IN ('completed','cancelled')\""
```

Expected: `0`. If not 0, stop and report: the legacy branch cannot be removed yet.

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_webtech_queue_contract.py`:

```python
from dagster_v3.defs.webtech.task_assets import envelope_partition_key


def test_envelope_key_depends_only_on_its_entries():
    assert envelope_partition_key(["b" * 64, "a" * 64]) == envelope_partition_key(["a" * 64, "b" * 64])
    assert envelope_partition_key(["a" * 64]) != envelope_partition_key(["b" * 64])
    assert envelope_partition_key(["a" * 64]).startswith("envelope-")
```

In `tests/test_webtech_draft_execution.py`, replace `test_results_asset_resumes_publication_then_clears_completed_inputs` with (keep the existing `failed_domain` fixture):

```python
def test_results_asset_publishes_continuously_resumes_and_clears(
    database, store, objects, monkeypatch, failed_domain
):
    import dagster as dg
    from dagster_v3.defs.common.processing import ProcessingResource
    from dagster_v3.defs.webtech import task_assets as module
    from dagster_v3.defs.webtech.client import WebtechApiResource
    from dagster_v3.defs.webtech.models import RemoteScanSnapshot, StoredResultReference
    from dagster_v3.defs.webtech.storage import WebtechS3Destination

    client, resource = database
    processing, dsn = store
    task_id = add(resource, processing, objects, targets=["novelic.com", "example.com", "a.com"])["task_id"]
    envelopes = {}
    published = []
    crash_once = [True]

    def outcome(root):
        return "navigation_error" if root == failed_domain else "success"

    def submit(api, manifest):
        del api
        document = json.loads(objects.read_bytes(manifest.uri.split("/", 3)[3]))
        envelopes[manifest.partition_key] = document["candidates"]
        now = datetime.now(UTC)
        return RemoteScanSnapshot(
            scan_id=manifest.partition_key, status="running", crawl_id=manifest.crawl_id,
            partition_key=manifest.partition_key, detector_version=WEBTECH_DETECTOR_VERSION,
            candidate_manifest_uri=manifest.uri, result_prefix_uri="s3://webtech/webtech/x",
            final_manifest_uri="s3://webtech/webtech/x/final.json",
            total_count=len(document["candidates"]), completed_count=0, outcome_counts={},
            technology_count=0, started_at=now, finished_at=None, last_progress_at=now,
            elapsed_seconds=0, progress_age_seconds=0, domains_per_minute=0,
            latest_event_sequence=0, error_message="",
        )

    def monitor(*, submission, on_results, on_poll, **kwargs):
        del kwargs
        candidates = envelopes[submission.scan_id]
        on_results([
            StoredResultReference(
                root_domain=row["root_domain"], harmonic_rank=0, input_id=row["input_id"],
                outcome=outcome(row["root_domain"]), timeout_stage=None, technology_count=0,
                duration_ms=1, object_key=f"webtech/pages/{row['input_id']}", sha256="0" * 64,
                size_bytes=1,
            )
            for row in candidates
        ])
        on_poll()
        now = datetime.now(UTC)
        return RemoteScanSnapshot(
            scan_id=submission.scan_id, status="completed", crawl_id=submission.manifest.crawl_id,
            partition_key=submission.scan_id, detector_version=WEBTECH_DETECTOR_VERSION,
            candidate_manifest_uri=submission.manifest.uri, result_prefix_uri="s3://webtech/webtech/x",
            final_manifest_uri="s3://webtech/webtech/x/final.json",
            total_count=len(candidates), completed_count=len(candidates), outcome_counts={},
            technology_count=0, started_at=now, finished_at=now, last_progress_at=now,
            elapsed_seconds=1, progress_age_seconds=0, domains_per_minute=60,
            latest_event_sequence=1, error_message="",
        )

    def index(**kwargs):
        references = kwargs["references"]
        if crash_once[0]:
            crash_once[0] = False
            raise RuntimeError("insert not acknowledged")
        published.extend(item.input_id for item in references)
        client.execute(
            "INSERT INTO corpscout.webtech_domain_scan_results (crawl_id,root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome,task_id,input_id) VALUES",
            [
                (kwargs["crawl_id"], item.root_domain, f"https://{item.root_domain}",
                 f"https://{item.root_domain}/", WEBTECH_DETECTOR_VERSION, datetime.now(UTC),
                 "scan", item.outcome, task_id, item.input_id)
                for item in references
            ],
        )
        return len(references)

    monkeypatch.setattr(module, "submit_envelope", submit)
    monkeypatch.setattr(module, "monitor_webtech_scan", monitor)
    monkeypatch.setattr(module, "index_result_references", index)
    monkeypatch.setattr(module, "read_final_manifest", lambda **kwargs: type("Final", (), {"results": []})())
    results = module.build_webtech_task_asset(WebtechS3Destination(bucket="webtech", prefix="webtech"))

    def run(**config):
        return dg.materialize(
            [results, dg.AssetSpec("webtech_scan_input")],
            resources={
                "clickhouse": resource,
                "processing": ProcessingResource(postgres_url=dsn),
                "webtech_object_store": objects,
                "webtech_api": WebtechApiResource(base_url="http://scanner.test", api_token="test"),
            },
            run_config={"ops": {"webtech_scan_results": {"config": {"task_id": task_id, "batch_size": 2, **config}}}},
            raise_on_error=False,
        )

    assert not run().success  # the first insert was not acknowledged
    execution_id = processing.task(task_id)["config"]["execution"]["execution_id"]
    assert run(execution_id=execution_id).success
    # Envelopes are rebuilt from what remains; every page is published exactly once.
    assert len(published) == 3 and len(set(published)) == 3
    task = processing.task(task_id)
    assert task["status"] == "completed"
    assert task["succeeded_count"] == 3 - int(failed_domain is not None)
    assert task["inputs_purged_at"] is not None
    assert client.execute("SELECT count() FROM corpscout.webtech_scan_input") == [(0,)]
    assert not [key for (_, key) in objects.client().objects if key.startswith("queue-executions/")]
```

- [ ] **Step 3: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py tests/test_webtech_draft_execution.py -q -p no:cacheprovider`
Expected: FAIL — `envelope_partition_key` and `submit_envelope` do not exist.

- [ ] **Step 4: Let the monitor hand over result references**

In `assets.py`, `monitor_webtech_scan`: add parameters `on_results: Callable[[list[StoredResultReference]], None] | None = None` and `on_poll: Callable[[], None] | None = None` (import `StoredResultReference` from `models`). Replace the poll call and cursor handling:

```python
        try:
            response = webtech_api.poll(
                submission.scan_id,
                after_event=latest_event_sequence,
                wait_seconds=0,
            )
            for event in response.events:
                if on_results is not None and event.results:
                    on_results(event.results)
                latest_event_sequence = max(latest_event_sequence, event.sequence)
            snapshot = response.scan
        except UnknownRemoteScanError:
            snapshot = webtech_api.submit(submission.manifest)
            latest_event_sequence = 0  # a restarted scanner numbers events from 1 again
```

Delete the later line `latest_event_sequence = snapshot.latest_event_sequence`, and call `on_poll()` (if given) right before each `sleep(poll_interval_seconds)`. Everything else in the function stays.

- [ ] **Step 5: Rewrite `task_assets.py`**

Replace the module body with:

```python
"""Process a frozen webtech task in envelopes until nothing remains."""

import hashlib
import json
from collections.abc import Sequence
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.common.result_buffer import ResultBuffer
from dagster_v3.defs.webtech.assets import monitor_webtech_scan
from dagster_v3.defs.webtech.client import WebtechApiResource
from dagster_v3.defs.webtech.execution import (
    execution_crawl_id,
    finish_execution,
    purge_completed_inputs,
    remaining_inputs,
    start_execution,
)
from dagster_v3.defs.webtech.input import INPUT_RELATION, PROCESSOR_VERSION
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    FinalScanReference,
    SubmittedScanReference,
    WebtechCandidate,
)
from dagster_v3.defs.webtech.storage import (
    WebtechS3Destination,
    index_result_references,
    read_final_manifest,
    write_candidate_manifest,
)


class WebtechTaskConfig(dg.Config):
    task_id: str
    execution_id: str | None = None
    force_rescan: bool = False
    recent_days: int = Field(default=30, ge=1, le=3650)
    batch_size: int = Field(default=5000, ge=1, le=10000, description="Pages per scanner envelope.")

    @field_validator("execution_id")
    @classmethod
    def valid_execution(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("task_id")
    @classmethod
    def valid_task(cls, value: str) -> str:
        return str(UUID(value))


def envelope_partition_key(input_ids: Sequence[str]) -> str:
    """Name an envelope by its entries only; nothing about it is stored."""
    digest = hashlib.sha256("\n".join(sorted(input_ids)).encode()).hexdigest()[:24]
    return f"envelope-{digest}"


def submit_envelope(api: WebtechApiResource, manifest):
    """Seam for tests; the scanner derives the scan ID from the envelope content."""
    return api.submit(manifest)


def complete_task(context, store: ProcessingStore, clickhouse: ClickhouseResource, task: dict) -> dict:
    """Website errors are published outcomes; only pipeline errors fail the run."""
    failed = task["terminal_failed_count"]
    outcome = "completed_with_errors" if failed else "completed"
    purge_completed_inputs(store, clickhouse, str(task["task_id"]))
    context.instance.add_run_tags(
        context.run.run_id,
        {
            "webtech/outcome": outcome,
            "webtech/succeeded_pages": str(task["succeeded_count"]),
            "webtech/failed_pages": str(failed),
            "webtech/skipped_pages": str(task["skipped_count"]),
        },
    )
    return {
        "completion_status": outcome,
        "succeeded_pages": task["succeeded_count"],
        "failed_pages": failed,
        "skipped_recent": task["skipped_count"],
        "inputs_purged": True,
    }


def build_webtech_task_asset(destination: WebtechS3Destination):
    @dg.asset(
        name="webtech_scan_results",
        group_name="webtech",
        deps=["webtech_scan_input"],
        kinds={"clickhouse", "s3", "browser"},
        pool="webtech_remote_scanner",
        description="Freeze a draft, then send remaining pages to the scanner in envelopes and publish "
        "results as they arrive. The same execution_id resumes from what remains. Completed tasks "
        "drop their input partition; rescans use a new queue.",
    )
    def results(
        context: dg.AssetExecutionContext,
        config: WebtechTaskConfig,
        clickhouse: ClickhouseResource,
        processing: ProcessingResource,
        webtech_api: WebtechApiResource,
        webtech_object_store: ObjectStoreResource,
    ) -> dg.MaterializeResult:
        with processing.get_store() as store, store.selection_lock(config.task_id):
            task = store.task(config.task_id)
            if task is None or task["processor"] != PROCESSOR_VERSION or task["queue_scope"] is None:
                raise ValueError("Prepare webtech_scan_input for this task_id before scanning")
            task = start_execution(
                store=store, clickhouse=clickhouse, task_id=config.task_id,
                execution_id=config.execution_id, force_rescan=config.force_rescan,
                recent_days=config.recent_days, batch_size=config.batch_size, run_id=context.run.run_id,
            )
            execution = task["config"]["execution"]
            crawl_id = execution_crawl_id(execution)
            context.instance.add_run_tags(
                context.run.run_id,
                {
                    "processing/task_id": config.task_id,
                    "webtech/execution_id": execution["execution_id"],
                    "webtech/execution": json.dumps(execution, sort_keys=True),
                },
            )
            if task["status"] == "completed":
                return dg.MaterializeResult(metadata={
                    "task_id": config.task_id, "execution_id": execution["execution_id"],
                    "already_completed": True, **complete_task(context, store, clickhouse, task),
                })
            queue = ClickHouseInputQueue(clickhouse, INPUT_RELATION, selection_task_id=config.task_id)
            if queue.inspect() != {
                key: task["source_info"][key]
                for key in ("relation", "table_uuid", "total", "upper_id", "selection_task_id")
            }:
                raise ValueError("Input selection changed after preparation")

            published = 0

            def publish(references):
                nonlocal published
                published += index_result_references(
                    clickhouse=clickhouse, object_store=webtech_object_store, destination=destination,
                    crawl_id=crawl_id, detector_version=WEBTECH_DETECTOR_VERSION,
                    references=references, dagster_run_id=context.run.run_id,
                )

            buffer = ResultBuffer(publish, max_items=500, max_seconds=5.0)
            envelopes = 0
            while True:
                with clickhouse.get_connection() as client:
                    rows = remaining_inputs(client, task, limit=execution["profile"]["batch_size"])
                if not rows:
                    break
                envelopes += 1
                candidates = tuple(
                    WebtechCandidate(root_domain=root, page_url=page, input_id=identity, task_id=config.task_id)
                    for identity, root, _origin, page in rows
                )
                manifest = write_candidate_manifest(
                    object_store=webtech_object_store, destination=destination, crawl_id=crawl_id,
                    partition_key=envelope_partition_key([row[0] for row in rows]),
                    dagster_run_id=execution["execution_id"], candidates=candidates, schema_version=3,
                )
                snapshot = submit_envelope(webtech_api, manifest)
                snapshot = monitor_webtech_scan(
                    context=context,
                    submission=SubmittedScanReference(scan_id=snapshot.scan_id, status=snapshot.status, manifest=manifest),
                    webtech_api=webtech_api, webtech_object_store=webtech_object_store, destination=destination,
                    on_results=buffer.add, on_poll=buffer.flush_if_due,
                )
                buffer.flush()
                # Reconcile with the final manifest: it lists every result of the scan,
                # including any whose event was missed. Re-publishing a page is idempotent.
                final = read_final_manifest(
                    object_store=webtech_object_store, destination=destination,
                    reference=FinalScanReference(
                        scan_id=snapshot.scan_id, crawl_id=snapshot.crawl_id, partition_key=snapshot.partition_key,
                        detector_version=snapshot.detector_version, uri=snapshot.final_manifest_uri,
                        total_count=snapshot.total_count, outcome_counts=snapshot.outcome_counts,
                        technology_count=snapshot.technology_count, elapsed_seconds=snapshot.elapsed_seconds,
                        domains_per_minute=snapshot.domains_per_minute,
                    ),
                )
                envelope_ids = [row[0] for row in rows]
                with clickhouse.get_connection() as client:
                    still = {row[0] for row in remaining_inputs(client, task, limit=len(rows), input_ids=envelope_ids)}
                missing = [item for item in final.results if item.input_id in still]
                if missing:
                    buffer.add(missing)
                    buffer.flush()
                with clickhouse.get_connection() as client:
                    unresolved = remaining_inputs(client, task, limit=len(rows), input_ids=envelope_ids)
                if unresolved:
                    raise ValueError(f"Scanner completed an envelope without results for {len(unresolved)} pages")
                context.log.info("Webtech task %s: envelope %s published, %s results so far", config.task_id, envelopes, published)
            task = finish_execution(store, clickhouse, config.task_id)
            return dg.MaterializeResult(metadata={
                "task_id": config.task_id, "execution_id": execution["execution_id"],
                "envelopes": envelopes, "published_results": published,
                **complete_task(context, store, clickhouse, task),
            })

    return results
```

- [ ] **Step 6: Run the webtech suites**

Run: `uv run --frozen --no-sync pytest tests/test_webtech_queue_contract.py tests/test_webtech_draft_execution.py tests/test_webtech_input.py tests/test_webtech_pages.py tests/test_webtech_pilot.py -q -p no:cacheprovider`
Expected: all pass.

- [ ] **Step 7: Validate definitions**

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.`

- [ ] **Step 8: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/webtech/assets.py services/dagster_v3/src/dagster_v3/defs/webtech/task_assets.py services/dagster_v3/tests/test_webtech_queue_contract.py services/dagster_v3/tests/test_webtech_draft_execution.py
git commit -m "feat(webtech): envelope loop publishes results continuously and resumes from what remains"
```

---

### Task 9: Deploy and verify on prod

**Files:** none (operations). Update the spec status line at the end.

- [ ] **Step 1: Preconditions**

- No active `webtech_scan_results` run: Dagster GraphQL `runsOrError(filter:{statuses:[STARTED,QUEUED], pipelineName:"webtech_scan_results_job"})` is empty.
- Not inside the Tuesday 01:05 Stockholm address-chain window.
- `corpscout.webtech_scan_input` has 0 rows.

- [ ] **Step 2: Apply the migration (single step)**

Run from `corpscout/`: `make clickhouse-migrate-up-one </dev/null`
Expected: `446/u corpscout_webtech_queue_contract`, ledger `446 0`.

- [ ] **Step 3: Deploy Dagster first (accepts the new event field)**

Tree must be clean and on main. Run: `cd services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 LC_ALL=en_US.UTF-8 ansible-playbook -i inventory.ini light_sync.yml </dev/null > <scratch>/light_sync.log 2>&1; echo rc=$?`
Expected: rc=0, code location `LOADED`, no service restart, running runs untouched.

- [ ] **Step 4: Deploy the scanner**

Run: `cd services/webtech/ansible && ansible-playbook site.yml </dev/null > <scratch>/webtech-deploy.log 2>&1; echo rc=$?`
Expected: rc=0; the playbook refuses during an active scan (then wait and retry); `/healthz` 200.

- [ ] **Step 5: End-to-end check with two pages**

Launch `webtech_scan_input_job` with `targets: [novelic.com, example.com]`, then `webtech_scan_results_job` with that `task_id` and `batch_size: 1`.
Expected: two envelopes; results rows in `webtech_domain_scan_results` with this task and `crawl_id = webtech-<execution_id>`; task `completed`; `webtech_scan_input` has no partition for the task; no new objects under `queue-inputs/` or `queue-executions/` in the `webtech` bucket.

- [ ] **Step 6: Mark the spec**

In the spec, change `Status: agreed with the owner on 2026-09-24. Not implemented yet.` to `Status: agreed 2026-09-24. Webtech implemented <date> (plan 2026-09-24-webtech-queue-contract); crawl, Brave and IP enrichment pending.` Commit and push.
