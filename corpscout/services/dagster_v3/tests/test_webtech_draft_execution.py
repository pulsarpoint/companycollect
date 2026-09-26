"""Freeze/import races, and remaining work, freshness, finish and purge derived from results."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg2
import pytest
from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.processing import ProcessingStore
from dagster_v3.defs.webtech.input import PROCESSOR_VERSION
from dagster_v3.defs.webtech.execution import (
    start_execution,
    execution_crawl_id,
    remaining_inputs,
    finish_execution,
    purge_completed_inputs,
)
from dagster_v3.defs.webtech.models import WEBTECH_DETECTOR_VERSION
from tests.test_webtech_input import add, database as database, objects as objects
from tests.test_ip_enrichment_input import server as server
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)


def start(processing, resource, task_id, **changes):
    settings = dict(
        task_id=task_id,
        execution_id=None,
        force_rescan=False,
        recent_days=30,
        run_id=str(uuid4()),
    )
    settings.update(changes)
    with processing.selection_lock(task_id):
        return start_execution(store=processing, clickhouse=resource, **settings)


def test_freeze_releases_default_draft_and_reuses_execution(database, store, objects):
    _, resource = database
    processing, _ = store
    original = add(resource, processing, objects, targets=["novelic.com"])
    task_id = original["task_id"]
    task = start(processing, resource, task_id)
    assert task["status"] == "selected" and task["frozen_at"] is not None
    next_draft = add(resource, processing, objects, targets=["example.com"])
    assert next_draft["task_id"] != task_id
    assert processing.task(task_id)["total"] == 1
    assert start(processing, resource, task_id)["config"] == task["config"]
    with pytest.raises(ValueError, match="frozen"):
        start(processing, resource, task_id, force_rescan=True)
    with pytest.raises(ValueError, match="existing execution"):
        start(processing, resource, task_id, execution_id=str(uuid4()))
    # Envelope size is transport only; executions frozen before it left the
    # profile still resume.
    with processing.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET config=jsonb_set(config,
            '{execution,profile,batch_size}','5000') WHERE task_id=%s""",
            (task_id,),
        )
    assert start(processing, resource, task_id)["status"] == "selected"
    with pytest.raises(ValueError, match="open draft"):
        add(resource, processing, objects, task_id=task_id, targets=["other.com"])
    # A completed import retry remains attached to the frozen task.
    assert (
        add(
            resource,
            processing,
            objects,
            submission_id=original["submission_id"],
            targets=["novelic.com"],
        )["task_id"]
        == task_id
    )


def test_failed_import_blocks_start_and_retains_draft(
    database, store, objects, monkeypatch
):
    from dagster_v3.defs.webtech import input as module

    _, resource = database
    processing, _ = store

    def fail(*args, **kwargs):
        raise RuntimeError("interrupted write")

    receipt_id = str(uuid4())
    with monkeypatch.context() as patch:
        patch.setattr(module, "insert_input_batch", fail)
        with pytest.raises(RuntimeError):
            add(
                resource,
                processing,
                objects,
                submission_id=receipt_id,
                targets=["novelic.com"],
            )
    receipt = draft_queue.submission(processing, receipt_id)
    task_id = str(receipt["task_id"])
    assert receipt["status"] == "failed"
    with pytest.raises(ValueError, match="outstanding imports"):
        start(processing, resource, task_id)
    assert processing.task(task_id)["status"] == "draft"
    add(
        resource, processing, objects, submission_id=receipt_id, targets=["novelic.com"]
    )
    assert start(processing, resource, task_id)["status"] == "selected"


def test_concurrent_draft_creation_and_scope_isolation(store):
    _, dsn = store

    def find(scope):
        with closing(psycopg2.connect(dsn)) as connection:
            return draft_queue.find_draft(
                ProcessingStore(connection),
                scope=scope,
                processor=PROCESSOR_VERSION,
                task_id=None,
            )

    with ThreadPoolExecutor(max_workers=4) as threads:
        identities = list(threads.map(find, ["workspace"] * 8))
    assert len(set(identities)) == 1
    assert find("other-workspace") != identities[0]


def test_import_and_start_use_the_same_lock(database, store, objects):
    _, resource = database
    processing, dsn = store
    task_id = add(resource, processing, objects, targets=["novelic.com"])["task_id"]
    with closing(psycopg2.connect(dsn)) as connection:
        competing = ProcessingStore(connection)
        with processing.selection_lock(task_id):
            with pytest.raises(ValueError, match="already being initialized"):
                with competing.selection_lock(task_id):
                    start_execution(
                        store=competing,
                        clickhouse=resource,
                        task_id=task_id,
                        execution_id=None,
                        force_rescan=False,
                        recent_days=30,
                        run_id="run",
                    )
        assert processing.task(task_id)["status"] == "draft"


def publish(
    client,
    task,
    rows,
    *,
    outcome="success",
    scanned_at=None,
    crawl_id=None,
    detector_version=WEBTECH_DETECTOR_VERSION,
    scan_id="scan-1",
):
    execution = task["config"]["execution"]
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (crawl_id,root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome,task_id,input_id) VALUES",
        [
            (
                crawl_id or execution_crawl_id(execution),
                root,
                origin,
                page,
                detector_version,
                scanned_at or datetime.now(UTC),
                scan_id,
                outcome,
                str(task["task_id"]),
                identity,
            )
            for identity, root, origin, page in rows
        ],
    )


def test_remaining_excludes_published_and_fresh_pages(database, store, objects):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome) VALUES",
        [
            (
                "fresh.com",
                "https://fresh.com",
                "https://fresh.com/",
                WEBTECH_DETECTOR_VERSION,
                datetime.now(UTC) - timedelta(days=1),
                "old",
                "success",
            ),
            (
                "failed.com",
                "https://failed.com",
                "https://failed.com/",
                WEBTECH_DETECTOR_VERSION,
                datetime.now(UTC) - timedelta(days=1),
                "old",
                "navigation_error",
            ),
        ],
    )
    task_id = add(
        resource,
        processing,
        objects,
        targets=["fresh.com", "failed.com", "a.com", "b.com"],
    )["task_id"]
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
        assert (
            remaining_inputs(connection, task, limit=10, input_ids=[first[2][0]])
            == first[2:3]
        )
        # An empty selection means no entries, not "no filter".
        assert remaining_inputs(connection, task, limit=10, input_ids=[]) == []


def test_large_envelope_reconciliation_excludes_published_and_unselected_pages(
    database, store, objects
):
    client, resource = database
    processing, _ = store
    task_id = add(
        resource,
        processing,
        objects,
        targets=[f"https://example.com/page/{index}" for index in range(10001)],
    )["task_id"]
    task = start(processing, resource, task_id)
    rows = remaining_inputs(client, task, limit=10001)
    envelope = rows[:10000]
    publish(client, task, envelope[:1])
    assert remaining_inputs(
        client, task, limit=10001, input_ids=[row[0] for row in envelope]
    ) == envelope[1:]
    publish(client, task, envelope[1:])
    assert remaining_inputs(
        client, task, limit=10001, input_ids=[row[0] for row in envelope]
    ) == []
    assert remaining_inputs(client, task, limit=10001) == rows[10000:]


def test_force_rescan_includes_fresh_pages(database, store, objects):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome) VALUES",
        [
            (
                "fresh.com",
                "https://fresh.com",
                "https://fresh.com/",
                WEBTECH_DETECTOR_VERSION,
                datetime.now(UTC) - timedelta(days=1),
                "old",
                "success",
            )
        ],
    )
    task_id = add(resource, processing, objects, targets=["fresh.com"])["task_id"]
    task = start(processing, resource, task_id, force_rescan=True)
    with resource.get_connection() as connection:
        assert [row[1] for row in remaining_inputs(connection, task, limit=10)] == [
            "fresh.com"
        ]


def test_remaining_freshness_semantics_across_crawls_and_detectors(
    database, store, objects
):
    """Freshness only skips a page when its latest result — from any crawl, under
    the current detector, inside [cutoff, started_at] — is a success. A later
    failure, a result outside the window, a stale detector, or a result after
    started_at all leave the page remaining, and finish refuses until every
    remaining page has a result of this execution's own crawl."""
    client, resource = database
    processing, _ = store
    task_id = add(
        resource,
        processing,
        objects,
        targets=["a.com", "b.com", "c.com", "d.com", "e.com"],
    )["task_id"]
    task = start(processing, resource, task_id)
    started_at = datetime.fromisoformat(task["config"]["execution"]["started_at"])
    with resource.get_connection() as connection:
        by_domain = {
            row[1]: row for row in remaining_inputs(connection, task, limit=10)
        }
    assert sorted(by_domain) == ["a.com", "b.com", "c.com", "d.com", "e.com"]

    # a: a later failure beats an earlier success (latest outcome wins) -> REMAINS.
    publish(
        client,
        task,
        [by_domain["a.com"]],
        crawl_id="other-crawl",
        outcome="success",
        scanned_at=started_at - timedelta(hours=2),
        scan_id="a-old",
    )
    publish(
        client,
        task,
        [by_domain["a.com"]],
        crawl_id="other-crawl",
        outcome="navigation_error",
        scanned_at=started_at - timedelta(hours=1),
        scan_id="a-new",
    )
    # b: a success from another crawl scanned AFTER this execution started -> REMAINS.
    publish(
        client,
        task,
        [by_domain["b.com"]],
        crawl_id="other-crawl",
        outcome="success",
        scanned_at=started_at + timedelta(hours=1),
        scan_id="b-1",
    )
    # c: a success inside the window from another crawl, but a stale detector -> REMAINS.
    publish(
        client,
        task,
        [by_domain["c.com"]],
        crawl_id="other-crawl",
        outcome="success",
        scanned_at=started_at - timedelta(hours=1),
        scan_id="c-1",
        detector_version="stale-detector",
    )
    # d: a success from another crawl, older than the freshness cutoff -> REMAINS.
    publish(
        client,
        task,
        [by_domain["d.com"]],
        crawl_id="other-crawl",
        outcome="success",
        scanned_at=started_at - timedelta(days=35),
        scan_id="d-1",
    )
    # e: a fresh success inside the window, from another crawl, current detector -> NOT remaining.
    publish(
        client,
        task,
        [by_domain["e.com"]],
        crawl_id="other-crawl",
        outcome="success",
        scanned_at=started_at - timedelta(hours=1),
        scan_id="e-1",
    )
    with resource.get_connection() as connection:
        remaining = remaining_inputs(connection, task, limit=10)
    assert sorted(row[1] for row in remaining) == ["a.com", "b.com", "c.com", "d.com"]

    # finish refuses while b has no result of this execution's own crawl, even
    # though a, c and d now do and e is a fresh skip.
    publish(client, task, [by_domain["a.com"]])
    publish(client, task, [by_domain["c.com"]])
    publish(client, task, [by_domain["d.com"]])
    with (
        processing.selection_lock(task_id),
        pytest.raises(ValueError, match="published outcome"),
    ):
        finish_execution(processing, resource, task_id)
    publish(client, task, [by_domain["b.com"]])
    with processing.selection_lock(task_id):
        finished = finish_execution(processing, resource, task_id)
    assert (
        finished["succeeded_count"],
        finished["terminal_failed_count"],
        finished["skipped_count"],
    ) == (4, 0, 1)

    # f: force_rescan disables the freshness skip (e's fresh success no longer
    # counts), but a page with THIS crawl's own result is still excluded.
    next_task = add(resource, processing, objects, targets=["e.com", "f.com"])[
        "task_id"
    ]
    forced = start(processing, resource, next_task, force_rescan=True)
    with resource.get_connection() as connection:
        forced_remaining = {
            row[1]: row for row in remaining_inputs(connection, forced, limit=10)
        }
    assert sorted(forced_remaining) == ["e.com", "f.com"]
    publish(client, forced, [forced_remaining["f.com"]])
    with resource.get_connection() as connection:
        assert [row[1] for row in remaining_inputs(connection, forced, limit=10)] == [
            "e.com"
        ]


def test_finish_counts_results_and_skips_then_purge_drops_the_partition(
    database, store, objects
):
    client, resource = database
    processing, _ = store
    client.execute(
        "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome) VALUES",
        [
            (
                "fresh.com",
                "https://fresh.com",
                "https://fresh.com/",
                WEBTECH_DETECTOR_VERSION,
                datetime.now(UTC) - timedelta(days=1),
                "old",
                "success",
            )
        ],
    )
    other = add(
        resource, processing, objects, queue_scope="other", targets=["kept.com"]
    )["task_id"]
    task_id = add(
        resource, processing, objects, targets=["fresh.com", "a.com", "b.com"]
    )["task_id"]
    task = start(processing, resource, task_id)
    with resource.get_connection() as connection:
        rows = remaining_inputs(connection, task, limit=10)
    with (
        processing.selection_lock(task_id),
        pytest.raises(ValueError, match="published outcome"),
    ):
        finish_execution(processing, resource, task_id)
    publish(client, task, rows[:1])
    publish(client, task, rows[1:], outcome="navigation_error")
    with processing.selection_lock(task_id):
        finished = finish_execution(processing, resource, task_id)
        assert (
            finished["succeeded_count"],
            finished["terminal_failed_count"],
            finished["skipped_count"],
        ) == (1, 1, 1)
        purge_completed_inputs(processing, resource, task_id)
        purge_completed_inputs(processing, resource, task_id)  # idempotent
    assert client.execute(
        "SELECT DISTINCT task_id FROM corpscout.webtech_scan_input"
    ) == [(other,)]
    assert processing.task(task_id)["inputs_purged_at"] is not None
    # The fresh/skipped page has no result in this execution, but remains in history.
    assert client.execute(
        "SELECT count() FROM corpscout.queue_task_sources FINAL WHERE task_id=%(task)s",
        {"task": task_id},
    ) == [(3,)]


class FakeScanner:
    """Seams of the results asset: submit, monitor, publish and the replay poll."""

    def __init__(self, client, objects, task_id, failed_domain=None):
        self.client = client
        self.objects = objects
        self.task_id = task_id
        self.failed_domain = failed_domain
        self.envelopes: dict[str, list[dict]] = {}
        self.submitted: list[str] = []
        self.published: list[str] = []
        self.insert_calls: list[int] = []  # batch size of every insert attempt
        self.failing_inserts = 0  # the next N inserts are not acknowledged
        self.missed_events: set[str] = set()  # pages only a replay from event 0 reports
        self.lost_pages: set[str] = set()  # pages not even the replay reports
        self.replays: list[tuple[str, int]] = []
        self.crash_after_pages: int | None = None  # the monitor dies once, mid-envelope

    def reference(self, row):
        from dagster_v3.defs.webtech.models import StoredResultReference

        failed = row["root_domain"] == self.failed_domain
        return StoredResultReference(
            root_domain=row["root_domain"],
            harmonic_rank=0,
            input_id=row["input_id"],
            outcome="navigation_error" if failed else "success",
            timeout_stage=None,
            technology_count=0,
            duration_ms=1,
            object_key=f"webtech/pages/{row['input_id']}",
            sha256="0" * 64,
            size_bytes=1,
        )

    def snapshot(self, crawl_id, key, total, *, completed):
        from dagster_v3.defs.webtech.models import RemoteScanSnapshot

        now = datetime.now(UTC)
        return RemoteScanSnapshot(
            scan_id=key,
            status="completed" if completed else "running",
            crawl_id=crawl_id,
            detector_version=WEBTECH_DETECTOR_VERSION,
            total_count=total,
            completed_count=total if completed else 0,
            outcome_counts={},
            technology_count=0,
            started_at=now,
            finished_at=now if completed else None,
            last_progress_at=now,
            elapsed_seconds=1,
            progress_age_seconds=0,
            domains_per_minute=60,
            latest_event_sequence=1 if completed else 0,
            error_message="",
        )

    def submit(self, api, *, crawl_id, candidates):
        del api
        # Like the scanner, the scan ID depends on the envelope's pages only.
        key = "scan-" + "-".join(sorted(item.input_id[:8] for item in candidates))
        self.envelopes[key] = [item.model_dump() for item in candidates]
        self.submitted.append(key)
        return self.snapshot(crawl_id, key, len(candidates), completed=False)

    def monitor(self, context, *, scan_id, crawl_id, on_results, on_poll, **kwargs):
        del context, kwargs
        candidates = self.envelopes[scan_id]
        for position, row in enumerate(candidates):
            if (
                self.crash_after_pages is not None
                and position == self.crash_after_pages
            ):
                self.crash_after_pages = None
                raise RuntimeError("Dagster step died mid-envelope")
            if row["input_id"] not in self.missed_events:
                on_results([self.reference(row)])
            on_poll()
        return self.snapshot(crawl_id, scan_id, len(candidates), completed=True)

    def poll(self, scan_id, *, after_event, wait_seconds):
        """The completed scan replays every stored result from ``after_event``."""
        from dagster_v3.defs.webtech.models import (
            RemoteScanPollResponse,
            RemoteScanProgressEvent,
        )

        del wait_seconds
        self.replays.append((scan_id, after_event))
        rows = [
            row
            for row in self.envelopes[scan_id]
            if row["input_id"] not in self.lost_pages
        ]
        event = RemoteScanProgressEvent(
            sequence=1,
            completed_count=len(rows),
            total_count=len(rows),
            window_count=max(1, len(rows)),
            window_outcome_counts={},
            window_technology_count=0,
            elapsed_seconds=1,
            domains_per_minute=60,
            results=[self.reference(row) for row in rows],
        )
        return RemoteScanPollResponse(
            scan=self.snapshot("", scan_id, len(rows), completed=True),
            events=[event],
        )

    def index(self, **kwargs):
        references = kwargs["references"]
        self.insert_calls.append(len(references))
        if self.failing_inserts:
            self.failing_inserts -= 1
            raise RuntimeError("insert not acknowledged")
        self.published.extend(item.input_id for item in references)
        self.client.execute(
            "INSERT INTO corpscout.webtech_domain_scan_results (crawl_id,root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome,task_id,input_id) VALUES",
            [
                (
                    kwargs["crawl_id"],
                    item.root_domain,
                    f"https://{item.root_domain}",
                    f"https://{item.root_domain}/",
                    WEBTECH_DETECTOR_VERSION,
                    datetime.now(UTC),
                    "scan",
                    item.outcome,
                    self.task_id,
                    item.input_id,
                )
                for item in references
            ],
        )
        return len(references)


def results_harness(
    database, store, objects, monkeypatch, *, failed_domain=None, eager_flush=False
):
    """Build the results asset around a FakeScanner; eager_flush publishes on every poll."""
    import dagster as dg
    from dagster_v3.defs.common.processing import ProcessingResource
    from dagster_v3.defs.common.result_buffer import ResultBuffer
    from dagster_v3.defs.webtech import task_assets as module
    from dagster_v3.defs.webtech.client import WebtechApiResource
    from dagster_v3.defs.webtech.storage import WebtechS3Destination

    client, resource = database
    processing, dsn = store
    task_id = add(
        resource, processing, objects, targets=["novelic.com", "example.com", "a.com"]
    )["task_id"]
    scanner = FakeScanner(client, objects, task_id, failed_domain)
    monkeypatch.setattr(module, "submit_envelope", scanner.submit)
    monkeypatch.setattr(module, "monitor_webtech_scan", scanner.monitor)
    monkeypatch.setattr(module, "index_result_references", scanner.index)
    monkeypatch.setattr(
        WebtechApiResource,
        "poll",
        lambda self, scan_id, **kwargs: scanner.poll(scan_id, **kwargs),
    )
    if eager_flush:
        monkeypatch.setattr(
            module,
            "ResultBuffer",
            lambda flush, **kwargs: ResultBuffer(flush, max_items=500, max_seconds=0.0),
        )
    results = module.build_webtech_task_asset(
        WebtechS3Destination(bucket="webtech", prefix="webtech")
    )

    instance = dg.DagsterInstance.ephemeral()

    def run(**config):
        return dg.materialize(
            [results, dg.AssetSpec("webtech_scan_input")],
            instance=instance,
            resources={
                "clickhouse": resource,
                "processing": ProcessingResource(postgres_url=dsn),
                "webtech_object_store": objects,
                "webtech_api": WebtechApiResource(
                    base_url="http://scanner.test", api_token="test"
                ),
            },
            run_config={
                "ops": {
                    "webtech_scan_results": {
                        "config": {"task_id": task_id, "batch_size": 2, **config}
                    }
                }
            },
            raise_on_error=False,
        )

    run.instance = instance
    return task_id, scanner, run


def assert_completed_once(database, store, objects, task_id, scanner, succeeded=3):
    client, _ = database
    processing, _ = store
    assert sorted(scanner.published) == sorted(set(scanner.published))
    assert len(scanner.published) == 3
    task = processing.task(task_id)
    assert task["status"] == "completed"
    assert task["succeeded_count"] == succeeded
    assert task["inputs_purged_at"] is not None
    assert client.execute("SELECT count() FROM corpscout.webtech_scan_input") == [(0,)]
    assert not [
        key
        for (_, key) in objects.client().objects
        if key.startswith("queue-executions/")
    ]


@pytest.mark.parametrize("failed_domain", [None, "example.com"])
def test_results_asset_publishes_continuously_resumes_and_clears(
    database, store, objects, monkeypatch, failed_domain
):
    processing, _ = store
    task_id, scanner, run = results_harness(
        database, store, objects, monkeypatch, failed_domain=failed_domain
    )
    scanner.failing_inserts = 1

    assert not run().success  # the end-of-envelope insert was not acknowledged
    execution_id = processing.task(task_id)["config"]["execution"]["execution_id"]
    assert run(execution_id=execution_id).success
    # Envelopes are rebuilt from what remains; one insert per envelope, not per page.
    assert scanner.insert_calls == [2, 2, 1]
    assert_completed_once(
        database, store, objects, task_id, scanner, 3 - int(failed_domain is not None)
    )


def test_results_asset_survives_a_failed_insert_while_polling(
    database, store, objects, monkeypatch
):
    task_id, scanner, run = results_harness(
        database, store, objects, monkeypatch, eager_flush=True
    )
    scanner.failing_inserts = 1

    result = run()
    assert result.success
    # The failed poll-time insert kept its page, and the next poll published it.
    assert scanner.insert_calls == [1, 1, 1, 1]
    messages = [entry.user_message for entry in run.instance.all_logs(result.run_id)]
    assert any("Webtech result publish failed while polling" in m for m in messages)
    assert_completed_once(database, store, objects, task_id, scanner)


def _first_input_id(database, task_id):
    return sorted(
        input_id
        for (input_id,) in database[0].execute(
            "SELECT input_id FROM corpscout.webtech_scan_input WHERE task_id=%(task)s",
            {"task": task_id},
        )
    )[0]


def test_results_asset_publishes_missed_pages_from_a_replay_of_the_scan(
    database, store, objects, monkeypatch
):
    task_id, scanner, run = results_harness(database, store, objects, monkeypatch)
    missed = _first_input_id(database, task_id)
    scanner.missed_events = {missed}

    assert run().success
    assert missed in scanner.published
    # The envelope's events (1 page) and the replay top-up (1 page), then envelope 2.
    assert scanner.insert_calls == [1, 1, 1]
    # Only the envelope with a missed page is replayed, once, from event 0.
    assert [after for _, after in scanner.replays] == [0]
    assert_completed_once(database, store, objects, task_id, scanner)


def test_results_asset_fails_when_the_scan_has_no_result_for_a_page(
    database, store, objects, monkeypatch
):
    task_id, scanner, run = results_harness(database, store, objects, monkeypatch)
    lost = _first_input_id(database, task_id)
    scanner.missed_events = {lost}
    scanner.lost_pages = {lost}

    result = run()
    assert not result.success
    (failure,) = result.get_step_failure_events()
    error = failure.step_failure_data.error
    assert "Scanner completed an envelope without results for 1 pages" in str(error)
    assert lost not in scanner.published


def test_results_asset_resumes_with_a_new_envelope_after_a_partial_publish(
    database, store, objects, monkeypatch
):
    processing, _ = store
    task_id, scanner, run = results_harness(
        database, store, objects, monkeypatch, eager_flush=True
    )
    scanner.crash_after_pages = 1

    assert not run().success
    assert len(scanner.published) == 1  # the first page was published before the crash
    execution_id = processing.task(task_id)["config"]["execution"]["execution_id"]
    assert run(execution_id=execution_id).success
    # What remains differs from the first envelope, so the retry sends a new envelope.
    assert len(scanner.submitted) == 2
    assert scanner.submitted[1] != scanner.submitted[0]
    assert_completed_once(database, store, objects, task_id, scanner)


def test_addition_losing_freeze_race_moves_to_next_draft(
    database, store, objects, monkeypatch
):
    _, resource = database
    processing, _ = store
    old_task = add(resource, processing, objects, targets=["novelic.com"])["task_id"]
    original = draft_queue.find_draft
    raced = False

    def find_then_freeze(*args, **kwargs):
        nonlocal raced
        identity = original(*args, **kwargs)
        if not raced:
            raced = True
            start(processing, resource, identity)
        return identity

    monkeypatch.setattr(draft_queue, "find_draft", find_then_freeze)
    addition = add(resource, processing, objects, targets=["example.com"])
    assert addition["task_id"] != old_task
    assert processing.task(old_task)["status"] == "selected"
    assert processing.task(old_task)["total"] == 1
    assert processing.task(addition["task_id"])["status"] == "draft"


def test_import_retry_fences_orphaned_clickhouse_insert(
    database, store, objects, monkeypatch
):
    import time
    from clickhouse_driver.errors import ServerException
    from dagster_v3.defs.webtech import input as module

    client, resource = database
    processing, _ = store
    # A sibling submission in the same draft must survive the retry untouched.
    other = add(
        resource, processing, objects, targets=["other.se"], source_name="manual"
    )
    receipt_id = str(uuid4())

    def interrupt(*args, **kwargs):
        raise RuntimeError("writer disappeared")

    with monkeypatch.context() as patch:
        patch.setattr(module, "insert_input_batch", interrupt)
        with pytest.raises(RuntimeError):
            add(
                resource,
                processing,
                objects,
                submission_id=receipt_id,
                targets=["novelic.com"],
            )
    receipt = draft_queue.submission(processing, receipt_id)
    task_id = str(receipt["task_id"])
    assert task_id == other["task_id"]
    identity, domain, origin, page = module.normalized_target("novelic.com")
    query_id = "webtech-submission:" + receipt_id

    def orphan():
        with resource.get_connection() as writer:
            # The prior process's PostgreSQL lock is gone, but its insert is alive:
            # a stable query under the retry's fencing query_id, still writing this
            # submission's own row (task_id, submission_id).
            return writer.execute(
                f"INSERT INTO {module.INPUT_RELATION} ({','.join(module.INPUT_COLUMNS)}) "
                "SELECT %(task)s,%(identity)s,%(root)s,%(origin)s,%(page)s,%(source)s,"
                "%(record)s,%(run)s,%(submission)s FROM numbers(1) WHERE sleep(3)=0",
                {
                    "task": task_id,
                    "identity": identity,
                    "root": domain,
                    "origin": origin,
                    "page": page,
                    "source": "manual",
                    "record": "novelic.com",
                    "run": str(uuid4()),
                    "submission": receipt_id,
                },
                query_id=query_id,
            )

    with ThreadPoolExecutor(max_workers=1) as threads:
        future = threads.submit(orphan)
        deadline = time.monotonic() + 2
        while client.execute(
            "SELECT count() FROM system.processes WHERE query_id=%(query)s",
            {"query": query_id},
        ) != [(1,)]:
            if time.monotonic() >= deadline:
                raise AssertionError("orphaned query did not start")
            time.sleep(0.01)
        result = add(
            resource,
            processing,
            objects,
            submission_id=receipt_id,
            targets=["novelic.com"],
        )
        with pytest.raises(
            ServerException, match="cancelled|cancel|QUERY_WAS_CANCELLED"
        ):
            future.result(timeout=10)
    # The retry killed the orphaned query and deleted only its own rows before
    # reselecting: exactly one row per submission, no duplicate, sibling intact.
    assert result["task_id"] == task_id
    assert result["input_count"] == 1
    assert result["total"] == 2
    assert sorted(
        client.execute(
            "SELECT root_domain, submission_id FROM corpscout.webtech_scan_input"
        )
    ) == sorted([("novelic.com", receipt_id), ("other.se", other["submission_id"])])
