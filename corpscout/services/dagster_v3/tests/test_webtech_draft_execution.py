"""Freeze/import races, saved freshness decisions and recoverable scan execution."""

import json
from collections import Counter
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
        batch_size=2,
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
                        batch_size=2,
                        run_id="run",
                    )
        assert processing.task(task_id)["status"] == "draft"


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


@pytest.mark.parametrize("failed_domain", [None, "example.com"])
def test_results_asset_resumes_publication_then_clears_completed_inputs(
    database, store, objects, monkeypatch, failed_domain
):
    import hashlib
    from urllib.parse import urlsplit

    import dagster as dg
    from dagster_v3.defs.common.processing import ProcessingResource
    from dagster_v3.defs.webtech import task_assets as module
    from dagster_v3.defs.webtech.client import WebtechApiResource
    from dagster_v3.defs.webtech.models import RemoteScanSnapshot
    from dagster_v3.defs.webtech.storage import WebtechS3Destination

    client, resource = database
    processing, dsn = store
    task_id = add(
        resource, processing, objects, targets=["novelic.com", "example.com"]
    )["task_id"]
    jobs = {}
    submissions = []
    indexed = []
    lose_ack = True

    def page_outcome(row):
        return "navigation_error" if row["root_domain"] == failed_domain else "success"

    def submit(api, manifest):
        del api
        scan_id = hashlib.sha256(manifest.uri.encode()).hexdigest()
        submissions.append(scan_id)
        if scan_id in jobs:
            return jobs[scan_id][0]
        candidate_document = json.loads(
            objects.read_bytes(urlsplit(manifest.uri).path.lstrip("/"))
        )
        candidates = candidate_document["candidates"]
        now = datetime.now(UTC)
        key = f"webtech/results/{scan_id}/final.json"
        snapshot = RemoteScanSnapshot(
            scan_id=scan_id,
            status="completed",
            crawl_id=manifest.crawl_id,
            partition_key=manifest.partition_key,
            detector_version=WEBTECH_DETECTOR_VERSION,
            candidate_manifest_uri=manifest.uri,
            result_prefix_uri=f"s3://webtech/webtech/results/{scan_id}",
            final_manifest_uri=f"s3://webtech/{key}",
            total_count=len(candidates),
            completed_count=len(candidates),
            outcome_counts=dict(Counter(page_outcome(row) for row in candidates)),
            technology_count=0,
            started_at=now,
            finished_at=now,
            last_progress_at=now,
            elapsed_seconds=1,
            progress_age_seconds=0,
            domains_per_minute=60,
            latest_event_sequence=1,
            error_message="",
        )
        objects.write_json(
            key,
            json.dumps(
                {
                    "schema_version": 1,
                    "scan_id": scan_id,
                    "crawl_id": manifest.crawl_id,
                    "partition_key": manifest.partition_key,
                    "detector_version": WEBTECH_DETECTOR_VERSION,
                    "candidate_manifest_uri": manifest.uri,
                    "candidate_manifest_sha256": manifest.sha256,
                    "started_at": now.isoformat(),
                    "finished_at": now.isoformat(),
                    "elapsed_seconds": 1,
                    "outcome_counts": snapshot.outcome_counts,
                    "technology_count": 0,
                    "scanner_settings": {},
                    "results": [
                        {
                            "root_domain": row["root_domain"],
                            "harmonic_rank": 0,
                            "input_id": row["input_id"],
                            "outcome": page_outcome(row),
                            "timeout_stage": None,
                            "technology_count": 0,
                            "duration_ms": 1,
                            "object_key": f"webtech/results/{scan_id}/{row['input_id']}.json",
                            "sha256": "0" * 64,
                            "size_bytes": 1,
                        }
                        for row in candidates
                    ],
                }
            ),
        )
        jobs[scan_id] = (snapshot, candidates)
        return snapshot

    def publish(**kwargs):
        nonlocal lose_ack
        reference = kwargs["reference"]
        snapshot, candidates = jobs[reference.scan_id]
        indexed.append(reference.scan_id)
        client.execute(
            "INSERT INTO corpscout.webtech_domain_scan_results (root_domain,website_origin,page_url,detector_version,scanned_at,scan_id,outcome,task_id,input_id) VALUES",
            [
                (
                    row["root_domain"],
                    row["page_url"].rstrip("/"),
                    row["page_url"],
                    WEBTECH_DETECTOR_VERSION,
                    snapshot.started_at,
                    reference.scan_id,
                    page_outcome(row),
                    task_id,
                    row["input_id"],
                )
                for row in candidates
            ],
        )
        if lose_ack:
            lose_ack = False
            raise RuntimeError("lost result publication acknowledgement")
        return len(candidates)

    monkeypatch.setattr(WebtechApiResource, "submit", submit)
    monkeypatch.setattr(
        module,
        "monitor_webtech_scan",
        lambda **kwargs: jobs[kwargs["submission"].scan_id][0],
    )
    monkeypatch.setattr(module, "index_final_results", publish)
    results = module.build_webtech_task_asset(
        WebtechS3Destination(bucket="webtech", prefix="webtech")
    )

    def run(**config):
        return dg.materialize(
            [results, dg.AssetSpec("webtech_scan_input")],
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
                        "config": {"task_id": task_id, "batch_size": 1, **config}
                    }
                }
            },
        )

    with pytest.raises(RuntimeError, match="lost result"):
        run()
    execution_id = processing.task(task_id)["config"]["execution"]["execution_id"]
    assert processing.task(task_id)["status"] == "ready"
    assert processing.task(task_id)["completed_at"] is None
    completed = run()
    assert completed.success
    metadata = completed.get_asset_materialization_events()[
        0
    ].event_specific_data.materialization.metadata
    assert metadata["completion_status"].value == (
        "completed_with_errors" if failed_domain else "completed"
    )
    assert metadata["failed_pages"].value == int(failed_domain is not None)
    assert (
        processing.task(task_id)["config"]["execution"]["execution_id"] == execution_id
    )
    assert len(jobs) == 2 and len(indexed) == 3
    assert indexed[0] == indexed[1]
    assert client.execute(
        "SELECT count() FROM corpscout.webtech_domain_scan_results FINAL"
    ) == [(2,)]
    assert processing.progress(task_id)["succeeded"] == 2 - int(
        failed_domain is not None
    )
    assert processing.task(task_id)["status"] == "completed"
    assert run().success  # Duplicate Start does not submit remote work again.
    assert len(submissions) == 3
    assert client.execute("SELECT count() FROM corpscout.webtech_scan_input") == [(0,)]
    assert processing.task(task_id)["inputs_purged_at"] is not None
    with pytest.raises(ValueError, match="Inputs were purged"):
        run(execution_id=str(uuid4()))
    # A deliberate rescan goes through a fresh queue, with prior results retained.
    old_task = task_id
    task_id = add(
        resource, processing, objects, targets=["novelic.com", "example.com"]
    )["task_id"]
    assert task_id != old_task
    assert run().success
    assert len(submissions) == 3 + int(
        failed_domain is not None
    )  # Retry errors; skip fresh successes.
    assert processing.progress(task_id)["skipped"] == 2 - int(failed_domain is not None)
    assert client.execute("SELECT count() FROM corpscout.webtech_scan_input") == [(0,)]


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
