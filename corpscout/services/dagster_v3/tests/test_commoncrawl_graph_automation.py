from datetime import UTC, datetime, timedelta, date
from uuid import uuid4

import dagster as dg

from dagster_v3.defs.commoncrawl_domain_graph.automation import (
    prepare_request,
    queue_automatic_requests,
    reconcile_requests,
    REQUEST_TAG,
)
from dagster_v3.defs.commoncrawl_domain_graph.catalog import GraphRelease, ReleaseFile
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphStore
from tests.test_commoncrawl_graph_migrations import catalog_db as catalog_db
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)


def test_schedule_names_match_backoffice_contract():
    from dagster_v3.defs.commoncrawl_domain_graph.discovery import (
        commoncrawl_graph_discovery_schedule,
    )
    from dagster_v3.defs.commoncrawl_domain_graph.retention import (
        commoncrawl_graph_cleanup_schedule,
    )

    assert (
        commoncrawl_graph_discovery_schedule.name
        == "commoncrawl_graph_discovery_schedule"
    )
    assert (
        commoncrawl_graph_cleanup_schedule.name == "commoncrawl_graph_cleanup_schedule"
    )


def release(key, end):
    return GraphRelease(
        key,
        "https://example.test",
        [],
        date(2026, 1, 1),
        end,
        [
            ReleaseFile(
                kind, f"https://example.test/{kind}", '"etag"', 123, 10, "available"
            )
            for kind in ("nodes", "edges", "ranks")
        ],
    )


def test_baseline_new_releases_pause_and_stable_launch(catalog_db):
    store = GraphStore(catalog_db[0])
    old = release("cc-main-2026-may-jun-jul", date(2026, 7, 1))
    newer = release("cc-main-2026-jun-jul-aug", date(2026, 8, 1))
    newest = release("cc-main-2026-jul-aug-sep", date(2026, 9, 1))
    baseline = datetime.now(UTC) - timedelta(days=1)
    store.save_catalog([old], baseline)
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_state SET automatic_imports_enabled=true"
        )
    queue_automatic_requests(store, set())
    assert (
        prepare_request(store) is None
    )  # First discovery never starts a historical graph backfill.
    store.save_catalog([old, newer, newest], datetime.now(UTC))
    queue_automatic_requests(store, set())
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT graph_release,selection FROM commoncrawl_graph_import_requests ORDER BY graph_release"
        )
        assert [tuple(r.values()) for r in cursor.fetchall()] == sorted(
            [(newest.graph_release, "full"), (newer.graph_release, "ranks")]
        )
        cursor.execute(
            "UPDATE commoncrawl_graph_state SET automatic_imports_enabled=false"
        )
    assert prepare_request(store) is None
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_state SET automatic_imports_enabled=true"
        )
    pending = prepare_request(store)
    assert pending == prepare_request(
        store
    )  # Lost sensor acknowledgement returns the same request/run key.
    run_id = str(uuid4())
    manifest = store.pin_run(
        pending["graph_release"],
        run_id,
        pending["selection"],
        str(pending["request_id"]),
    )
    assert manifest == pending["source_manifest"]
    assert prepare_request(store) is None
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_release_files SET source_etag='changed'"
        )
    assert (
        store.pin_run(
            pending["graph_release"],
            run_id,
            pending["selection"],
            str(pending["request_id"]),
        )
        == manifest
    )


def test_reconcile_failed_run_and_bounded_delayed_retry(catalog_db):
    store = GraphStore(catalog_db[0])
    value = release("cc-main-2026-jul-aug-sep", date(2026, 9, 1))
    store.save_catalog([value], datetime.now(UTC))
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_state SET automatic_imports_enabled=true,discovery_baseline_at=now()-interval '1 day'"
        )
    queue_automatic_requests(store, set())
    pending = prepare_request(store)

    @dg.job
    def test_job():
        pass

    with dg.DagsterInstance.ephemeral() as instance:
        run = instance.create_run_for_job(
            test_job, tags={REQUEST_TAG: str(pending["request_id"])}
        )
        instance.report_run_failed(run)
        reconcile_requests(store, instance)
    queue_automatic_requests(store, set())
    assert prepare_request(store) is None  # Backoff.
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_import_requests SET finished_at=now()-interval '2 hours'"
        )
    queue_automatic_requests(store, set())
    retry = prepare_request(store)
    assert retry["request_id"] != pending["request_id"]
    assert retry["retry_of"] == pending["request_id"]
    assert retry["source_manifest"] == pending["source_manifest"]
