"""Native publication, parent failure and origin isolation contracts."""

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from clickhouse_driver import Client
from corpscout_identity.observations import register_crawl_results
from corpscout_identity.registration import identify_website
from corpscout_identity.urls import website_reference
from dagster_v3.defs.website_crawl.queue_execution import (
    dispatchable_entries,
    remaining_crawl_entries,
)

from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_crawl_draft_queue import add, start
from tests.test_crawl_draft_queue import db as db
from tests.test_domain_result_references import archive_url as archive_url
from tests.test_domain_result_references import migrate
from tests.test_domain_result_references import previous_schema as previous_schema
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)
from tests.test_processing_store import store as store
from tests.test_website_crawl_input_assets import server as server


def lookup_writer():
    # This module depends only on the shared identity package and native client;
    # exercise the deployed crawler publisher against the same migration fixture.
    path = (
        Path(__file__).parents[2]
        / "crawler_service/src/crawler_service/company_lookup_results.py"
    )
    spec = importlib.util.spec_from_file_location("lookup_result_publisher", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def result(url="https://example.se/"):
    stamp = datetime.now(UTC).isoformat()
    return dict(
        country="SE",
        domain="example.se",
        website_url=url,
        website_id=website_reference(url),
        request_id="identity-publication",
        attempt=1,
        started_at=stamp,
        finished_at=stamp,
        status="not_found",
        result_path="saved/result.json",
        work_key="settings",
        site_info_result={
            "crawl": {
                "status": "finished",
                "started_at": stamp,
                "finished_at": stamp,
                "site_info": {},
                "usage": {},
                "stop_reason": "done",
                "pages": [
                    {
                        "page_id": "p0001",
                        "requested_url": url,
                        "source_url": "https://www.example.com/about",
                        "fetch_status": "fetched",
                        "fetched_at": stamp,
                        "redirects": [
                            {"url": url, "location": "https://www.example.com/about"}
                        ],
                    }
                ],
            }
        },
    )


def test_parent_registration_native_destination_and_replay(
    previous_schema, identity_postgres, monkeypatch
):
    client = previous_schema
    migrate(client)
    document = result()
    writer = lookup_writer()

    with pytest.raises(ValueError, match="missing registered"):
        writer.publish(client, [document])
    assert client.execute(
        "SELECT count() FROM corpscout.website_company_lookup_results"
    ) == [(0,)]
    # A coordinator failure prevents registration, with no child publication.
    with pytest.raises(ValueError, match="PROCESSING_PG_URL"):
        monkeypatch.delenv("PROCESSING_PG_URL")
        register_crawl_results(client, [document], source="test", run_id="test")
    monkeypatch.setenv("PROCESSING_PG_URL", identity_postgres)
    register_crawl_results(client, [document], source="test", run_id="test")
    writer.publish(client, [document])
    # Retry a saved payload, without another crawl or a new attempt.
    register_crawl_results(client, [document], source="test", run_id="test")
    writer.publish(client, [document])
    for table in (
        "website_company_lookup_results",
        "website_site_info_results",
    ):
        assert client.execute(
            f"SELECT website_id,count() FROM corpscout.{table} FINAL GROUP BY website_id"
        ) == [(document["website_id"], 1)]
    target = identify_website("https://www.example.com/about")
    assert client.execute(
        "SELECT page_id,website_id,domain_id FROM corpscout.pages WHERE page_url=%(url)s",
        {"url": target.page_url},
    ) == [(target.page_id, target.website_id, target.domain_id)]
    assert client.execute(
        "SELECT uniqExact(website_id),count() FROM corpscout.websites"
    ) == [(2, 2)]


    # Registration in one database cannot authorize another publication destination.
    wrong = Client("another-database")
    monkeypatch.setattr(wrong, "execute", lambda *_args, **_kwargs: [])
    with pytest.raises(ValueError, match="missing registered"):
        writer.publish(wrong, [document])


def test_batch_admission_keeps_origins_separate_and_request_ids_stable(db):
    client, resource, _, _ = db
    targets = [
        "https://example.se/",
        "http://example.se/",
        "https://www.example.se/",
        "https://jobs.example.se/",
        "https://example.se:8443/",
    ]
    task_id = add(db, targets=targets)["task_id"]
    assert client.execute(
        "SELECT count(),uniqExact(website_id) FROM corpscout.website_crawl_task_domains"
    ) == [(5, 5)]
    assert client.execute(
        "SELECT count() FROM corpscout.domains WHERE root_domain='example.se'"
    ) == [(1,)]
    assert client.execute(
        "SELECT count() FROM corpscout.websites WHERE root_domain='example.se'"
    ) == [(5,)]
    task, config = start(db, task_id)
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        first = dispatchable_entries(
            connection, rows, task=task, crawl_type="site_info", config=config
        )
        replay = dispatchable_entries(
            connection, rows, task=task, crawl_type="site_info", config=config
        )
    assert len({item["request_id"] for item in first}) == 5
    assert first == replay
    assert all(
        json.loads(item["request_json"])["website_id"] == item["website_id"]
        for item in first
    )
    assert all(row["request_identity_version"] == 2 for row in rows)


def test_invalid_member_refuses_whole_batch_before_queue_or_parent_writes(db):
    client, _, _, _ = db
    with pytest.raises(ValueError, match="registrable domain"):
        add(db, targets=["https://valid.se/", "https://invalid.not-a-real-tld/"])
    assert client.execute(
        "SELECT count() FROM corpscout.website_crawl_task_domains"
    ) == [(0,)]
    assert client.execute(
        "SELECT count() FROM corpscout.domains WHERE root_domain='valid.se'"
    ) == [(0,)]


def test_saved_crawl_observations_update_existing_inventory(
    previous_schema, identity_postgres
):
    import logging

    from corpscout_identity.registration import WebsiteObservation, register_websites
    from dagster_clickhouse import ClickhouseResource
    from dagster_v3.defs.web_inventory.assets import (
        WebInventoryConfig,
        publish_web_inventory,
    )

    client = previous_schema
    migrate(client)
    item = identify_website("https://example.se/about")
    stamp = datetime(2026, 9, 1, tzinfo=UTC)
    register_websites(
        client,
        [WebsiteObservation(item, stamp, None, None)],
        source="queue",
        run_id="queue",
    )
    # Use the real published view/completion marker; unpublished pages do not fold.
    norm = "00000000-0000-0000-0000-000000000111"
    client.execute(
        """INSERT INTO corpscout.website_crawl_pages
        (domain,website_id,crawl_type,request_id,attempt,normalization_id,page_id,resource_page_id,fetch_status,fetched_at)
        VALUES ('example.se',%(site)s,'site_info','observed',1,%(norm)s,'p0001',%(page)s,'fetched','2026-09-02')""",
        {"site": item.website_id, "norm": norm, "page": item.page_id},
    )
    client.execute(
        """INSERT INTO corpscout.website_crawl_scans
        (domain,website_id,crawl_type,request_id,attempt,normalization_id,normalization_revision,parser_version)
        VALUES ('example.se',%(site)s,'site_info','observed',1,%(norm)s,1,'2')""",
        {"site": item.website_id, "norm": norm},
    )
    resource = ClickhouseResource(
        host="127.0.0.1",
        port=client.connection.port,
        user="test",
        password="test",
        database="corpscout",
    )
    publish_web_inventory(
        resource,
        config=WebInventoryConfig(sources=["crawler"]),
        run_id="fold",
        log=logging.getLogger(__name__),
    )
    assert client.execute(
        "SELECT first_seen_at,last_successful_fetch_at FROM corpscout.pages WHERE page_id=%(id)s",
        {"id": item.page_id},
    ) == [(stamp, datetime(2026, 9, 2, tzinfo=UTC))]
