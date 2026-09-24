"""Real ClickHouse source imports, repeatability and publication failure safety."""

import logging
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest
from clickhouse_driver import Client

from dagster_v3.defs.web_inventory.assets import (
    COMMONCRAWL_URLS,
    WebInventoryConfig,
    normalize_target,
    publish_web_inventory,
    web_inventory,
)
from tests.test_ip_enrichment_input import server as server


@pytest.fixture
def database(server):
    client, resource = server
    migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
    for name in (
        "000046_corpscout_commoncrawl_domains", "000125_corpscout_commoncrawl_page_evidence",
        "000127_corpscout_commoncrawl_page_jsonld", "000079_corpscout_commoncrawl_domain_page_meta",
        "000436_corpscout_webtech_pages", "000441_corpscout_domains_inventory",
        "000442_corpscout_websites_and_pages",
    ):
        for statement in (migrations / f"{name}.up.sql").read_text(encoding="utf-8").split(";"):
            if statement.strip():
                client.execute(statement)
    client.execute("CREATE TABLE IF NOT EXISTS corpscout.webtech_domain_scan_results AS corpscout.webtech_domain_scan_results_v2")
    for table in ("domains", "pages", "websites", "webtech_domain_scan_results",
                  *(name for name, _ in COMMONCRAWL_URLS)):
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    client.execute("INSERT INTO corpscout.domains (root_domain) VALUES ('example.com'),('old.se')")
    return client, resource


def build(resource, sources=None):
    config = WebInventoryConfig(
        sources=sources if sources is not None else ["commoncrawl", "webtech"],
        insert_batch_rows=2, merge_batch_rows=2,
    )
    return publish_web_inventory(resource, config=config, run_id="test-inventory", log=logging.getLogger(__name__))


def test_combines_sources_normalizes_pages_and_preserves_history(database):
    client, resource = database
    for table, column in COMMONCRAWL_URLS:
        client.execute(f"INSERT INTO corpscout.{table} (root_domain,{column},resolved_at) VALUES "
                       "('example.com','HTTPS://EXAMPLE.com.:443/about#top','2026-08-01')")
    client.execute("""INSERT INTO corpscout.webtech_domain_scan_results
        (root_domain,page_url,requested_url,final_url,scanned_at,outcome,scan_id)
        VALUES ('example.com','https://example.com/about','https://example.com/about',
            'https://www.example.com/about','2026-09-01','success','one'),
        ('example.com','https://example.com/failed','https://example.com/failed','',
            '2026-09-02','navigation_error','two')""")
    result = build(resource)
    assert result["pages"] == 2
    assert result["websites"] == 2
    assert client.execute("""SELECT page_url,sources,evidence_status,last_observed_at,
        last_successful_fetch_at FROM corpscout.pages ORDER BY page_url""") == [
        ("https://example.com/about", ["commoncrawl", "webtech"], "observed",
         datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 1, tzinfo=UTC)),
        ("https://www.example.com/about", ["webtech"], "observed",
         datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 1, tzinfo=UTC)),
    ]
    original = client.execute("SELECT page_id,first_seen_at,last_observed_at,sources FROM corpscout.pages ORDER BY page_id")
    # Simulate later failures replacing source rows and a partial source refresh.
    client.execute("TRUNCATE TABLE corpscout.webtech_domain_scan_results")
    build(resource, ["commoncrawl"])
    assert client.execute("SELECT page_id,first_seen_at,last_observed_at,sources FROM corpscout.pages ORDER BY page_id") == original
    assert client.execute("SELECT count() FROM corpscout.pages p INNER JOIN corpscout.websites w USING website_id") == [(2,)]
    assert client.execute("SELECT uniqExact(page_id),count() FROM corpscout.pages") == [(2, 2)]


def test_commoncrawl_does_not_claim_live_fetch_and_keeps_pages_without_technology(database):
    client, resource = database
    client.execute("""INSERT INTO corpscout.commoncrawl_domains (root_domain,url,resolved_at)
        VALUES ('example.com','https://example.com/no-tech','2026-01-01')""")
    result = build(resource, ["commoncrawl"])
    assert result["pages"] == 1
    assert client.execute("SELECT evidence_status,last_successful_fetch_at FROM corpscout.websites") == [("observed", None)]
    assert client.execute("SELECT last_successful_fetch_at FROM corpscout.pages") == [(None,)]


def test_webtech_only_uses_successes_including_zero_technologies(database):
    client, resource = database
    client.execute("""INSERT INTO corpscout.webtech_domain_scan_results
        (root_domain,requested_url,final_url,scanned_at,outcome,scan_id)
        VALUES ('example.com','https://example.com/','https://elsewhere.com/','2026-09-01','success','one')""")
    # Excluded sources are not required or read when processing Webtech only.
    client.execute("DROP TABLE corpscout.commoncrawl_domains")
    result = build(resource, ["webtech"])
    assert result["pages"] == 1
    assert result["source_counts"]["webtech_domain_scan_results"]["rejected_urls"] == 1
    assert client.execute("SELECT sources FROM corpscout.pages") == [(["webtech"],)]


def test_empty_invalid_and_missing_parent_leave_publication_intact(database):
    client, resource = database
    client.execute("""INSERT INTO corpscout.commoncrawl_domains (root_domain,url,resolved_at)
        VALUES ('example.com','https://example.com/','2026-01-01')""")
    build(resource)
    before = client.execute("SELECT * FROM corpscout.pages")
    for table, _ in COMMONCRAWL_URLS:
        client.execute(f"TRUNCATE TABLE corpscout.{table}")
    with pytest.raises(ValueError, match="No valid source pages"):
        build(resource)
    client.execute("""INSERT INTO corpscout.commoncrawl_domains (root_domain,url,resolved_at)
        VALUES ('example.com','not-a-url','2026-01-01')""")
    with pytest.raises(ValueError, match="No valid source pages"):
        build(resource)
    client.execute("""INSERT INTO corpscout.commoncrawl_domains (root_domain,url,resolved_at)
        VALUES ('absent.com','https://absent.com/','2026-01-01')""")
    with pytest.raises(ValueError, match="missing from domains"):
        build(resource)
    assert client.execute("SELECT * FROM corpscout.pages") == before
    assert client.execute("SELECT name FROM system.tables WHERE database='corpscout' AND match(name,'^(pages|websites)_(sources|stage)_')") == []


def test_second_exchange_failure_retains_parents_and_retry_converges(database, monkeypatch):
    client, resource = database
    client.execute("""INSERT INTO corpscout.commoncrawl_domains (root_domain,url,resolved_at)
        VALUES ('example.com','https://example.com/','2026-01-01')""")
    build(resource)
    client.execute("""INSERT INTO corpscout.commoncrawl_domains (root_domain,url,resolved_at)
        VALUES ('old.se','https://old.se/','2026-01-01')""")
    execute = Client.execute

    def fail_page_publish(self, query, *args, **kwargs):
        if query.startswith("EXCHANGE TABLES corpscout.pages_stage_"):
            raise RuntimeError("injected second exchange failure")
        return execute(self, query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Client, "execute", fail_page_publish)
        with pytest.raises(RuntimeError, match="second exchange"):
            build(resource)
    assert client.execute("SELECT count() FROM corpscout.pages") == [(1,)]
    assert client.execute("SELECT count() FROM corpscout.websites") == [(2,)]
    assert client.execute("SELECT count() FROM corpscout.pages p INNER JOIN corpscout.websites w USING website_id") == [(1,)]
    assert build(resource)["pages"] == 2
    assert client.execute("SELECT name FROM system.tables WHERE database='corpscout' AND match(name,'^(pages|websites)_(sources|stage)_')") == []


def test_assets_materialize_both_inventory_tables(database):
    client, resource = database
    client.execute("""INSERT INTO corpscout.commoncrawl_domains (root_domain,url,resolved_at)
        VALUES ('example.com','https://example.com/','2026-01-01')""")
    result = dg.materialize(
        [web_inventory], resources={"clickhouse": resource},
        run_config={"ops": {"web_inventory": {"config": {"sources": ["commoncrawl"]}}}},
    )
    assert result.success
    assert {event.asset_key.to_user_string() for event in result.get_asset_materialization_events()} == {"websites", "pages"}


def test_idna_and_query_identity_match_webtech():
    assert normalize_target("BÜCHER.de", "https://BÜCHER.de:443/A?x=1#top") == (
        "xn--bcher-kva.de", "https://xn--bcher-kva.de", "https://xn--bcher-kva.de/A?x=1",
    )
    with pytest.raises(ValueError):
        normalize_target("example.com", "https://notexample.com/")
