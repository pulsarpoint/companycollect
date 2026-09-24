"""Real ClickHouse publication: restricted sources and failure isolation."""

import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
from clickhouse_driver import Client

from dagster_v3.defs.domains.assets import DomainsConfig, publish_inventory
from tests.test_ip_enrichment_input import server as server

SOURCE_TABLES = (
    "company_website_domains", "company_domains_resolved", "se_company_domain",
    "open_page_rank_domains", "commoncrawl_domains", "commoncrawl_domain_graph_nodes",
    "commoncrawl_domain_graph_snapshots", "commoncrawl_domain_graph_signals",
    "commoncrawl_domain_dns_scan", "webtech_domain_scan_results", "website_crawl_results",
)


@pytest.fixture
def database(server):
    client, resource = server
    client.execute("DROP TABLE IF EXISTS corpscout.domains")
    client.execute("CREATE TABLE IF NOT EXISTS corpscout.domain_inventory (root_domain String) ENGINE=MergeTree ORDER BY root_domain")
    migration = Path(__file__).parents[3] / "clickhouse/migrations/000441_corpscout_domains_inventory.up.sql"
    for statement in migration.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            client.execute(statement)
    assert client.execute("EXISTS TABLE corpscout.domain_inventory") == [(0,)]
    for name in SOURCE_TABLES:
        client.execute(f"DROP TABLE IF EXISTS corpscout.{name}")
        client.execute(f"""CREATE TABLE corpscout.{name} (
            root_domain String DEFAULT '', domain String DEFAULT '', graph_release String DEFAULT '',
            is_current UInt8 DEFAULT 1, is_active UInt8 DEFAULT 1, version UInt64 DEFAULT 1
        ) ENGINE=ReplacingMergeTree(version) ORDER BY (root_domain,domain,graph_release)""")
    return client, resource


def build(resource, run_id="test-build"):
    return publish_inventory(resource, run_id=run_id, config=DomainsConfig(merge_batch_rows=2), log=logging.getLogger(__name__))


def test_combines_only_requested_sources_and_preserves_inventory_first_seen(database):
    client, resource = database
    client.execute("INSERT INTO corpscout.se_company_domain (root_domain) VALUES ('only-sweden.se'),('novelic.com')")
    client.execute("INSERT INTO corpscout.commoncrawl_domains (root_domain) VALUES ('novelic.com'),('NOVELIC.COM.'),('invalid@email.com'),('127.0.0.1')")
    client.execute("INSERT INTO corpscout.commoncrawl_domain_graph_snapshots (graph_release) VALUES ('published'),('older')")
    client.execute("INSERT INTO corpscout.commoncrawl_domain_graph_nodes (root_domain,graph_release) VALUES ('novelic.com','published'),('graph-only.com','published'),('graph-only.com','older'),('unfinished.com','loading')")
    # Populate excluded tables so any accidental expansion of source membership fails.
    for table in SOURCE_TABLES:
        if table not in ("se_company_domain", "commoncrawl_domains", "commoncrawl_domain_graph_nodes", "commoncrawl_domain_graph_snapshots"):
            client.execute(f"INSERT INTO corpscout.{table} (root_domain,domain,graph_release) VALUES ('excluded.example','excluded.example','published')")
    first_seen = datetime(2020, 1, 1, tzinfo=UTC)
    client.execute("INSERT INTO corpscout.domains VALUES", [("novelic.com", ["old-source"], first_seen, first_seen, "previous")])
    assert build(resource)["domains"] == 3
    rows = dict(client.execute("SELECT root_domain,sources FROM corpscout.domains"))
    assert rows == {
        "novelic.com": ["commoncrawl", "commoncrawl_graph", "se_company_domain"],
        "graph-only.com": ["commoncrawl_graph"],
        "only-sweden.se": ["se_company_domain"],
    }
    assert client.execute("SELECT first_seen_at FROM corpscout.domains WHERE root_domain='novelic.com'") == [(first_seen,)]
    client.execute("TRUNCATE TABLE corpscout.se_company_domain")
    assert build(resource, "refresh")["domains"] == 2
    assert client.execute("SELECT sources,source_run_id FROM corpscout.domains WHERE root_domain='novelic.com'") == [(["commoncrawl", "commoncrawl_graph"], "refresh")]
    assert client.execute("SELECT count() FROM corpscout.domains WHERE root_domain='only-sweden.se'") == [(0,)]


def test_missing_source_keeps_published_inventory_and_removes_build_tables(database):
    client, resource = database
    stamp = datetime.now(UTC)
    client.execute("INSERT INTO corpscout.domains VALUES", [("retained.com", ["commoncrawl"], stamp, stamp, "previous")])
    client.execute("INSERT INTO corpscout.se_company_domain (root_domain) VALUES ('new.com')")
    client.execute("DROP TABLE corpscout.commoncrawl_domains")
    with pytest.raises(Exception, match="commoncrawl_domains"):
        build(resource)
    assert client.execute("SELECT root_domain,source_run_id FROM corpscout.domains") == [("retained.com", "previous")]
    assert client.execute("SELECT count() FROM system.tables WHERE database='corpscout' AND startsWith(name,'domains_')") == [(0,)]


def test_empty_sources_cannot_erase_inventory(database):
    client, resource = database
    stamp = datetime.now(UTC)
    client.execute("INSERT INTO corpscout.domains VALUES", [("retained.com", ["commoncrawl"], stamp, stamp, "previous")])
    with pytest.raises(ValueError, match="All domain inventory sources are empty"):
        build(resource)
    assert client.execute("SELECT root_domain FROM corpscout.domains") == [("retained.com",)]


def test_graph_merge_uses_bounded_ranges_spills_and_preserves_existing_dates(database, monkeypatch):
    client, resource = database
    client.execute("INSERT INTO corpscout.commoncrawl_domain_graph_snapshots (graph_release) VALUES ('published')")
    client.execute("""INSERT INTO corpscout.commoncrawl_domain_graph_nodes (root_domain,graph_release)
        SELECT concat('n',toString(number),'.example'),'published' FROM numbers(500000)""")
    first_seen = datetime(2020, 1, 1, tzinfo=UTC)
    client.execute("INSERT INTO corpscout.domains VALUES", [("n7.example", ["commoncrawl_graph"], first_seen, first_seen, "previous")])
    execute = Client.execute
    merge_ids = []

    def limited_execute(self, query, *args, **kwargs):
        query_id = kwargs.get("query_id", "")
        if ":fold-" in query_id:
            merge_ids.append(query_id)
            kwargs["settings"] = {
                **kwargs["settings"],
                "max_threads": 2,
                "max_memory_usage": 256 * 1024**2,
                "max_bytes_before_external_group_by": 256 * 1024,
                "max_bytes_before_external_sort": 256 * 1024,
                "log_queries": 1,
            }
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", limited_execute)
    result = publish_inventory(resource, run_id="bounded-merge", config=DomainsConfig(merge_batch_rows=100000), log=logging.getLogger(__name__))
    assert result["domains"] == 500000
    assert result["merge_batches"] == 5
    assert client.execute("SELECT uniqExact(root_domain) FROM corpscout.domains") == [(500000,)]
    assert client.execute("SELECT first_seen_at FROM corpscout.domains WHERE root_domain='n7.example'") == [(first_seen,)]
    client.execute("SYSTEM FLUSH LOGS")
    [(spill_parts,)] = client.execute("""SELECT sum(ProfileEvents['ExternalAggregationWritePart'])
        FROM system.query_log WHERE query_id=%(id)s AND type='QueryFinish'""", {"id": merge_ids[0]})
    assert spill_parts > 0


def test_later_merge_failure_keeps_previous_publication(database, monkeypatch):
    client, resource = database
    stamp = datetime.now(UTC)
    client.execute("INSERT INTO corpscout.domains VALUES", [("retained.com", ["commoncrawl"], stamp, stamp, "previous")])
    client.execute("INSERT INTO corpscout.se_company_domain (root_domain) VALUES ('a.example'),('b.example'),('c.example')")
    execute = Client.execute

    def fail_later_range(self, query, *args, **kwargs):
        if kwargs.get("query_id", "").endswith(":fold-1"):
            raise RuntimeError("Second range failed")
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", fail_later_range)
    with pytest.raises(RuntimeError, match="Second range failed"):
        publish_inventory(resource, run_id="failed-range", config=DomainsConfig(merge_batch_rows=1), log=logging.getLogger(__name__))
    assert client.execute("SELECT root_domain,source_run_id FROM corpscout.domains") == [("retained.com", "previous")]
    assert client.execute("SELECT count() FROM system.tables WHERE database='corpscout' AND startsWith(name,'domains_')") == [(0,)]
