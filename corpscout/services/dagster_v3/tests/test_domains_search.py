"""Canonical membership, filter facts and atomic publication in real ClickHouse."""

import logging
from pathlib import Path

import pytest
from clickhouse_driver import Client

from dagster_v3.defs.domains_search.assets import DomainsSearchConfig, publish_domains_search
from tests.test_ip_enrichment_input import server as server


@pytest.fixture
def database(server):
    client, resource = server
    migrations = Path(__file__).resolve().parents[3] / 'clickhouse/migrations'
    for name in ('000441_corpscout_domains_inventory', '000442_corpscout_websites_and_pages', '000443_corpscout_domains_search'):
        for statement in (migrations / f'{name}.up.sql').read_text(encoding='utf-8').split(';'):
            if statement.strip():
                client.execute(statement)
    client.execute('CREATE TABLE IF NOT EXISTS corpscout.se_company_domain (company_id String,root_domain String,active UInt8,association String,version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY (company_id,root_domain)')
    client.execute('CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records (root_domain String,last_seen DateTime64(3)) ENGINE=MergeTree ORDER BY root_domain')
    for table in ('domains','websites','domains_search','se_company_domain','commoncrawl_domain_dns_records'):
        client.execute(f'TRUNCATE TABLE corpscout.{table}')
    client.execute("""INSERT INTO corpscout.domains (root_domain,sources,first_seen_at,last_seen_at) VALUES
        ('a.se',['se_company_domain'],'2026-01-01','2026-09-01'),
        ('b.com',['commoncrawl','commoncrawl_graph'],'2026-02-01','2026-09-01'),
        ('c.org',['commoncrawl_graph'],'2026-03-01','2026-09-01')""")
    return client, resource


def build(resource):
    return publish_domains_search(resource, config=DomainsSearchConfig(batch_rows=1), run_id='test-search', log=logging.getLogger(__name__))


def test_enriches_only_canonical_members_and_refresh_removes_stale_flags(database):
    client, resource = database
    client.execute("""INSERT INTO corpscout.se_company_domain VALUES
        ('1','a.se',1,'connected',1),('1','a.se',1,'connected',1),
        ('2','a.se',0,'connected',1),('3','a.se',1,'uncertain',1),
        ('4','a.se',1,'not_connected',1),('5','outside.net',1,'connected',1)""")
    client.execute("""INSERT INTO corpscout.commoncrawl_domain_dns_records VALUES
        ('b.com','2026-08-01'),('b.com','2026-09-01'),('b.com','2026-09-01'),('outside.net','2026-09-01')""")
    client.execute("""INSERT INTO corpscout.websites
        (root_domain,website_origin,sources,first_seen_at,last_seen_at,last_observed_at) VALUES
        ('a.se','https://a.se',['se_company_domain'],'2026-01-01','2026-09-01',NULL),
        ('a.se','https://www.a.se',['webtech'],'2026-01-01','2026-09-01','2026-09-01'),
        ('outside.net','https://outside.net',['webtech'],'2026-01-01','2026-09-01','2026-09-01')""")
    assert build(resource)['dagster/row_count'] == 3
    assert client.execute('SELECT root_domain,has_dns_records,website_count,observed_website_count,has_website,company_count,has_company FROM corpscout.domains_search ORDER BY root_domain') == [
        ('a.se',0,2,1,1,1,1),('b.com',1,0,0,0,0,0),('c.org',0,0,0,0,0,0)]
    assert client.execute('SELECT root_domain,sources,first_seen_at FROM corpscout.domains_search ORDER BY root_domain') == client.execute('SELECT root_domain,sources,first_seen_at FROM corpscout.domains ORDER BY root_domain')
    client.execute('TRUNCATE TABLE corpscout.websites')
    client.execute("INSERT INTO corpscout.se_company_domain VALUES ('1','a.se',0,'connected',2)")
    build(resource)
    assert client.execute("SELECT has_website,has_company FROM corpscout.domains_search WHERE root_domain='a.se'") == [(0,0)]


def test_empty_or_failed_build_preserves_previous_publication(database, monkeypatch):
    client, resource = database
    build(resource)
    original = client.execute('SELECT * FROM corpscout.domains_search ORDER BY root_domain')
    execute = Client.execute
    def fail_later_batch(self, query, *args, **kwargs):
        if kwargs.get('query_id','').endswith('batch-1'):
            raise RuntimeError('injected later batch failure')
        return execute(self,query,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Client,'execute',fail_later_batch)
        with pytest.raises(RuntimeError, match='later batch'):
            build(resource)
    assert client.execute('SELECT * FROM corpscout.domains_search ORDER BY root_domain') == original
    client.execute('TRUNCATE TABLE corpscout.domains')
    with pytest.raises(ValueError,match='empty'):
        build(resource)
    assert client.execute('SELECT * FROM corpscout.domains_search ORDER BY root_domain') == original
    assert client.execute("SELECT name FROM system.tables WHERE database='corpscout' AND match(name,'^domains_search_(roots|companies|stage)_')") == []


def test_company_projection_supports_selective_filter(database):
    client, _ = database
    client.execute("""INSERT INTO corpscout.domains_search (root_domain,has_dns_records,company_count)
        SELECT concat(leftPad(toString(number),8,'0'),'.example'),0,toUInt64(number>=99900)
        FROM numbers(100000)""")
    # Verify both exact results and actual optimizer use of the alternate ordering.
    query = "SELECT root_domain FROM corpscout.domains_search WHERE has_company=1 ORDER BY root_domain LIMIT 25"
    assert len(client.execute(query)) == 25
    plan = '\n'.join(row[0] for row in client.execute('EXPLAIN projections=1 '+query))
    assert 'by_company' in plan


def test_membership_is_frozen_before_concurrent_inventory_updates(database, monkeypatch):
    client, resource = database
    execute = Client.execute
    changed = False

    def update_inventory_after_clone(self, query, *args, **kwargs):
        nonlocal changed
        result = execute(self, query, *args, **kwargs)
        if 'CLONE AS corpscout.domains' in query and not changed:
            changed = True
            execute(self, "INSERT INTO corpscout.domains (root_domain,sources,first_seen_at,last_seen_at) VALUES ('later.net',['commoncrawl'],'2026-09-24','2026-09-24')")
        return result

    monkeypatch.setattr(Client, 'execute', update_inventory_after_clone)
    assert build(resource)['dagster/row_count'] == 3
    assert client.execute('SELECT count() FROM corpscout.domains') == [(4,)]
    assert client.execute("SELECT count() FROM corpscout.domains_search WHERE root_domain='later.net'") == [(0,)]
