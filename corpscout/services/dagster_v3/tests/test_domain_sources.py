"""Parent/source/country ordering, recovery and stable global identities."""

import logging
from datetime import UTC, datetime
from hashlib import sha256

import pytest
from clickhouse_driver import Client

from dagster_v3.defs.domains.source_backfill import (
    DomainSourcesBackfillConfig,
    copy_source_index,
)
from dagster_v3.defs.domains.sources import publish_source_index
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.batch import insert_rows
from tests.domain_sources_schema import MIGRATIONS, central_schema_sql, execute_sql
from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_ip_enrichment_input import server as server
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)
from tests.test_se_company_domain_clickhouse_local import setup_sql

pytestmark = pytest.mark.usefixtures("identity_postgres")


@pytest.fixture
def database(server):
    client, _ = server
    execute_sql(client, "DROP DATABASE corpscout SYNC; CREATE DATABASE corpscout;")
    execute_sql(client, setup_sql())
    execute_sql(client, central_schema_sql())
    execute_sql(client, (MIGRATIONS / "000462_corpscout_domains_sources.up.sql").read_text())
    execute_sql(client, (MIGRATIONS / "000463_corpscout_domain_source_readers.up.sql").read_text())
    return client


def association(root="new.se", company="5561234567"):
    stamp = datetime(2026, 9, 28, tzinfo=UTC)
    return dict(root_domain=root, company_id=company, association="connected", confidence=.95,
                active=1, first_seen_at=stamp, last_seen_at=stamp, folded_at=stamp,
                source_run_id="test")


def test_backfill_keeps_existing_company_evidence_and_links_websites(database):
    client = database
    expected = sha256(b"legacy.se").hexdigest()
    assert client.execute("SELECT domain_id,root_domain FROM corpscout.se_company_domain_resolved") == [(expected,"legacy.se")]
    assert client.execute("SELECT domain_id,country_code,company_id,is_active,confidence FROM corpscout.domains_sources FINAL") == [(expected,"SE","5561552760",1,1.)]
    client.execute("INSERT INTO corpscout.websites (root_domain,website_origin,sources) VALUES ('legacy.se','https://www.legacy.se',['test'])")
    assert client.execute("SELECT count() FROM corpscout.websites w INNER JOIN corpscout.domains d ON w.domain_id=d.domain_id") == [(1,)]


def test_bulk_backfill_resume_preserves_both_sources_without_duplicate_copies(database):
    roots = [f"bulk-{i}.se" for i in range(128)]
    # Exercise every bounded membership set, including both ends of the ID space.
    assert {int(sha256(root.encode()).hexdigest()[0], 16) // 2 for root in roots} == set(range(8))
    database.execute("INSERT INTO corpscout.domains (root_domain,sources) VALUES",
                     [(root, ["commoncrawl", "commoncrawl_graph"]) for root in roots])
    migration = (MIGRATIONS / "000462_corpscout_domains_sources.up.sql").read_text()
    bulk = [sql for sql in migration.split(';') if 'FROM corpscout.domains PREWHERE' in sql]
    assert len(bulk) == 16
    # Resume after one full source and part of the second source were written.
    for sql in bulk[:11]:
        database.execute(sql)
    for sql in bulk:
        database.execute(sql)
    assert database.execute("SELECT source_table,count(),uniqExact(domain_id) FROM corpscout.domains_sources WHERE country_code='' GROUP BY source_table ORDER BY source_table") == [
        ('commoncrawl_domain_graph_nodes',128,128),('commoncrawl_domains',128,128)]



def activate_compact(client):
    execute_sql(client, (MIGRATIONS / "000467_corpscout_compact_domain_sources.up.sql").read_text())
    result = copy_source_index(client, config=DomainSourcesBackfillConfig(execute=True, page_size=2), log=logging.info)
    assert result["complete"]
    client.execute("EXCHANGE TABLES corpscout.domains_sources AND corpscout.domains_sources_next")
    client.execute("RENAME TABLE corpscout.domains_sources_next TO corpscout.domains_sources_legacy")


@pytest.fixture
def compact_database(database):
    activate_compact(database)
    return database


def publish(client, sources=None):
    return publish_source_index(client, source_tables=sources or ["se_company_domain"], run_id="test", log=logging.info)


def insert_company(client, row):
    insert_rows(client, tables.MAIN_TABLE, tuple(row), [row])


def test_country_write_waits_for_parent_but_not_derived_index(compact_database, monkeypatch):
    client = compact_database
    row = association()
    original = Client.execute
    writes = []
    fail_parent = True

    def execute(self, query, *args, **kwargs):
        if query.startswith("INSERT INTO corpscout."):
            table = query.split()[2]
            writes.append(table)
            if table == "corpscout.domains" and fail_parent:
                raise RuntimeError("parent unavailable")
        return original(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", execute)
    with pytest.raises(RuntimeError, match="parent unavailable"):
        insert_company(client, row)
    assert client.execute("SELECT count() FROM corpscout.se_company_domain WHERE company_id='5561234567'") == [(0,)]
    fail_parent = False
    insert_company(client, row)
    assert writes == ["corpscout.domains", "corpscout.domains", "corpscout.se_company_domain"]
    assert client.execute("SELECT count() FROM corpscout.domains_sources WHERE domain_id=lower(hex(SHA256('new.se')))") == [(0,)]
    publish(client)
    assert client.execute("SELECT source_table FROM corpscout.domains_sources WHERE domain_id=lower(hex(SHA256('new.se')))") == [("se_company_domain",)]


def test_two_companies_and_withdrawal_keep_one_table_contribution(compact_database):
    client = compact_database
    row = association()
    insert_company(client, row)
    insert_company(client, association(company="5569999999"))
    publish(client)
    before = client.execute("SELECT domain_id,source_table,first_seen_at,last_seen_at FROM corpscout.domains_sources ORDER BY domain_id")
    for company in (row["company_id"], "5569999999"):
        insert_company(client, {**row, "company_id":company, "active":0, "association":"not_connected", "folded_at":datetime(2026,9,29,tzinfo=UTC)})
    publish(client)
    assert client.execute("SELECT domain_id,source_table,first_seen_at,last_seen_at FROM corpscout.domains_sources ORDER BY domain_id") == before
    # Historical contribution outlives even physical removal of country rows.
    client.execute("TRUNCATE TABLE corpscout.se_company_domain")
    publish(client)
    assert client.execute("SELECT domain_id,source_table,first_seen_at,last_seen_at FROM corpscout.domains_sources ORDER BY domain_id") == before


def test_partial_refresh_preserves_other_countries_and_bulk_sources(compact_database):
    client = compact_database
    insert_company(client, association())
    publish(client)
    for source in ("no_company_domain", "commoncrawl_domains", "commoncrawl_domain_graph_nodes"):
        client.execute("""INSERT INTO corpscout.domains_sources
            SELECT * REPLACE (%(source)s AS source_table) FROM (SELECT * FROM corpscout.domains_sources
            WHERE domain_id=lower(hex(SHA256('new.se'))) AND source_table='se_company_domain')""", {"source":source})
    other = client.execute("SELECT * FROM corpscout.domains_sources WHERE source_table!='se_company_domain' ORDER BY source_table")
    publish(client)
    assert client.execute("SELECT * FROM corpscout.domains_sources WHERE source_table!='se_company_domain' ORDER BY source_table") == other
    assert client.execute("SELECT count() FROM corpscout.domains_sources WHERE domain_id=lower(hex(SHA256('new.se')))") == [(4,)]
    with pytest.raises(ValueError, match="Unregistered"):
        publish(client, ["se_company_domain; DROP TABLE domains"])


def test_failed_index_publish_rebuilds_from_saved_summary(compact_database, monkeypatch):
    client = compact_database
    insert_company(client, association())
    execute = Client.execute
    before = client.execute("SELECT * FROM corpscout.domains_sources")
    def fail_publish(self, query, *args, **kwargs):
        if query.startswith("ALTER TABLE corpscout.domains_sources REPLACE"):
            raise RuntimeError("index publication unavailable")
        return execute(self, query, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Client, "execute", fail_publish)
        with pytest.raises(RuntimeError, match="publication unavailable"):
            publish(client)
    assert client.execute("SELECT * FROM corpscout.domains_sources") == before
    assert client.execute("SELECT count() FROM corpscout.se_company_domain FINAL WHERE company_id='5561234567'") == [(1,)]
    publish(client)
    assert client.execute("SELECT count() FROM corpscout.domains_sources WHERE domain_id=lower(hex(SHA256('new.se')))") == [(1,)]
    assert client.execute("SELECT name FROM system.tables WHERE database='corpscout' AND startsWith(name,'domains_sources_stage_')") == []


def test_missing_parent_blocks_index_publication(compact_database):
    client = compact_database
    row = association("missing.se")
    client.execute(f"INSERT INTO corpscout.se_company_domain ({','.join(row)}) VALUES", [row])
    with pytest.raises(Exception, match="no registered parent"):
        publish(client)
    assert client.execute("SELECT count() FROM corpscout.domains_sources WHERE domain_id=lower(hex(SHA256('missing.se')))") == [(0,)]


def test_bounded_copy_preserves_all_sources_dates_and_replays_after_lost_ack(database, monkeypatch):
    client = database
    execute_sql(client, (MIGRATIONS / "000467_corpscout_compact_domain_sources.up.sql").read_text())
    # Unknown future country names are preserved as values, not queried as tables.
    client.execute("""INSERT INTO corpscout.domains_sources
        SELECT * REPLACE ('no_company_domain' AS source_table,'NO' AS country_code)
        FROM corpscout.domains_sources FINAL""")
    client.execute("""INSERT INTO corpscout.domains_sources
        SELECT * REPLACE ('second-company' AS source_record_id,'second-company' AS company_id,
            toDateTime64('2020-01-01',3,'UTC') AS first_seen_at)
        FROM (SELECT * FROM corpscout.domains_sources FINAL WHERE source_table='se_company_domain')""")
    execute = Client.execute
    lost = False
    def lose_ack(self, query, *args, **kwargs):
        nonlocal lost
        result = execute(self, query, *args, **kwargs)
        if query.startswith("INSERT INTO corpscout.domains_sources_next") and not lost:
            lost = True
            raise RuntimeError("ack lost")
        return result
    config = DomainSourcesBackfillConfig(execute=True, page_size=1)
    with monkeypatch.context() as patch:
        patch.setattr(Client, "execute", lose_ack)
        with pytest.raises(RuntimeError, match="ack lost"):
            copy_source_index(client, config=config, log=logging.info)
    result = copy_source_index(client, config=config, log=logging.info)
    assert result["complete"] and result["domains"] == 1 and result["contributions"] == 2
    assert client.execute("SELECT source_table,toYear(first_seen_at) FROM corpscout.domains_sources_next FINAL ORDER BY source_table") == [("no_company_domain",2026),("se_company_domain",2020)]
    assert client.execute("SELECT count() FROM corpscout.domains_sources FINAL") == [(3,)]
    # A verified resume cursor finishes without copying any rows again.
    assert copy_source_index(client, config=DomainSourcesBackfillConfig(execute=True,after_domain_id=result["after_domain_id"]), log=logging.info)["domains"] == 0


def test_bulk_publication_preserves_history_and_updates_only_selected_partition(compact_database):
    client = compact_database
    client.execute("INSERT INTO corpscout.domains (root_domain,sources,first_seen_at,last_seen_at) VALUES ('bulk.se',['commoncrawl','commoncrawl_graph'],'2020-01-01','2026-09-28')")
    publish(client, ["commoncrawl_domains", "commoncrawl_domain_graph_nodes"])
    before = client.execute("SELECT domain_id,source_table,first_seen_at,last_seen_at FROM corpscout.domains_sources ORDER BY domain_id,source_table")
    # Rebuild the inventory without that currently active bulk source. Historical
    # discovery is still retained by the compact source publisher.
    client.execute("ALTER TABLE corpscout.domains UPDATE sources=[] WHERE root_domain='bulk.se' SETTINGS mutations_sync=2")
    publish(client, ["commoncrawl_domains"])
    assert client.execute("SELECT domain_id,source_table,first_seen_at,last_seen_at FROM corpscout.domains_sources ORDER BY domain_id,source_table") == before


def test_preparation_is_replayable_and_publisher_refuses_old_schema(database):
    client = database
    migration = (MIGRATIONS / "000467_corpscout_compact_domain_sources.up.sql").read_text()
    execute_sql(client, migration)
    copy_source_index(client, config=DomainSourcesBackfillConfig(execute=True), log=logging.info)
    before = client.execute("SELECT * FROM corpscout.domains_sources_next FINAL")
    execute_sql(client, migration)
    assert client.execute("SELECT * FROM corpscout.domains_sources_next FINAL") == before
    with pytest.raises(ValueError, match="cutover"):
        publish(client)


def test_backfill_cursor_bounds_all_contributions_for_each_domain(database):
    client = database
    execute_sql(client, (MIGRATIONS / "000467_corpscout_compact_domain_sources.up.sql").read_text())
    client.execute("INSERT INTO corpscout.domains (root_domain) VALUES", [(f"cursor-{i}.se",) for i in range(5)])
    client.execute("""INSERT INTO corpscout.domains_sources
        SELECT domain_id,source,'',source,'','','',0.,0,
            first_seen_at,last_seen_at,now64(6),'test'
        FROM corpscout.domains ARRAY JOIN ['commoncrawl_domains','commoncrawl_domain_graph_nodes'] AS source
        WHERE startsWith(root_domain,'cursor-')""")
    cursor = ""
    total_domains = total_contributions = 0
    while True:
        result = copy_source_index(client, config=DomainSourcesBackfillConfig(
            execute=True, page_size=2, max_domains=2, after_domain_id=cursor), log=logging.info)
        assert result["domains"] <= 2
        total_domains += result["domains"]
        total_contributions += result["contributions"]
        cursor = result["after_domain_id"]
        if result["complete"]:
            break
    assert (total_domains, total_contributions) == (6,11)
    assert client.execute("SELECT count() FROM corpscout.domains_sources_next FINAL") == [(11,)]


def test_lost_partition_publication_ack_can_be_replayed(compact_database, monkeypatch):
    client = compact_database
    insert_company(client, association())
    execute = Client.execute
    lost = False
    def lose_ack(self, query, *args, **kwargs):
        nonlocal lost
        result = execute(self, query, *args, **kwargs)
        if query.startswith("ALTER TABLE corpscout.domains_sources REPLACE") and not lost:
            lost = True
            raise RuntimeError("partition ack lost")
        return result
    with monkeypatch.context() as patch:
        patch.setattr(Client, "execute", lose_ack)
        with pytest.raises(RuntimeError, match="partition ack lost"):
            publish(client)
    published = client.execute("SELECT domain_id,source_table,first_seen_at,last_seen_at FROM corpscout.domains_sources ORDER BY domain_id")
    publish(client)
    assert client.execute("SELECT domain_id,source_table,first_seen_at,last_seen_at FROM corpscout.domains_sources ORDER BY domain_id") == published
