"""Real ClickHouse/PostgreSQL identity registration, replay and publication races."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Barrier

import psycopg2
import pytest
from clickhouse_driver import Client

from corpscout_identity.registration import (
    WebsiteObservation,
    identify_domain,
    identify_website,
    register_domains,
    register_websites,
)
from corpscout_identity.coordination import inventory_publication_lock
from tests.domain_sources_schema import central_schema_sql, execute_sql
from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)

STAMP = datetime(2026, 9, 28, tzinfo=UTC)


@pytest.fixture
def registry_db(graph_ch, identity_postgres):
    execute_sql(graph_ch, "DROP DATABASE corpscout SYNC; CREATE DATABASE corpscout;")
    execute_sql(graph_ch, central_schema_sql())
    return graph_ch


def observation(url, *, observed=None, fetched=None):
    return WebsiteObservation(identify_website(url), STAMP, observed, fetched)


def register(client, observations):
    return register_websites(
        client,
        observations,
        source="website_site_info_results",
        run_id="registration-test",
    )


def another_client(client):
    return Client(
        "127.0.0.1", port=client.connection.port, user="test", password="test"
    )


def test_identity_keeps_subdomains_schemes_ports_queries_and_private_suffixes():
    first = identify_website("HTTPS://WWW.Example.SE.:443/A?x=1#part")
    assert first.website_origin == "https://www.example.se"
    assert first.page_url == "https://www.example.se/A?x=1"
    sites = [
        identify_website(url)
        for url in (
            "https://www.example.se/",
            "http://www.example.se/",
            "https://www.example.se:8443/",
            "https://shop.example.se/",
        )
    ]
    assert len({site.website_id for site in sites}) == 4
    assert len({site.domain_id for site in sites}) == 1
    assert identify_domain("WWW.BÜCHER.DE.").root_domain == "xn--bcher-kva.de"
    assert (
        identify_website("https://shop.tenant.github.io/").root_domain
        == "tenant.github.io"
    )
    assert (
        identify_website("https://example.se/?").page_id
        != identify_website("https://example.se/").page_id
    )


@pytest.mark.parametrize(
    "url",
    [
        "example.se",
        "https://user:secret@example.se/",
        "ftp://example.se/",
        "http://127.0.0.1/",
        "http://[::1]/",
        "https://localhost/",
        "https://co.uk/",
        "https://bad..se/",
        "https://a_b.se/",
        "https://example.se/with space",
    ],
)
def test_invalid_inputs_do_not_produce_identities(url):
    with pytest.raises((ValueError, UnicodeError)):
        identify_website(url)


def test_batch_registers_all_parents_once_and_retains_observation_times(registry_db):
    client = registry_db
    rows = [
        observation("https://shop.example.se/", observed=STAMP - timedelta(days=1)),
        observation("https://shop.example.se/about", observed=STAMP, fetched=STAMP),
        observation("http://shop.example.se/"),
        observation("https://jobs.example.se/"),
    ]
    assert register(client, [*rows, rows[0]]) == {
        "domains_inserted": 1,
        "websites_inserted": 3,
        "pages_inserted": 4,
    }
    before = client.execute("SELECT * FROM corpscout.websites ORDER BY website_origin")
    assert register(client, list(reversed(rows))) == {
        "domains_inserted": 0,
        "websites_inserted": 0,
        "pages_inserted": 0,
    }
    assert (
        client.execute("SELECT * FROM corpscout.websites ORDER BY website_origin")
        == before
    )
    assert client.execute(
        "SELECT evidence_status,last_observed_at,last_successful_fetch_at FROM corpscout.websites WHERE website_origin='https://shop.example.se'"
    ) == [("observed", STAMP, STAMP)]
    assert client.execute(
        "SELECT evidence_status,last_successful_fetch_at FROM corpscout.websites WHERE website_origin='https://jobs.example.se'"
    ) == [("assumed", None)]
    assert client.execute(
        "SELECT count() FROM corpscout.pages p INNER JOIN corpscout.websites w ON p.website_id=w.website_id INNER JOIN corpscout.domains d ON w.domain_id=d.domain_id"
    ) == [(4,)]
    assert client.execute("SELECT count() FROM corpscout.domains_sources") == [(0,)]


def test_domain_only_registration_does_not_invent_websites(registry_db):
    client = registry_db
    item = identify_domain("www.example.se")
    args = dict(source="se_company_domain", discovered_at=STAMP, run_id="test")
    assert register_domains(client, [item, item], **args) == {"domains_inserted": 1}
    assert register_domains(client, [item], **args) == {"domains_inserted": 0}
    assert client.execute("SELECT count() FROM corpscout.websites") == [(0,)]
    assert client.execute("SELECT count() FROM corpscout.domains_sources") == [(0,)]


def test_entire_batch_is_validated_before_first_insert(registry_db):
    good = observation("https://example.se/")
    invalid = replace(good, identity=replace(good.identity, domain_id="not-the-parent"))
    with pytest.raises(ValueError, match="does not match"):
        register(registry_db, [good, invalid])
    with pytest.raises(ValueError, match="timezone-aware"):
        register(registry_db, [replace(good, discovered_at=STAMP.replace(tzinfo=None))])
    with pytest.raises(ValueError, match="matching observation"):
        register(registry_db, [replace(good, fetched_at=STAMP)])
    assert registry_db.execute("SELECT count() FROM corpscout.domains") == [(0,)]


def test_corrupt_parent_reference_fails_without_writing_other_identities(registry_db):
    client = registry_db
    # This satisfies the historical table's suffix check but is not an eTLD+1.
    client.execute(
        "INSERT INTO corpscout.websites (root_domain,website_origin,sources) VALUES ('sub.example.se','https://shop.sub.example.se',['old'])"
    )
    with pytest.raises(ValueError, match="Mismatched central websites"):
        register(
            client,
            [
                observation("https://good.se/"),
                observation("https://shop.sub.example.se/"),
            ],
        )
    assert client.execute("SELECT count() FROM corpscout.domains") == [(0,)]
    assert client.execute("SELECT count() FROM corpscout.pages") == [(0,)]


def test_lost_insert_acknowledgement_reuses_durable_parents(registry_db, monkeypatch):
    client = registry_db
    original = Client.execute
    failed = False

    def lose_ack(self, query, *args, **kwargs):
        nonlocal failed
        result = original(self, query, *args, **kwargs)
        if query.startswith("INSERT INTO corpscout.websites ") and not failed:
            failed = True
            raise RuntimeError("acknowledgement lost after insert")
        return result

    monkeypatch.setattr(Client, "execute", lose_ack)
    batch = [observation("https://example.se/")]
    with pytest.raises(RuntimeError, match="acknowledgement lost"):
        register(client, batch)
    assert register(client, batch) == {
        "domains_inserted": 0,
        "websites_inserted": 0,
        "pages_inserted": 1,
    }
    for table in ("domains", "websites", "pages"):
        assert client.execute(f"SELECT count() FROM corpscout.{table}") == [(1,)]


def test_concurrent_workers_register_one_logical_parent(registry_db):
    ready = Barrier(2)

    def worker():
        client = another_client(registry_db)
        try:
            ready.wait(timeout=10)
            return register(client, [observation("https://example.se/")])
        finally:
            client.disconnect()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker), pool.submit(worker)]
        counts = [future.result(timeout=30) for future in futures]
    assert sum(row["domains_inserted"] for row in counts) == 1
    assert sum(row["websites_inserted"] for row in counts) == 1
    assert registry_db.execute("SELECT count() FROM corpscout.pages") == [(1,)]


def test_lock_timeout_does_not_publish_and_retry_succeeds(registry_db, monkeypatch):
    monkeypatch.setenv("INVENTORY_LOCK_TIMEOUT_MS", "50")

    def worker():
        client = another_client(registry_db)
        try:
            return register(client, [observation("https://example.se/")])
        finally:
            client.disconnect()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with inventory_publication_lock():
            pending = pool.submit(worker)
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                pending.result(timeout=10)
            assert registry_db.execute("SELECT count() FROM corpscout.domains") == [
                (0,)
            ]
        assert pool.submit(worker).result(timeout=10)["pages_inserted"] == 1


def test_missing_coordination_configuration_fails_closed(registry_db, monkeypatch):
    monkeypatch.delenv("PROCESSING_PG_URL")
    with pytest.raises(ValueError, match="PROCESSING_PG_URL is required"):
        register(registry_db, [observation("https://example.se/")])
    assert registry_db.execute("SELECT count() FROM corpscout.domains") == [(0,)]


def test_private_suffix_provider_homepages_are_valid_without_merging_tenants():
    for host in ("123minsida.se", "com.se", "iopsys.se", "itcouldbewor.se", "myspreadshop.se"):
        assert identify_website(f"https://{host}/").root_domain == host
        assert identify_website(f"https://tenant.{host}/").root_domain == f"tenant.{host}"
    with pytest.raises(ValueError):
        identify_website("https://co.uk/")
