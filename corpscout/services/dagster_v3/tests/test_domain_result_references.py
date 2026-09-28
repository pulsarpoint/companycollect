"""Prepared website references and country evidence against disposable ClickHouse."""

import json
import re
import threading
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import UUID

import pytest
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

from dagster_v3.defs.website_crawl.normalization.load import (
    assert_schema,
    publish_attempt,
)
from dagster_v3.defs.website_crawl.normalization.tables import COLUMNS
from tests.identity_registration_support import identity_postgres as identity_postgres
from tests.test_processing_store import processing_postgres_url as processing_postgres_url
from tests.domain_sources_schema import MIGRATIONS, central_schema_sql, execute_sql
from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_se_company_domain_clickhouse_local import setup_sql
from tests.test_website_crawl_normalization import payload as payload, source as source

MIGRATION = "000466_corpscout_domain_result_references"
RESULT_TABLES = tuple(
    f"website_{kind}_results" for kind in ("site_info", "full_crawl", "jobs_crawl")
)
LOOKUP_TABLES = tuple(
    f"website_company_lookup_{kind}"
    for kind in ("results", "candidates", "evidence", "searches")
)
NORMALIZED_TABLES = tuple(f"website_crawl_{kind}" for kind in COLUMNS)
REQUEST_TABLES = tuple(
    f"website_{kind}_requests" for kind in ("site_info", "full_crawl", "jobs_crawl")
)
REFERENCE_TABLES = (
    *RESULT_TABLES,
    *LOOKUP_TABLES,
    *NORMALIZED_TABLES,
    *REQUEST_TABLES,
    "website_crawl_submissions",
    "website_crawl_task_domains",
    "website_crawl_results",
)
STAMP = datetime(2026, 9, 28, tzinfo=UTC)
NORMALIZATION_ID = UUID("00000000-0000-0000-0000-000000000001")


def migrate(client: Client) -> None:
    sql = (MIGRATIONS / f"{MIGRATION}.up.sql").read_text()
    for statement in sql.split(";"):
        if statement.strip():
            try:
                client.execute(statement)
            except Exception as error:
                error.add_note(f"Migration statement: {statement[:400]}")
                raise


def insert(client: Client, table: str, row: dict) -> None:
    client.execute(f"INSERT INTO corpscout.{table} ({','.join(row)}) VALUES", [row])


@pytest.fixture(scope="module")
def archive_url():
    class EmptyBucket(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>archive</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>'
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("0.0.0.0", 0), EmptyBucket)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://host.docker.internal:{http.server_port}/archive/"
    finally:
        http.shutdown()
        http.server_close()
        thread.join()


@pytest.fixture
def previous_schema(graph_ch: Client, archive_url: str) -> Client:
    execute_sql(graph_ch, "DROP DATABASE corpscout SYNC; CREATE DATABASE corpscout;")
    execute_sql(graph_ch, setup_sql())
    execute_sql(graph_ch, central_schema_sql())
    for name in (
        "000462_corpscout_domains_sources",
        "000463_corpscout_domain_source_readers",
        "000429_corpscout_website_crawl_requests",
        "000430_corpscout_website_crawl_type_results",
        "000431_corpscout_website_crawl_task_domains",
        "000448_corpscout_crawl_queue_contract",
        "000452_corpscout_website_crawl_normalized",
        "000459_corpscout_website_company_lookup_results",
    ):
        execute_sql(graph_ch, (MIGRATIONS / f"{name}.up.sql").read_text())
    legacy = (MIGRATIONS / "000420_corpscout_website_crawl_results.up.sql").read_text()
    execute_sql(graph_ch, legacy.split("-- Provision the company_crawl_results")[0])
    # View creation can inspect the archive. Keep that traffic on an empty local bucket.
    graph_ch.execute(
        "CREATE NAMED COLLECTION IF NOT EXISTS company_crawl_results AS url=%(url)s, access_key_id='test', secret_access_key='test'",
        {"url": archive_url},
    )
    return graph_ch


def old_row(table: str, host: str, request: str) -> dict:
    row = dict(domain=host, request_id=request, attempt=1)
    if table in RESULT_TABLES:
        row.update(
            website_url=f"https://{host}",
            state="failed",
            crawl_status="failed",
            error="timeout",
            finished_at=STAMP,
        )
    elif table in LOOKUP_TABLES:
        row.update(country="SE", finished_at=STAMP)
        if table == "website_company_lookup_results":
            row.update(status="failed")
    elif table in NORMALIZED_TABLES:
        row.update(crawl_type="full", normalization_id=NORMALIZATION_ID)
        if table == "website_crawl_scans":
            row.update(normalization_revision=1, parser_version="1", finished_at=STAMP)
        else:
            row.update(page_id="p0001")
        if table == "website_crawl_structured_data":
            row.update(value_type="null")
    elif table in REQUEST_TABLES:
        row = dict(
            domain=host,
            website_url=f"https://{host}",
            created_at=STAMP,
            updated_at=STAMP,
            revision=1,
        )
    elif table == "website_crawl_submissions":
        row = dict(
            domain=host, request_id=request, crawl_type="full", request_json="{}"
        )
    elif table == "website_crawl_task_domains":
        row = dict(
            domain=host,
            website_url=f"https://{host}",
            task_id=request,
            crawl_type="full",
            source_name="test",
            submission_id=request,
        )
    elif table == "website_crawl_results":
        row = dict(
            domain=host,
            request_id=request,
            result_id=sha256(request.encode()).hexdigest(),
            result_kind="error",
            metadata="{}",
        )
    return row


def seed_website(client: Client, root: str, origin: str) -> tuple[str, str]:
    """Use the real inventory schema; this is fixture data, not publisher validation."""
    if not client.execute(
        "SELECT domain_id FROM corpscout.domains WHERE root_domain=%(root)s",
        {"root": root},
    ):
        insert(client, "domains", dict(root_domain=root))
    insert(
        client,
        "websites",
        dict(
            root_domain=root,
            website_origin=origin,
            sources=["test"],
            first_seen_at=STAMP,
            last_seen_at=STAMP,
        ),
    )
    return client.execute(
        "SELECT website_id,domain_id FROM corpscout.websites WHERE website_origin=%(origin)s",
        {"origin": origin},
    )[0]


def test_required_website_references_preserve_old_data_and_keys(
    previous_schema: Client,
) -> None:
    client = previous_schema
    with pytest.raises(ValueError, match="schema mismatch"):
        assert_schema(client)
    for table in REFERENCE_TABLES:
        insert(client, table, old_row(table, "www.old.se", "old"))
    keys = client.execute(
        "SELECT name, sorting_key FROM system.tables WHERE database='corpscout' AND name IN %(tables)s ORDER BY name",
        {"tables": REFERENCE_TABLES},
    )
    migrate(client)
    migrate(client)
    assert (
        client.execute(
            "SELECT name, sorting_key FROM system.tables WHERE database='corpscout' AND name IN %(tables)s ORDER BY name",
            {"tables": REFERENCE_TABLES},
        )
        == [(name, key + ", website_id" if name in (*REQUEST_TABLES, "website_crawl_task_domains") else key) for name, key in keys]
    )
    website_id, domain_id = seed_website(client, "example.se", "https://www.example.se")
    for table in REFERENCE_TABLES:
        columns = {r[0] for r in client.execute(f"DESCRIBE TABLE corpscout.{table}")}
        assert "website_id" in columns
        assert not {"root_domain", "domain_id"} & columns
        # Old parts are preserved for controlled backfill, not accepted as new writes.
        assert client.execute(f"SELECT website_id FROM corpscout.{table}") == [("",)]
        for extra in ({}, {"website_id": ""}):
            with pytest.raises(ServerException, match="required_website_reference"):
                insert(
                    client, table, {**old_row(table, "missing.se", "missing"), **extra}
                )
        insert(
            client,
            table,
            {**old_row(table, "www.example.se", "new"), "website_id": website_id},
        )
        assert client.execute(
            f"SELECT w.domain_id FROM corpscout.{table} AS r INNER JOIN corpscout.websites AS w ON r.website_id=w.website_id WHERE r.domain='www.example.se'"
        ) == [(domain_id,)]
        assert client.execute(f"SELECT count() FROM corpscout.{table}") == [(2,)]
    assert client.execute(
        "SELECT DISTINCT type,default_kind,default_expression FROM system.columns WHERE database='corpscout' AND table IN %(tables)s AND name='website_id'",
        {"tables": REFERENCE_TABLES},
    ) == [("String", "", "")]
    assert client.execute(
        "SELECT root_domain,review_status FROM corpscout.se_company_domain_resolved"
    ) == [("legacy.se", "confirmed_related")]


def test_subdomains_and_schemes_keep_distinct_website_references(
    previous_schema: Client,
) -> None:
    client = previous_schema
    migrate(client)
    origins = (
        ("shop.example.se", "https://shop.example.se"),
        ("careers.example.se", "https://careers.example.se"),
        ("shop.example.se", "http://shop.example.se"),
    )
    for index, (host, origin) in enumerate(origins):
        website_id, _ = seed_website(client, "example.se", origin)
        row = old_row("website_site_info_results", host, f"site-{index}")
        insert(
            client,
            "website_site_info_results",
            dict(row, website_id=website_id, website_url=origin),
        )
    assert client.execute(
        "SELECT uniqExact(r.website_id),uniqExact(w.domain_id),count() FROM corpscout.website_site_info_results r INNER JOIN corpscout.websites w ON r.website_id=w.website_id"
    ) == [(3, 1, 3)]


def test_views_expose_website_ids_without_changing_attempt_selection(
    previous_schema: Client,
) -> None:
    client = previous_schema
    migrate(client)
    views = client.execute(
        "SELECT name FROM system.tables WHERE database='corpscout' AND engine='View' AND (name LIKE 'website_crawl_%' OR name LIKE 'website_company_lookup_%' OR name LIKE 'website_site_info_%' OR name LIKE 'website_full_crawl_%' OR name LIKE 'website_jobs_crawl_%')"
    )
    for (view,) in views:
        columns = {
            name for name, *_ in client.execute(f"DESCRIBE TABLE corpscout.{view}")
        }
        assert "website_id" in columns, view
        assert not {"root_domain", "domain_id"} & columns, view
    website_id, _ = seed_website(client, "example.se", "https://www.example.se")
    for table in RESULT_TABLES:
        base = {**old_row(table, "www.example.se", "test"), "website_id": website_id}
        insert(
            client,
            table,
            dict(
                base,
                attempt=1,
                successful=True,
                state="completed",
                crawl_status="finished",
                error="",
            ),
        )
        insert(client, table, dict(base, attempt=2))
        assert client.execute(
            f"SELECT attempt,website_id FROM corpscout.{table}_latest"
        ) == [(2, website_id)]
        assert client.execute(
            f"SELECT attempt FROM corpscout.{table}_latest_success"
        ) == [(1,)]
    matched = dict(
        country="SE",
        domain="www.example.se",
        website_id=website_id,
        request_id="lookup",
        attempt=1,
        status="matched",
        found=True,
        company_id="5561234567",
        confidence=0.9,
        finished_at=STAMP,
    )
    insert(client, "website_company_lookup_results", matched)
    assert client.execute(
        "SELECT company_id,website_id FROM corpscout.website_company_lookup_proposals"
    ) == [("5561234567", website_id)]
    insert(
        client,
        "website_company_lookup_results",
        dict(
            matched,
            attempt=2,
            status="failed",
            found=False,
            company_id="",
            confidence=None,
        ),
    )
    assert client.execute(
        "SELECT count() FROM corpscout.website_company_lookup_proposals"
    ) == [(0,)]
    assert client.execute(
        "SELECT count() FROM corpscout.website_company_lookup_results"
    ) == [(2,)]


def test_normalizer_requires_reference_schema_and_publishes_registered_pages(
    previous_schema: Client, source: dict, payload: dict, identity_postgres,
) -> None:
    client = previous_schema
    with pytest.raises(ValueError, match="schema mismatch"):
        assert_schema(client)
    migrate(client)
    assert_schema(client)
    counts = publish_attempt(client, source, payload, "after-migration")
    assert counts["jobs"] == 1
    for kind in COLUMNS:
        assert client.execute(f"SELECT DISTINCT website_id FROM corpscout.website_crawl_{kind}") in ([], [(source["website_id"],)])
    assert client.execute("""SELECT count() FROM corpscout.website_crawl_pages r
        INNER JOIN corpscout.pages p ON r.resource_page_id=p.page_id
        INNER JOIN corpscout.websites w ON p.website_id=w.website_id
        INNER JOIN corpscout.domains d ON w.domain_id=d.domain_id""") == [(counts["pages"],)]


def test_source_claims_support_multiple_domains_updates_and_withdrawals(
    previous_schema: Client,
) -> None:
    client = previous_schema
    migrate(client)
    first_website, first_domain = seed_website(
        client, "first.se", "https://www.first.se"
    )
    _, second_domain = seed_website(client, "second.se", "https://www.second.se")
    base = dict(
        company_id="5561234567",
        source="brave",
        slot="first",
        domain_id=first_domain,
        website_id=first_website,
        association="uncertain",
        confidence=0.5,
        source_record_id="one-brave-result",
        evidence="answer",
        observed_at=STAMP,
        suggested_at=STAMP,
        suggestion_id="first-claim",
    )
    for omitted in (None, ""):
        invalid = dict(base)
        if omitted is None:
            invalid.pop("domain_id")
        else:
            invalid["domain_id"] = omitted
        with pytest.raises(ServerException, match="required_domain_reference"):
            insert(client, "se_company_domain_sources", invalid)
    with pytest.raises(ServerException, match="valid_website_reference"):
        insert(client, "se_company_domain_sources", dict(base, website_id=""))
    insert(client, "se_company_domain_sources", base)
    # A domain-only claim is valid; do not invent a website or scheme for it.
    insert(
        client,
        "se_company_domain_sources",
        dict(
            base,
            slot="second",
            domain_id=second_domain,
            website_id=None,
            suggestion_id="second-claim",
        ),
    )
    insert(
        client,
        "se_company_domain_sources",
        dict(base, source="esef", source_record_id="filing", confidence=0.8),
    )
    assert client.execute(
        "SELECT d.root_domain,s.source_record_id FROM corpscout.se_company_domain_sources AS s FINAL INNER JOIN corpscout.domains AS d ON s.domain_id=d.domain_id WHERE s.source='brave' ORDER BY d.root_domain"
    ) == [("first.se", "one-brave-result"), ("second.se", "one-brave-result")]
    # A source can revise its slot to another domain; the old candidate is replaced.
    insert(
        client,
        "se_company_domain_sources",
        dict(
            base,
            domain_id=second_domain,
            website_id=None,
            suggested_at=STAMP + timedelta(seconds=1),
        ),
    )
    insert(
        client,
        "se_company_domain_sources",
        dict(
            base,
            domain_id=second_domain,
            website_id=None,
            removed=1,
            suggested_at=STAMP + timedelta(seconds=2),
        ),
    )
    assert client.execute(
        "SELECT source,slot,domain_id FROM corpscout.se_company_domain_sources FINAL WHERE removed=0 ORDER BY source,slot"
    ) == [("brave", "second", second_domain), ("esef", "first", first_domain)]
    assert client.execute("SELECT count() FROM corpscout.se_company_domain") == [(1,)]
    assert client.execute(
        "SELECT count() FROM corpscout.se_company_domain_suggestion"
    ) == [(0,)]
    assert "root_domain" not in {
        r[0]
        for r in client.execute("DESCRIBE TABLE corpscout.se_company_domain_sources")
    }


def test_preparation_does_not_change_the_deployed_source_index_or_filter(
    previous_schema: Client,
) -> None:
    client = previous_schema
    tables = (
        "domains_sources",
        "domains_company_filter",
        "se_company_domain",
        "se_company_domain_suggestion",
    )
    before = {
        table: client.execute(f"SHOW CREATE TABLE corpscout.{table}")
        for table in tables
    }
    rows = client.execute("SELECT * FROM corpscout.domains_sources FINAL")
    counts = client.execute(
        "SELECT root_domain,company_count FROM corpscout.domains_company_filter"
    )
    migrate(client)
    assert {
        table: client.execute(f"SHOW CREATE TABLE corpscout.{table}")
        for table in tables
    } == before
    assert client.execute("SELECT * FROM corpscout.domains_sources FINAL") == rows
    assert (
        client.execute(
            "SELECT root_domain,company_count FROM corpscout.domains_company_filter"
        )
        == counts
    )
    grants = " ".join(
        row[0]
        for row in client.execute("SHOW GRANTS FOR corpscout_domain_reference_writer")
    )
    assert "domains_sources" not in grants
    assert "corpscout.websites" in grants


def test_archive_reads_explicit_website_id_without_guessing_from_root(
    previous_schema: Client,
) -> None:
    client = previous_schema
    migrate(client)
    ddl = (MIGRATIONS / f"{MIGRATION}.up.sql").read_text(encoding="utf-8")
    archive = re.search(
        r"CREATE OR REPLACE VIEW corpscout.website_crawl_results_s3_archive SQL SECURITY INVOKER AS\n(.*?);",
        ddl,
        re.S,
    )[1]
    archive = re.sub(
        r"FROM s3\(.*?\)\nSETTINGS",
        "FROM (SELECT %(payload)s AS result_json, '/test/attempts/0001/result.json.gz' AS _path, 'result.json.gz' AS _file, 1 AS _size, now() AS _time)\nSETTINGS",
        archive,
        flags=re.S,
    )
    website_id, _ = seed_website(client, "example.co.uk", "https://shop.example.co.uk")
    for explicit_id in (None, website_id):
        payload = {
            "request_id": "test",
            "input_url": "https://shop.example.co.uk",
            "root_domain": "example.co.uk",
        }
        if explicit_id is not None:
            payload["website_id"] = explicit_id
        rows = client.execute(
            f"SELECT domain,website_id,request_id,attempt FROM ({archive})",
            {"payload": json.dumps(payload)},
        )
        assert rows == [("shop.example.co.uk", explicit_id or "", "test", 1)]
