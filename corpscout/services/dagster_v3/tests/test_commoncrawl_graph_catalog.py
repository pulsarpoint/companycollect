"""Official-index discovery through HTTP and transactional catalog persistence."""

import json
import threading
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import psycopg2
import dagster as dg
from dlt.sources.helpers import requests

from dagster_v3.defs.commoncrawl_domain_graph.catalog import (
    GraphRelease,
    ReleaseFile,
    crawl_coverage,
    discover_releases,
    trusted_release_url,
)
from dagster_v3.defs.commoncrawl_domain_graph.source import validate_graph_release
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphStore
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource
from dagster_v3.defs.commoncrawl_domain_graph import discovery
from dagster_v3.defs.commoncrawl_domain_graph.assets import PARTITIONS
from tests.test_commoncrawl_graph_migrations import catalog_db as catalog_db
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)


@pytest.fixture
def catalog_http():
    responses = {}
    hits = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.reply(True)

        def do_HEAD(self):
            self.reply(False)

        def reply(self, body):
            hits.append((self.command, self.path))
            if self.path not in responses:
                self.send_error(404)
                return
            content = responses[self.path]
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("ETag", '"source-etag"')
            self.end_headers()
            if body:
                self.wfile.write(content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    release = "cc-main-2025-26-dec-jan-feb"
    entry = {
        "id": release,
        "index": f"{base}/{release}/index.html",
        "crawls": ["CC-MAIN-2025-51", "CC-MAIN-2026-05"],
        "stats": {"domain": {"nodes": 3, "arcs": 4}},
    }
    responses["/graphs.json"] = json.dumps([entry]).encode()
    responses["/crawls.json"] = json.dumps(
        [
            {
                "id": "CC-MAIN-2025-51",
                "from": "2025-12-01T12:00:00",
                "to": "2025-12-10T12:00:00",
            },
            {
                "id": "CC-MAIN-2026-05",
                "from": "2026-02-01T12:00:00",
                "to": "2026-02-10T12:00:00",
            },
        ]
    ).encode()
    links = []
    for kind in ("vertices", "edges", "ranks"):
        name = f"{release}-domain-{kind}.txt.gz"
        links.append(f'<a href="domain/{name}">file</a>')
        responses[f"/{release}/domain/{name}"] = b"not downloaded by discovery"
    responses[f"/{release}/index.html"] = "\n".join(links).encode()
    try:
        yield base, release, entry, responses, hits
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def discover_fixture(base):
    with requests.Session() as session:
        return discover_releases(
            session, f"{base}/graphs.json", f"{base}/crawls.json", base
        )


def test_discovery_uses_coverage_and_only_heads_bulk_files(catalog_http):
    base, release, _, _, hits = catalog_http
    results = discover_fixture(base)
    assert len(results) == 1
    assert results[0].graph_release == release
    assert (results[0].coverage_start, results[0].coverage_end) == (
        date(2025, 12, 1),
        date(2026, 2, 10),
    )
    assert [
        (file.artifact_kind, file.expected_rows, file.availability)
        for file in results[0].files
    ] == [
        ("nodes", 3, "available"),
        ("edges", 4, "available"),
        ("ranks", 3, "available"),
    ]
    assert all(method == "HEAD" for method, path in hits if path.endswith(".gz"))


def test_missing_rank_link_does_not_hide_available_graph(catalog_http):
    base, release, _, responses, _ = catalog_http
    responses[f"/{release}/index.html"] = (
        f'<a href="domain/{release}-domain-edges.txt.gz">edges</a>'.encode()
    )
    files = discover_fixture(base)[0].files
    assert [file.availability for file in files] == [
        "unavailable",
        "available",
        "unavailable",
    ]


def test_listed_but_not_yet_uploaded_file_is_unavailable(catalog_http):
    base, release, _, responses, _ = catalog_http
    del responses[f"/{release}/domain/{release}-domain-ranks.txt.gz"]
    assert discover_fixture(base)[0].files[-1].availability == "unavailable"


def test_host_only_release_is_cataloged_without_domain_downloads(catalog_http):
    base, _, entry, responses, hits = catalog_http
    entry["stats"]["domain"] = {"nodes": 0, "arcs": 0}
    responses["/graphs.json"] = json.dumps([entry]).encode()
    assert all(
        file.availability == "unsupported" for file in discover_fixture(base)[0].files
    )
    assert len(hits) == 2


def test_untrusted_download_link_is_rejected_before_request(catalog_http):
    base, release, _, responses, hits = catalog_http
    responses[f"/{release}/index.html"] = (
        b'<a href="http://untrusted.invalid/file-domain-ranks.txt.gz">ranks</a>'
    )
    with pytest.raises(ValueError, match="outside"):
        discover_fixture(base)
    assert not any(method == "HEAD" for method, _ in hits)


def test_legacy_official_http_links_are_upgraded_to_https():
    release = "cc-main-2018-jan"
    url = f"http://data.commoncrawl.org/projects/hyperlinkgraph/{release}/domain/{release}-domain-ranks.txt.gz"
    assert trusted_release_url(url, release) == url.replace("http:", "https:", 1)
    with pytest.raises(ValueError, match="outside"):
        trusted_release_url(
            url.replace("data.commoncrawl.org", "untrusted.invalid"), release
        )


def test_empty_or_duplicate_catalog_fails(catalog_http):
    base, _, entry, responses, _ = catalog_http
    for entries in ([], [entry, entry]):
        responses["/graphs.json"] = json.dumps(entries).encode()
        with pytest.raises(ValueError):
            discover_fixture(base)


@pytest.mark.parametrize(
    "release",
    [
        "cc-main-2025-26-dec-jan-feb",
        "cc-main-2017-feb-mar-apr-hostgraph",
        "cc-main-2018-jan",
    ],
)
def test_historical_release_ids_are_supported(release):
    assert validate_graph_release(release) == release


@pytest.mark.parametrize(
    "path", ["../other/file.gz", "%2e%2e/file.gz", "file.gz?x=1", "file.gz#part"]
)
def test_url_validation_rejects_escape_and_modifiers(path):
    with pytest.raises(ValueError):
        trusted_release_url(
            f"https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-jan/{path}",
            "cc-main-2026-jan",
        )


def test_unknown_coverage_remains_unknown():
    assert crawl_coverage(["missing"], {}) == (None, None)


def test_catalog_refresh_preserves_baseline_retention_and_last_success(catalog_db):
    connection, _ = catalog_db
    store = GraphStore(connection)
    first = datetime(2026, 9, 25, tzinfo=UTC)
    release = GraphRelease(
        "cc-main-2026-jun-jul-aug",
        "https://example.test/index.html",
        [],
        None,
        None,
        [
            ReleaseFile(
                "ranks", "https://example.test/ranks.gz", '"a"', 123, 3, "available"
            )
        ],
    )
    store.record_discovery_attempt(first)
    store.save_catalog([release], first)
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_releases SET graph_status='retired', graph_retired_at=now() WHERE graph_release=%s",
            (release.graph_release,),
        )
    second = first + timedelta(days=1)
    store.record_discovery_attempt(second)
    store.save_catalog(
        [replace(release, files=[replace(release.files[0], source_etag='"b"')])], second
    )
    store.record_discovery_attempt(second + timedelta(days=1))
    store.record_discovery_failure("HTTPError")
    with store.transaction() as cursor:
        cursor.execute("SELECT * FROM commoncrawl_graph_state")
        state = cursor.fetchone()
        assert state["discovery_baseline_at"] == first
        assert state["discovery_succeeded_at"] == second
        assert state["discovery_error"] == "HTTPError"
        cursor.execute(
            "SELECT * FROM commoncrawl_graph_releases WHERE graph_release=%s",
            (release.graph_release,),
        )
        stored = cursor.fetchone()
        assert stored["discovered_at"] == first
        assert stored["graph_status"] == "retired"
        cursor.execute("SELECT count(*) FROM commoncrawl_graph_release_files")
        assert cursor.fetchone()["count"] == 1


def test_invalid_refresh_rolls_back_all_catalog_writes(catalog_db):
    connection, _ = catalog_db
    store = GraphStore(connection)
    release = GraphRelease(
        "cc-main-2026-jun-jul-aug",
        "https://example.test/index.html",
        [],
        None,
        None,
        [],
    )
    with pytest.raises(psycopg2.errors.CheckViolation):
        store.save_catalog(
            [release, replace(release, graph_release="invalid")], datetime.now(UTC)
        )
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT count(*) FROM commoncrawl_graph_releases WHERE graph_release=%s",
            (release.graph_release,),
        )
        assert cursor.fetchone()["count"] == 0


def test_discovery_asset_registers_partitions_without_importing(
    catalog_db, catalog_http, monkeypatch
):
    connection, dsn = catalog_db
    base, release, _, _, _ = catalog_http
    monkeypatch.setattr(
        discovery,
        "discover_releases",
        lambda session: discover_releases(
            session, f"{base}/graphs.json", f"{base}/crawls.json", base
        ),
    )
    with dg.DagsterInstance.ephemeral() as instance:
        result = dg.materialize(
            [discovery.commoncrawl_graph_release_catalog],
            instance=instance,
            resources={"graph_catalog": GraphCatalogResource(postgres_url=dsn)},
        )
        assert result.success
        assert instance.get_dynamic_partitions(PARTITIONS.name) == [release]
    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM commoncrawl_graph_import_requests")
        assert cursor.fetchone() == (0,)
