"""Real gzip HTTP → ClickHouse, atomic publication and adjacency semantics."""

import gzip
import hashlib
import logging
import shutil
import subprocess
import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

import pytest
from clickhouse_driver import Client

from dagster_v3.defs.commoncrawl_domain_graph import source as source_module
from dagster_v3.defs.commoncrawl_domain_graph.load import (
    load_graph_file,
    publish_snapshot,
)

MIGRATION = (
    Path(__file__).parents[3]
    / "clickhouse/migrations/000418_corpscout_commoncrawl_domain_graph.up.sql"
)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def graph_http():
    release = "cc-main-2026-jun-jul-aug"
    prefix = f"/{release}"
    vertices = f"{release}-domain-vertices.txt.gz"
    edges = f"{release}-domain-edges.txt.gz"
    bodies = {
        f"{prefix}/index.html": f'<a href="domain/{vertices}">vertices</a><a href="domain/{edges}">edges</a>'.encode(),
        f"{prefix}/domain/{release}-domain.stats": b"nodes=6\narcs=8\nloops=1\n",
        f"{prefix}/domain/{vertices}": gzip.compress(
            b"0\tcom.original\t2\n1\tse.related\t4\n2\tcom.oneway\t1\n3\tcom.cycle\t1\n4\tcom.return\t1\n5\tcom.inbound\t1\n"
        ),
        f"{prefix}/domain/{edges}": gzip.compress(
            b"0\t1\n1\t0\n0\t0\n0\t2\n0\t3\n3\t4\n4\t0\n5\t0\n"
        ),
    }
    gets = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.reply(False)

        def do_GET(self):
            gets.append(self.path)
            self.reply(True)

        def reply(self, include_body):
            body = bodies.get(self.path)
            if body is None:
                self.send_error(404)
                return
            etag = '"' + hashlib.sha256(body).hexdigest() + '"'
            if self.headers.get("If-Match", etag) != etag:
                self.send_error(412)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", etag)
            self.end_headers()
            if include_body:
                self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield release, server.server_port, gets
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="module")
def graph_ch(tmp_path_factory):
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for the ClickHouse integration test")
    name = "commoncrawl-graph-test-" + uuid4().hex[:12]
    users = tmp_path_factory.mktemp("graph-clickhouse") / "named-collections.xml"
    users.write_text(
        "<clickhouse><users><test><named_collection_control>1</named_collection_control></test></users></clickhouse>"
    )
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-v",
            f"{users}:/etc/clickhouse-server/users.d/named-collections.xml:ro",
            "--add-host",
            "host.docker.internal:host-gateway",
            "-p",
            "127.0.0.1::9000",
            "-e",
            "CLICKHOUSE_USER=test",
            "-e",
            "CLICKHOUSE_PASSWORD=test",
            "-e",
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
            "clickhouse/clickhouse-server:26.5",
        ],
        check=True,
        capture_output=True,
    )
    client = None
    try:
        port = int(
            subprocess.check_output(["docker", "port", name, "9000"], text=True)
            .strip()
            .rsplit(":", 1)[1]
        )
        client = Client(
            "127.0.0.1",
            port=port,
            user="test",
            password="test",
            send_receive_timeout=30,
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                client.execute("SELECT 1")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        for statement in MIGRATION.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
        index_migration = (
            MIGRATION.parent / "000419_corpscout_domain_graph_lookup_index.up.sql"
        )
        for statement in index_migration.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
        yield client
    finally:
        if client is not None:
            client.disconnect()
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


def test_bulk_graph_queries_and_retry_publication(graph_ch, graph_http, monkeypatch):
    client = graph_ch
    release, port, gets = graph_http
    monkeypatch.setattr(source_module, "BASE_URL", f"http://127.0.0.1:{port}")
    source = source_module.resolve_graph_source(release)
    source = replace(
        source,
        vertices_url=source.vertices_url.replace("127.0.0.1", "host.docker.internal"),
        edges_url=source.edges_url.replace("127.0.0.1", "host.docker.internal"),
    )
    assert load_graph_file(client, source, "nodes", "first", LOG) == (6, False)
    # No partial release is queryable, and publication requires both files.
    query = "SELECT connected_domain,outgoing,incoming,reciprocal FROM corpscout.commoncrawl_domain_connections(graph_release=%(release)s,domain=%(domain)s) ORDER BY connected_domain SETTINGS use_query_condition_cache=0"
    params = {"release": release, "domain": "original.com"}
    assert client.execute(query, params) == []
    with pytest.raises(ValueError, match="validation"):
        publish_snapshot(client, source, "incomplete")
    assert load_graph_file(client, source, "edges", "first", LOG) == (8, False)
    assert publish_snapshot(client, source, "first") is False
    assert client.execute(query, params) == [
        ("cycle.com", 1, 0, 0),
        ("inbound.com", 0, 1, 0),
        ("oneway.com", 1, 0, 0),
        ("related.se", 1, 1, 1),
        ("return.com", 0, 1, 0),
    ]
    assert client.execute(query, {**params, "domain": "absent.com"}) == []
    assert (
        client.execute(query, {**params, "release": "cc-main-2025-jun-jul-aug"}) == []
    )
    downloads = len(gets)
    assert load_graph_file(client, source, "nodes", "retry", LOG) == (6, True)
    assert load_graph_file(client, source, "edges", "retry", LOG) == (8, True)
    assert publish_snapshot(client, source, "retry") is True
    assert len(gets) == downloads
    assert (
        client.execute(
            "SELECT count() FROM corpscout.commoncrawl_domain_graph_snapshots"
        )[0][0]
        == 1
    )
    with pytest.raises(ValueError, match="changed upstream"):
        load_graph_file(
            client, replace(source, vertices_etag='"changed"'), "nodes", "changed", LOG
        )

    # A bad new release never replaces the old partition or publishes partial data.
    broken = replace(source, graph_release="cc-main-2026-mar-apr-may", edges=9)
    with pytest.raises(ValueError, match="validation"):
        load_graph_file(client, broken, "edges", "bad-count", LOG)
    assert (
        client.execute("SELECT count() FROM corpscout.commoncrawl_domain_graph_edges")[
            0
        ][0]
        == 8
    )
    assert (
        client.execute(
            "SELECT count() FROM system.tables WHERE database='corpscout' AND name LIKE '%_stage_%'"
        )[0][0]
        == 0
    )
