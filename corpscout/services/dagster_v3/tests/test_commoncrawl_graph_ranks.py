"""Real HTTP gzip → object cache → native ClickHouse ranking publication."""

import gzip
import hashlib
import logging
import subprocess
import threading
import time
from dataclasses import asdict, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from psycopg2.extras import Json

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_domain_graph.download import (
    ArtifactSource,
    cache_artifact,
)
from dagster_v3.defs.commoncrawl_domain_graph.ranks import load_ranks
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphStore
from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_commoncrawl_graph_migrations import CH_UP, catalog_db as catalog_db
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)

LOG = logging.getLogger(__name__)
HEADER = b"#harmonicc_pos\t#harmonicc_val\t#pr_pos\t#pr_val\t#host_rev\t#n_hosts\n"


@pytest.fixture(scope="module")
def graph_objects():
    name = "graph-cache-test-" + uuid4().hex[:12]
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            "127.0.0.1::9000",
            "-e",
            "RUSTFS_ACCESS_KEY=graph-test",
            "-e",
            "RUSTFS_SECRET_KEY=graph-test-secret",
            "rustfs/rustfs:latest",
            "/data",
        ],
        check=True,
        capture_output=True,
    )
    try:
        port = int(
            subprocess.check_output(["docker", "port", name, "9000"], text=True)
            .strip()
            .rsplit(":", 1)[1]
        )
        client = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{port}",
            aws_access_key_id="graph-test",
            aws_secret_access_key="graph-test-secret",
            region_name="us-east-1",
            config=Config(
                s3={"addressing_style": "path"},
                connect_timeout=1,
                read_timeout=3,
                retries={"max_attempts": 0},
            ),
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                client.list_buckets()
                break
            except BotoCoreError, ClientError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
        objects = ObjectStoreResource(
            s3_client=client,
            bucket="commoncrawl-graphs",
            endpoint_url=f"http://127.0.0.1:{port}",
            access_key="graph-test",
            secret_key="graph-test-secret",
        )
        objects.ensure_bucket()
        yield objects, port
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


@pytest.fixture
def rank_http():
    state = {
        "body": gzip.compress(
            HEADER + b"1\t10\t2\t0.4\tuk.co.example\t2\n2\t9\t1\t0.6\tcom.example\t1\n"
        ),
        "gets": 0,
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["gets"] += 1
            body = state["body"]
            etag = '"' + hashlib.sha256(body).hexdigest() + '"'
            if self.headers.get("If-Match") != etag:
                self.send_error(412)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", etag)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state, f"http://127.0.0.1:{server.server_port}/ranks.txt.gz"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def rank_source(state, url, release="cc-main-2026-jul-aug-sep"):
    return ArtifactSource(
        release,
        "ranks",
        url,
        '"' + hashlib.sha256(state["body"]).hexdigest() + '"',
        len(state["body"]),
        2,
    )


@pytest.fixture
def rank_ch(graph_ch, graph_objects):
    _, port = graph_objects
    for statement in CH_UP.read_text().split(";"):
        if statement.strip():
            graph_ch.execute(statement)
    graph_ch.execute("DROP NAMED COLLECTION IF EXISTS commoncrawl_graph_cache")
    graph_ch.execute(
        """CREATE NAMED COLLECTION commoncrawl_graph_cache AS
        url=%(url)s,access_key_id='graph-test',secret_access_key='graph-test-secret'""",
        {"url": f"http://host.docker.internal:{port}/commoncrawl-graphs/"},
    )
    yield graph_ch
    graph_ch.execute("TRUNCATE TABLE corpscout.commoncrawl_domain_graph_ranks")


def test_complete_rank_import_cache_reuse_and_source_identity(
    rank_http, graph_objects, rank_ch, catalog_db
):
    state, url = rank_http
    objects, _ = graph_objects
    source = rank_source(state, url)
    artifact = cache_artifact(source, objects, LOG)
    assert cache_artifact(source, objects, LOG) == artifact
    assert state["gets"] == 1
    run_id = str(uuid4())
    connection, _ = catalog_db
    store = GraphStore(connection)
    with store.transaction() as cursor:
        cursor.execute(
            "INSERT INTO commoncrawl_graph_releases (graph_release,source_index_url) VALUES (%s,'https://example.test')",
            (source.graph_release,),
        )
        cursor.execute(
            """INSERT INTO commoncrawl_graph_import_requests
            (request_id,graph_release,selection,origin,requested_by,status,dagster_run_id,source_manifest)
            VALUES (%s,%s,'ranks','manual','test','running',%s,%s)""",
            (
                str(uuid4()),
                source.graph_release,
                run_id,
                Json({"ranks": asdict(source)}),
            ),
        )
    store.assert_rank_source(rank_ch, source)
    assert load_ranks(rank_ch, artifact, run_id, LOG) == (2, False)
    store.assert_rank_source(rank_ch, source)
    assert load_ranks(rank_ch, artifact, run_id, LOG) == (2, True)
    assert rank_ch.execute(
        "SELECT root_domain,cc_harmonic_rank,cc_pagerank_rank FROM corpscout.commoncrawl_domain_graph_ranks ORDER BY root_domain"
    ) == [
        ("example.co.uk", 1, 2),
        ("example.com", 2, 1),
    ]
    with pytest.raises(ValueError, match="different"):
        store.assert_rank_source(rank_ch, replace(source, source_etag='"changed"'))
    # A lost success acknowledgement does not hide the already published partition.
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT status FROM commoncrawl_graph_import_requests WHERE dagster_run_id=%s",
            (run_id,),
        )
        assert cursor.fetchone()["status"] == "running"


@pytest.mark.parametrize(
    "rows",
    [
        b"1\t10\t2\t0.4\tcom.same\t2\n2\t9\t1\t0.6\tcom.same\t1\n",
        b"1\tNaN\t2\t0.4\tcom.one\t2\n2\t9\t1\t0.6\tcom.two\t1\n",
        b"1\t10\t2\t0.4\tcom.one\t2\n",
    ],
)
def test_invalid_ranks_never_publish(rank_http, graph_objects, rank_ch, rows):
    state, url = rank_http
    state["body"] = gzip.compress(HEADER + rows)
    artifact = cache_artifact(rank_source(state, url), graph_objects[0], LOG)
    with pytest.raises(ValueError, match="validation"):
        load_ranks(rank_ch, artifact, str(uuid4()), LOG)
    assert rank_ch.execute(
        "SELECT count() FROM corpscout.commoncrawl_domain_graph_ranks"
    ) == [(0,)]
    assert (
        rank_ch.execute(
            "SELECT name FROM system.tables WHERE database='corpscout' AND name LIKE 'commoncrawl_domain_graph_ranks_stage_%'"
        )
        == []
    )


def test_truncated_gzip_and_unknown_header_never_enter_cache(rank_http, graph_objects):
    state, url = rank_http
    objects, _ = graph_objects
    before = set(objects.list_keys("domain/"))
    state["body"] = state["body"][:-5]
    with pytest.raises(EOFError):
        cache_artifact(rank_source(state, url), objects, LOG)
    assert set(objects.list_keys("domain/")) == before
    state["body"] = gzip.compress(b"unexpected header\n")
    with pytest.raises(ValueError, match="header"):
        cache_artifact(rank_source(state, url), objects, LOG)
    assert set(objects.list_keys("domain/")) == before


@pytest.mark.parametrize(
    "harmonic_header", [b"#harmonicc_pos\t#harmonicc_val", b"#hc_pos\t#hc_val"]
)
def test_historical_rank_file_without_host_counts_keeps_null(
    rank_http, graph_objects, rank_ch, harmonic_header
):
    state, url = rank_http
    state["body"] = gzip.compress(
        harmonic_header
        + b"\t#pr_pos\t#pr_val\t#host_rev\n1\t10\t2\t0.4\tcom.one\n2\t9\t1\t0.6\tcom.two\n"
    )
    artifact = cache_artifact(rank_source(state, url), graph_objects[0], LOG)
    assert cache_artifact(artifact.source, graph_objects[0], LOG) == artifact
    assert load_ranks(rank_ch, artifact, str(uuid4()), LOG) == (2, False)
    assert rank_ch.execute(
        "SELECT n_hosts FROM corpscout.commoncrawl_domain_graph_ranks"
    ) == [(None,), (None,)]
