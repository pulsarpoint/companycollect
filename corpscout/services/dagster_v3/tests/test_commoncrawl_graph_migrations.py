"""Additive graph schemas against disposable PostgreSQL and ClickHouse servers."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest

from tests.test_commoncrawl_domain_graph_integration import graph_ch as graph_ch
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)

ROOT = Path(__file__).parents[3]
PG_UP = ROOT / "database/migrations/000126_commoncrawl_graph_catalog.up.sql"
PG_DOWN = PG_UP.with_name("000126_commoncrawl_graph_catalog.down.sql")
CH_UP = (
    ROOT
    / "clickhouse/migrations/000447_corpscout_commoncrawl_domain_graph_ranks.up.sql"
)
CH_DOWN = CH_UP.with_name("000447_corpscout_commoncrawl_domain_graph_ranks.down.sql")
RELEASE = "cc-main-2025-26-dec-jan-feb"
REQUEST_SQL = """INSERT INTO commoncrawl_graph_import_requests
    (request_id, graph_release, selection, origin, requested_by)
    VALUES (%s, %s, %s, 'manual', 'test-operator')"""


@pytest.fixture
def catalog_db(processing_postgres_url):
    """Every test gets its own database, never the configured application database."""
    with closing(psycopg2.connect(processing_postgres_url)) as admin:
        admin.autocommit = True
        database = "graph_test_" + uuid4().hex
        with admin.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {database}")
        dsn = processing_postgres_url.rsplit("/", 1)[0] + "/" + database
        try:
            with closing(psycopg2.connect(dsn)) as connection:
                with connection, connection.cursor() as cursor:
                    cursor.execute("CREATE TABLE existing_data (value text)")
                    cursor.execute("INSERT INTO existing_data VALUES ('preserved')")
                    cursor.execute(PG_UP.read_text())
                    cursor.execute(
                        """INSERT INTO commoncrawl_graph_releases
                        (graph_release, source_index_url) VALUES (%s, %s)""",
                        (RELEASE, "https://data.commoncrawl.org/example/index.html"),
                    )
                yield connection, dsn
        finally:
            with admin.cursor() as cursor:
                cursor.execute(f"DROP DATABASE {database}")


def test_catalog_defaults_and_rollback_preserve_existing_data(catalog_db):
    connection, _ = catalog_db
    with connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT active_graph_release, automatic_imports_enabled, discovery_baseline_at FROM commoncrawl_graph_state"
        )
        assert cursor.fetchall() == [(None, False, None)]
        cursor.execute(
            "SELECT graph_status, coverage_start, coverage_end FROM commoncrawl_graph_releases"
        )
        assert cursor.fetchall() == [("not_imported", None, None)]
        cursor.execute(PG_DOWN.read_text())
        cursor.execute("SELECT value FROM existing_data")
        assert cursor.fetchall() == [("preserved",)]
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        assert cursor.fetchall() == [("existing_data",)]
        cursor.execute(PG_UP.read_text())


def test_concurrent_full_and_ranks_requests_share_release_lock(catalog_db):
    _, dsn = catalog_db

    def submit(selection):
        with closing(psycopg2.connect(dsn)) as connection:
            try:
                with connection, connection.cursor() as cursor:
                    cursor.execute(REQUEST_SQL, (str(uuid4()), RELEASE, selection))
                return "created"
            except psycopg2.errors.UniqueViolation:
                return "existing"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(submit, ("full", "ranks"))) == [
            "created",
            "existing",
        ]


def test_request_failure_allows_new_attempt_and_pins_retry_to_release(catalog_db):
    connection, _ = catalog_db
    first, retry = str(uuid4()), str(uuid4())
    with connection, connection.cursor() as cursor:
        cursor.execute(REQUEST_SQL, (first, RELEASE, "full"))
        cursor.execute(
            "UPDATE commoncrawl_graph_import_requests SET status='failed', finished_at=now() WHERE request_id=%s",
            (first,),
        )
        cursor.execute(REQUEST_SQL, (retry, RELEASE, "ranks"))
        cursor.execute(
            "UPDATE commoncrawl_graph_import_requests SET retry_of=%s WHERE request_id=%s",
            (first, retry),
        )
        cursor.execute(
            "INSERT INTO commoncrawl_graph_releases (graph_release,source_index_url) VALUES ('cc-main-2026-jan-feb-mar','https://example.test')"
        )
    with (
        pytest.raises(psycopg2.errors.ForeignKeyViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "UPDATE commoncrawl_graph_import_requests SET graph_release='cc-main-2026-jan-feb-mar' WHERE request_id=%s",
            (retry,),
        )


@pytest.mark.parametrize(
    "assignment",
    [
        "status='launching'",
        "status='running',source_manifest='{\"ranks\":{}}'",
        "status='succeeded'",
        "status='failed'",
        "source_manifest='[]'",
        "selection='host'",
        "origin='unknown'",
        "retry_of=request_id",
    ],
)
def test_invalid_request_transitions_are_rejected(catalog_db, assignment):
    connection, _ = catalog_db
    with connection, connection.cursor() as cursor:
        cursor.execute(REQUEST_SQL, (str(uuid4()), RELEASE, "ranks"))
    with (
        pytest.raises(psycopg2.errors.CheckViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(f"UPDATE commoncrawl_graph_import_requests SET {assignment}")


def test_one_dagster_run_cannot_be_associated_with_two_requests(catalog_db):
    connection, _ = catalog_db
    first, second, run = str(uuid4()), str(uuid4()), str(uuid4())
    with connection, connection.cursor() as cursor:
        cursor.execute(REQUEST_SQL, (first, RELEASE, "ranks"))
        cursor.execute(
            """UPDATE commoncrawl_graph_import_requests
            SET status='succeeded', finished_at=now(), dagster_run_id=%s,
                source_manifest='{"ranks":{"etag":"test"}}'
            WHERE request_id=%s""",
            (run, first),
        )
        cursor.execute(REQUEST_SQL, (second, RELEASE, "ranks"))
    with (
        pytest.raises(psycopg2.errors.UniqueViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "UPDATE commoncrawl_graph_import_requests SET dagster_run_id=%s WHERE request_id=%s",
            (run, second),
        )


@pytest.mark.parametrize(
    "assignment",
    [
        "coverage_start='2026-02-01',coverage_end='2026-01-01'",
        "graph_status='retired'",
        "graph_retired_at=now()",
        "graph_release='../../escape'",
    ],
)
def test_invalid_release_metadata_is_rejected(catalog_db, assignment):
    connection, _ = catalog_db
    with (
        pytest.raises(psycopg2.errors.CheckViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(f"UPDATE commoncrawl_graph_releases SET {assignment}")


def test_control_state_is_singleton_and_references_known_release(catalog_db):
    connection, _ = catalog_db
    with (
        pytest.raises(psycopg2.errors.CheckViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("INSERT INTO commoncrawl_graph_state (singleton) VALUES (false)")
    with (
        pytest.raises(psycopg2.errors.UniqueViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("INSERT INTO commoncrawl_graph_state DEFAULT VALUES")
    with (
        pytest.raises(psycopg2.errors.ForeignKeyViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "UPDATE commoncrawl_graph_state SET active_graph_release='cc-main-missing'"
        )
    with connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_state SET active_graph_release=%s", (RELEASE,)
        )


@pytest.mark.parametrize(
    "assignment",
    [
        "source_bytes=-1",
        "expected_rows=-1",
        "artifact_kind='pages'",
        "graph_level='host'",
        "cache_key='partial-upload'",
        "source_url=NULL",
    ],
)
def test_invalid_file_metadata_is_rejected(catalog_db, assignment):
    connection, _ = catalog_db
    with connection, connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO commoncrawl_graph_release_files
            (graph_release,artifact_kind,source_url,availability)
            VALUES (%s,'ranks','https://example.test/ranks.gz','available')""",
            (RELEASE,),
        )
    with (
        pytest.raises(psycopg2.errors.CheckViolation),
        connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(f"UPDATE commoncrawl_graph_release_files SET {assignment}")


def test_discovery_file_update_does_not_change_attempt_manifest(catalog_db):
    connection, _ = catalog_db
    with connection, connection.cursor() as cursor:
        cursor.execute(REQUEST_SQL, (str(uuid4()), RELEASE, "ranks"))
        cursor.execute(
            'UPDATE commoncrawl_graph_import_requests SET status=\'launching\',source_manifest=\'{"ranks":{"etag":"old"}}\''
        )
        cursor.execute(
            """INSERT INTO commoncrawl_graph_release_files
            (graph_release,artifact_kind,source_url,source_etag,availability)
            VALUES (%s,'ranks','https://example.test/ranks.gz','old','available')""",
            (RELEASE,),
        )
        cursor.execute("UPDATE commoncrawl_graph_release_files SET source_etag='new'")
        cursor.execute("SELECT source_manifest FROM commoncrawl_graph_import_requests")
        assert cursor.fetchone()[0] == {"ranks": {"etag": "old"}}


def test_rank_partition_replacement_and_rollback_preserve_legacy_data(graph_ch):
    client = graph_ch
    migrations = ROOT / "clickhouse/migrations"
    for path in (
        migrations / "000073_corpscout_commoncrawl_domain_graph_signals.up.sql",
        CH_UP,
    ):
        for statement in path.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
    client.execute("""INSERT INTO corpscout.commoncrawl_domain_graph_signals
        VALUES ('legacy','example.com',10,1,0.5,1,2,'legacy',now64(3))""")
    client.execute("""INSERT INTO corpscout.commoncrawl_domain_graph_nodes
        VALUES ('preserved',0,'example.com',2,'etag','legacy')""")
    schema = client.execute(
        "SELECT engine,partition_key,sorting_key FROM system.tables WHERE database='corpscout' AND name='commoncrawl_domain_graph_ranks'"
    )
    assert schema == [("MergeTree", "graph_release", "root_domain, graph_release")]
    columns = client.execute("DESCRIBE corpscout.commoncrawl_domain_graph_ranks")
    assert [(row[0], row[1]) for row in columns] == [
        ("graph_release", "LowCardinality(String)"),
        ("root_domain", "String"),
        ("cc_harmonic_centrality", "Float64"),
        ("cc_harmonic_rank", "UInt64"),
        ("cc_pagerank", "Float64"),
        ("cc_pagerank_rank", "UInt64"),
        ("n_hosts", "Nullable(UInt32)"),
        ("source_run_id", "String"),
        ("loaded_at", "DateTime64(3, 'UTC')"),
    ]
    client.execute(
        "CREATE TABLE corpscout.test_rank_stage AS corpscout.commoncrawl_domain_graph_ranks"
    )
    try:
        for release in (RELEASE, "cc-main-2026-jan-feb-mar"):
            client.execute(
                """INSERT INTO corpscout.test_rank_stage
                VALUES (%(release)s,'example.com',10,1,0.5,1,NULL,'run',now64(3))""",
                {"release": release},
            )
        assert client.execute(
            "SELECT count() FROM corpscout.commoncrawl_domain_graph_ranks"
        ) == [(0,)]
        for release in (RELEASE, "cc-main-2026-jan-feb-mar", RELEASE):
            client.execute(
                "ALTER TABLE corpscout.commoncrawl_domain_graph_ranks REPLACE PARTITION %(release)s FROM corpscout.test_rank_stage",
                {"release": release},
            )
        assert client.execute(
            "SELECT graph_release,count(),any(n_hosts) FROM corpscout.commoncrawl_domain_graph_ranks GROUP BY graph_release ORDER BY graph_release"
        ) == [
            (RELEASE, 1, None),
            ("cc-main-2026-jan-feb-mar", 1, None),
        ]
        client.execute(CH_DOWN.read_text())
        assert client.execute(
            "SELECT count() FROM corpscout.commoncrawl_domain_graph_signals"
        ) == [(1,)]
        assert client.execute(
            "SELECT root_domain FROM corpscout.commoncrawl_domain_graph_nodes WHERE graph_release='preserved'"
        ) == [("example.com",)]
    finally:
        client.execute("DROP TABLE IF EXISTS corpscout.test_rank_stage")
