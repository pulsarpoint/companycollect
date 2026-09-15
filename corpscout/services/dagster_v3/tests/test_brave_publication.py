"""Brave's outbox publication through real PostgreSQL and ClickHouse servers."""

import os
import subprocess
import time
from contextlib import closing
from uuid import uuid4
from urllib.parse import urlsplit
from pathlib import Path

from dotenv import dotenv_values

import dagster as dg
import pytest
from clickhouse_driver import Client
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.company_domains.assets import (
    BraveSearchConfig,
    PUBLISH_SQL,
    company_brave_search_results,
    publish_results,
    snapshot_inputs,
)
from dagster_v3.defs.company_domains import browser as brave
from tests.test_company_domains_brave import BrowserFixture, PROXIES
from tests.test_company_domains_brave_integration import MIGRATION
from tests.test_processing_store import freeze, claim, complete

pytest_plugins = ["tests.test_processing_store"]


@pytest.fixture
def clickhouse(store, tmp_path):
    queue, dsn = store
    database = dsn.rsplit("/", 1)[1]
    name = "brave-publication-" + uuid4().hex
    privileges = tmp_path / "privileges.xml"
    privileges.write_text(
        "<clickhouse><users><test><named_collection_control>1</named_collection_control></test></users></clickhouse>"
    )
    pg_port = urlsplit(dsn).port
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-v",
            f"{privileges}:/etc/clickhouse-server/users.d/processing.xml:ro",
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
            host="127.0.0.1",
            port=port,
            user="test",
            password="test",
            send_receive_timeout=10,
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                client.execute("SELECT 1")
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        client.execute("CREATE DATABASE corpscout")
        client.execute(
            "CREATE TABLE corpscout.se_company_basic_info (company_id String, legal_name Nullable(String), status String) ENGINE=ReplacingMergeTree ORDER BY company_id"
        )
        for statement in MIGRATION.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
        client.execute(
            f"CREATE NAMED COLLECTION processing_postgres AS host='host.docker.internal' NOT OVERRIDABLE, "
            f"port={pg_port} NOT OVERRIDABLE, database='{database}' NOT OVERRIDABLE, "
            "user='processing_test' NOT OVERRIDABLE, password='processing_test' NOT OVERRIDABLE"
        )
        yield (
            client,
            ClickhouseResource(
                host="127.0.0.1",
                port=port,
                user="test",
                password="test",
                database="corpscout",
            ),
        )
    finally:
        if client:
            client.disconnect()
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


def test_import_survives_partial_insert_and_lost_acknowledgment(
    store, clickhouse, monkeypatch
):
    queue, _ = store
    client, _ = clickhouse
    task = freeze(queue)
    first, second = claim(queue, task), claim(queue, task)
    complete(queue, first, answer="Original åäö\nhttps://example.se/")
    complete(queue, second)
    batch = queue.export_batch(task, limit=100, destination="company_brave_info_v1")
    # Simulate an interrupted import after only part of the closed batch arrived.
    client.execute(PUBLISH_SQL + " AND input_id='0'", {"batch_id": batch.batch_id})
    real_ack = queue.acknowledge

    def lost_ack(_):
        raise ConnectionError("acknowledgment was lost")

    monkeypatch.setattr(queue, "acknowledge", lost_ack)
    with pytest.raises(ConnectionError):
        publish_results(queue, client, task, batch_size=100)
    assert queue.progress(task)["unpublished"] == 2
    monkeypatch.setattr(queue, "acknowledge", real_ack)
    assert publish_results(queue, client, task, batch_size=100) == 2
    assert queue.progress(task)["unpublished"] == 0
    rows = client.execute(
        "SELECT input_id,answer_text FROM corpscout.company_brave_info_deduplicated ORDER BY input_id"
    )
    assert rows == [
        ("0", "Original åäö\nhttps://example.se/"),
        ("1", "Copied response"),
    ]
    assert publish_results(queue, client, task, batch_size=100) == 0


def test_materialization_freezes_renders_processes_and_resumes_without_searching(
    store, clickhouse, monkeypatch
):
    queue, dsn = store
    client, resource = clickhouse
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [(str(i), f"Company {i} AB", "active") for i in range(8)],
    )
    task = str(uuid4())
    config = {
        "task_id": task,
        "query_type": "website",
        "query_template": "Find {company_name}",
        "export_batch_size": 4,
    }
    resources = {
        "clickhouse": resource,
        "processing_clickhouse": resource,
        "processing": ProcessingResource(postgres_url=dsn),
        "company_brave_browser": brave.BraveBrowserResource(**PROXIES),
    }
    result = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={"ops": {"company_brave_search_results": {"config": config}}},
    )
    assert result.success
    assert queue.progress(task)["succeeded"] == 8
    assert queue.progress(task)["unpublished"] == 0
    assert client.execute(
        "SELECT count() FROM corpscout.company_brave_info_deduplicated"
    ) == [(8,)]

    def unexpected_launch(**kwargs):
        pytest.fail("resume repeated a saved Brave search")

    monkeypatch.setattr(brave, "launch", unexpected_launch)
    client.execute("TRUNCATE TABLE corpscout.se_company_basic_info")
    result = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {"company_brave_search_results": {"config": {"task_id": task}}}
        },
    )
    assert result.success
    assert queue.progress(task)["total"] == 8


def test_named_relation_snapshot_and_custom_template_columns(store, clickhouse):
    queue, _ = store
    client, resource = clickhouse
    client.execute(
        "CREATE VIEW corpscout.domain_test_input AS SELECT 'example.se' AS input_id, 'example.se' AS domain"
    )
    config = BraveSearchConfig(
        input_relation="corpscout.domain_test_input",
        query_template="Who owns {domain}?",
    )
    task = str(uuid4())
    with closing(snapshot_inputs(resource, config)) as inputs:
        queue.freeze(
            task,
            processor="brave-v2",
            config={},
            work_config={},
            inputs=inputs,
            query_template=config.query_template,
            freshness_days=0,
        )
    assert claim(queue, task).query == "Who owns example.se?"
    with pytest.raises(ValueError):
        BraveSearchConfig(
            input_relation="corpscout.domain_test_input; DROP TABLE anything"
        )


def test_clickhouse_outage_does_not_repeat_saved_browser_work(
    store, clickhouse, monkeypatch
):
    queue, dsn = store
    client, resource = clickhouse
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [(str(i), f"Company {i}", "active") for i in range(8)],
    )
    task = str(uuid4())
    resources = {
        "clickhouse": resource,
        "processing_clickhouse": resource,
        "processing": ProcessingResource(postgres_url=dsn),
        "company_brave_browser": brave.BraveBrowserResource(**PROXIES),
    }
    client.execute(
        "RENAME TABLE corpscout.company_brave_info TO corpscout.unavailable_brave_info"
    )
    result = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {
                "company_brave_search_results": {
                    "config": {"task_id": task, "export_batch_size": 4}
                }
            }
        },
        raise_on_error=False,
    )
    assert not result.success
    assert queue.progress(task)["succeeded"] == 8
    assert queue.progress(task)["unpublished"] == 8

    def unexpected_browser(**kwargs):
        pytest.fail("export recovery repeated an already saved search")

    monkeypatch.setattr(brave, "launch", unexpected_browser)
    client.execute(
        "RENAME TABLE corpscout.unavailable_brave_info TO corpscout.company_brave_info"
    )
    result = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {
                "company_brave_search_results": {
                    "config": {"task_id": task, "mode": "publish"}
                }
            }
        },
    )
    assert result.success
    assert queue.progress(task)["unpublished"] == 0
    assert client.execute(
        "SELECT count() FROM corpscout.company_brave_info_deduplicated"
    ) == [(8,)]


def test_provisioned_publisher_can_import_but_cannot_update_postgres(
    store, clickhouse, tmp_path
):
    import psycopg2

    queue, dsn = store
    client, resource = clickhouse
    credentials_file = tmp_path / "processing.env"
    environment = {
        **os.environ,
        "PROCESSING_ADMIN_PG_URL": dsn,
        "CLICKHOUSE_HOST": "127.0.0.1",
        "CLICKHOUSE_NATIVE_PORT": str(resource.port),
        "CLICKHOUSE_USER": "test",
        "CLICKHOUSE_PASSWORD": "test",
        "CLICKHOUSE_SECURE": "false",
    }
    script = Path(__file__).parents[1] / "scripts/provision-processing-storage.py"
    import sys

    for _ in range(2):
        provision = subprocess.run(
            [
                sys.executable,
                str(script),
                "--credentials-file",
                str(credentials_file),
                "--postgres-host-for-clients",
                "host.docker.internal",
            ],
            env=environment,
            capture_output=True,
            text=True,
        )
        assert provision.returncode == 0, provision.stdout + provision.stderr
    credentials = dotenv_values(credentials_file)
    assert credentials_file.stat().st_mode & 0o777 == 0o600
    task = freeze(queue, names=("A",))
    complete(queue, claim(queue, task))
    publisher = Client(
        host="127.0.0.1",
        port=resource.port,
        user=credentials["PROCESSING_CLICKHOUSE_USER"],
        password=credentials["PROCESSING_CLICKHOUSE_PASSWORD"],
    )
    try:
        assert publish_results(queue, publisher, task, batch_size=100) == 1
        with pytest.raises(Exception):
            publisher.execute("TRUNCATE TABLE corpscout.company_brave_info")
    finally:
        publisher.disconnect()
    with closing(psycopg2.connect(dsn)) as admin:
        with admin, admin.cursor() as cursor:
            cursor.execute("SET ROLE processing_reader")
            cursor.execute("SELECT count(*) FROM processing.brave_export")
            assert cursor.fetchone() == (1,)
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cursor.execute("DELETE FROM processing.items")
