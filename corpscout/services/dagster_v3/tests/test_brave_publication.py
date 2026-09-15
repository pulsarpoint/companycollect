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
)
from dagster_v3.defs.company_domains import browser as brave
from tests.test_company_domains_brave import BrowserFixture, PROXIES
from tests.test_company_domains_brave_integration import MIGRATION
from tests.test_processing_store import prepare_task, claim, complete
from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import render_query

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
        queue_migration = MIGRATION.with_name(
            "000411_corpscout_company_processing_input.up.sql"
        )
        for statement in queue_migration.read_text().split(";"):
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
    task = prepare_task(queue)
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


def test_materialization_pages_renders_processes_and_resumes_without_searching(
    store, clickhouse, monkeypatch
):
    queue, dsn = store
    client, resource = clickhouse
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    client.execute(
        "INSERT INTO corpscout.company_processing_input VALUES",
        [(str(i), str(i), f"Company {i} AB", "SE") for i in range(8)],
    )
    task = str(uuid4())
    config = {
        "task_id": task,
        "input_relation": "corpscout.company_processing_input",
        "input_batch_size": 4,
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
    client.execute("TRUNCATE TABLE corpscout.company_processing_input")
    result = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {"company_brave_search_results": {"config": {"task_id": task}}}
        },
    )
    assert result.success
    assert queue.progress(task)["total"] == 8


def test_physical_queue_supports_custom_template_columns_and_rejects_views(
    store, clickhouse
):
    queue, _ = store
    client, resource = clickhouse
    client.execute(
        "CREATE TABLE corpscout.domain_test_input (input_id String, domain String) ENGINE=MergeTree ORDER BY input_id"
    )
    client.execute(
        "INSERT INTO corpscout.domain_test_input VALUES", [("example.se", "example.se")]
    )
    source = ClickHouseInputQueue(resource, "corpscout.domain_test_input")
    info = source.inspect()
    assert (
        render_query("Who owns {domain}?", source.read(info)[0])
        == "Who owns example.se?"
    )
    client.execute(
        "CREATE VIEW corpscout.domain_test_view AS SELECT * FROM corpscout.domain_test_input"
    )
    with pytest.raises(ValueError, match="physical"):
        ClickHouseInputQueue(resource, "corpscout.domain_test_view").inspect()
    with pytest.raises(ValueError):
        BraveSearchConfig(
            input_relation="corpscout.domain_test_input; DROP TABLE anything"
        )
    client.execute("DROP TABLE corpscout.domain_test_input")
    client.execute(
        "CREATE TABLE corpscout.domain_test_input (input_id String, domain String) ENGINE=MergeTree ORDER BY input_id"
    )
    with pytest.raises(ValueError, match="replaced"):
        source.read(info)


def test_three_million_clickhouse_rows_admit_only_a_bounded_page(store, clickhouse):
    queue, _ = store
    client, resource = clickhouse
    client.execute(
        "INSERT INTO corpscout.company_processing_input SELECT toString(number),toString(number),concat('Company ',toString(number)),'SE' FROM numbers(3000000)"
    )
    source = ClickHouseInputQueue(resource, "corpscout.company_processing_input")
    started = time.monotonic()
    info = source.inspect()
    task = str(uuid4())
    queue.register(
        task, processor="brave-v2", config={}, work_config={}, source_info=info
    )
    assert info["total"] == 3_000_000
    assert queue.task(task)["admitted_count"] == 0
    rows = source.read(info, limit=100)
    assert len(rows) == 100
    assert queue.admit(
        task, after=None, input_ids=[row["input_id"] for row in rows], capacity=100
    )
    assert queue.progress(task)["remaining"] == 3_000_000
    with queue.transaction() as cursor:
        cursor.execute(
            "SELECT count(*) FROM processing.items WHERE task_id=%s", (task,)
        )
        assert cursor.fetchone()["count"] == 100
    print(
        f"3m inputs: inspect/register/read/admit took {time.monotonic() - started:.3f}s, PostgreSQL contains 100 progress IDs"
    )
    slow, fast = claim(queue, task), claim(queue, task)
    complete(queue, fast)
    queue.release("run-1", max_attempts=3)
    recovered = claim(queue, task)
    # Admission order is separate from completion order; neither ID is lost.
    assert source.read(info, input_id=slow.input_id)[0]["input_id"] == slow.input_id
    assert queue.progress(task)["remaining"] == 2_999_999
    assert recovered is not None
    next_rows = source.read(info, after=queue.task(task)["source_cursor"], limit=1)
    assert next_rows[0]["input_id"] not in {row["input_id"] for row in rows}
    assert queue.admit(
        task,
        after=queue.task(task)["source_cursor"],
        input_ids=[next_rows[0]["input_id"]],
        capacity=100,
    )


def test_queue_rejects_duplicate_ids_and_missing_retry_inputs(store, clickhouse):
    _, resource = clickhouse
    client, _ = clickhouse
    source = ClickHouseInputQueue(resource, "corpscout.company_processing_input")
    client.execute(
        "INSERT INTO corpscout.company_processing_input VALUES", [("1", "1", "A", "SE")]
    )
    info = source.inspect()
    with pytest.raises(ValueError, match="missing"):
        source.read(info, input_id="0")
    client.execute(
        "INSERT INTO corpscout.company_processing_input VALUES", [("1", "1", "B", "SE")]
    )
    with pytest.raises(ValueError, match="unique"):
        source.inspect()
    with pytest.raises(ValueError, match="duplicated"):
        source.read(info, input_id="1")


def test_clickhouse_outage_does_not_repeat_saved_browser_work(
    store, clickhouse, monkeypatch
):
    queue, dsn = store
    client, resource = clickhouse
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    client.execute(
        "INSERT INTO corpscout.company_processing_input VALUES",
        [(str(i), str(i), f"Company {i}", "SE") for i in range(8)],
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
                    "config": {
                        "task_id": task,
                        "input_relation": "corpscout.company_processing_input",
                        "export_batch_size": 4,
                    }
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
    task = prepare_task(queue, names=("A",))
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


def test_fixed_upper_bound_excludes_later_inputs_and_missing_rows_cannot_finish(
    store, clickhouse, monkeypatch
):
    queue, dsn = store
    client, resource = clickhouse
    client.execute(
        "INSERT INTO corpscout.company_processing_input VALUES",
        [("1", "1", "First", "SE"), ("2", "2", "Second", "SE")],
    )
    source = ClickHouseInputQueue(resource, "corpscout.company_processing_input")
    info = source.inspect()
    client.execute(
        "INSERT INTO corpscout.company_processing_input VALUES",
        [("3", "3", "Later", "SE")],
    )
    assert [row["input_id"] for row in source.read(info, limit=100)] == ["1", "2"]
    task = str(uuid4())
    queue.register(
        task,
        processor="brave-v2",
        config={"query_template": "Find {company_name}", "freshness_days": 0},
        work_config={},
        source_info=info,
    )
    client.execute("TRUNCATE TABLE corpscout.company_processing_input")

    def unexpected_launch(**kwargs):
        pytest.fail("empty source must not launch a Brave request")

    monkeypatch.setattr(brave, "launch", unexpected_launch)
    result = dg.materialize(
        [company_brave_search_results],
        resources={
            "clickhouse": resource,
            "processing_clickhouse": resource,
            "processing": ProcessingResource(postgres_url=dsn),
            "company_brave_browser": brave.BraveBrowserResource(**PROXIES),
        },
        run_config={
            "ops": {"company_brave_search_results": {"config": {"task_id": task}}}
        },
        raise_on_error=False,
    )
    assert not result.success
    assert queue.progress(task)["remaining"] == queue.progress(task)["total"] == 2
    assert queue.task(task)["source_cursor"] is None
