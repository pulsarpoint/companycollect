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
    company_brave_search_results,
)
from dagster_v3.defs.company_domains import browser as brave
from dagster_v3.defs.company_domains import publication
from dagster_v3.defs.company_domains.publication import (
    PUBLISH_SQL,
    EXPORT_DESTINATION,
    publish_results,
)
from tests.test_company_domains_brave import BrowserFixture, PROXIES
from tests.test_company_domains_brave_integration import MIGRATION
from tests.test_processing_store import prepare_task, claim, complete
from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import render_query

pytest_plugins = ["tests.test_processing_store"]


@pytest.fixture(scope="session")
def archive_s3():
    import boto3

    name = "brave-archive-" + uuid4().hex
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
            "RUSTFS_ACCESS_KEY=brave_test",
            "-e",
            "RUSTFS_SECRET_KEY=brave_test_secret",
            "rustfs/rustfs:latest",
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
        endpoint = f"http://127.0.0.1:{port}"
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="brave_test",
            aws_secret_access_key="brave_test_secret",
            region_name="us-east-1",
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                s3.list_buckets()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        yield s3, endpoint, f"http://host.docker.internal:{port}"
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


@pytest.fixture
def clickhouse(store, tmp_path, archive_s3):
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
        s3, _, internal_endpoint = archive_s3
        bucket = "brave-test-" + uuid4().hex
        s3.create_bucket(Bucket=bucket)
        client.execute(
            "CREATE NAMED COLLECTION brave_history AS url=%(url)s NOT OVERRIDABLE, access_key_id='brave_test' NOT OVERRIDABLE, secret_access_key='brave_test_secret' NOT OVERRIDABLE",
            {"url": f"{internal_endpoint}/{bucket}/"},
        )
        client.execute("CREATE USER processing_publisher IDENTIFIED BY 'test'")
        client.execute(
            "GRANT S3, CREATE TEMPORARY TABLE ON *.* TO processing_publisher"
        )
        client.execute(
            "GRANT NAMED COLLECTION ON brave_history TO processing_publisher"
        )
        for statement in MIGRATION.read_text().split(";"):
            if statement.strip():
                client.execute(statement)
        for migration in (
            "000415_corpscout_brave_history_query_settings.up.sql",
            "000416_corpscout_brave_history_reader.up.sql",
            "000411_corpscout_company_processing_input.up.sql",
            "000412_corpscout_company_brave_search_input.up.sql",
        ):
            for statement in MIGRATION.with_name(migration).read_text().split(";"):
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
    batch = queue.export_batch(task, limit=100, destination=EXPORT_DESTINATION)
    # Simulate an interrupted import after only part of the closed batch arrived.
    client.execute(
        PUBLISH_SQL.format(table="se_company_brave_domains") + " AND input_id='0'",
        {"batch_id": batch.batch_id, "country": "SE", "path": "pending"},
    )
    real_ack = publication.acknowledge_archive

    def lost_ack(*args):
        raise ConnectionError("acknowledgment was lost")

    monkeypatch.setattr(publication, "acknowledge_archive", lost_ack)
    with pytest.raises(ConnectionError):
        publish_results(queue, client, task, batch_size=100)
    assert queue.progress(task)["unpublished"] == 2
    monkeypatch.setattr(publication, "acknowledge_archive", real_ack)
    assert publish_results(queue, client, task, batch_size=100) == 2
    assert queue.progress(task)["unpublished"] == 0
    rows = client.execute(
        "SELECT input_id,answer_text FROM corpscout.se_company_brave_domains FINAL ORDER BY input_id"
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
        "INSERT INTO corpscout.company_brave_search_input VALUES",
        [(str(i), str(i), f"Company {i} AB", "SE") for i in range(8)],
    )
    task = str(uuid4())
    config = {
        "task_id": task,
        "input_relation": "corpscout.company_brave_search_input",
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
        "SELECT count() FROM corpscout.se_company_brave_domains FINAL"
    ) == [(8,)]

    def unexpected_launch(**kwargs):
        pytest.fail("resume repeated a saved Brave search")

    monkeypatch.setattr(brave, "launch", unexpected_launch)
    client.execute("TRUNCATE TABLE corpscout.company_brave_search_input")
    result = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {"company_brave_search_results": {"config": {"task_id": task}}}
        },
    )
    assert result.success
    assert queue.progress(task)["total"] == 8


def test_adaptive_timeouts_survive_resume_and_retry_only_failed_items(
    store, clickhouse, monkeypatch
):
    queue, dsn = store
    client, resource = clickhouse
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    attempts = {str(i): [] for i in range(5)}

    def copy_answer(page, query, *, timeout_ms, answer_timeout_ms):
        company = query.rsplit(" ", 1)[1]
        attempts[company].append(answer_timeout_ms)
        attempt = len(attempts[company])
        if company == "0" and attempt <= 3:
            raise brave.BraveStepError("answer_generation", "TimeoutError")
        if company == "1" and attempt == 1:
            raise brave.BraveStepError("copy", "TimeoutError")
        if company == "2" and attempt == 1:
            raise brave.BraveStepError("answer_generation", "RuntimeError")
        if company == "4" and attempt == 1:
            # Historical timeouts have no stage; they still qualify for a longer wait.
            raise brave.BraveStepError("", "TimeoutError")
        page.url = "https://search.brave.com/ask"
        return f"Answer for {query}"

    monkeypatch.setattr(brave, "copy_brave_answer", copy_answer)
    client.execute(
        "INSERT INTO corpscout.company_brave_search_input VALUES",
        [(str(i), str(i), f"Company {i}", "SE") for i in range(5)],
    )
    task = str(uuid4())
    resources = {
        "clickhouse": resource,
        "processing_clickhouse": resource,
        "processing": ProcessingResource(postgres_url=dsn),
        "company_brave_browser": brave.BraveBrowserResource(**PROXIES),
    }
    config = {
        "task_id": task,
        "input_relation": "corpscout.company_brave_search_input",
        "query_template": "Find {company_name}",
        "max_attempts": 1,
        "retry_seconds": 0,
    }
    first = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={"ops": {"company_brave_search_results": {"config": config}}},
        raise_on_error=False,
    )
    assert not first.success
    assert queue.progress(task)["terminal_failed"] == 4
    assert queue.progress(task)["succeeded"] == 1
    assert all(timeouts == [60_000] for timeouts in attempts.values())
    assert queue.retry_failed(task, max_attempts=1) == 0

    # A larger budget alone must not silently restart a finished failed selection.
    config = {"task_id": task, "max_attempts": 5, "retry_seconds": 0}
    resumed = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={"ops": {"company_brave_search_results": {"config": config}}},
        raise_on_error=False,
    )
    assert not resumed.success
    assert all(timeouts == [60_000] for timeouts in attempts.values())
    config["retry_failed"] = True
    retried = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={"ops": {"company_brave_search_results": {"config": config}}},
    )
    assert retried.success
    assert attempts == {
        "0": [60_000, 120_000, 180_000, 180_000],
        "1": [60_000, 60_000],
        "2": [60_000, 60_000],
        "3": [60_000],
        "4": [60_000, 120_000],
    }
    assert queue.progress(task)["succeeded"] == 5
    assert queue.progress(task)["remaining"] == 0
    assert queue.progress(task)["terminal_failed"] == 0
    assert queue.progress(task)["unpublished"] == 0
    assert queue.retry_failed(task, max_attempts=5) == 0
    with queue.transaction() as cursor:
        cursor.execute(
            "SELECT attempt,payload FROM processing.results WHERE task_id=%s AND input_id='0' ORDER BY attempt",
            (task,),
        )
        history = cursor.fetchall()
    assert [row["attempt"] for row in history] == [1, 2, 3, 4]
    assert [row["payload"]["answer_timeout_ms"] for row in history] == attempts["0"]
    assert all(
        row["payload"]["error_stage"] == "answer_generation" for row in history[:3]
    )
    assert all("elapsed_ms" in row["payload"] for row in history)
    assert all("answer_text" not in row["payload"] for row in history)
    assert client.execute(
        "SELECT count() FROM corpscout.se_company_brave_domains FINAL"
    ) == [(5,)]
    assert client.execute(
        "SELECT count() FROM corpscout.se_company_brave_domains_history"
    ) == [(11,)]


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
        "INSERT INTO corpscout.company_brave_search_input SELECT toString(number),toString(number),concat('Company ',toString(number)),'SE' FROM numbers(3000000)"
    )
    source = ClickHouseInputQueue(resource, "corpscout.company_brave_search_input")
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
    source = ClickHouseInputQueue(resource, "corpscout.company_brave_search_input")
    client.execute(
        "INSERT INTO corpscout.company_brave_search_input VALUES",
        [("1", "1", "A", "SE")],
    )
    info = source.inspect()
    with pytest.raises(ValueError, match="missing"):
        source.read(info, input_id="0")
    client.execute(
        "INSERT INTO corpscout.company_brave_search_input VALUES",
        [("1", "1", "B", "SE")],
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
        "INSERT INTO corpscout.company_brave_search_input VALUES",
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
        "RENAME TABLE corpscout.se_company_brave_domains TO corpscout.unavailable_brave_info"
    )
    result = dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {
                "company_brave_search_results": {
                    "config": {
                        "task_id": task,
                        "input_relation": "corpscout.company_brave_search_input",
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
        "RENAME TABLE corpscout.unavailable_brave_info TO corpscout.se_company_brave_domains"
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
        "SELECT count() FROM corpscout.se_company_brave_domains FINAL"
    ) == [(8,)]


def test_provisioned_publisher_can_import_but_cannot_update_postgres(
    store, clickhouse, tmp_path, archive_s3
):
    import psycopg2

    queue, dsn = store
    client, resource = clickhouse
    _, endpoint, internal_endpoint = archive_s3
    credentials_file = tmp_path / "processing.env"
    environment = {
        **os.environ,
        "PROCESSING_ADMIN_PG_URL": dsn,
        "CORPSCOUT_S3_ENDPOINT": endpoint,
        "CORPSCOUT_S3_ACCESS_KEY": "brave_test",
        "CORPSCOUT_S3_SECRET_KEY": "brave_test_secret",
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
                "--s3-endpoint-for-clickhouse",
                internal_endpoint,
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
            publisher.execute("TRUNCATE TABLE corpscout.se_company_brave_domains")
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
        "INSERT INTO corpscout.company_brave_search_input VALUES",
        [("1", "1", "First", "SE"), ("2", "2", "Second", "SE")],
    )
    source = ClickHouseInputQueue(resource, "corpscout.company_brave_search_input")
    info = source.inspect()
    client.execute(
        "INSERT INTO corpscout.company_brave_search_input VALUES",
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
    client.execute("TRUNCATE TABLE corpscout.company_brave_search_input")

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


def test_archive_preserves_attempts_current_never_regresses_and_can_rebuild(
    store, clickhouse
):
    queue, _ = store
    client, _ = clickhouse
    older = prepare_task(queue, names=("A",))
    complete(queue, claim(queue, older), answer="Original answer")
    newer = prepare_task(queue, names=("A",))
    complete(queue, claim(queue, newer), answer="New answer åäö\nhttps://example.se")
    assert publish_results(queue, client, newer, batch_size=100) == 1
    # An older task published later must not replace the more recent response.
    assert publish_results(queue, client, older, batch_size=100) == 1
    failed = prepare_task(queue, names=("A",))
    complete(
        queue, claim(queue, failed), status="error", answer="Failed attempt diagnostic"
    )
    assert publish_results(queue, client, failed, batch_size=100) == 1
    assert client.execute(
        "SELECT answer_text FROM corpscout.se_company_brave_domains FINAL"
    ) == [("New answer åäö\nhttps://example.se",)]
    history = client.execute(
        "SELECT answer_text FROM corpscout.se_company_brave_domains_history ORDER BY completed_at"
    )
    assert history == [
        ("Original answer",),
        ("New answer åäö\nhttps://example.se",),
        ("Failed attempt diagnostic",),
    ]
    with queue.transaction() as cursor:
        cursor.execute(
            "SELECT count(*) FROM processing.results WHERE payload ? 'answer_text'"
        )
        assert cursor.fetchone()["count"] == 0
        cursor.execute(
            "SELECT count(*) FROM processing.export_batches WHERE archived_at IS NOT NULL AND archive_manifest IS NOT NULL"
        )
        assert cursor.fetchone()["count"] == 3
    # Repeated filtered history reads must stay complete with the server cache enabled.
    for status, count in (("error", 1), ("success", 2), ("missing", 0), ("success", 2)):
        assert client.execute(
            "SELECT count() FROM corpscout.se_company_brave_domains_history WHERE status=%(status)s",
            {"status": status},
            settings={"use_query_condition_cache": 1},
        ) == [(count,)]
    # Cache identity survives removal of the large answer from PostgreSQL.
    cached = prepare_task(queue, names=("A",))
    item = claim(queue, cached)
    assert queue.skip_if_fresh(item, work_key="test-work-0", freshness_days=30)
    assert queue.progress(cached)["skipped"] == 1
    # Rebuild the current table entirely with SQL from S3, after PG payload pruning.
    client.execute("TRUNCATE TABLE corpscout.se_company_brave_domains")
    client.execute(
        "INSERT INTO corpscout.se_company_brave_domains ("
        + publication.COLUMNS_SQL
        + ", archive_path) "
        "SELECT "
        + publication.COLUMNS_SQL
        + ", _path FROM corpscout.se_company_brave_domains_history WHERE status='success'"
    )
    assert client.execute(
        "SELECT answer_text FROM corpscout.se_company_brave_domains FINAL"
    ) == [("New answer åäö\nhttps://example.se",)]


def test_archive_mismatch_retains_postgres_payload_and_leaves_batch_pending(
    store, clickhouse
):
    queue, _ = store
    client, _ = clickhouse
    task = prepare_task(queue, names=("A",))
    complete(queue, claim(queue, task), answer="Correct response")
    batch = queue.export_batch(task, limit=100, destination=EXPORT_DESTINATION)
    path = f"v1/country=SE/batch_id={batch.batch_id}/results.parquet"
    damaged_columns = publication.SOURCE_COLUMNS_SQL.replace(
        "answer_text", "'Damaged response' AS answer_text"
    )
    client.execute(
        "INSERT INTO FUNCTION s3(brave_history, filename=%(path)s, format='Parquet') "
        f"SELECT {damaged_columns} FROM {publication.SOURCE_SQL} WHERE export_batch_id=%(batch_id)s",
        {"path": path, "batch_id": batch.batch_id},
    )
    with pytest.raises(ValueError, match="S3 archive contents"):
        publish_results(queue, client, task, batch_size=100)
    assert queue.progress(task)["unpublished"] == 1
    with queue.transaction() as cursor:
        cursor.execute(
            "SELECT payload->>'answer_text' AS answer FROM processing.results WHERE task_id=%s",
            (task,),
        )
        assert cursor.fetchone()["answer"] == "Correct response"
    assert client.execute(
        "SELECT count() FROM corpscout.se_company_brave_domains FINAL"
    ) == [(0,)]


def test_other_country_requires_its_own_destination_and_is_not_misrouted(
    store, clickhouse
):
    queue, _ = store
    client, _ = clickhouse
    task = prepare_task(queue, names=("A",))
    complete(queue, claim(queue, task))
    with queue.transaction() as cursor:
        cursor.execute(
            'UPDATE processing.results SET payload=payload || \'{"country_code":"NO"}\'::jsonb WHERE task_id=%s',
            (task,),
        )
    with pytest.raises(ValueError, match="no_company_brave_domains"):
        publish_results(queue, client, task, batch_size=100)
    assert queue.progress(task)["unpublished"] == 1
    assert client.execute(
        "SELECT count() FROM corpscout.se_company_brave_domains FINAL"
    ) == [(0,)]


def test_history_reader_only_needs_select_on_the_country_view(store, clickhouse):
    queue, _ = store
    client, resource = clickhouse
    task = prepare_task(queue, names=("A",))
    complete(queue, claim(queue, task))
    publish_results(queue, client, task, batch_size=100)
    client.execute("CREATE USER brave_reader IDENTIFIED BY 'test'")
    client.execute(
        "GRANT SELECT ON corpscout.se_company_brave_domains_history TO brave_reader"
    )
    reader = Client(
        host="127.0.0.1", port=resource.port, user="brave_reader", password="test"
    )
    try:
        assert reader.execute(
            "SELECT count() FROM corpscout.se_company_brave_domains_history WHERE status='success'"
        ) == [(1,)]
        with pytest.raises(Exception):
            reader.execute(
                "SELECT count() FROM s3(brave_history, filename='v1/country=SE/batch_id=*/*.parquet', format='Parquet')"
            )
    finally:
        reader.disconnect()
