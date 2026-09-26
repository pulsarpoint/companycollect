"""Input selection and retry recovery against disposable ClickHouse/PostgreSQL."""

import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource
from pydantic import ValidationError

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.ip_enrichment.input import (
    INPUT_RELATION,
    IpEnrichmentInputConfig,
    ip_enrichment_input,
)
from tests.clickhouse_local import CLICKHOUSE_IMAGE, clickhouse_local_command
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
    store as store,
)


@pytest.fixture(scope="module")
def server():
    clickhouse_local_command()
    name = "ip-input-test-" + uuid4().hex
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
            "CLICKHOUSE_USER=test",
            "-e",
            "CLICKHOUSE_PASSWORD=test",
            CLICKHOUSE_IMAGE,
        ],
        capture_output=True,
        check=True,
    )
    try:
        port = int(
            subprocess.check_output(
                ["docker", "port", name, "9000"],
                text=True,
            )
            .strip()
            .rsplit(":", 1)[1]
        )
        deadline = time.monotonic() + 30
        while True:
            probe = subprocess.run(
                [
                    "docker",
                    "exec",
                    name,
                    "clickhouse-client",
                    "--user",
                    "test",
                    "--password",
                    "test",
                    "--query",
                    "SELECT 1",
                ],
                capture_output=True,
                check=False,
            )
            if probe.returncode == 0:
                break
            assert time.monotonic() < deadline, probe.stderr.decode()
            time.sleep(0.2)
        resource = ClickhouseResource(
            host="127.0.0.1",
            port=port,
            user="test",
            password="test",
            database="default",
        )
        with resource.get_connection() as client:
            migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
            for name in (
                "000433_corpscout_ip_enrichment.up.sql",
                "000458_corpscout_ip_enrichment_queue_contract.up.sql",
            ):
                for statement in (
                    (migrations / name).read_text(encoding="utf-8").split(";")
                ):
                    if statement.strip():
                        client.execute(statement)
            yield client, resource
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


@pytest.fixture
def database(server):
    client, resource = server
    client.execute(f"TRUNCATE TABLE {INPUT_RELATION}")
    client.execute("TRUNCATE TABLE corpscout.ip_enrichment_results")
    client.execute("DROP TABLE IF EXISTS corpscout.ip_source_test")
    client.execute("""CREATE TABLE corpscout.ip_source_test (
        record_id String, address Nullable(String), active UInt8, country String,
        observed_at DateTime64(6, 'UTC'), version UInt64
    ) ENGINE=ReplacingMergeTree(version) ORDER BY (record_id, ifNull(address, ''))""")
    return client, resource


def materialize(resource, dsn, **config):
    return dg.materialize(
        [ip_enrichment_input],
        resources={
            "clickhouse": resource,
            "processing": ProcessingResource(postgres_url=dsn),
        },
        run_config={"ops": {"ip_enrichment_input": {"config": config}}},
    )


def metadata(result):
    return {
        key: value.value
        for key, value in result.asset_materializations_for_node("ip_enrichment_input")[
            0
        ].metadata.items()
    }


def scope():
    return "scope-" + uuid4().hex


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"ips": []},
        {"ips": ["8.8.8.8/24"]},
        {"ips": ["fe80::1%eth0"]},
        {"ips": ["bad"]},
        {"ips": ["8.8.8.8"], "source_relation": "corpscout.ips"},
        {"ips": ["8.8.8.8"], "filters": {"country": ["US"]}},
        {"source_relation": "corpscout.ips"},
        {"source_relation": INPUT_RELATION, "select_all": True},
        {"source_relation": "corpscout.ips; DROP TABLE x", "select_all": True},
        {"source_relation": "corpscout.ips", "ip_column": "ip`", "select_all": True},
        {"source_relation": "corpscout.ips", "filters": {"ip": []}},
        {"ips": ["8.8.8.8"], "max_rows": 0},
        {"ips": ["8.8.8.8"], "submission_id": "not-a-uuid"},
        {"ips": ["8.8.8.8"], "queue_scope": "  "},
        {"retry_failed_task_id": "not-a-uuid"},
        {"retry_failed_task_id": str(uuid4()), "ips": ["8.8.8.8"]},
        {"retry_failed_task_id": str(uuid4()), "filters": {"country": ["US"]}},
        {"retry_failed_task_id": str(uuid4()), "source_relation": "corpscout.ips"},
    ],
)
def test_invalid_or_ambiguous_selection_is_rejected(config):
    with pytest.raises(ValidationError):
        IpEnrichmentInputConfig(**config)


def test_explicit_ips_are_canonical_deduplicated_and_appended_to_one_draft(
    database, store
):
    client, resource = database
    queue, dsn = store
    space = scope()
    submission = str(uuid4())
    ips = [
        " 8.8.8.8 ",
        "8.8.8.8",
        "2001:4860:4860:0:0:0:0:8888",
        "127.0.0.1",
        "::ffff:0808:0808",
    ]
    first = metadata(
        materialize(
            resource,
            dsn,
            queue_scope=space,
            submission_id=submission,
            ips=ips,
            source_name="manual-test",
        )
    )
    assert (first["input_count"], first["total"]) == (4, 4)
    assert client.execute(
        f"SELECT ip, ip_version, source_name, source_record_id, submission_id FROM {INPUT_RELATION} ORDER BY ip"
    ) == [
        ("127.0.0.1", 4, "manual-test", "127.0.0.1", submission),
        ("2001:4860:4860::8888", 6, "manual-test", "2001:4860:4860::8888", submission),
        ("8.8.8.8", 4, "manual-test", "8.8.8.8", submission),
        ("::ffff:8.8.8.8", 6, "manual-test", "::ffff:8.8.8.8", submission),
    ]
    assert client.execute(
        # "%%" (not "%"): clickhouse_driver's params-bound execute() runs `query % escaped`,
        # so a literal LIKE wildcard must be doubled to survive substitution.
        f"SELECT count() FROM {INPUT_RELATION} WHERE input_id LIKE '___:%%' AND toString(task_id) = %(task)s",
        {"task": first["task_id"]},
    ) == [(4,)]
    rows_before = client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id")
    # The same submission with the same selection is a no-op, even with a changed source.
    replay = metadata(
        materialize(
            resource,
            dsn,
            queue_scope=space,
            submission_id=submission,
            ips=list(reversed(ips)),
            source_name="manual-test",
        )
    )
    assert replay == first
    assert (
        client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id")
        == rows_before
    )
    # Another submission appends to the same draft; overlapping addresses are kept once.
    second = metadata(
        materialize(
            resource,
            dsn,
            queue_scope=space,
            ips=["8.8.8.8", "1.1.1.1"],
            source_name="manual-test",
        )
    )
    assert second["task_id"] == first["task_id"]
    assert (second["input_count"], second["total"]) == (1, 5)
    task = queue.task(first["task_id"])
    assert (
        task["status"] == "draft"
        and task["queue_scope"] == space
        and task["total"] == 5
    )
    with queue.transaction() as cursor:
        cursor.execute("SELECT count(*) AS n FROM processing.items")
        assert cursor.fetchone()["n"] == 0
        cursor.execute(
            "SELECT status, input_count, selection_config->'ips' AS ips FROM processing.input_submissions WHERE submission_id=%s",
            (submission,),
        )
        receipt = cursor.fetchone()
    assert receipt["status"] == "completed" and receipt["input_count"] == 4
    assert set(receipt["ips"]) == {
        "count",
        "sha256",
    }  # bulk values stay out of PostgreSQL
    with pytest.raises(ValueError, match="different selection"):
        materialize(
            resource,
            dsn,
            queue_scope=space,
            submission_id=submission,
            ips=["9.9.9.9"],
            source_name="manual-test",
        )


def test_table_filters_final_and_source_lineage(database, store):
    client, resource = database
    queue, dsn = store
    client.execute("""INSERT INTO corpscout.ip_source_test VALUES
        ('a', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('a', '8.8.8.8', 1, 'US', '2026-09-02', 2),
        ('b', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('a', '2001:4860:4860:0:0:0:0:8888', 1, 'US', '2026-09-01', 1),
        ('c', '1.1.1.1', 1, 'US', '2026-09-01', 1),
        ('c', '1.1.1.1', 0, 'US', '2026-09-02', 2),
        ('d', '9.9.9.9', 1, 'DE', '2026-09-01', 1),
        ('e', 'bad', 1, 'US', '2026-09-01', 1),
        ('f', NULL, 1, 'US', '2026-09-01', 1)""")
    space = scope()
    config = dict(
        queue_scope=space,
        source_relation="corpscout.ip_source_test",
        ip_column="address",
        source_record_id_column="record_id",
        observed_at_column="observed_at",
        source_final=True,
        filters={"active": ["1"], "country": ["US"]},
    )
    submission = str(uuid4())
    first = metadata(materialize(resource, dsn, submission_id=submission, **config))
    assert client.execute(
        f"SELECT ip, source_record_id, toString(observed_at), source_name FROM {INPUT_RELATION} ORDER BY ip, source_record_id"
    ) == [
        (
            "2001:4860:4860::8888",
            "a",
            "2026-09-01 00:00:00.000000",
            "corpscout.ip_source_test",
        ),
        ("8.8.8.8", "a", "2026-09-02 00:00:00.000000", "corpscout.ip_source_test"),
        ("8.8.8.8", "b", "2026-09-01 00:00:00.000000", "corpscout.ip_source_test"),
    ]
    client.execute(
        "INSERT INTO corpscout.ip_source_test VALUES ('new', '4.4.4.4', 1, 'US', '2026-09-01', 1)"
    )
    # A completed receipt is not re-evaluated; a new submission sees the new row.
    assert (
        metadata(materialize(resource, dsn, submission_id=submission, **config))
        == first
    )
    assert queue.task(first["task_id"])["total"] == 3
    later = metadata(materialize(resource, dsn, **config))
    assert (later["input_count"], later["total"]) == (1, 4)
    # A bounded selection in another scope freezes only the first two distinct submissions.
    bounded = metadata(
        materialize(resource, dsn, **{**config, "queue_scope": scope(), "max_rows": 2})
    )
    assert (bounded["input_count"], bounded["total"]) == (2, 2)
    empty = metadata(
        materialize(
            resource,
            dsn,
            **{
                **config,
                "queue_scope": scope(),
                "filters": {"country": ["US') OR 1=1 --"]},
            },
        )
    )
    assert (empty["input_count"], empty["total"]) == (0, 0)
    assert queue.task(empty["task_id"])["status"] == "draft"


def test_retry_replaces_only_its_own_submission_rows(database, store, monkeypatch):
    from clickhouse_driver import Client

    from dagster_v3.defs.common import draft_queue

    client, resource = database
    queue, dsn = store
    space = scope()
    kept = metadata(materialize(resource, dsn, queue_scope=space, ips=["1.1.1.1"]))
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if (
            query.lstrip().startswith(f"INSERT INTO {INPUT_RELATION}")
            and not interrupted
        ):
            interrupted = True
            raise ConnectionError("lost insert acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    submission = str(uuid4())
    with pytest.raises(ConnectionError, match="acknowledgement"):
        materialize(
            resource,
            dsn,
            queue_scope=space,
            submission_id=submission,
            ips=["8.8.8.8", "1.1.1.1"],
        )
    assert draft_queue.submission(queue, submission)["status"] == "failed"
    # The lost insert did land; the retry deletes only this submission's rows and re-inserts them.
    result = metadata(
        materialize(
            resource,
            dsn,
            queue_scope=space,
            submission_id=submission,
            ips=["8.8.8.8", "1.1.1.1"],
        )
    )
    assert result["task_id"] == kept["task_id"] and (
        result["input_count"],
        result["total"],
    ) == (1, 2)
    assert client.execute(
        f"SELECT ip, submission_id FROM {INPUT_RELATION} ORDER BY ip"
    ) == [("1.1.1.1", kept["submission_id"]), ("8.8.8.8", submission)]
    assert draft_queue.submission(queue, submission)["status"] == "completed"


def test_missing_source_column_fails_the_submission_without_rows(database, store):
    from dagster_v3.defs.common import draft_queue

    client, resource = database
    queue, dsn = store
    submission = str(uuid4())
    with pytest.raises(ValueError, match="source is missing columns: missing"):
        materialize(
            resource,
            dsn,
            queue_scope=scope(),
            submission_id=submission,
            source_relation="corpscout.ip_source_test",
            ip_column="missing",
            select_all=True,
        )
    assert draft_queue.submission(queue, submission)["status"] == "failed"
    assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(0,)]


@pytest.mark.parametrize(
    ("search", "excluded", "expected"),
    [
        ("8.8.", ["8.8.4.4"], ["8.8.8.8"]),
        ("8.8.8.8", [], ["8.8.8.8"]),
        ("2001:4860:0:0:0:0:0:8888", [], ["2001:4860::8888"]),
        ("", ["2001:4860:0:0:0:0:0:8888"], ["8.8.4.4", "8.8.8.8", "9.9.9.9"]),
    ],
)
def test_inventory_search_and_exclusions_match_all_pages(
    database, store, search, excluded, expected
):
    client, resource = database
    client.execute("""INSERT INTO corpscout.ip_source_test VALUES
        ('a', '8.8.8.8', 1, 'US', '2026-09-01', 1),
        ('b', '8.8.8.8', 1, 'US', '2026-09-02', 1),
        ('c', '8.8.4.4', 1, 'US', '2026-09-01', 1),
        ('d', '9.9.9.9', 1, 'US', '2026-09-01', 1),
        ('e', '2001:4860::8888', 1, 'US', '2026-09-01', 1),
        ('f', '8.8.0.1', 0, 'US', '2026-09-01', 1)""")
    assert materialize(
        resource,
        store[1],
        queue_scope=scope(),
        source_relation="corpscout.ip_source_test",
        ip_column="address",
        observed_at_column="observed_at",
        filters={"active": ["1"]},
        select_all=True,
        ip_search=search,
        excluded_ips=excluded,
    ).success
    assert [
        row[0] for row in client.execute(f"SELECT ip FROM {INPUT_RELATION} ORDER BY ip")
    ] == expected


@pytest.mark.parametrize(
    "config",
    [
        {"ips": ["8.8.8.8"], "ip_search": "8.8."},
        {"ips": ["8.8.8.8"], "excluded_ips": ["8.8.8.8"]},
        {"source_relation": "corpscout.ips", "ip_search": "%' OR 1=1"},
        {
            "source_relation": "corpscout.ips",
            "select_all": True,
            "excluded_ips": ["bad"],
        },
    ],
)
def test_invalid_inventory_filters(config):
    with pytest.raises(ValidationError):
        IpEnrichmentInputConfig(**config)


def test_failed_results_of_a_task_can_be_queued_again(database, store):
    client, resource = database
    failed_task = str(uuid4())
    completed = datetime(2026, 9, 1, tzinfo=UTC)
    client.execute(
        "INSERT INTO corpscout.ip_enrichment_results (ip, result_id, task_id, execution_id, input_id, completed_at, city_lookup_status, asn_lookup_status, rdap_lookup_status) VALUES",
        [
            (
                "8.8.8.8",
                str(uuid4()),
                failed_task,
                failed_task,
                "a",
                completed,
                "found",
                "found",
                "retryable_error",
            ),
            (
                "1.1.1.1",
                str(uuid4()),
                failed_task,
                failed_task,
                "b",
                completed,
                "found",
                "found",
                "found",
            ),
            (
                "9.9.9.9",
                str(uuid4()),
                str(uuid4()),
                failed_task,
                "c",
                completed,
                "terminal_error",
                "found",
                "found",
            ),
        ],
    )
    result = metadata(
        materialize(
            resource, store[1], queue_scope=scope(), retry_failed_task_id=failed_task
        )
    )
    assert (result["input_count"], result["total"]) == (1, 1)
    assert client.execute(
        f"SELECT ip, source_name, source_record_id FROM {INPUT_RELATION}"
    ) == [("8.8.8.8", "retry:" + failed_task, "8.8.8.8")]


def test_new_submissions_after_start_form_the_next_draft(database, store):
    from dagster_v3.defs.common import queue_execution
    from dagster_v3.defs.ip_enrichment.input import PROCESSOR_VERSION

    _, resource = database
    queue, dsn = store
    space = scope()
    first = metadata(materialize(resource, dsn, queue_scope=space, ips=["8.8.8.8"]))
    with queue.selection_lock(first["task_id"]):
        queue_execution.start_execution(
            queue,
            task_id=first["task_id"],
            processor=PROCESSOR_VERSION,
            profile={},
            execution_id=None,
            freshness_days=30,
            run_id=str(uuid4()),
            snapshot=lambda: ({"relation": INPUT_RELATION, "total": 1}, 1),
        )
    second = metadata(materialize(resource, dsn, queue_scope=space, ips=["1.1.1.1"]))
    assert second["task_id"] != first["task_id"]
    with pytest.raises(ValueError, match="open draft"):
        materialize(
            resource, dsn, queue_scope=space, task_id=first["task_id"], ips=["1.1.1.1"]
        )


def test_entry_table_follows_the_queue_contract(database):
    from clickhouse_driver.errors import ServerException

    from dagster_v3.defs.ip_enrichment.input import INPUT_ID_SQL

    client, _ = database
    assert client.execute(
        "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='corpscout' AND name='ip_enrichment_input'"
    ) == [("MergeTree", "task_id", "task_id, input_id")]
    identity = INPUT_ID_SQL.format(
        ip="'8.8.8.8'", source="'manual'", record="'8.8.8.8'"
    )
    # input_id is the bucket-prefixed identity computed in ClickHouse, nothing else.
    with pytest.raises(ServerException, match="valid_identity"):
        client.execute(
            f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id,submission_id) VALUES",
            [("task", "017:x", "8.8.8.8", "manual", "8.8.8.8", "run", "submission")],
        )
    # Every row names its submission; the retry delete relies on it.
    with pytest.raises(ServerException, match="valid_identity"):
        client.execute(
            f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id) "
            f"SELECT 'task', {identity}, '8.8.8.8', 'manual', '8.8.8.8', 'run'"
        )
    client.execute(
        f"INSERT INTO {INPUT_RELATION} (task_id,input_id,ip,source_name,source_record_id,source_run_id,submission_id) "
        f"SELECT 'task', {identity}, '8.8.8.8', 'manual', '8.8.8.8', 'run', 'submission'"
    )
    [(input_id, bucket)] = client.execute(
        f"SELECT input_id, bucket FROM {INPUT_RELATION}"
    )
    assert input_id == f"{bucket:03d}:" + '["manual","8.8.8.8","8.8.8.8"]'
