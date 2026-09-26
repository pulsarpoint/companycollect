"""Input selection and retry recovery against disposable ClickHouse/PostgreSQL."""

import subprocess
import time
from pathlib import Path
from uuid import uuid4

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource
from pydantic import ValidationError

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
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
                "000453_corpscout_ip_enrichment_queue_contract.up.sql",
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
    ],
)
def test_invalid_or_ambiguous_selection_is_rejected(config):
    with pytest.raises(ValidationError):
        IpEnrichmentInputConfig(**config)


def test_explicit_ips_are_canonical_deduplicated_and_frozen(database, store):
    client, resource = database
    queue, dsn = store
    task = str(uuid4())
    ips = [
        " 8.8.8.8 ",
        "8.8.8.8",
        "2001:4860:4860:0:0:0:0:8888",
        "127.0.0.1",
        "::ffff:0808:0808",
    ]
    result = materialize(
        resource, dsn, task_id=task, ips=ips, source_name="manual-test"
    )
    assert result.success
    assert client.execute(
        f"SELECT ip, ip_version, source_name, source_record_id FROM {INPUT_RELATION} ORDER BY ip"
    ) == [
        ("127.0.0.1", 4, "manual-test", "127.0.0.1"),
        ("2001:4860:4860::8888", 6, "manual-test", "2001:4860:4860::8888"),
        ("8.8.8.8", 4, "manual-test", "8.8.8.8"),
        ("::ffff:8.8.8.8", 6, "manual-test", "::ffff:8.8.8.8"),
    ]
    first_rows = client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id")
    assert materialize(
        resource, dsn, task_id=task, ips=list(reversed(ips)), source_name="manual-test"
    ).success
    assert (
        client.execute(f"SELECT * FROM {INPUT_RELATION} ORDER BY input_id")
        == first_rows
    )
    assert queue.task(task)["status"] == "selected"
    info = queue.task(task)["source_info"]
    assert info["total"] == info["unique_ips"] == 4
    assert (
        len(
            ClickHouseInputQueue(resource, INPUT_RELATION, selection_task_id=task).read(
                info, limit=10
            )
        )
        == 4
    )
    with queue.transaction() as cursor:
        cursor.execute("SELECT count(*) AS n FROM processing.items")
        assert cursor.fetchone()["n"] == 0
    with pytest.raises(ValueError, match="different selection"):
        materialize(
            resource, dsn, task_id=task, ips=["1.1.1.1"], source_name="manual-test"
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
    task = str(uuid4())
    config = dict(
        task_id=task,
        source_relation="corpscout.ip_source_test",
        ip_column="address",
        source_record_id_column="record_id",
        observed_at_column="observed_at",
        source_final=True,
        filters={"active": ["1"], "country": ["US"]},
    )
    assert materialize(resource, dsn, **config).success
    assert client.execute(
        f"SELECT ip, source_record_id, toString(observed_at) FROM {INPUT_RELATION} ORDER BY ip, source_record_id"
    ) == [
        ("2001:4860:4860::8888", "a", "2026-09-01 00:00:00.000000"),
        ("8.8.8.8", "a", "2026-09-02 00:00:00.000000"),
        ("8.8.8.8", "b", "2026-09-01 00:00:00.000000"),
    ]
    assert queue.task(task)["source_info"]["unique_ips"] == 2
    client.execute(
        "INSERT INTO corpscout.ip_source_test VALUES ('new', '4.4.4.4', 1, 'US', '2026-09-01', 1)"
    )
    assert materialize(resource, dsn, **config).success
    assert queue.task(task)["total"] == 3
    # A bounded new selection freezes only the first two distinct submission rows.
    bounded = materialize(
        resource, dsn, **{**config, "task_id": str(uuid4()), "max_rows": 2}
    )
    assert (
        bounded.asset_materializations_for_node("ip_enrichment_input")[0]
        .metadata["selected_inputs"]
        .value
        == 2
    )
    empty_task = str(uuid4())
    assert materialize(
        resource,
        dsn,
        **{**config, "task_id": empty_task, "filters": {"country": ["US') OR 1=1 --"]}},
    ).success
    assert queue.task(empty_task)["total"] == 0


def test_retry_cleans_only_its_partial_task(database, store, monkeypatch):
    client, resource = database
    queue, dsn = store
    retained, interrupted = str(uuid4()), str(uuid4())
    assert materialize(resource, dsn, task_id=retained, ips=["1.1.1.1"]).success
    real_inspect = ClickHouseInputQueue.inspect

    def fail_after_insert(self):
        raise ConnectionError("interrupted after insert")

    monkeypatch.setattr(ClickHouseInputQueue, "inspect", fail_after_insert)
    with pytest.raises(ConnectionError, match="interrupted after insert"):
        materialize(resource, dsn, task_id=interrupted, ips=["8.8.8.8", "1.1.1.1"])
    assert queue.task(interrupted)["status"] == "preparing"
    monkeypatch.setattr(ClickHouseInputQueue, "inspect", real_inspect)
    assert materialize(
        resource, dsn, task_id=interrupted, ips=["8.8.8.8", "1.1.1.1"]
    ).success
    assert client.execute(
        f"SELECT toString(task_id) AS task, count() FROM {INPUT_RELATION} GROUP BY task ORDER BY task"
    ) == sorted([(retained, 1), (interrupted, 2)])


def test_missing_source_column_does_not_admit_task(database, store):
    client, resource = database
    queue, dsn = store
    task = str(uuid4())
    with pytest.raises(ValueError, match="source is missing columns: missing"):
        materialize(
            resource,
            dsn,
            task_id=task,
            source_relation="corpscout.ip_source_test",
            ip_column="missing",
            select_all=True,
        )
    assert queue.task(task)["status"] == "preparing"
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
