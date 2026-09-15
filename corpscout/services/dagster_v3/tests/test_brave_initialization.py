"""Filter selection, crash recovery and the two-asset workflow at real DB boundaries."""

from contextlib import closing
from uuid import uuid4

import dagster as dg
import psycopg2
import pytest

from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore
from dagster_v3.defs.company_domains import browser as brave
from dagster_v3.defs.company_domains.assets import company_brave_search_results
from dagster_v3.defs.company_domains.input import (
    INPUT_RELATION,
    BraveInputConfig,
    company_brave_search_input,
)
from tests.test_brave_publication import clickhouse as clickhouse
from tests.test_company_domains_brave import BrowserFixture, PROXIES
from tests.test_company_domains_brave_integration import MIGRATION

pytest_plugins = ["tests.test_processing_store"]


@pytest.fixture
def task_inputs(clickhouse):
    client, resource = clickhouse
    for statement in (
        MIGRATION.with_name("000413_corpscout_company_brave_input_tasks.up.sql")
        .read_text()
        .split(";")
    ):
        if statement.strip():
            client.execute(statement)
    return client, resource


def initialize(resource, dsn, task, **filters):
    config = {
        "source_relation": "corpscout.se_company_basic_info",
        "company_name_column": "legal_name",
        "country_code": "SE",
        "source_final": True,
        **filters,
    }
    if task is not None:
        config["task_id"] = task
    return dg.materialize(
        [company_brave_search_input],
        resources={
            "clickhouse": resource,
            "processing": ProcessingResource(postgres_url=dsn),
        },
        run_config={"ops": {"company_brave_search_input": {"config": config}}},
    )


def test_filters_prepare_only_selected_companies_without_postgres_input_rows(
    store, task_inputs
):
    queue, dsn = store
    client, resource = task_inputs
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [
            ("1", "First AB", "active"),
            ("2", "Second AB", "inactive"),
            ("3", "Third AB", "active"),
        ],
    )
    task = str(uuid4())
    assert initialize(
        resource, dsn, task, company_ids=["1", "2"], filters={"status": ["active"]}
    ).success
    assert queue.task(task)["status"] == "selected"
    assert queue.progress(task)["total"] == queue.progress(task)["remaining"] == 1
    assert queue.task(task)["admitted_count"] == 0
    with queue.transaction() as cursor:
        cursor.execute("SELECT count(*) FROM processing.items")
        assert cursor.fetchone()["count"] == 0
    assert client.execute(
        f"SELECT task_id,input_id,company_name FROM {INPUT_RELATION}"
    ) == [(task, "SE:1", "First AB")]
    assert client.execute("SELECT count() FROM corpscout.se_company_basic_info") == [
        (3,)
    ]
    # Filter values are bound data, even when they resemble SQL.
    empty = str(uuid4())
    assert initialize(
        resource, dsn, empty, filters={"status": ["active') OR 1=1 --"]}
    ).success
    assert queue.progress(empty)["total"] == 0
    assert queue.task(empty)["status"] == "selected"


def test_selections_are_isolated_and_rematerialization_keeps_original_values(
    store, task_inputs
):
    queue, dsn = store
    client, resource = task_inputs
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [("1", "Original", "active"), ("2", "Second", "active")],
    )
    first, second = str(uuid4()), str(uuid4())
    initialize(resource, dsn, first, company_ids=["1"])
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [("1", "Renamed", "active")],
    )
    initialize(resource, dsn, second, company_ids=["1", "2"])
    initialize(resource, dsn, first, company_ids=["1"])
    assert client.execute(
        f"SELECT task_id,count() FROM {INPUT_RELATION} GROUP BY task_id ORDER BY task_id"
    ) == sorted([(first, 1), (second, 2)])
    first_source = ClickHouseInputQueue(
        resource, INPUT_RELATION, selection_task_id=first
    )
    second_source = ClickHouseInputQueue(
        resource, INPUT_RELATION, selection_task_id=second
    )
    assert (
        first_source.read(queue.task(first)["source_info"])[0]["company_name"]
        == "Original"
    )
    assert (
        second_source.read(queue.task(second)["source_info"])[0]["company_name"]
        == "Renamed"
    )
    assert "task_id" not in first_source.read(queue.task(first)["source_info"])[0]
    with pytest.raises(ValueError, match="different selection"):
        initialize(resource, dsn, first, company_ids=["2"])
    assert first_source.inspect()["total"] == 1


def test_retry_replaces_only_unconfirmed_rows_for_its_own_task(
    store, task_inputs, monkeypatch
):
    queue, dsn = store
    client, resource = task_inputs
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [("1", "First", "active"), ("2", "Second", "active")],
    )
    retained, interrupted = str(uuid4()), str(uuid4())
    initialize(resource, dsn, retained, company_ids=["1"])
    real_inspect = ClickHouseInputQueue.inspect

    def interrupted_after_insert(self):
        raise ConnectionError("simulated interruption after ClickHouse insert")

    monkeypatch.setattr(ClickHouseInputQueue, "inspect", interrupted_after_insert)
    with pytest.raises(ConnectionError, match="simulated interruption"):
        initialize(resource, dsn, interrupted, company_ids=["1", "2"])
    assert queue.task(interrupted)["status"] == "preparing"
    assert client.execute(
        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
        {"task": interrupted},
    ) == [(2,)]
    monkeypatch.setattr(ClickHouseInputQueue, "inspect", real_inspect)
    assert initialize(resource, dsn, interrupted, company_ids=["1", "2"]).success
    assert queue.task(interrupted)["status"] == "selected"
    assert client.execute(
        f"SELECT task_id,count() FROM {INPUT_RELATION} GROUP BY task_id ORDER BY task_id"
    ) == sorted([(retained, 1), (interrupted, 2)])


def test_initialization_lock_fences_competing_preparers(store):
    queue, dsn = store
    task = str(uuid4())
    with queue.selection_lock(task), closing(psycopg2.connect(dsn)) as connection:
        other = ProcessingStore(connection)
        with pytest.raises(ValueError, match="already being initialized"):
            with other.selection_lock(task):
                pytest.fail("competing preparer acquired the same selection")
    with queue.selection_lock(task):
        pass


@pytest.mark.parametrize(
    "explicit_task_id", [None, "b3509ce0-624c-40a7-be0b-9ea87b7b0625"]
)
def test_combined_materialization_shares_task_and_saves_custom_queries(
    store, task_inputs, monkeypatch, explicit_task_id
):
    queue, dsn = store
    client, resource = task_inputs
    fixture = BrowserFixture()
    fixture.release_slow.set()
    monkeypatch.setattr(brave, "launch", fixture.launch)
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [(str(i), f"Company {i}", "active") for i in range(8)],
    )
    resources = {
        "clickhouse": resource,
        "processing_clickhouse": resource,
        "processing": ProcessingResource(postgres_url=dsn),
        "company_brave_browser": brave.BraveBrowserResource(**PROXIES),
    }
    config = {
        "source_relation": "corpscout.se_company_basic_info",
        "company_name_column": "legal_name",
        "country_code": "SE",
        "source_final": True,
        "company_ids": [str(i) for i in range(8)],
    }
    if explicit_task_id is not None:
        config["task_id"] = explicit_task_id
    result = dg.materialize(
        [company_brave_search_input, company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {
                "company_brave_search_input": {"config": config},
                "company_brave_search_results": {
                    "config": {
                        "query_template": "Who owns {company_name}?",
                        "query_type": "owner",
                        "freshness_days": 0,
                        "input_batch_size": 4,
                    }
                },
            }
        },
    )
    assert result.success
    task = explicit_task_id or result.run_id
    assert queue.task(task)["source_info"]["selection_task_id"] == task
    assert queue.progress(task)["succeeded"] == 8
    assert queue.progress(task)["remaining"] == queue.progress(task)["unpublished"] == 0
    assert fixture.total_peak == 4
    assert client.execute(
        "SELECT count(),uniqExact(query) FROM corpscout.company_brave_info WHERE task_id=%(task)s AND query_type='owner' AND startsWith(query,'Who owns Company ')",
        {"task": task},
    ) == [(8, 8)]
    # Rerunning initialization after processing is also idempotent.
    initialize(resource, dsn, task, company_ids=config["company_ids"])
    before = len(fixture.queries)
    assert dg.materialize(
        [company_brave_search_results],
        resources=resources,
        run_config={
            "ops": {"company_brave_search_results": {"config": {"task_id": task}}}
        },
    ).success
    assert len(fixture.queries) == before


def test_initialization_keeps_three_million_selected_rows_in_clickhouse(
    store, task_inputs
):
    queue, dsn = store
    client, resource = task_inputs
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info SELECT toString(number),concat('Company ',toString(number)),'active' FROM numbers(3000000)"
    )
    task = str(uuid4())
    assert initialize(resource, dsn, task, filters={"status": ["active"]}).success
    assert queue.task(task)["status"] == "selected"
    assert queue.progress(task)["remaining"] == 3_000_000
    assert queue.task(task)["admitted_count"] == 0
    with queue.transaction() as cursor:
        cursor.execute("SELECT count(*) FROM processing.items")
        assert cursor.fetchone()["count"] == 0
        cursor.execute("SELECT count(*) FROM processing.results")
        assert cursor.fetchone()["count"] == 0
    assert client.execute(
        f"SELECT count(),uniqExact(input_id) FROM {INPUT_RELATION} WHERE task_id=%(task)s",
        {"task": task},
    ) == [(3_000_000, 3_000_000)]


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"filters": {"status": []}},
        {"filters": {"status; DROP TABLE x": ["active"]}},
        {"company_ids": ["1"], "company_name_column": "legal_name)"},
    ],
)
def test_initialization_requires_explicit_selection_and_plain_columns(values):
    with pytest.raises(ValueError):
        BraveInputConfig(
            source_relation="corpscout.se_company_basic_info",
            country_code="SE",
            **values,
        )


def test_populated_queue_migration_preserves_legacy_selection_and_table_uuid(
    store, clickhouse
):
    queue, dsn = store
    client, resource = clickhouse
    client.execute(
        f"INSERT INTO {INPUT_RELATION} VALUES", [("SE:1", "1", "Legacy name", "SE")]
    )
    legacy_info = ClickHouseInputQueue(resource, INPUT_RELATION).inspect()
    legacy_task = str(uuid4())
    queue.register(
        legacy_task,
        processor="brave-v2",
        config={"input_relation": INPUT_RELATION},
        work_config={},
        source_info=legacy_info,
    )
    for statement in (
        MIGRATION.with_name("000413_corpscout_company_brave_input_tasks.up.sql")
        .read_text()
        .split(";")
    ):
        if statement.strip():
            client.execute(statement)
    from tests.test_processing_store import MIGRATION as PG_MIGRATION

    with queue.transaction() as cursor:
        cursor.execute(
            PG_MIGRATION.with_name(
                "000122_processing_input_initialization.up.sql"
            ).read_text()
        )
    legacy_info = queue.task(legacy_task)["source_info"]
    assert legacy_info["selection_task_id"] == ""
    client.execute(
        "INSERT INTO corpscout.se_company_basic_info VALUES",
        [("1", "New selection name", "active")],
    )
    initialize(resource, dsn, str(uuid4()), company_ids=["1"])
    source = ClickHouseInputQueue(resource, INPUT_RELATION, selection_task_id="")
    assert source.inspect() == legacy_info
    assert source.read(legacy_info)[0]["company_name"] == "Legacy name"
    assert queue.progress(legacy_task)["total"] == 1
