"""Tests for the sensor that drains ESEF processed weeks whose stored document
artifacts predate the parser's current ``ARTIFACT_SCHEMA_VERSION``.

``weeks_to_launch`` is a pure selection function tested in isolation (no
ClickHouse, no Dagster instance). The sensor evaluation tests drive the real
``@dg.sensor``-decorated definition through ``evaluate_tick`` against a fake
ClickHouse client -- same scripted-client technique as
``tests/test_se_company_common.py``'s ``FakeClient``/``FakeClickhouse`` pair --
and an ephemeral ``DagsterInstance`` so ``context.instance.get_run_records``
answers from real (empty) run storage rather than a mock.
"""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION
from dagster_v3.defs.esef_filings.stale_weeks_sensor import (
    ESEF_STALE_WEEKS_MAX_INFLIGHT,
    ESEF_STALE_WEEKS_PARSE_WORKERS,
    esef_stale_weeks_sensor,
    stale_weeks_sql,
    weeks_to_launch,
)
from dagster_v3.definitions import defs as load_project_defs


# --- weeks_to_launch (pure selection) ---------------------------------------


def test_weeks_to_launch_with_nothing_in_flight_takes_the_first_two() -> None:
    result = weeks_to_launch(
        stale=["2024-01-07", "2024-01-14", "2024-01-21"],
        in_flight=[],
        recently_failed=[],
        max_inflight=2,
    )

    assert result == ["2024-01-07", "2024-01-14"]


def test_weeks_to_launch_with_max_inflight_already_running_launches_none() -> None:
    result = weeks_to_launch(
        stale=["2024-01-07", "2024-01-14", "2024-01-21"],
        in_flight=["2024-01-07", "2024-01-14"],
        recently_failed=[],
        max_inflight=2,
    )

    assert result == []


def test_weeks_to_launch_skips_a_recently_failed_first_week() -> None:
    result = weeks_to_launch(
        stale=["2024-01-07", "2024-01-14", "2024-01-21"],
        in_flight=["2024-01-28"],
        recently_failed=["2024-01-07"],
        max_inflight=2,
    )

    assert result == ["2024-01-14"]


def test_weeks_to_launch_respects_max_inflight_below_available_stale_weeks() -> None:
    result = weeks_to_launch(
        stale=["2024-01-07", "2024-01-14", "2024-01-21"],
        in_flight=[],
        recently_failed=[],
        max_inflight=1,
    )

    assert result == ["2024-01-07"]


# --- stale_weeks_sql ---------------------------------------------------------


def test_stale_weeks_sql_binds_schema_version_and_reads_esef_disclosures() -> None:
    sql = stale_weeks_sql()

    assert "corpscout.esef_disclosures" in sql
    assert "%(schema_version)s" in sql
    assert "order by week" in sql.lower()


# --- sensor evaluation --------------------------------------------------------


class FakeClient:
    """Records executed SQL; answers the one read query with canned rows."""

    def __init__(self, rows: list[tuple]) -> None:
        self.executed: list[tuple[str, object]] = []
        self.rows = rows

    def execute(self, sql: str, parameters: object = None) -> list[tuple]:
        self.executed.append((sql, parameters))
        return self.rows


class FakeClickhouse:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def _context(client: FakeClient) -> dg.SensorEvaluationContext:
    return dg.build_sensor_context(
        instance=dg.DagsterInstance.ephemeral(),
        resources={"clickhouse": FakeClickhouse(client)},
        definitions=load_project_defs(),
    )


def test_sensor_launches_a_run_request_per_stale_week_when_nothing_is_running() -> None:
    client = FakeClient(rows=[("2024-01-07",), ("2024-01-14",)])
    context = _context(client)

    execution_data = esef_stale_weeks_sensor.evaluate_tick(context)

    assert execution_data.run_requests is not None
    assert len(execution_data.run_requests) == 2
    partition_keys = {request.partition_key for request in execution_data.run_requests}
    assert partition_keys == {"2024-01-07", "2024-01-14"}
    for request in execution_data.run_requests:
        assert request.run_config == {
            "ops": {
                "esef_document_artifacts_s3": {
                    "config": {"parse_workers": ESEF_STALE_WEEKS_PARSE_WORKERS}
                }
            }
        }
        assert request.tags["dagster/priority"] == "5"
        assert request.tags["launched_by"] == "esef_stale_weeks_sensor"

    # The query bound the parser's current schema version.
    executed_sql, executed_params = client.executed[0]
    assert executed_params == {"schema_version": ARTIFACT_SCHEMA_VERSION}
    assert "corpscout.esef_disclosures" in executed_sql


def test_sensor_skips_a_week_that_is_not_a_valid_partition_key() -> None:
    client = FakeClient(rows=[("2024-01-07",), ("2029-13-45",)])
    context = _context(client)

    execution_data = esef_stale_weeks_sensor.evaluate_tick(context)

    assert execution_data.run_requests is not None
    assert len(execution_data.run_requests) == 1
    assert execution_data.run_requests[0].partition_key == "2024-01-07"


def test_sensor_skips_when_no_weeks_are_stale() -> None:
    client = FakeClient(rows=[])
    context = _context(client)

    execution_data = esef_stale_weeks_sensor.evaluate_tick(context)

    assert not execution_data.run_requests
    assert execution_data.skip_message is not None


def test_sensor_respects_max_inflight_configuration() -> None:
    assert ESEF_STALE_WEEKS_MAX_INFLIGHT == 2


# --- registration -------------------------------------------------------------


def test_sensor_is_registered_running_every_thirty_minutes() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sensor_def = repo.get_sensor_def("esef_stale_weeks_sensor")

    assert sensor_def.minimum_interval_seconds == 1800
    assert sensor_def.default_status == dg.DefaultSensorStatus.RUNNING
