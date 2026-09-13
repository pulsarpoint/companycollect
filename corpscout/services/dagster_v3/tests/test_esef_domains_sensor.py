"""The sensor that launches esef_domains_job while stale documents exist."""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
)
from dagster_v3.defs.esef_filings.domains_sensor import (
    ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS,
    ESEF_DOMAINS_SENSOR_WORKERS,
    esef_domains_stale_sensor,
    should_launch,
)
from dagster_v3.definitions import defs as load_project_defs


def test_should_launch_only_with_stale_documents_and_nothing_running_or_failed() -> (
    None
):
    assert should_launch(stale_count=3, in_flight_count=0, recently_failed_count=0)
    assert not should_launch(stale_count=0, in_flight_count=0, recently_failed_count=0)
    assert not should_launch(stale_count=3, in_flight_count=1, recently_failed_count=0)
    assert not should_launch(stale_count=3, in_flight_count=0, recently_failed_count=1)


class FakeClient:
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


def test_sensor_launches_one_run_when_documents_are_stale() -> None:
    client = FakeClient(rows=[(1234,)])

    execution_data = esef_domains_stale_sensor.evaluate_tick(_context(client))

    assert execution_data.run_requests is not None
    assert len(execution_data.run_requests) == 1
    request = execution_data.run_requests[0]
    assert request.run_config == {
        "ops": {
            "esef_domains_clickhouse": {
                "config": {
                    "max_documents": ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS,
                    "workers": ESEF_DOMAINS_SENSOR_WORKERS,
                }
            }
        }
    }
    assert request.tags["dagster/priority"] == "5"
    assert request.tags["launched_by"] == "esef_domains_stale_sensor"
    executed_sql, executed_params = client.executed[0]
    assert executed_sql.startswith("SELECT count() FROM (")
    assert executed_params == {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION}


def test_sensor_skips_when_nothing_is_stale() -> None:
    client = FakeClient(rows=[(0,)])

    execution_data = esef_domains_stale_sensor.evaluate_tick(_context(client))

    assert not execution_data.run_requests
    assert execution_data.skip_message is not None
    assert "0 stale" in execution_data.skip_message


def test_sensor_is_registered_and_running_by_default() -> None:
    assert esef_domains_stale_sensor.name == "esef_domains_stale_sensor"
    assert esef_domains_stale_sensor.default_status == dg.DefaultSensorStatus.RUNNING
    assert esef_domains_stale_sensor.minimum_interval_seconds == 1800
    assert esef_domains_stale_sensor.job.name == "esef_domains_job"
