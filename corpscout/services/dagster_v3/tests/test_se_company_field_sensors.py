"""The field-registry sensors: the field-value sensor re-pointed under its old name
(the backoffice names it), and the candidate sensor with its own cursor on
extracted_at. Mirrors the ledger-sensor tests in test_se_company_common.py."""

import uuid
from contextlib import contextmanager

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.common import EPOCH
from dagster_v3.defs.se_company.fields.resolve import RESOLVE_ASSET
from dagster_v3.defs.se_company.fields.sensors import (
    MAX_SCOPED_COMPANY_IDS,
    build_candidate_cursor_sql,
    build_candidate_touched_sql,
    candidate_sensor,
    se_company_field_candidate_sensor,
    se_company_info_field_value_sensor,
)
from tests.test_se_company_common import _FakeLedgerClient

COMPANY = "5020077862"
OTHER = "5560125220"
T0 = "2026-08-20 11:59:59.000"
T1 = "2026-08-20 12:00:00.000"
T2 = "2026-08-20 12:00:01.000"


def _patch(monkeypatch, client) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self):
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def _tick(sensor: dg.SensorDefinition, resource: ClickhouseResource, cursor: str | None):
    return sensor.evaluate_tick(dg.build_sensor_context(cursor=cursor, resources={"clickhouse": resource}))


def test_the_field_value_sensor_keeps_its_name_and_launches_the_resolve_asset_for_real(monkeypatch) -> None:
    """A new decision must re-resolve its company through the registry-driven asset,
    for real (execute) -- and under the name dagster.server.ts starts and stops, so the
    backoffice needs no change and Dagster keeps the sensor's cursor across the deploy."""
    ledger = _FakeLedgerClient()
    ledger.append(COMPANY, str(uuid.UUID(int=7)), T1)
    resource = _patch(monkeypatch, ledger)

    assert se_company_info_field_value_sensor.name == "se_company_info_field_value_sensor"
    assert se_company_info_field_value_sensor.job_name == "se_company_field_resolve_job"
    assert se_company_info_field_value_sensor.default_status == dg.DefaultSensorStatus.STOPPED
    data = _tick(se_company_info_field_value_sensor, resource, None)
    assert data.run_requests is not None and data.run_requests[0].run_config == {
        "ops": {RESOLVE_ASSET: {"config": {"execute": True, "company_ids": [COMPANY]}}}}
    statements = [sql for sql, _ in ledger.executed]
    assert any("corpscout.se_company_info_field_value" in sql for sql in statements)
    assert all("correction_id" not in sql for sql in statements)
    assert any("argMax(value_id, (created_at, value_id))" in sql for sql in statements)


class _FakeCandidateClient:
    """In-memory candidate table answering candidate_sensor's two queries."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str]] = []  # (company_id, extracted_at)
        self.executed: list[tuple[str, object]] = []

    def append(self, company_id: str, extracted_at: str) -> None:
        self.rows.append((company_id, extracted_at))

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[tuple[object, ...]]:
        self.executed.append((sql, params))
        if "max(extracted_at)" in sql:
            if not self.rows:
                return [(0, "")]
            return [(len(self.rows), max(extracted_at for _, extracted_at in self.rows))]
        if "SELECT DISTINCT company_id" in sql:
            assert params is not None
            touched = sorted({company for company, extracted_at in self.rows if extracted_at >= str(params["since"])})
            return [(company,) for company in touched[: int(params["limit"])]]
        raise AssertionError(sql)


def test_candidate_sensor_sql_shapes_and_declaration() -> None:
    assert build_candidate_cursor_sql("se_company_field_candidate") == (
        "SELECT count(), if(count() = 0, '', toString(max(extracted_at)))\n"
        "FROM corpscout.se_company_field_candidate")
    assert build_candidate_touched_sql("se_company_field_candidate") == (
        "SELECT DISTINCT company_id\n"
        "FROM corpscout.se_company_field_candidate\n"
        "WHERE extracted_at >= parseDateTime64BestEffort(%(since)s, 3, 'UTC')\n"
        "ORDER BY company_id\n"
        "LIMIT %(limit)s")
    assert MAX_SCOPED_COMPANY_IDS == 20_000
    assert se_company_field_candidate_sensor.name == "se_company_field_candidate_sensor"
    assert se_company_field_candidate_sensor.job_name == "se_company_field_resolve_job"
    assert se_company_field_candidate_sensor.default_status == dg.DefaultSensorStatus.STOPPED
    assert se_company_field_candidate_sensor.minimum_interval_seconds == 60


def test_candidate_sensor_first_tick_uses_the_epoch_boundary_and_scopes_the_run(monkeypatch) -> None:
    client = _FakeCandidateClient()
    client.append(COMPANY, T1)
    client.append(OTHER, T2)
    resource = _patch(monkeypatch, client)

    data = _tick(se_company_field_candidate_sensor, resource, None)

    touched = next(entry for entry in client.executed if "SELECT DISTINCT company_id" in entry[0])
    assert touched[1] == {"since": EPOCH, "limit": MAX_SCOPED_COMPANY_IDS + 1}
    assert data.cursor == f"2:{T2}"
    assert data.run_requests is not None and len(data.run_requests) == 1
    assert data.run_requests[0].run_key == f"se_company_field_candidate:2:{T2}"
    assert data.run_requests[0].run_config == {
        "ops": {RESOLVE_ASSET: {"config": {"execute": True, "company_ids": [COMPANY, OTHER]}}}}


def test_candidate_sensor_skips_on_an_empty_table_and_on_an_unchanged_cursor(monkeypatch) -> None:
    empty = _tick(se_company_field_candidate_sensor, _patch(monkeypatch, _FakeCandidateClient()), None)
    assert empty.skip_message == "No rows in se_company_field_candidate"

    client = _FakeCandidateClient()
    client.append(COMPANY, T1)
    unchanged = _tick(se_company_field_candidate_sensor, _patch(monkeypatch, client), f"1:{T1}")
    assert unchanged.skip_message == "No new rows in se_company_field_candidate" and not unchanged.run_requests
    assert len(client.executed) == 1  # the unchanged cursor never issues the touched scan


def test_candidate_sensor_re_selects_the_boundary_company_after_a_mid_run_tick(monkeypatch) -> None:
    """An extractor stamps ONE extracted_at for its whole run and reuses it for every
    page, so a tick that lands mid-run cursors that instant while pages are still being
    written. The boundary must therefore be inclusive: the later pages carry the SAME
    extracted_at, and a strict > would advance the cursor without ever resolving them.
    Re-resolving the boundary company is the price, and it is the cheap side."""
    client = _FakeCandidateClient()
    client.append(COMPANY, T1)  # page 2 of a run whose page 1 was cursored at T1
    client.append(OTHER, T0)
    data = _tick(se_company_field_candidate_sensor, _patch(monkeypatch, client), f"1:{T1}")
    assert data.cursor == f"2:{T1}"
    assert data.run_requests is not None and len(data.run_requests) == 1
    assert data.run_requests[0].run_config == {
        "ops": {RESOLVE_ASSET: {"config": {"execute": True, "company_ids": [COMPANY]}}}}


def test_candidate_sensor_launches_an_unscoped_run_past_the_id_cap(monkeypatch) -> None:
    """A full extraction touches millions of companies: rather than carrying them all in
    run config, the sensor launches an unscoped run and the changed-company scan finds
    them through new_candidates."""
    sensor = candidate_sensor(
        name="capped", table="se_company_field_candidate",
        job=dg.define_asset_job("capped_job", selection=dg.AssetSelection.assets(RESOLVE_ASSET)),
        asset_names=(RESOLVE_ASSET,), extra_config={"execute": True}, max_scoped_company_ids=2)
    client = _FakeCandidateClient()
    for index in range(3):
        client.append(f"55600000{index:02d}", T1)
    data = _tick(sensor, _patch(monkeypatch, client), None)
    touched = next(entry for entry in client.executed if "SELECT DISTINCT company_id" in entry[0])
    assert touched[1]["limit"] == 3  # cap + 1: one row more than the cap is the overflow signal
    assert data.run_requests is not None
    assert data.run_requests[0].run_config == {"ops": {RESOLVE_ASSET: {"config": {"execute": True, "company_ids": []}}}}
