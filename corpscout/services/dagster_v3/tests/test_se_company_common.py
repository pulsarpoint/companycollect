import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.se_company.common import (
    EPOCH,
    SE_COMPANY_ID_PATTERN,
    ZERO_UUID,
    LedgerRow,
    ObservationResult,
    PublishCounts,
    StoredObservation,
    build_ledger_cursor_sql,
    build_ledger_sql,
    build_observations_sql,
    build_touched_companies_sql,
    effective_ledger,
    input_hash_for,
    ledger_row_from_row,
    ledger_sensor,
    normalized_se_company_ids,
    observation_from_row,
    publish_with_stage,
    reuse_or_call,
)

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)
KIND_ORDER = {"override_field": 0, "approve_suggestion": 1, "reject_suggestion": 1}
COMPANY = "5565200028"
SOLE_TRADER = "196408233412"  # a 12-digit personnummer-based enskild firma id


def test_company_ids_accept_sole_traders() -> None:
    """se_companies carries 12-digit personnummer-based ids for enskild firma, and the
    se_company finals' has_company CHECK admits them -- so a scoped run must too.
    company_people.draft.normalized_company_ids validates 10 digits only, which is why
    this exists rather than being reused from there."""
    assert normalized_se_company_ids([SOLE_TRADER, COMPANY]) == (SOLE_TRADER, COMPANY)
    assert SE_COMPANY_ID_PATTERN == "^([0-9]{10}|[0-9]{12})$"
    for bad in ("55652000", "5565200028X", "55652000281", "", "556-520-0028"):
        with pytest.raises(ValueError):
            normalized_se_company_ids([bad])


def test_normalized_se_company_ids_sorts_dedupes_and_rejects_non_ids() -> None:
    assert normalized_se_company_ids([f" {COMPANY} ", COMPANY, SOLE_TRADER]) == (
        SOLE_TRADER, COMPANY)
    with pytest.raises(ValueError, match="10 or 12 digits"):
        normalized_se_company_ids(["not-an-id"])



class FakeClient:
    """Records executed SQL; answers read queries from a scripted list.

    A read is anything starting with SELECT or WITH (a CTE query reads too);
    everything else -- CREATE/INSERT/DROP -- is recorded and answers nothing.
    """

    def __init__(self, answers: list[list[tuple]]) -> None:
        self.executed: list[tuple[str, object]] = []
        self.answers = answers

    def execute(self, sql: str, parameters: object = None) -> list[tuple]:
        self.executed.append((sql, parameters))
        if sql.lstrip().upper().startswith(("SELECT", "WITH")):
            return self.answers.pop(0)
        return []


class FakeClickhouse:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def _ledger(index: int, kind: str, *, supersedes: int | None = None) -> LedgerRow:
    return LedgerRow(
        correction_id=uuid.UUID(int=index), company_id="5565200028", kind=kind,
        payload={}, evidence_hash="0" * 64,
        supersedes_correction_id=None if supersedes is None else uuid.UUID(int=supersedes),
        created_at=NOW + timedelta(seconds=index),
    )


def test_publish_with_stage_validates_then_inserts_and_drops_the_stage() -> None:
    client = FakeClient(answers=[[(2, 0)], [(10,)], [(12,)]])  # validation, existing count, final count
    counts = publish_with_stage(
        clickhouse=FakeClickhouse(client), target="se_company_info_scb",
        insert_columns=("company_id", "source_record_uid"),
        rows=[("5565200028", "r1"), ("5565200028", "r2")],
        invalid_condition="trim(company_id) = ''",
    )
    sql = [entry[0] for entry in client.executed]
    assert counts == PublishCounts(staged=2, inserted=2, total=12)
    assert sql[0].startswith("CREATE TABLE `corpscout`.`_tmp_se_company_info_scb_")
    assert "INSERT INTO `corpscout`.`_tmp_se_company_info_scb_" in sql[1]
    assert "countIf(trim(company_id) = '')" in sql[2]
    assert "INSERT INTO `corpscout`.`se_company_info_scb` (company_id,\n    source_record_uid)" in sql[4]
    assert sql[-1].startswith("DROP TABLE IF EXISTS `corpscout`.`_tmp_se_company_info_scb_")
    assert not any("ANTI JOIN" in statement for statement in sql)


def test_publish_with_stage_raises_on_invalid_rows_and_still_drops_the_stage() -> None:
    client = FakeClient(answers=[[(2, 1)]])
    with pytest.raises(ValueError, match="invalid=1"):
        publish_with_stage(
            clickhouse=FakeClickhouse(client), target="se_company_info_scb",
            insert_columns=("company_id",), rows=[("5565200028",), ("",)],
            invalid_condition="trim(company_id) = ''",
        )
    assert client.executed[-1][0].startswith("DROP TABLE IF EXISTS")


def test_publish_with_stage_new_versions_only_counts_the_anti_join_before_inserting() -> None:
    # existing=10, total=15 -- a before/after delta (5) would NOT equal the scripted
    # anti-join survivor count (1). Scripting them apart proves `inserted` comes from
    # the anti-join COUNT query itself, not a subtraction a concurrent background
    # merge on the ReplacingMergeTree target could skew (round-1 review finding).
    client = FakeClient(answers=[[(2, 0)], [(10,)], [(1,)], [(15,)]])  # validation, existing, anti-join count, final count
    counts = publish_with_stage(
        clickhouse=FakeClickhouse(client), target="se_company_info_scb",
        insert_columns=("company_id", "source_record_uid"),
        rows=[("5565200028", "r1"), ("5565200028", "r2")],
        invalid_condition="trim(company_id) = ''",
        new_versions_only=True,
    )
    sql = [entry[0] for entry in client.executed]
    count_sql = next(statement for statement in sql if statement.startswith("SELECT count() FROM `corpscout`.`_tmp_se_company_info_scb_"))
    insert_sql = next(statement for statement in sql if statement.startswith("INSERT INTO `corpscout`.`se_company_info_scb`"))
    assert sql.index(count_sql) < sql.index(insert_sql)  # counted BEFORE the target insert
    for statement in (count_sql, insert_sql):
        assert "LEFT ANTI JOIN `corpscout`.`se_company_info_scb` AS existing" in statement
        assert (
            "ON existing.company_id = stage.company_id "
            "AND existing.source_record_uid = stage.source_record_uid "
            "AND existing.evidence_hash = stage.evidence_hash"
        ) in statement
    assert "SELECT stage.company_id,\n    stage.source_record_uid FROM" in insert_sql
    # inserted (1) equals the scripted anti-join count, not staged (2) or total-existing (5).
    assert counts == PublishCounts(staged=2, inserted=1, total=15)


def test_effective_ledger_drops_superseded_undo_unknown_and_orders_by_step() -> None:
    rows = (
        _ledger(1, "approve_suggestion"), _ledger(2, "override_field"),
        _ledger(3, "undo", supersedes=1), _ledger(4, "reject_suggestion"), _ledger(5, "bogus"),
    )
    assert [row.correction_id.int for row in effective_ledger(rows, KIND_ORDER)] == [2, 4]


def test_ledger_sql_and_row_mapper_round_trip() -> None:
    sql = build_ledger_sql("se_company_info_correction")
    assert "FROM corpscout.se_company_info_correction" in sql
    assert "WHERE company_id IN %(company_ids)s" in sql
    row = ledger_row_from_row((uuid.UUID(int=9), "5565200028", "override_field",
                               json.dumps({"description": "x"}), "a" * 64, None, NOW))
    assert row.payload == {"description": "x"} and row.supersedes_correction_id is None


def test_input_hash_ignores_key_order_and_includes_prompt_version() -> None:
    a = input_hash_for({"model": "m", "messages": [{"role": "user", "content": "x"}]}, "v1")
    b = input_hash_for({"messages": [{"content": "x", "role": "user"}], "model": "m"}, "v1")
    assert a == b and len(a) == 64
    assert input_hash_for({"model": "m", "messages": []}, "v2") != input_hash_for({"model": "m", "messages": []}, "v1")


def test_reuse_or_call_prefers_the_newest_stored_row_with_the_same_hash() -> None:
    stored = [
        StoredObservation(uuid.UUID(int=1), "5565200028", "h" * 64, {"description": "old"}, "deepseek", "m", "v1", NOW),
        StoredObservation(uuid.UUID(int=2), "5565200028", "h" * 64, {"description": "new"}, "deepseek", "m", "v1", NOW + timedelta(seconds=1)),
    ]
    result, reused = reuse_or_call(input_hash="h" * 64, stored=stored, call=lambda: pytest.fail("must not call"))
    assert reused and result.suggestion == {"description": "new"} and result.suggestion_id == uuid.UUID(int=2)

    fresh = ObservationResult({"description": "fresh"}, "{}", "deepseek", "m", "v1", 1, 1, uuid.UUID(int=7))
    result, reused = reuse_or_call(input_hash="z" * 64, stored=stored, call=lambda: fresh)
    assert not reused and result is fresh


def test_observation_sql_and_mapper() -> None:
    # Positional order matters: `ledger_row_from_row` / `observation_from_row` read
    # both SELECTs by index, so a reordered column would silently swap values
    # instead of raising -- pin the exact SELECT clause of both queries here.
    ledger_sql = build_ledger_sql("se_company_info_correction")
    assert "WHERE company_id IN %(company_ids)s" in ledger_sql
    assert (
        "SELECT\n"
        "    correction_id, company_id, correction_kind, payload, toString(evidence_hash),\n"
        "    supersedes_correction_id, created_at"
    ) in ledger_sql

    sql = build_observations_sql("se_company_info_enrichment_observation")
    assert "FROM corpscout.se_company_info_enrichment_observation" in sql
    assert "WHERE company_id IN %(company_ids)s" in sql
    assert (
        "SELECT\n"
        "    suggestion_id, company_id, toString(input_hash), suggestion,\n"
        "    toString(model_provider), model_name, prompt_version, created_at"
    ) in sql
    row = observation_from_row((uuid.UUID(int=3), "5565200028", "h" * 64, json.dumps({"description": "d"}), "deepseek", "m", "v1", NOW))
    assert row.suggestion == {"description": "d"}


def test_ledger_sensor_scopes_every_asset_and_uses_a_tuple_boundary() -> None:
    job = dg.define_asset_job("se_company_info_review_job", selection=dg.AssetSelection.assets("se_company_info_clickhouse"))
    sensor = ledger_sensor(name="se_company_info_correction_sensor", table="se_company_info_correction",
                           job=job, asset_names=("se_company_info_clickhouse",))
    assert sensor.name == "se_company_info_correction_sensor" and sensor.minimum_interval_seconds == 60
    # Stays stopped until a later phase promotes it explicitly.
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
    assert "argMax(correction_id, (created_at, correction_id))" in build_ledger_cursor_sql("se_company_info_correction")
    assert "(created_at, correction_id) > (parseDateTime64BestEffort(%(since)s, 3, 'UTC'), toUUID(%(since_id)s))" in build_touched_companies_sql("se_company_info_correction")


class _FakeLedgerClient:
    """In-memory ledger client answering `ledger_sensor`'s two queries
    (`build_ledger_cursor_sql` / `build_touched_companies_sql`). Mirrors
    `_FakeCorrectionLedgerClient` in `test_se_company_person_corrections.py`
    so the boundary behaviour under test isn't reimplemented differently here.
    """

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []  # (company_id, correction_id, created_at)
        self.executed: list[tuple[str, object]] = []

    def append(self, company_id: str, correction_id: str, created_at: str) -> None:
        self.rows.append((company_id, correction_id, created_at))

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[tuple[object, ...]]:
        self.executed.append((sql, params))
        if "argMax(correction_id" in sql:
            if not self.rows:
                return [(0, "", "")]
            _, last_id, last_created_at = max(self.rows, key=lambda row: (row[2], row[1]))
            return [(len(self.rows), last_id, last_created_at)]
        if "SELECT DISTINCT company_id" in sql:
            assert params is not None
            since = (str(params["since"]), str(params["since_id"]))
            touched = sorted({
                company_id
                for company_id, correction_id, created_at in self.rows
                if (created_at, correction_id) > since
            })
            return [(company_id,) for company_id in touched]
        raise AssertionError(sql)


def _patch_ledger_clickhouse(
    monkeypatch: pytest.MonkeyPatch, client: _FakeLedgerClient
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[_FakeLedgerClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


_LEDGER_SENSOR_TABLE = "se_company_info_correction"
_LEDGER_SENSOR_ASSETS = ("se_company_info_clickhouse", "se_company_info_esef_clickhouse")
_LEDGER_ID_A = str(uuid.UUID(int=1))
_LEDGER_ID_B = str(uuid.UUID(int=2))
_LEDGER_COMPANY_A = "5565200028"
_LEDGER_COMPANY_B = "5565200036"
_LEDGER_CREATED_AT_0 = "2026-08-22 08:59:59.000"
_LEDGER_CREATED_AT_1 = "2026-08-22 09:00:00.000"
_LEDGER_CREATED_AT_2 = "2026-08-22 09:00:01.000"


def _build_ledger_test_sensor(**kwargs) -> dg.SensorDefinition:
    job = dg.define_asset_job(
        "se_company_info_review_job", selection=dg.AssetSelection.assets(*_LEDGER_SENSOR_ASSETS)
    )
    return ledger_sensor(
        name="se_company_info_correction_sensor", table=_LEDGER_SENSOR_TABLE,
        job=job, asset_names=_LEDGER_SENSOR_ASSETS, **kwargs,
    )


def test_ledger_sensor_merges_extra_config_into_every_assets_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sensor-launched run carries only the config this factory writes, so an asset
    whose config gates what the run DOES (se_company_info's `execute`, and the model
    profile it may call) has to be told here -- otherwise the run silently falls back to
    the asset's defaults and resolves nothing."""
    client = _FakeLedgerClient()
    client.append(_LEDGER_COMPANY_A, _LEDGER_ID_A, _LEDGER_CREATED_AT_1)
    resource = _patch_ledger_clickhouse(monkeypatch, client)
    context = dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})

    extra = {"execute": True, "llm": {"provider": "deepseek", "model": "m"}}
    execution_data = _build_ledger_test_sensor(extra_config=extra).evaluate_tick(context)

    assert execution_data.run_requests is not None
    assert execution_data.run_requests[0].run_config == {
        "ops": {
            asset_name: {"config": {**extra, "company_ids": [_LEDGER_COMPANY_A]}}
            for asset_name in _LEDGER_SENSOR_ASSETS
        }
    }
    # The default stays exactly what it was: company_ids and nothing else.
    plain = _build_ledger_test_sensor().evaluate_tick(
        dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})
    )
    assert plain.run_requests is not None
    assert plain.run_requests[0].run_config["ops"][_LEDGER_SENSOR_ASSETS[0]]["config"] == {
        "company_ids": [_LEDGER_COMPANY_A]
    }


def test_ledger_sensor_unchanged_cursor_skips_and_issues_only_the_cursor_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeLedgerClient()
    client.append(_LEDGER_COMPANY_A, _LEDGER_ID_A, _LEDGER_CREATED_AT_1)
    resource = _patch_ledger_clickhouse(monkeypatch, client)
    cursor = f"1:{_LEDGER_ID_A}:{_LEDGER_CREATED_AT_1}"
    context = dg.build_sensor_context(cursor=cursor, resources={"clickhouse": resource})

    execution_data = _build_ledger_test_sensor().evaluate_tick(context)

    assert execution_data.skip_message == f"No new rows in {_LEDGER_SENSOR_TABLE}"
    assert not execution_data.run_requests
    assert len(client.executed) == 1  # unchanged cursor never issues the touched-companies scan
    assert "argMax(correction_id" in client.executed[0][0]


def test_ledger_sensor_first_tick_uses_the_epoch_boundary_and_scopes_every_asset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeLedgerClient()
    client.append(_LEDGER_COMPANY_A, _LEDGER_ID_A, _LEDGER_CREATED_AT_1)
    client.append(_LEDGER_COMPANY_B, _LEDGER_ID_B, _LEDGER_CREATED_AT_2)
    resource = _patch_ledger_clickhouse(monkeypatch, client)
    context = dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})

    execution_data = _build_ledger_test_sensor().evaluate_tick(context)

    touched_call = next(entry for entry in client.executed if "SELECT DISTINCT company_id" in entry[0])
    assert touched_call[1] == {"since": EPOCH, "since_id": ZERO_UUID}

    expected_cursor = f"2:{_LEDGER_ID_B}:{_LEDGER_CREATED_AT_2}"
    assert execution_data.cursor == expected_cursor
    assert execution_data.run_requests is not None and len(execution_data.run_requests) == 1
    request = execution_data.run_requests[0]
    assert request.run_key == f"{_LEDGER_SENSOR_TABLE}:{expected_cursor}"
    assert request.run_config == {
        "ops": {
            asset_name: {"config": {"company_ids": [_LEDGER_COMPANY_A, _LEDGER_COMPANY_B]}}
            for asset_name in _LEDGER_SENSOR_ASSETS
        }
    }


def test_ledger_sensor_advances_cursor_without_a_run_request_when_nothing_is_touched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # B lands EARLIER (CREATED_AT_0) than the already-cursored row A (CREATED_AT_1)
    # -- e.g. a writer with clock skew. The ledger grew -- count changes, so the
    # fresh cursor differs from context.cursor -- but B never satisfies the strict
    # tuple filter against A's boundary, so no company is touched. The sensor must
    # still return a run-less SensorResult (not raise, not get stuck) and still
    # advance the cursor, rather than re-evaluate the same stale boundary forever.
    client = _FakeLedgerClient()
    client.append(_LEDGER_COMPANY_A, _LEDGER_ID_A, _LEDGER_CREATED_AT_1)
    client.append(_LEDGER_COMPANY_B, _LEDGER_ID_B, _LEDGER_CREATED_AT_0)
    resource = _patch_ledger_clickhouse(monkeypatch, client)
    cursor = f"1:{_LEDGER_ID_A}:{_LEDGER_CREATED_AT_1}"
    context = dg.build_sensor_context(cursor=cursor, resources={"clickhouse": resource})

    execution_data = _build_ledger_test_sensor().evaluate_tick(context)

    assert execution_data.cursor == f"2:{_LEDGER_ID_A}:{_LEDGER_CREATED_AT_1}"
    assert execution_data.run_requests == []
