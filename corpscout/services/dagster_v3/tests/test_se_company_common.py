import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import dagster as dg
import pytest

from dagster_v3.defs.se_company.common import (
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
    observation_from_row,
    publish_with_stage,
    reuse_or_call,
)

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)
KIND_ORDER = {"override_field": 0, "approve_suggestion": 1, "reject_suggestion": 1}


class FakeClient:
    """Records executed SQL; answers count queries from a scripted list."""

    def __init__(self, answers: list[list[tuple]]) -> None:
        self.executed: list[tuple[str, object]] = []
        self.answers = answers

    def execute(self, sql: str, parameters: object = None) -> list[tuple]:
        self.executed.append((sql, parameters))
        if sql.lstrip().upper().startswith("SELECT"):
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


def test_publish_with_stage_new_versions_only_emits_the_anti_join_and_counts_the_delta() -> None:
    client = FakeClient(answers=[[(2, 0)], [(10,)], [(11,)]])  # validation, existing count, final count
    counts = publish_with_stage(
        clickhouse=FakeClickhouse(client), target="se_company_info_scb",
        insert_columns=("company_id", "source_record_uid"),
        rows=[("5565200028", "r1"), ("5565200028", "r2")],
        invalid_condition="trim(company_id) = ''",
        new_versions_only=True,
    )
    sql = [entry[0] for entry in client.executed]
    insert_sql = next(statement for statement in sql if statement.startswith("INSERT INTO `corpscout`.`se_company_info_scb`"))
    assert "LEFT ANTI JOIN `corpscout`.`se_company_info_scb` AS existing" in insert_sql
    assert (
        "ON existing.company_id = stage.company_id "
        "AND existing.source_record_uid = stage.source_record_uid "
        "AND existing.evidence_hash = stage.evidence_hash"
    ) in insert_sql
    assert "SELECT stage.company_id,\n    stage.source_record_uid FROM" in insert_sql
    # 2 staged, but only 1 of them was new (existing 10 -> total 11): the anti-join dropped one.
    assert counts == PublishCounts(staged=2, inserted=1, total=11)


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
    sql = build_observations_sql("se_company_info_enrichment_observation")
    assert "FROM corpscout.se_company_info_enrichment_observation" in sql
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
