import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_people.corrections import (
    CORRECTION_COLUMNS,
    CORRECTION_KINDS,
    KIND_ORDER,
    PERSON_CORRECTION_KINDS,
    ROLE_CORRECTION_KINDS,
    SUGGESTION_COLUMNS,
    UNDO_KIND,
    PersonCorrection,
    StoredSuggestion,
    build_company_corrections_sql,
    build_company_suggestions_sql,
    build_correction_cursor_sql,
    build_touched_companies_sql,
    correction_from_row,
    correction_set_hash,
    effective_company_corrections_cte,
    effective_corrections,
    review_run_request,
    se_company_person_correction_sensor,
    suggestion_from_row,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _sql(name: str) -> str:
    return (MIGRATIONS_DIR / name).read_text(encoding="utf-8")


def test_correction_and_suggestion_tables_match_insert_contracts() -> None:
    sql = _sql("000295_corpscout_se_company_person_corrections.up.sql")
    down = _sql("000295_corpscout_se_company_person_corrections.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_correction" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_enrichment_observation" in sql
    assert sql.count("ENGINE = MergeTree") == 2

    # Scope each column-order check to its own CREATE TABLE block: company_id and
    # created_at appear in BOTH tables, so searching the whole file for a column's
    # position could find the wrong table's occurrence and miss a reorder.
    correction_block = sql[
        sql.index(
            "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_correction"
        ) : sql.index(
            "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_enrichment_observation"
        )
    ]
    suggestion_block = sql[
        sql.index(
            "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_enrichment_observation"
        ) : sql.index("ALTER TABLE corpscout.se_company_person")
    ]

    correction_positions = []
    for column in CORRECTION_COLUMNS:
        assert f"    {column} " in correction_block, column
        correction_positions.append(correction_block.index(f"    {column} "))
    # INSERT INTO t(a, b) SELECT ... writes positionally in ClickHouse -- aliases
    # are ignored -- so membership alone can't catch a reorder that swaps values
    # between same-typed adjacent columns (e.g. subject_person_id/target_person_id
    # are both UUID-family, as are draft_ids/evidence_hash's neighbors).
    assert correction_positions == sorted(correction_positions), (
        "the migration's se_company_person_correction column order must match "
        "CORRECTION_COLUMNS order exactly -- a mismatch here means a positional "
        "INSERT will silently swap values between same-typed adjacent columns"
    )

    suggestion_positions = []
    for column in SUGGESTION_COLUMNS:
        assert f"    {column} " in suggestion_block, column
        suggestion_positions.append(suggestion_block.index(f"    {column} "))
    assert suggestion_positions == sorted(suggestion_positions), (
        "the migration's se_company_person_enrichment_observation column order "
        "must match SUGGESTION_COLUMNS order exactly -- a mismatch here means a "
        "positional INSERT will silently swap values between same-typed adjacent "
        "columns"
    )

    assert "CONSTRAINT valid_payload CHECK isValidJSON(payload)" in sql
    assert "ALTER TABLE corpscout.se_company_person" in sql
    assert "correction_ids Array(UUID) DEFAULT []" in sql
    assert "correction_set_hash FixedString(64) MATERIALIZED" in sql
    assert "arraySort(arrayMap(id -> toString(id), correction_ids))" in sql
    assert "suggestion_id Nullable(UUID)" in sql
    assert "merged_into_person_id Nullable(UUID)" in sql
    assert "ALTER TABLE corpscout.se_company_person_role" in sql

    assert "DROP TABLE IF EXISTS corpscout.se_company_person_enrichment_observation" in down
    assert "DROP TABLE IF EXISTS corpscout.se_company_person_correction" in down
    assert "DROP COLUMN IF EXISTS correction_ids" in down


def test_writer_grants_are_insert_only() -> None:
    sql = _sql("000296_corpscout_se_company_person_correction_writer_grants.up.sql")
    down = _sql("000296_corpscout_se_company_person_correction_writer_grants.down.sql")

    assert (
        "GRANT INSERT ON corpscout.se_company_person_correction\n"
        "TO corpscout_person_correction_writer"
    ) in sql
    assert (
        "GRANT INSERT ON corpscout.se_company_person_enrichment_observation\n"
        "TO corpscout_person_correction_writer"
    ) in sql
    assert "GRANT SELECT" not in sql
    assert "GRANT ALL" not in sql
    assert "CREATE USER" not in sql
    assert "REVOKE INSERT ON corpscout.se_company_person_correction" in down


def test_correction_kinds_are_closed_and_ordered() -> None:
    assert PERSON_CORRECTION_KINDS == (
        "merge_persons",
        "reassign_draft",
        "split_person",
        "approve_suggestion",
        "reject_suggestion",
        "override_field",
    )
    assert ROLE_CORRECTION_KINDS == ("set_role", "remove_role")
    assert UNDO_KIND == "undo"
    assert CORRECTION_KINDS == frozenset(
        (*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS, UNDO_KIND)
    )


NOW = datetime(2026, 8, 22, 9, tzinfo=UTC)
COMPANY_ID = "5565200028"


def _correction(
    index: int,
    kind: str,
    *,
    supersedes: int | None = None,
    payload: dict[str, object] | None = None,
) -> PersonCorrection:
    return PersonCorrection(
        correction_id=uuid.UUID(int=index),
        company_id=COMPANY_ID,
        kind=kind,
        subject_person_id=uuid.UUID(int=1000),
        target_person_id=None,
        draft_ids=(),
        payload=payload or {},
        evidence_hash="0" * 64,
        supersedes_correction_id=None if supersedes is None else uuid.UUID(int=supersedes),
        created_at=NOW + timedelta(seconds=index),
    )


def test_effective_corrections_drop_superseded_and_undo_rows_and_order_by_kind() -> None:
    rows = (
        _correction(1, "override_field", payload={"name": "First"}),
        _correction(2, "merge_persons"),
        _correction(3, "undo", supersedes=1),
        _correction(4, "override_field", payload={"name": "Second"}),
        _correction(5, "set_role"),
        _correction(6, "not_a_kind"),
        # Rows 7 and 8 share spec step 2, so the order between them is the
        # order they were decided in, not the alphabet of their kinds.
        _correction(7, "reject_suggestion"),
        _correction(8, "approve_suggestion"),
    )

    effective = effective_corrections(rows)

    assert [c.correction_id.int for c in effective] == [2, 7, 8, 4, 5]


def test_kind_order_gives_every_spec_step_one_shared_rank() -> None:
    """Spec §4.1: evidence moves, then the profile source, then fields, then roles."""
    steps = (
        ("merge_persons", "reassign_draft", "split_person"),
        ("approve_suggestion", "reject_suggestion"),
        ("override_field",),
        ("set_role", "remove_role"),
    )
    ranks = [{KIND_ORDER[kind] for kind in step} for step in steps]

    assert [sorted(rank) for rank in ranks] == [[0], [1], [2], [3]]
    assert set(KIND_ORDER) == set(CORRECTION_KINDS) - {UNDO_KIND}


def test_correction_set_hash_sorts_string_ids_like_clickhouse() -> None:
    ids = [uuid.UUID(int=2), uuid.UUID(int=1)]

    assert correction_set_hash(ids) == (
        "7a70c782c5d30f61a8f57b905eaa11c41ec8ffeafaa3a0e99c4fca60044a28d4"
    )
    assert correction_set_hash([]) == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_row_mappers_parse_payload_and_nullable_columns() -> None:
    company_id, correction = correction_from_row(
        (
            uuid.UUID(int=7),
            COMPANY_ID,
            "override_field",
            uuid.UUID(int=1000),
            None,
            [],
            json.dumps({"name": "Anna K. Svensson"}),
            "a" * 64,
            None,
            NOW,
        )
    )
    assert company_id == COMPANY_ID
    assert correction.kind == "override_field"
    assert correction.payload == {"name": "Anna K. Svensson"}
    assert correction.target_person_id is None

    company_id, suggestion = suggestion_from_row(
        (
            uuid.UUID(int=8),
            COMPANY_ID,
            uuid.UUID(int=1000),
            "b" * 64,
            [uuid.UUID(int=1)],
            json.dumps(
                {
                    "existing_person_id": None,
                    "name": "Anna Svensson",
                    "description": None,
                    "draft_ids": [str(uuid.UUID(int=1))],
                }
            ),
            "deepseek",
            "deepseek-v4-flash",
            "se-company-people-v2",
            NOW,
        )
    )
    assert company_id == COMPANY_ID
    assert isinstance(suggestion, StoredSuggestion)
    assert suggestion.name == "Anna Svensson"
    assert suggestion.draft_ids == (uuid.UUID(int=1),)
    assert suggestion.model_provider == "deepseek"
    assert suggestion.model_name == "deepseek-v4-flash"
    assert suggestion.prompt_version == "se-company-people-v2"
    assert suggestion.created_at == NOW


def test_loader_sql_scopes_by_selected_companies() -> None:
    for sql in (build_company_corrections_sql(), build_company_suggestions_sql()):
        assert "WHERE company_id IN %(selected_company_ids)s" in sql
        assert "ORDER BY company_id" in sql
    assert "FROM corpscout.se_company_person_correction" in build_company_corrections_sql()
    assert "FROM corpscout.se_company_person_enrichment_observation" in build_company_suggestions_sql()

    # suggestion_from_row reads this SELECT positionally, so pin the exact order:
    # the provenance columns sit between the payload and created_at, and swapping
    # two same-typed String columns would be invisible to a membership check.
    suggestions_sql = build_company_suggestions_sql()
    select_block = suggestions_sql[
        suggestions_sql.index("SELECT") + len("SELECT") : suggestions_sql.index("FROM")
    ]
    assert [line.strip().rstrip(",") for line in select_block.strip().splitlines()] == [
        "suggestion_id",
        "company_id",
        "person_id",
        "toString(input_hash)",
        "draft_ids",
        "suggestion",
        "model_provider",
        "model_name",
        "prompt_version",
        "created_at",
    ]

    cte = effective_company_corrections_cte()
    assert "effective_company_corrections AS (" in cte
    assert "supersedes_correction_id IS NOT NULL" in cte
    assert "correction_kind IN ('merge_persons'" in cte
    assert "'undo'" not in cte
    # The undo lookup carries the same company scope as the outer scan, so it
    # never degenerates into a full-ledger read.
    assert cte.count("%(all_companies)s OR") == 2
    assert cte.count("company_id IN %(company_ids)s") == 2


def test_review_job_selects_person_and_role_assets_without_draft_import() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    keys = {
        key.path[-1]
        for key in repository.get_job("se_company_person_review_job").asset_layer.executable_asset_keys
    }
    assert keys == {
        "se_company_person_clickhouse",
        "se_company_person_role_draft_clickhouse",
        "se_company_person_role_clickhouse",
    }
    sensor = repository.get_sensor_def("se_company_person_correction_sensor")
    assert sensor.job_name == "se_company_person_review_job"
    assert sensor.minimum_interval_seconds == 60


def test_sensor_sql_reads_cursor_and_touched_companies() -> None:
    assert "argMax(correction_id, (created_at, correction_id))" in build_correction_cursor_sql()
    assert "toString(max(created_at))" in build_correction_cursor_sql()
    touched = build_touched_companies_sql()
    assert (
        "WHERE (created_at, correction_id) > "
        "(parseDateTime64BestEffort(%(since)s, 3, 'UTC'), toUUID(%(since_id)s))"
    ) in touched
    assert "SELECT DISTINCT company_id" in touched
    assert "ORDER BY company_id" in touched


def test_run_request_scopes_every_asset_to_touched_companies() -> None:
    request = review_run_request("3:abc:2026-08-22 09:00:00.000", ("5565200028",))

    assert request.run_key == "se-company-person-correction:3:abc:2026-08-22 09:00:00.000"
    assert request.run_config == {
        "ops": {
            "se_company_person_clickhouse": {"config": {"company_ids": ["5565200028"]}},
            "se_company_person_role_draft_clickhouse": {"config": {"company_ids": ["5565200028"]}},
            "se_company_person_role_clickhouse": {"config": {"company_ids": ["5565200028"]}},
        }
    }


class _FakeCorrectionLedgerClient:
    """In-memory `se_company_person_correction` ledger answering the sensor's
    two queries. Faithfully mirrors the tuple ordering used by both
    `build_correction_cursor_sql` (argMax by `(created_at, correction_id)`) and
    `build_touched_companies_sql` (`(created_at, correction_id) > (since, since_id)`)
    so the boundary behaviour under test isn't reimplemented differently here.
    """

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []  # (company_id, correction_id, created_at)

    def append(self, company_id: str, correction_id: str, created_at: str) -> None:
        self.rows.append((company_id, correction_id, created_at))

    def execute(
        self, sql: str, params: dict[str, object] | None = None
    ) -> list[tuple[object, ...]]:
        if "argMax(correction_id" in sql:
            if not self.rows:
                return [(0, "", "")]
            _, last_id, last_created_at = max(self.rows, key=lambda row: (row[2], row[1]))
            return [(len(self.rows), last_id, last_created_at)]
        if "SELECT DISTINCT company_id" in sql:
            assert params is not None
            since = (str(params["since"]), str(params["since_id"]))
            touched = sorted(
                {
                    company_id
                    for company_id, correction_id, created_at in self.rows
                    if (created_at, correction_id) > since
                }
            )
            return [(company_id,) for company_id in touched]
        raise AssertionError(sql)


def _patch_correction_clickhouse(
    monkeypatch: pytest.MonkeyPatch, client: _FakeCorrectionLedgerClient
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeCorrectionLedgerClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def _client_with_rows(
    rows: list[tuple[str, str, str]],
) -> _FakeCorrectionLedgerClient:
    client = _FakeCorrectionLedgerClient()
    for company_id, correction_id, created_at in rows:
        client.append(company_id, correction_id, created_at)
    return client


_CORRECTION_ID_A = str(uuid.UUID(int=1))
_CORRECTION_ID_B = str(uuid.UUID(int=2))
_CORRECTION_ID_C = str(uuid.UUID(int=3))
_CORRECTION_ID_D = str(uuid.UUID(int=4))

_REVIEW_COMPANY_A = "5560000001"
_REVIEW_COMPANY_B = "5560000002"
_REVIEW_COMPANY_C = "5560000003"
_REVIEW_COMPANY_D = "5560000004"

_CREATED_AT_0 = "2026-08-22 08:59:59.000"
_CREATED_AT_1 = "2026-08-22 09:00:00.000"
_CREATED_AT_2 = "2026-08-22 09:00:01.000"


def test_sensor_skips_when_the_correction_ledger_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource = _patch_correction_clickhouse(monkeypatch, _FakeCorrectionLedgerClient())
    context = dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})

    execution_data = se_company_person_correction_sensor.evaluate_tick(context)

    assert execution_data.skip_message == "No Sweden company-person corrections exist"
    assert not execution_data.run_requests


def test_sensor_first_tick_scopes_every_asset_to_touched_companies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client_with_rows(
        [
            (_REVIEW_COMPANY_A, _CORRECTION_ID_A, _CREATED_AT_1),
            (_REVIEW_COMPANY_B, _CORRECTION_ID_B, _CREATED_AT_2),
        ]
    )
    resource = _patch_correction_clickhouse(monkeypatch, client)
    context = dg.build_sensor_context(cursor=None, resources={"clickhouse": resource})

    execution_data = se_company_person_correction_sensor.evaluate_tick(context)

    assert execution_data.cursor == f"2:{_CORRECTION_ID_B}:{_CREATED_AT_2}"
    assert execution_data.run_requests is not None
    assert len(execution_data.run_requests) == 1
    run_config = execution_data.run_requests[0].run_config
    for asset_name in (
        "se_company_person_clickhouse",
        "se_company_person_role_draft_clickhouse",
        "se_company_person_role_clickhouse",
    ):
        assert run_config["ops"][asset_name]["config"]["company_ids"] == [
            _REVIEW_COMPANY_A,
            _REVIEW_COMPANY_B,
        ]


def test_sensor_second_tick_with_unchanged_cursor_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client_with_rows(
        [
            (_REVIEW_COMPANY_A, _CORRECTION_ID_A, _CREATED_AT_1),
            (_REVIEW_COMPANY_B, _CORRECTION_ID_B, _CREATED_AT_2),
        ]
    )
    resource = _patch_correction_clickhouse(monkeypatch, client)
    cursor = f"2:{_CORRECTION_ID_B}:{_CREATED_AT_2}"
    context = dg.build_sensor_context(cursor=cursor, resources={"clickhouse": resource})

    execution_data = se_company_person_correction_sensor.evaluate_tick(context)

    assert execution_data.skip_message == "No new Sweden company-person corrections"
    assert not execution_data.run_requests


def test_sensor_rescopes_a_row_sharing_the_previous_max_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # C's correction lands in the SAME millisecond as B's (the previous tick's
    # cursor boundary) but with a greater correction_id. A plain
    # `created_at > since` filter would silently never re-scope C -- this is
    # the boundary Important-1 fixes.
    client = _client_with_rows(
        [
            (_REVIEW_COMPANY_A, _CORRECTION_ID_A, _CREATED_AT_1),
            (_REVIEW_COMPANY_B, _CORRECTION_ID_B, _CREATED_AT_2),
            (_REVIEW_COMPANY_C, _CORRECTION_ID_C, _CREATED_AT_2),
        ]
    )
    resource = _patch_correction_clickhouse(monkeypatch, client)
    cursor = f"2:{_CORRECTION_ID_B}:{_CREATED_AT_2}"
    context = dg.build_sensor_context(cursor=cursor, resources={"clickhouse": resource})

    execution_data = se_company_person_correction_sensor.evaluate_tick(context)

    assert execution_data.cursor == f"3:{_CORRECTION_ID_C}:{_CREATED_AT_2}"
    assert execution_data.run_requests is not None
    assert len(execution_data.run_requests) == 1
    run_config = execution_data.run_requests[0].run_config
    assert run_config["ops"]["se_company_person_clickhouse"]["config"][
        "company_ids"
    ] == [_REVIEW_COMPANY_C]


def test_sensor_advances_cursor_without_a_run_request_when_no_company_crosses_the_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # D's correction lands with an EARLIER created_at than the previous cursor's
    # boundary (e.g. a writer with clock skew). The ledger grew -- count changes,
    # so the fresh cursor differs from context.cursor -- but D never satisfies the
    # strict tuple filter, so no company is touched. The sensor must still return
    # a run-less SensorResult (not raise, not get stuck) and still advance the
    # cursor, rather than re-evaluate the same stale boundary forever.
    client = _client_with_rows(
        [
            (_REVIEW_COMPANY_A, _CORRECTION_ID_A, _CREATED_AT_1),
            (_REVIEW_COMPANY_B, _CORRECTION_ID_B, _CREATED_AT_2),
            (_REVIEW_COMPANY_C, _CORRECTION_ID_C, _CREATED_AT_2),
            (_REVIEW_COMPANY_D, _CORRECTION_ID_D, _CREATED_AT_0),
        ]
    )
    resource = _patch_correction_clickhouse(monkeypatch, client)
    cursor = f"3:{_CORRECTION_ID_C}:{_CREATED_AT_2}"
    context = dg.build_sensor_context(cursor=cursor, resources={"clickhouse": resource})

    execution_data = se_company_person_correction_sensor.evaluate_tick(context)

    assert execution_data.cursor == f"4:{_CORRECTION_ID_C}:{_CREATED_AT_2}"
    assert execution_data.run_requests == []
