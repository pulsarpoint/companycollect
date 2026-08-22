import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dagster_v3.defs.company_people.corrections import (
    CORRECTION_COLUMNS,
    CORRECTION_KINDS,
    PERSON_CORRECTION_KINDS,
    ROLE_CORRECTION_KINDS,
    SUGGESTION_COLUMNS,
    UNDO_KIND,
    PersonCorrection,
    StoredSuggestion,
    build_company_corrections_sql,
    build_company_suggestions_sql,
    correction_from_row,
    correction_set_hash,
    effective_company_corrections_cte,
    effective_corrections,
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
    )

    effective = effective_corrections(rows)

    assert [c.correction_id.int for c in effective] == [2, 4, 5]


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
