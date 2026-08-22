from pathlib import Path

from dagster_v3.defs.company_people.corrections import (
    CORRECTION_COLUMNS,
    CORRECTION_KINDS,
    PERSON_CORRECTION_KINDS,
    ROLE_CORRECTION_KINDS,
    SUGGESTION_COLUMNS,
    UNDO_KIND,
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
