"""Migration 000402: roles as rows, and the pin that keeps its body machine-rendered.

`corpscout.se_company_person_role` (spec 2026-09-09 section 11, person slice 5) is a
REFRESHABLE materialized view over the two tables the entity already has: one row per
published ACTIVE person and per role-carrying normalized observation they were folded
from. Nothing writes it -- the fold, the normalizer, the rules and the precedence never
see it -- so the only thing that can drift is the SELECT itself. This file couples the
two halves: the migration's body must be `build_se_company_person_role_sql()`'s render,
and the DDL around it must be the refreshable form this repo uses (engine INSIDE the
view, `EMPTY`, hourly at :20), the same coupling tests/test_se_companies_serving_mv.py
keeps for the serving view.

THE NAME IS REUSED. corpscout.se_company_person_role was the 2026-08-19 model's role
table, dropped by hand in person slice 0 on 2026-09-09 -- the freed-name story migration
000398 already played out for se_company_person. The two guards that know the name are
tests/test_clickhouse_migrations.py (no up migration may DECLARE a slice-0 dropped
object) and tests/test_se_person_retirement_drops.py (the spent drop script names no
kept object); both move in this task.
"""

from pathlib import Path

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.tables import build_se_company_person_role_sql

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000402_corpscout_se_company_person_role"
VIEW = "corpscout.se_company_person_role"
MAIN = "corpscout.se_company_person"
NORMALIZED = "corpscout.se_company_person_normalized"


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    """The statement without the comment lines that precede it."""
    lines = statement.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    body = "\n".join(lines).strip()
    assert body, f"no statement left after stripping comments: {statement[:80]!r}"
    return body


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _executable(sql: str) -> str:
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


def _view_body(sql: str) -> str:
    """The SELECT the CREATE MATERIALIZED VIEW installs, without its semicolon."""
    [statement] = [s for s in _statements(sql) if "CREATE MATERIALIZED VIEW" in s]
    marker = "\nEMPTY\nAS "
    return statement[statement.index(marker) + len(marker) :]


def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    assert _normalized(_view_body(_sql("up"))) == _normalized(
        build_se_company_person_role_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    body = _view_body(_sql("up"))
    assert body.startswith("SELECT\n")
    assert f"FROM {MAIN} AS p FINAL" in body
    assert "ARRAY JOIN p.normalized_ids AS member_id" in body
    assert f"INNER JOIN {NORMALIZED} AS n FINAL" in body
    assert "ON n.company_id = p.company_id AND n.normalized_id = member_id" in body
    assert "WHERE p.active = 1 AND n.role_code IS NOT NULL" in body
    assert "has(p.current_roles, assumeNotNull(n.role_code)) AS is_current" in body
    # Whole-name matching: MAIN prefixes NORMALIZED and the view itself, so the main
    # table is counted with the alias that follows it.
    assert body.count(f"{MAIN} AS p FINAL") == 1
    assert VIEW not in body  # the view never reads itself
    for column in tables.ROLE_VIEW_COLUMNS:
        assert f" AS {column}" in body, column


def test_the_up_migration_creates_one_refreshable_view_created_empty() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 2
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    create = _body(statements[1])
    assert create.startswith(f"CREATE MATERIALIZED VIEW {VIEW}\n")
    assert "\nREFRESH EVERY 1 HOUR OFFSET 20 MINUTE\n" in create
    # The engine lives INSIDE the view (000326/000391's form), so the view IS the table
    # every reader queries and there is no second object to keep in step.
    assert "\nENGINE = MergeTree\n" in create
    assert f"\nORDER BY ({', '.join(tables.ROLE_VIEW_ORDER_BY)})\n" in create
    # EMPTY: the CREATE returns at once and the first build is an explicit SYSTEM
    # REFRESH VIEW in the runbook -- never a SYSTEM WAIT VIEW, which outlives the
    # migrate client's read_timeout of 300 s and leaves the ledger dirty.
    assert "\nEMPTY\nAS SELECT\n" in create
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    # Nothing else moves on the way up: no table, no ALTER, no drop.
    assert "CREATE TABLE" not in _sql("up")
    assert "ALTER TABLE" not in _sql("up")
    assert "DROP" not in _executable(_sql("up")).upper()


def test_the_down_migration_drops_the_view_and_with_it_its_inner_table() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 2
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"DROP VIEW IF EXISTS {VIEW}"
    # One object in, one object out: the inline engine means the view owns its MergeTree
    # and DROP VIEW takes the data with it.
    assert "DROP TABLE" not in _sql("down")
    assert "CREATE MATERIALIZED VIEW" not in _sql("down")


def test_the_sort_key_carries_no_nullable_column() -> None:
    """`allow_nullable_key` is off (dagster_v3/CLAUDE.md). `role_code` and `role_year`
    are Nullable on the normalized row and BOTH are in the sort key, so the SELECT
    unwraps them -- and only them: the other four key columns are already non-nullable,
    and the three Nullable columns outside the key stay Nullable on purpose (a missing
    birth year or an open span must read as NULL, not as a zero)."""
    body = _view_body(_sql("up"))

    assert "assumeNotNull(n.role_code) AS role_code" in body
    assert "ifNull(n.role_year, 0) AS role_year" in body
    assert "n.role_code AS role_code" not in body
    assert "n.role_year AS role_year" not in body
    assert "p.birth_year AS birth_year" in body
    assert "n.role_from AS role_from" in body
    assert "n.role_to AS role_to" in body
