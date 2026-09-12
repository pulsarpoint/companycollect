"""Migration 000398: the SE person entity takes its final name and the serving view follows.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view.

WHAT 000398 CHANGES (person slice 4). `corpscout.se_company_person_v2` -- created by 000396,
filled by slice 2's fold and read by slice 3's backoffice -- is renamed
`corpscout.se_company_person`, and `has_people`, `people_bolagsverket` and `people_esef` read
it under the new name. ONE pair in the RENAME: slice 0 dropped the 2026-08-19 table that held
that name, so nothing has to be parked. The definition is otherwise UNCHANGED, so this is the
in-place `ALTER TABLE ... MODIFY QUERY` of 000393 and 000396, not the staged swap of
000391/000392: no `_next`, no `SYSTEM WAIT VIEW`, no drop.

The drift pin couples the migration's MODIFY QUERY body to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000398_corpscout_se_company_person_rename"
PREVIOUS_MIGRATION = "000396_corpscout_se_company_person_entity"
VIEW = "corpscout.se_companies_serving"
ENTITY = "corpscout.se_company_person"
ENTITY_V2 = "corpscout.se_company_person_v2"
# The 2026-08-19 model's role table, dropped in slice 0. It must never come back into the
# view's body, and its name is a prefix trap of its own. THE NAME ITSELF IS LIVE AGAIN as of
# migration 000402 -- a separate object, the slice-5 roles view -- but this view still does
# not read it, so the assertion below is unchanged.
RETIRED_ROLE_TABLE = "corpscout.se_company_person_role"
# The five tables ENTITY is a PREFIX of. No whole-name match on ENTITY may hit one of them.
SIBLING_TABLES = (
    "corpscout.se_company_person_suggestion",
    "corpscout.se_company_person_normalized",
    "corpscout.se_company_person_history",
    "corpscout.se_company_person_rule",
    "corpscout.se_company_person_precedence",
)


def _sql_of(migration: str, suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{migration}.{suffix}.sql").read_text(encoding="utf-8")


def _sql(suffix: str) -> str:
    return _sql_of(MIGRATION, suffix)


def _statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
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


def _modify_query_body(sql: str) -> str:
    """The SELECT an ALTER TABLE ... MODIFY QUERY installs, without its trailing semicolon."""
    [statement] = [s for s in _statements(sql) if "MODIFY QUERY" in s]
    marker = "MODIFY QUERY\n"
    return statement[statement.index(marker) + len(marker) :]


def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    assert _normalized(_modify_query_body(_sql("up"))) == _normalized(
        build_se_companies_serving_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    body = _modify_query_body(_sql("up"))
    assert len(body) > 2000
    assert "groupArray" in body
    assert "primary_geocode_class" in body
    assert "corpscout.se_company_address AS a FINAL" in body
    # Whole-name matching: ENTITY prefixes all five siblings, so the count is taken on the
    # name PLUS the token that follows it in the three people subqueries.
    assert body.count(f"{ENTITY} FINAL") == 3
    assert "has(sources, 'bolagsverket')" in body and "has(sources, 'esef')" in body
    assert ENTITY_V2 not in body
    assert f"{RETIRED_ROLE_TABLE} " not in body and f"{RETIRED_ROLE_TABLE}\n" not in body
    for sibling in SIBLING_TABLES:
        assert sibling not in body, sibling
    assert "SETTINGS join_algorithm = 'grace_hash,hash'" in body
    assert "max_memory_usage = 12884901888" in body


def test_the_up_migration_stops_renames_repoints_and_starts() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]) == f"RENAME TABLE {ENTITY_V2} TO {ENTITY}"
    assert _body(statements[3]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # A rename, nothing else: no staged swap, no new table, no drop on either side.
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    assert "CREATE MATERIALIZED VIEW" not in _sql("up")
    assert "CREATE TABLE" not in _sql("up")
    for suffix in ("up", "down"):
        assert "DROP" not in _executable(_sql(suffix)).upper(), suffix


def test_the_down_migration_renames_back_and_restores_000396s_render() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]) == f"RENAME TABLE {ENTITY} TO {ENTITY_V2}"
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # The restored query is 000396's, modulo whitespace (_normalized collapses runs of
    # whitespace before comparing, so this is not a character-for-character check).
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )


def test_the_up_migration_documents_the_interrupted_repoint_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 398" in up
