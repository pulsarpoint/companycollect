"""Migration 000396: the SE person entity is created and the serving view's people flags
move onto it.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view.

WHAT 000396 CHANGES (person slice 0). `has_people`, `people_bolagsverket` and `people_esef`
stop reading `corpscout.se_company_person` and `corpscout.se_company_person_role` -- the
2026-08-19 model, dropped by hand in this same slice -- and read the active rows of the new
`corpscout.se_company_person_v2` instead. The definition is otherwise UNCHANGED, so this is
the in-place `ALTER TABLE ... MODIFY QUERY` of 000393, not the staged swap of 000391/000392.
The new table is empty until slice 2's first fold, so all three flags read 0 in between.

The drift pin couples the migration's MODIFY QUERY body to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000396_corpscout_se_company_person_entity"
PREVIOUS_MIGRATION = "000393_corpscout_se_company_address_rename"
VIEW = "corpscout.se_companies_serving"
ENTITY = "corpscout.se_company_person_v2"
RETIRED_PERSON_TABLE = "corpscout.se_company_person"
RETIRED_ROLE_TABLE = "corpscout.se_company_person_role"
NEW_TABLES = (
    "corpscout.se_company_person_suggestion",
    "corpscout.se_company_person_normalized",
    "corpscout.se_company_person_v2",
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
    assert body.count(f"{ENTITY} FINAL") == 3
    assert "has(sources, 'bolagsverket')" in body and "has(sources, 'esef')" in body
    # Whole-name matching: se_company_person prefixes the entity and both retired tables.
    assert f"{RETIRED_ROLE_TABLE} " not in body and f"{RETIRED_ROLE_TABLE}\n" not in body
    assert f"{RETIRED_PERSON_TABLE} " not in body and f"{RETIRED_PERSON_TABLE}\n" not in body
    assert "SETTINGS join_algorithm = 'grace_hash,hash'" in body
    assert "max_memory_usage = 12884901888" in body


def test_the_up_migration_creates_six_tables_then_stops_repoints_and_starts() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 10
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    for index, table in enumerate(NEW_TABLES, start=1):
        assert _body(statements[index]).startswith(f"CREATE TABLE IF NOT EXISTS {table}\n"), table
    assert _body(statements[7]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[8]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[9]) == f"SYSTEM START VIEW {VIEW}"
    # No staged swap and no drop: this migration only adds tables and re-points a query.
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    assert "CREATE MATERIALIZED VIEW" not in _sql("up")
    assert "DROP" not in _executable(_sql("up")).upper()


def test_the_down_migration_restores_000393s_render_then_drops_the_six_tables() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 10
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    # The restored query is 000393's, character for character.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )
    # Reverse creation order, so nothing is dropped while something still names it.
    dropped = [_body(statement) for statement in statements[4:]]
    assert dropped == [f"DROP TABLE IF EXISTS {table}" for table in reversed(NEW_TABLES)]


def test_the_up_migration_documents_the_interrupted_repoint_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 396" in up
