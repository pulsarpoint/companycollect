"""Migration 000403: workplace-only address rows leave the serving view.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view.

WHAT 000403 CHANGES (Ratsit address slice 3). The Ratsit extractor publishes one address per
ESTABLISHMENT, so the address entity now holds hundreds of rows for some companies -- one
holds 1,607 -- and 355 companies have establishments but no `visiting_or_postal` row at all.
The `addresses` array is an uncapped `groupArray` and the primary address is picked by a
tiebreak over kinds, so unchanged this view would publish a ~400 KB JSON blob for that one
company and could print a branch office as another company's own address. The
`company_addresses` CTE therefore gains `AND NOT (a.kinds = ['workplace'])` -- one CTE, so the
array, `address_count` and the primary pick drop those rows together. A row the FOLD merged
(an establishment repeating the company's own postal street and postcode, kinds
`['postal', 'workplace']`) is the company's address and stays. Nothing is deleted: the rows
remain in `se_company_address` and on the backoffice Address tab.

The definition changes and nothing else does, so this is the in-place `ALTER TABLE ... MODIFY
QUERY` of 000393, 000396 and 000398, not the staged swap of 000391/000392: no `_next`, no
`SYSTEM WAIT VIEW` (a refresh takes 13 to 15 minutes against a 300-second client read
timeout), no drop.

The drift pin couples the migration's MODIFY QUERY body to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000403_corpscout_se_companies_serving_no_workplace"
PREVIOUS_MIGRATION = "000398_corpscout_se_company_person_rename"
VIEW = "corpscout.se_companies_serving"
ENTITY = "corpscout.se_company_person"
ENTITY_V2 = "corpscout.se_company_person_v2"
# The row source of the address half, with the slice-3 exclusion on it. The array, the count
# and the primary pick all read this ONE CTE, which is why the exclusion is written once.
ADDRESS_ROW_SOURCE = (
    "  FROM corpscout.se_company_address AS a FINAL\n"
    "  WHERE a.active = 1 AND NOT (a.kinds = ['workplace'])"
)
# The 2026-08-19 model's role table, dropped in person slice 0. It must never come back into
# the view's body, and its name is a prefix trap of its own. THE NAME ITSELF IS LIVE AGAIN as
# of migration 000402 -- a separate object, the slice-5 roles view -- but this view still does
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


def test_the_builder_serves_no_workplace_only_address_row() -> None:
    """Slice 3's ruling, on the builder rather than on the file: the exclusion sits on the
    row source the address half reads, so one clause takes those rows out of `addresses`, out
    of `address_count` and out of the primary tiebreak at once. It tests the WHOLE kinds
    array for equality and never `has(kinds, 'workplace')`: a merged `['postal', 'workplace']`
    row is the company's own published address and must survive."""
    sql = build_se_companies_serving_sql()

    assert ADDRESS_ROW_SOURCE in sql
    assert sql.count("kinds = ['workplace']") == 1
    assert "has(a.kinds, 'workplace')" not in sql
    # The address half is read once: the CTE the exclusion sits on is the only place the
    # entity is named, so neither the aggregate nor the primary pick can bypass it.
    assert sql.count("corpscout.se_company_address") == 1
    assert sql.count("FROM company_addresses") == 2


def test_the_pin_is_not_vacuous() -> None:
    body = _modify_query_body(_sql("up"))
    assert len(body) > 2000
    assert "groupArray" in body
    assert "primary_geocode_class" in body
    assert ADDRESS_ROW_SOURCE in body
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


def test_the_up_migration_stops_repoints_and_starts() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 4
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    # A repoint, nothing else: no staged swap, no rename, no new table, no drop on either
    # side. The refresh this view runs takes 13 to 15 minutes and the migrate client's read
    # timeout is 300 seconds, so a SYSTEM WAIT VIEW here would drop the client mid-migration.
    # Taken on the EXECUTABLE text, because the file's comments name every one of these to
    # say it is not here.
    executable = _executable(_sql("up"))
    assert "SYSTEM WAIT VIEW" not in executable
    assert "RENAME TABLE" not in executable
    assert "CREATE MATERIALIZED VIEW" not in executable
    assert "CREATE TABLE" not in executable
    for suffix in ("up", "down"):
        assert "DROP" not in _executable(_sql(suffix)).upper(), suffix


def test_the_down_migration_restores_000398s_render() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 4
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    assert "SYSTEM WAIT VIEW" not in _executable(_sql("down"))
    # The restored query is 000398's, modulo whitespace (_normalized collapses runs of
    # whitespace before comparing, so this is not a character-for-character check) -- and it
    # is the render in which a workplace-only row still counts.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )
    assert "kinds = ['workplace']" not in _modify_query_body(_sql("down"))


def test_the_up_migration_documents_the_interrupted_repoint_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 403" in up
