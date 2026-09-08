"""Migration 000393: the address entity takes its final name, in place.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view.

WHAT 000393 CHANGES (address slice 4b). `corpscout.se_company_address_v2` -- the address
entity 000392 repointed the address half at -- is renamed to its final name,
`corpscout.se_company_address`; the old final table of the 2026-08-24 model parks under
`se_company_address_legacy`, dropped by hand in slice 4c. The view's definition is otherwise
UNCHANGED -- only the table name the address half reads changes -- so this is NOT a staged
swap like 000391 and 000392. Those replaced the view's definition (build a second view, wait
for its first refresh, atomically rename). 000393 changes only what name the existing
definition reads, and `ALTER TABLE ... MODIFY QUERY` does that in place: stop the view, rename
the tables, install the re-rendered query, start the view again. No `_next` view is built and
no `SYSTEM WAIT VIEW` is needed.

The drift pin couples the migration's MODIFY QUERY body to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000393_corpscout_se_company_address_rename"
PREVIOUS_MIGRATION = "000392_corpscout_se_companies_serving_address_entity"
VIEW = "corpscout.se_companies_serving"
ENTITY = "corpscout.se_company_address"
ENTITY_V2 = "corpscout.se_company_address_v2"
LEGACY = "corpscout.se_company_address_legacy"


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


def _previous_view_body(sql: str) -> str:
    """000392's embedded SELECT: the render the down file must restore."""
    [statement] = [s for s in _statements(sql) if "CREATE MATERIALIZED VIEW" in s]
    marker = "\nAS "
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
    assert f"{ENTITY} AS a FINAL" in body
    assert ENTITY_V2 not in body
    assert "se_address_geocodes_served" not in body
    assert "corpscout.se_company_basic_info AS i FINAL" in body
    # The SETTINGS block travels with the body: a MODIFY QUERY that dropped it would leave
    # the hourly refresh running without the grace-hash join and the external-sort budget.
    assert "SETTINGS join_algorithm = 'grace_hash,hash'" in body
    assert "max_memory_usage = 12884901888" in body


def test_the_up_migration_stops_renames_repoints_and_starts() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    rename = _body(statements[2])
    assert rename.startswith("RENAME TABLE")
    assert f"{ENTITY} TO {LEGACY}" in rename
    assert f"{ENTITY_V2} TO {ENTITY}" in rename
    assert _body(statements[3]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # No staged swap: the definition is unchanged apart from the table name it reads.
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    assert "CREATE MATERIALIZED VIEW" not in _sql("up")


def test_neither_file_drops_anything() -> None:
    """Slice 4b renames; slice 4c drops, by hand, under the ledger policy."""
    for suffix in ("up", "down"):
        executable = _executable(_sql(suffix))
        assert "DROP" not in executable.upper(), suffix


def test_the_down_migration_restores_the_v2_render_after_renaming_back() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 5
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    rename = _body(statements[2])
    assert f"{ENTITY} TO {ENTITY_V2}" in rename
    assert f"{LEGACY} TO {ENTITY}" in rename
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # The restored query is 000392's, character for character.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _previous_view_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )


def test_the_up_migration_documents_the_interrupted_rename_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 393" in up
