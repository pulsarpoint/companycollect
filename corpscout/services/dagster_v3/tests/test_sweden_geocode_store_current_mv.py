"""Migration 000320: the serving name is a view over the store, and it says the same thing.

`corpscout.se_address_geocodes_current` used to be a table Dagster rebuilt weekly from the
versioned store. It is now a REFRESHABLE MATERIALIZED VIEW holding the store's two-stage
read, so nothing recomputes it and readers still hit a MergeTree ORDER BY address_id under
the same name.

That moves ONE expression of the read rule into a SQL file, where Python cannot reach it.
`geocode_store.build_current_geocodes_sql` is still the single source of truth -- the
migration was generated from it, not typed -- but a file cannot re-derive itself when the
builder changes, and a builder change that the deployed view does not carry is exactly the
failure the store's docstring spends four paragraphs preventing: two readers disagreeing
about which stored outcome is current, with neither side raising.

So the pin below is the coupling. Edit the builder without shipping a migration that
carries the new rendering and this test goes red, naming both halves. It is the only thing
standing between a refactor of the rank and a production view still ranking the old way.
"""

import re
from pathlib import Path

from dagster_v3.defs.sweden_company.geocode_store import (
    SERVING_COLUMNS,
    build_current_geocodes_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000320_corpscout_se_address_geocodes_current_mv"
VIEW = "corpscout.se_address_geocodes_current"
RETIRED = "corpscout.se_address_geocodes_current_retired"
STAGING = "corpscout.se_address_geocodes_current_next"


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    """The statements the runner sees.

    `migrate/migrate` runs this ledger with `x-multi-statement=true` (corpscout/Makefile),
    which splits the file on semicolons -- so splitting the same way here is what the
    server will actually be asked to execute, not an approximation of it.
    """
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    """A statement with the `--` commentary that precedes it stripped off.

    Splitting on semicolons hands the header block to whichever statement follows it,
    so the migration's long rationale arrives glued to the front of the CREATE. Dropping
    the leading comment lines is what lets the assertions below anchor on the statement's
    first VERB rather than merely find the verb somewhere inside a wall of prose.
    """
    lines = statement.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    body = "\n".join(lines).strip()
    assert body, f"no statement left after stripping comments: {statement[:80]!r}"
    return body


def _create_view_statement(sql: str) -> str:
    [statement] = [s for s in _statements(sql) if "CREATE MATERIALIZED VIEW" in s]
    return statement


def _embedded_select(sql: str) -> str:
    """The view's body: everything after the `AS` that introduces it."""
    statement = _create_view_statement(sql)
    marker = "\nAS "
    return statement[statement.index(marker) + len(marker) :]


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _executable(sql: str) -> str:
    """The file with its `--` commentary stripped: what the server is actually told to do.

    The DROP guards below have to read this rather than the raw text, because the file
    EXPLAINS at length why it does not drop the retired table -- and a guard that matched
    the word anywhere would be tripped by its own rationale.
    """
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


def test_the_view_body_is_the_versioned_read_and_has_not_drifted_from_it() -> None:
    """THE PIN. Red here means the builder and the deployed view have parted company.

    To fix it, do NOT edit the SQL file by hand to match -- write the next migration that
    replaces the view with the new rendering, and point this pin at it. A file edit alone
    changes what a fresh environment builds and leaves every already-migrated one, including
    production, ranking the old way.
    """
    assert _normalized(_embedded_select(_sql("up"))) == _normalized(
        build_current_geocodes_sql(columns=SERVING_COLUMNS)
    )


def test_the_view_serves_the_store_columns_and_not_the_version_columns() -> None:
    """26 columns, in the store's own declaration order, and neither key column among them.

    policy_version and reference_md5 are how the store keeps several attributable outcomes
    per identity. The serving contract has never carried them, and a view that started to
    would widen a table four backoffice modules read positionally in places.
    """
    projection = _embedded_select(_sql("up")).split("FROM (")[0]
    columns = re.findall(r"^    (\w+),?$", projection, re.MULTILINE)

    assert tuple(columns) == SERVING_COLUMNS
    assert len(columns) == 26
    assert "policy_version" not in columns
    assert "reference_md5" not in columns


def test_the_view_column_set_is_exactly_the_retired_tables_column_set() -> None:
    """The name's schema does not change, so no reader has to be told anything.

    Read out of the migrations that BUILT the old table -- 000275 plus the one column
    000277 added -- rather than hand-listed, so a column some later migration put on the
    serving table and not into this view is caught here rather than by a backoffice page
    rendering a blank field.
    """
    old_table = re.findall(
        r"^    (\w+) ",
        _sql_of("000275_corpscout_se_address_geocodes_current"),
        re.MULTILINE,
    )
    assert old_table, "the 000275 column parser needs updating"
    added_later = re.findall(
        r"ADD COLUMN IF NOT EXISTS (\w+) ",
        _sql_of("000277_corpscout_se_address_geocode_spread"),
    )
    assert "coordinate_spread_meters" in added_later

    assert set(old_table) | set(added_later) == set(SERVING_COLUMNS)


def _sql_of(migration: str) -> str:
    return (MIGRATIONS_DIR / f"{migration}.up.sql").read_text(encoding="utf-8")


def test_the_view_is_refreshable_and_keeps_the_serving_tables_engine() -> None:
    """A plain VIEW was measured and rejected: 1,396ms against the table's 26ms on a full
    read, and 1,188ms on a single-company join, because a filter cannot push through
    LIMIT BY into the inner rank. What keeps the read fast is that a refreshable MV stores
    its result in a real MergeTree sorted the way the old table was sorted."""
    statement = _create_view_statement(_sql("up"))

    assert _body(statement).startswith(f"CREATE MATERIALIZED VIEW {STAGING}\n")
    assert "REFRESH EVERY 1 HOUR" in statement
    assert "ENGINE = MergeTree" in statement
    assert "ORDER BY address_id\nAS " in statement
    # APPEND would accumulate every refresh's output instead of replacing it, turning one
    # row per identity into one per identity per hour.
    assert "APPEND" not in statement
    # A refreshable MV computes from its SELECT on a timer. POPULATE and TO belong to the
    # insert-triggered kind, which would only ever see rows an INSERT delivers -- nothing
    # writes this store through a path that fires them.
    assert "POPULATE" not in statement
    assert re.search(r"\bTO\s+corpscout\.", statement) is None



def test_the_up_migration_builds_under_a_staging_name_and_swaps_in_one_rename() -> None:
    """The apply order, pinned -- it is the difference between a clean cutover and two
    distinct ways of serving nothing.

    This ledger runs with x-multi-statement=true, so each statement is a separate
    round-trip and the backoffice reads between them. Measured on ClickHouse 26.5 with
    concurrent readers, the obvious ordering fails twice: RENAME-then-CREATE leaves the
    name absent (Code 60, UNKNOWN_TABLE -- 3 of 133 samples raised), and a freshly created
    refreshable MV is EMPTY until its first scheduled refresh lands (7 of those 133 read
    zero rows and reported no error, which is the worse failure). Building as _next,
    blocking on SYSTEM WAIT VIEW, then swapping BOTH names in a single atomic RENAME gave
    zero errors and zero empty reads across 124 samples.

    So all three properties below are load-bearing, and a reordering that kept them
    individually present but changed their sequence would reopen one of the two windows.
    """
    statements = _statements(_sql("up"))

    assert len(statements) == 4
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]).startswith(f"CREATE MATERIALIZED VIEW {STAGING}\n")
    # The wait sits BETWEEN the build and the swap. Ahead of the CREATE it would wait on
    # nothing; after the RENAME the swap would already have published an empty view.
    assert statements[2] == f"SYSTEM WAIT VIEW {STAGING}"
    # ONE rename, carrying both pairs. Two RENAME statements would be two round-trips with
    # the serving name missing in between -- the UNKNOWN_TABLE window, reintroduced.
    swap = statements[3]
    assert swap.startswith("RENAME TABLE")
    assert f"{VIEW} TO {RETIRED}" in swap
    assert f"{STAGING} TO {VIEW}" in swap
    assert swap.count("RENAME TABLE") == 1
    assert swap.index(f"{VIEW} TO {RETIRED}") < swap.index(f"{STAGING} TO {VIEW}")
    # The serving name is never itself created -- it only ever acquires the staging view.
    assert f"CREATE MATERIALIZED VIEW {VIEW}\n" not in _sql("up")


def test_the_up_migration_renames_the_old_table_and_drops_nothing() -> None:
    """The gated drop stays out of the ledger -- an owner ruling, paid for in UNDROPs.

    Dropping the old 2,090,981-row table has a precondition (the view has to have served
    correctly for long enough), and `migrate up` walks the ledger without knowing that. So
    this migration renames instead, and the drop is direct SQL a controller runs by hand
    once the gate holds. The rename is also the rollback: the down file renames it back.
    """
    up = _sql("up")

    assert f"    {VIEW} TO {RETIRED},\n" in up
    assert "DROP" not in _executable(up).upper()

    down = _sql("down")
    executable_down = _executable(down)
    assert f"DROP VIEW IF EXISTS {VIEW};" in executable_down
    assert f"RENAME TABLE {RETIRED}\n    TO {VIEW};" in executable_down
    # The down file's drop takes away only what its own up file created -- the view. It
    # must never reach the store, which holds outcomes (the adopted import above all) that
    # no asset can reproduce, nor the retired table it exists to hand back.
    assert "DROP TABLE" not in executable_down.upper()
    assert "se_address_geocodes;" not in executable_down
    assert RETIRED not in executable_down.split("RENAME TABLE")[0]


def test_no_up_migration_drops_the_retired_serving_table() -> None:
    """The same guard the gated legacy drops get, over the whole ledger.

    Mirrors test_no_drop_migration_file_carries_these_retirements in
    tests/test_sweden_company_address_geocoding.py: matching DROP TABLE forms rather than
    one spelling, because the likeliest way back in is someone pasting a hand-run statement
    into an up file.

    EXIT CONDITION: delete this guard once the retired table has actually been dropped by
    hand and that is recorded beside the ledger entry for the apply. Until then a
    rebuilt-from-ledger environment must keep the rollback copy.
    """
    up_files = sorted(MIGRATIONS_DIR.glob("*.up.sql"))
    assert up_files, "no migration up files found -- this guard would pass vacuously"
    pattern = re.compile(
        r"drop\s+table\s+(?:if\s+exists\s+)?(?:corpscout\.)?"
        r"se_address_geocodes_current_retired\b",
        re.IGNORECASE,
    )
    for path in up_files:
        assert pattern.search(path.read_text(encoding="utf-8")) is None, path.name
