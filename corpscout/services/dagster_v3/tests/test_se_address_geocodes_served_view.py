"""Migration 000325: the served overlay is a VIEW, and it says exactly what the builder says.

`corpscout.se_address_geocodes_served` is the SE geocode serving overlay
(geocode_serving_overlay.build_served_geocodes_sql) exposed as a queryable ClickHouse view,
so the backoffice and other consumers read coarse-centroid fallback coordinates off a real
object instead of re-rendering the SQL per caller. It is a SIBLING of the fast serving table
se_address_geocodes_current (migration 000320): that table is untouched, and this view reads
it as its precise base via fast_serving_base_sql, so it never re-ranks the versioned store.

That moves one expression of the overlay rules into a SQL file, where Python cannot reach
it. `build_served_geocodes_sql` stays the single source of truth -- the migration was
generated from it, not typed -- but a file cannot re-derive itself when the builder changes.
The pin below is the coupling: edit the builder (or fast_serving_base_sql) without shipping a
migration that carries the new rendering and this test goes red, naming both halves.

Same anti-vacuous shape as tests/test_sweden_geocode_store_current_mv.py's pin for 000320:
the embedded body is extracted and normalized, and compared against a FRESH render of the
builder, so a stale hand-edit on either side is caught rather than papered over.
"""

import re
from pathlib import Path

from dagster_v3.defs.sweden_company.geocode_serving_overlay import (
    build_served_geocodes_sql,
    fast_serving_base_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000325_corpscout_se_address_geocodes_served_view"
VIEW = "corpscout.se_address_geocodes_served"
BASE_TABLE = "corpscout.se_address_geocodes_current"


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    """The statements the runner sees: `migrate/migrate` splits on `;` under
    x-multi-statement=true (corpscout/Makefile), so splitting the same way is what the
    server is actually asked to execute."""
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    """A statement with its leading `--` commentary stripped, so an assertion can anchor on
    the statement's first VERB and not find it inside the migration's rationale."""
    lines = statement.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    body = "\n".join(lines).strip()
    assert body, f"no statement left after stripping comments: {statement[:80]!r}"
    return body


def _create_view_statement(sql: str) -> str:
    [statement] = [s for s in _statements(sql) if "CREATE VIEW" in s]
    return statement


def _embedded_select(sql: str) -> str:
    """The view's body: everything after the `AS` that introduces it."""
    statement = _create_view_statement(sql)
    marker = " AS\n"
    return statement[statement.index(marker) + len(marker) :]


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _executable(sql: str) -> str:
    """The file with its `--` commentary stripped: what the server is told to do. The DROP
    guards read this rather than the raw text, since the up-file EXPLAINS that it drops
    nothing and a guard matching the word anywhere would trip on its own rationale."""
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


def test_the_view_body_is_the_served_overlay_and_has_not_drifted_from_it() -> None:
    """THE PIN. Red here means the builder and the deployed view have parted company.

    To fix it, do NOT edit the SQL file by hand to match -- write the next migration that
    replaces the view with the new rendering, and point this pin at it. A file edit alone
    changes what a fresh environment builds and leaves every already-migrated one, including
    production, serving the old overlay.
    """
    assert _normalized(_embedded_select(_sql("up"))) == _normalized(
        build_served_geocodes_sql(base_sql=fast_serving_base_sql())
    )


def test_the_pin_is_not_vacuous() -> None:
    """Guards the extraction: the embedded body is real, non-trivial SQL and the render it is
    compared to actually reads the fast base table -- so a broken extractor cannot make the
    pin pass by comparing two empty strings."""
    embedded = _normalized(_embedded_select(_sql("up")))
    rendered = _normalized(build_served_geocodes_sql(base_sql=fast_serving_base_sql()))
    assert len(embedded) > 500
    assert "centroid_fallback" in embedded
    assert f"FROM {BASE_TABLE}" in rendered
    assert "se_postcode_centroids" in embedded
    assert "se_city_centroids" in embedded


def test_the_up_migration_is_a_plain_view_over_the_fast_base_and_drops_nothing() -> None:
    """A plain VIEW (not a refreshable MV): measured filter pushdown makes a single-identity
    lookup fast over the already materialized base, so no second materialized copy is needed.
    And a brand-new sibling name means the up-file only CREATEs -- no rename, no drop."""
    statements = _statements(_sql("up"))
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    view_statement = _create_view_statement(_sql("up"))
    assert _body(view_statement).startswith(f"CREATE VIEW IF NOT EXISTS {VIEW} AS\n")
    # Not the materialized kinds: this is a view over an already materialized base. Read the
    # comment-stripped file so the rationale ("NOT A REFRESHABLE MV") cannot trip the guard.
    executable_up = _executable(_sql("up"))
    assert "MATERIALIZED VIEW" not in executable_up
    assert "REFRESH" not in executable_up.upper()
    # It reads the fast serving table, never the raw versioned store, as its precise base.
    assert f"FROM {BASE_TABLE}" in view_statement
    assert re.search(r"corpscout\.se_address_geocodes\b", view_statement) is None
    # No DROP in the up-file (house rule) -- the new name displaces nothing.
    assert "DROP" not in _executable(_sql("up")).upper()


def test_the_down_migration_drops_only_the_view_it_created() -> None:
    """The down-file drops the view and nothing else -- never the fast base table it read,
    which 000320 owns, nor the store or centroid reference tables."""
    executable_down = _executable(_sql("down"))
    assert f"DROP VIEW IF EXISTS {VIEW};" in executable_down
    assert "DROP TABLE" not in executable_down.upper()
    assert BASE_TABLE not in executable_down.split("DROP VIEW")[0]
