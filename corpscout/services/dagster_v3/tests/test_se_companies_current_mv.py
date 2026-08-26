"""Migration 000326: the per-company serving name is a refreshable MV, and it says the same
thing the builder says.

`corpscout.se_companies_current` is a NEW refreshable materialized view holding one denormalized
row per SE company (companies_current.build_se_companies_current_sql). It is a plain CREATE, not
000320's staged swap: nothing reads the name before this migration creates it, so there is no
concurrent reader to keep whole across a cutover and no empty-view window to hide -- the same
reasoning migration 000325 used for its brand-new sibling view. SYSTEM WAIT VIEW still follows the
CREATE so `migrate up` returns only once the first refresh has populated the view.

The drift pin is the coupling: the migration's embedded SELECT is extracted, normalized and
compared against a FRESH render of the builder, so editing the builder without shipping a
migration that carries the new rendering -- or hand-editing this file -- turns this test red,
naming both halves. Same anti-vacuous shape as the pins for 000320 and 000325.

The check-function tests exercise companies_current_refresh_is_healthy, the predicate on
system.view_refreshes that Task 2b attaches to the sweden_companies_current_clickhouse asset.
"""

import re
from datetime import UTC, datetime
from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    MAX_COMPANIES_CURRENT_REFRESH_AGE,
    SE_COMPANIES_CURRENT_REFRESH_SQL,
    build_se_companies_current_sql,
    companies_current_refresh_is_healthy,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000326_corpscout_se_companies_current"
VIEW = "corpscout.se_companies_current"


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    """The statements the runner sees: `migrate/migrate` splits on `;` under
    x-multi-statement=true (corpscout/Makefile), so splitting the same way is what the server
    is actually asked to execute."""
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    """A statement with its leading `--` commentary stripped, so an assertion can anchor on the
    statement's first VERB and not find it inside the migration's rationale."""
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
    """The file with its `--` commentary stripped: what the server is told to do. The DROP
    guards read this rather than the raw text, since the up-file EXPLAINS that it drops nothing
    and a guard matching the word anywhere would trip on its own rationale."""
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    """THE PIN. Red here means the builder and the deployed view have parted company.

    To fix it, do NOT edit the SQL file by hand to match -- write the next migration that
    replaces the view with the new rendering, and point this pin at it. A file edit alone
    changes what a fresh environment builds and leaves every already-migrated one, including
    production, serving the old per-company shape.
    """
    assert _normalized(_embedded_select(_sql("up"))) == _normalized(
        build_se_companies_current_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    """Guards the extraction: the embedded body is real, non-trivial SQL that names the view's
    own aggregation, so a broken extractor cannot make the pin pass by comparing empty strings."""
    embedded = _embedded_select(_sql("up"))
    assert len(embedded) > 1000
    assert "se_companies_current" in _sql("up")
    assert "groupArray" in embedded
    assert "se_address_geocodes_served" in embedded
    assert "primary_geocode_class" in embedded


def test_the_up_migration_is_a_refreshable_mv_created_plain_and_waited_on() -> None:
    """A refreshable MV (not a plain view): the read it holds is the expensive one, so it is
    materialized into a real MergeTree ORDER BY company_id and recomputed hourly. A plain CREATE
    (not 000320's staged swap) because the name is brand new -- nothing reads it before this
    migration, so there is no cutover to keep whole. SYSTEM WAIT VIEW follows so the CREATE is
    reported done only once the first refresh has populated the view."""
    statements = _statements(_sql("up"))

    assert len(statements) == 3
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"

    create = _create_view_statement(_sql("up"))
    assert _body(create).startswith(f"CREATE MATERIALIZED VIEW {VIEW}\n")
    assert "REFRESH EVERY 1 HOUR" in create
    assert "ENGINE = MergeTree" in create
    assert "ORDER BY company_id\nAS " in create
    # APPEND would accumulate every refresh's output; POPULATE and `TO` belong to the
    # insert-triggered kind, which a timer-refreshed view is not.
    assert "APPEND" not in create
    assert "POPULATE" not in create
    assert re.search(r"\bTO\s+corpscout\.", create) is None

    # The wait sits AFTER the create, blocking on the first refresh of the final name.
    assert statements[2] == f"SYSTEM WAIT VIEW {VIEW}"


def test_the_up_migration_creates_a_brand_new_name_and_drops_nothing() -> None:
    """A brand-new name displaces nothing: no rename, no staging swap, no DROP in the up-file
    (house rule). The single created name is the final serving name itself."""
    up = _sql("up")
    assert "DROP" not in _executable(up).upper()
    assert "RENAME TABLE" not in _executable(up).upper()
    # Created directly under the final name -- no _next staging view.
    assert f"CREATE MATERIALIZED VIEW {VIEW}\n" in _body(_create_view_statement(up))
    assert f"{VIEW}_next" not in up


def test_the_down_migration_drops_only_the_view_it_created() -> None:
    """The down-file drops the view and nothing else -- never the inputs it reads
    (se_company_address, se_company_info, se_address_geocodes_served), which other migrations
    own and this one leaves untouched."""
    executable_down = _executable(_sql("down"))
    assert f"DROP VIEW IF EXISTS {VIEW};" in executable_down
    assert "DROP TABLE" not in executable_down.upper()
    assert "se_company_address" not in executable_down
    assert "se_company_info" not in executable_down
    assert "se_address_geocodes_served" not in executable_down


# --- the refresh-health predicate (Task 2b attaches it to the asset) ------------------------

_NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


def _epoch(hours_ago: float) -> int:
    return int((_NOW.timestamp()) - hours_ago * 3600)


def test_refresh_sql_targets_this_view_in_the_serving_database() -> None:
    assert "system.view_refreshes" in SE_COMPANIES_CURRENT_REFRESH_SQL
    assert "database = 'corpscout'" in SE_COMPANIES_CURRENT_REFRESH_SQL
    assert "view = 'se_companies_current'" in SE_COMPANIES_CURRENT_REFRESH_SQL
    # The instant crosses the driver as an unambiguous epoch int, not a tz-naive DateTime.
    assert "toUnixTimestamp(last_success_time)" in SE_COMPANIES_CURRENT_REFRESH_SQL


def test_a_recent_success_with_no_exception_is_healthy() -> None:
    assert companies_current_refresh_is_healthy(
        row_found=True,
        exception="",
        last_success_epoch_seconds=_epoch(hours_ago=1),
        now=_NOW,
    )


def test_a_missing_row_fails() -> None:
    """No row means the view is not a refreshable view on this server -- migration not applied,
    or the view was replaced by a table."""
    assert not companies_current_refresh_is_healthy(
        row_found=False,
        exception="",
        last_success_epoch_seconds=_epoch(hours_ago=0),
        now=_NOW,
    )


def test_an_exception_fails_even_when_recent() -> None:
    """A throwing refresh keeps serving its last contents fast; staleness would not report for
    three hours, so the exception is the failure."""
    assert not companies_current_refresh_is_healthy(
        row_found=True,
        exception="Code 497: definer lacks SELECT",
        last_success_epoch_seconds=_epoch(hours_ago=0),
        now=_NOW,
    )


def test_a_never_succeeded_view_fails() -> None:
    """None means the view has never completed a refresh: it answers empty and raises nothing."""
    assert not companies_current_refresh_is_healthy(
        row_found=True,
        exception="",
        last_success_epoch_seconds=None,
        now=_NOW,
    )


def test_a_stale_success_fails_at_the_three_hour_budget() -> None:
    just_over = MAX_COMPANIES_CURRENT_REFRESH_AGE.total_seconds() / 3600 + 0.01
    just_under = MAX_COMPANIES_CURRENT_REFRESH_AGE.total_seconds() / 3600 - 0.01
    assert not companies_current_refresh_is_healthy(
        row_found=True,
        exception="",
        last_success_epoch_seconds=_epoch(hours_ago=just_over),
        now=_NOW,
    )
    assert companies_current_refresh_is_healthy(
        row_found=True,
        exception="",
        last_success_epoch_seconds=_epoch(hours_ago=just_under),
        now=_NOW,
    )
