"""Migration 000335: the consolidated companies serving view, pinned to its builder.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags the backoffice used to
recompute as IN-set subqueries per page load, and the address JSON + primary geocode summary
`se_companies_current` (migration 000326) carried. It is a plain CREATE under a brand-new
name (nothing reads it before the backoffice repoint ships) followed by SYSTEM WAIT VIEW,
exactly the 000326 shape at a 15-minute cadence.

The drift pin couples the migration's embedded SELECT to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
Same anti-vacuous shape as the pins for 000320/000325/000326.
"""

import re
from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000335_corpscout_se_companies_serving"
VIEW = "corpscout.se_companies_serving"


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
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
    statement = _create_view_statement(sql)
    marker = "\nAS "
    return statement[statement.index(marker) + len(marker) :]


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _executable(sql: str) -> str:
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    """THE PIN. Red here means the builder and the deployed view have parted company. Fix it
    with the NEXT migration carrying the new rendering, never by hand-editing the SQL file."""
    assert _normalized(_embedded_select(_sql("up"))) == _normalized(
        build_se_companies_serving_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    embedded = _embedded_select(_sql("up"))
    assert len(embedded) > 2000
    assert "se_companies_serving" in _sql("up")
    assert "groupArray" in embedded
    assert "se_address_geocodes_served" in embedded
    assert "primary_geocode_class" in embedded
    # The consolidated part: presence flags and source flags live IN the view now.
    assert "has_financial" in embedded
    assert "source_bolagsverket" in embedded
    assert "se_bolagsverket_financial_metrics" in embedded
    assert "se_financial_reports" in embedded
    assert "se_company_person" in embedded
    assert "company_domains" in embedded
    # The base is ALL of se_company_info, LEFT-joined to the address aggregation --
    # a company with no current address still gets a row.
    assert "LEFT JOIN aggregated" in embedded
    assert "LEFT JOIN primary_address" in embedded
    assert "INNER JOIN corpscout.se_company_info" not in embedded


def test_the_up_migration_is_a_refreshable_mv_created_plain_and_waited_on() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 3
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"

    create = _create_view_statement(_sql("up"))
    assert _body(create).startswith(f"CREATE MATERIALIZED VIEW {VIEW}\n")
    assert "REFRESH EVERY 15 MINUTE" in create
    assert "ENGINE = MergeTree" in create
    assert "ORDER BY company_id\nAS " in create
    assert "APPEND" not in create
    assert "POPULATE" not in create
    assert re.search(r"\bTO\s+corpscout\.", create) is None

    assert statements[2] == f"SYSTEM WAIT VIEW {VIEW}"


def test_the_up_migration_creates_a_brand_new_name_and_drops_nothing() -> None:
    """se_companies_current keeps serving its remaining readers until they are repointed;
    its drop is a separate migration with its own zero-reader proof."""
    up = _sql("up")
    assert "DROP" not in _executable(up).upper()
    assert "RENAME TABLE" not in _executable(up).upper()
    assert f"CREATE MATERIALIZED VIEW {VIEW}\n" in _body(_create_view_statement(up))
    assert f"{VIEW}_next" not in up


def test_the_down_migration_drops_only_the_serving_view() -> None:
    down = _executable(_sql("down"))
    assert f"DROP VIEW IF EXISTS {VIEW}" in down
    assert "se_companies_current" not in down
