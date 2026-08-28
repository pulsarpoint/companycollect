"""Migration 000338: the serving view widened with translations, pinned to its builder.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view. Because the name
now has live readers, 000338 is 000320's staged swap -- build under _next, SYSTEM WAIT, one
atomic RENAME -- not 000335's plain CREATE.

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
MIGRATION = "000338_corpscout_se_companies_serving_translations"
VIEW = "corpscout.se_companies_serving"
NEXT = "corpscout.se_companies_serving_next"
RETIRED = "corpscout.se_companies_serving_retired"


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
    # The absorbed translation joins (the retired se_companies_translated's contract).
    assert "text_translations" in embedded
    assert "activity_description_en" in embedded
    assert "se_code_labels" in embedded
    assert "status_reason_label_en" in embedded
    assert "bolagsverket_source_record_uid" in embedded
    assert "LEFT JOIN aggregated" in embedded
    assert "LEFT JOIN primary_address" in embedded
    assert "INNER JOIN corpscout.se_company_info" not in embedded


def test_the_up_migration_is_a_staged_swap_waited_on_before_the_rename() -> None:
    """000320's pattern, for 000320's reason: the serving name has LIVE readers now, so the
    widened view builds under _next, its first refresh is waited on, and ONE atomic RENAME
    swaps both names -- a reader sees the old view or the fully populated new one, never
    UNKNOWN_TABLE and never an empty view."""
    statements = _statements(_sql("up"))

    assert len(statements) == 4
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"

    create = _create_view_statement(_sql("up"))
    assert _body(create).startswith(f"CREATE MATERIALIZED VIEW {NEXT}\n")
    assert "REFRESH EVERY 15 MINUTE" in create
    assert "ENGINE = MergeTree" in create
    assert "ORDER BY company_id\nAS " in create
    assert "APPEND" not in create
    assert "POPULATE" not in create

    assert statements[2] == f"SYSTEM WAIT VIEW {NEXT}"

    rename = _body(statements[3])
    assert rename.startswith("RENAME TABLE")
    assert f"{VIEW} TO {RETIRED}" in rename
    assert f"{NEXT} TO {VIEW}" in rename


def test_the_up_migration_drops_nothing() -> None:
    """The pre-translation view keeps its machinery under the _retired name; its drop is the
    follow-up migration's, together with se_companies_translated once the dbt model repoint
    is deployed."""
    up = _sql("up")
    assert "DROP" not in _executable(up).upper()


def test_the_down_migration_swaps_back_and_discards_the_translated_render() -> None:
    down = _executable(_sql("down"))
    assert f"{RETIRED} TO {VIEW}" in down
    assert "DROP VIEW IF EXISTS corpscout.se_companies_serving_translated_discard" in down
