"""Migration 000392: the serving view reads the address entity, pinned to its builder.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view. Because the name
has live readers, every render since 000338 is 000320's staged swap -- build under _next,
SYSTEM WAIT, one atomic RENAME -- not 000335's plain CREATE.

THE SPINE, since 000391 (basic-info slice 4), is `se_company_basic_info`: legal name, status,
legal form and the two descriptions come from the folded row, the register fields from
`se_bolagsverket_companies`, the legal-form labels from `se_code_labels`. `se_company_info`,
`se_companies` and `text_translations` are gone from the view.

WHAT 000392 CHANGES (address slice 4a, task 3). The address half now reads
`corpscout.se_company_address_v2` -- the address entity, one row per company and published
address, `active = 1` -- instead of the old `se_company_address` final table LEFT-JOINed to
the `se_address_geocodes_served` overlay. The coordinate, status, precision and the derived
provider all sit on the entity row, so the join is gone; `matched_area` is what the overlay
used to stamp `centroid_fallback` on, and the derived provider keeps such a row classifying
`coarse`. The primary pick gains a `has_location` rank ahead of the kind ranks. The refresh
cadence is 000366's hourly one, carried into the _next definition (a CREATE cannot inherit
it), and the first statement frees the `_retired` name 000391's own staged swap left
occupied -- the drop 000345 and 000348 each ran as a separate follow-up migration.

The drift pin couples the migration's embedded SELECT to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

import re
from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000392_corpscout_se_companies_serving_address_entity"
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
    # The address half reads the entity table directly -- no served-overlay join left.
    assert "corpscout.se_company_address_v2 AS a FINAL" in embedded
    assert "se_address_geocodes_served" not in embedded
    assert "a.is_current" not in embedded
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
    assert "corpscout.se_company_basic_info AS i FINAL" in embedded
    assert "corpscout.se_bolagsverket_companies" in embedded
    assert "text_translations" not in embedded
    assert "corpscout.se_company_info" not in embedded
    assert "corpscout.se_companies AS" not in embedded
    assert "activity_description_en" in embedded
    assert "se_code_labels" in embedded
    assert "code_type = 'legal_form'" in embedded
    assert "status_reason_label_en" in embedded
    assert "bolagsverket_source_record_uid" in embedded
    # The market flags (owner 2026-08-28).
    assert "is_publicly_traded" in embedded
    assert "has_government_contracts" in embedded
    assert "has_job_ads" in embedded
    assert "company_traded_symbols" in embedded
    assert "se_government_contracts" in embedded
    assert "company_job_history" in embedded
    assert "LEFT JOIN aggregated" in embedded
    assert "LEFT JOIN primary_address" in embedded
    assert "se_company_info" not in embedded


def test_the_up_migration_is_a_staged_swap_waited_on_before_the_rename() -> None:
    """000320's pattern, for 000320's reason: the serving name has LIVE readers now, so the
    widened view builds under _next, its first refresh is waited on, and ONE atomic RENAME
    swaps both names -- a reader sees the old view or the fully populated new one, never
    UNKNOWN_TABLE and never an empty view."""
    statements = _statements(_sql("up"))

    assert len(statements) == 6
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    # 000391's own staged swap left the _retired name occupied, so this render frees it
    # first -- the drop 000345 and 000348 each ran as a separate follow-up migration.
    assert _body(statements[1]) == f"DROP TABLE IF EXISTS {RETIRED}"
    # Learned from 000338's apply: the old view's refresh is stopped first so the _next
    # build cannot OOM-collide with it on the server memory cap.
    assert _body(statements[2]) == f"SYSTEM STOP VIEW {VIEW}"

    create = _create_view_statement(_sql("up"))
    assert _body(create).startswith(f"CREATE MATERIALIZED VIEW {NEXT}\n")
    # 000366's cadence, restated: a CREATE does not inherit the live view's refresh clause.
    assert "REFRESH EVERY 1 HOUR OFFSET 45 MINUTE" in create
    assert "ENGINE = MergeTree" in create
    assert "ORDER BY company_id\nAS " in create
    assert "APPEND" not in create
    assert "POPULATE" not in create

    assert statements[4] == f"SYSTEM WAIT VIEW {NEXT}"

    rename = _body(statements[5])
    assert rename.startswith("RENAME TABLE")
    assert f"{VIEW} TO {RETIRED}" in rename
    assert f"{NEXT} TO {VIEW}" in rename


def test_the_up_migration_drops_only_the_occupied_retired_name() -> None:
    """The view this render replaces keeps its machinery under the _retired name so the down
    file can swap it back; the ONE drop is of the PREVIOUS retiree 000391 parked there, which
    000345 and 000348 each cleared in a follow-up migration of their own."""
    executable = _executable(_sql("up"))
    drops = [line for line in executable.splitlines() if "DROP" in line.upper()]
    assert drops == [f"DROP TABLE IF EXISTS {RETIRED};"]


def test_the_up_migration_documents_the_interrupted_wait_recovery() -> None:
    """If the migrate client drops during SYSTEM WAIT VIEW the STOP has landed and the RENAME
    has not, and the recovery is by hand. The runbook lives in the address design doc; the
    migration header points at it so whoever is holding the failed apply reads it there."""
    up = _sql("up")
    assert "SYSTEM WAIT VIEW" in up
    assert "migrate force 392" in up


def test_the_down_migration_swaps_back_and_discards_the_entity_render() -> None:
    down = _executable(_sql("down"))
    discard = "corpscout.se_companies_serving_address_entity_discard"
    assert f"{RETIRED} TO {VIEW}" in down
    assert f"SYSTEM START VIEW {VIEW}" in down
    assert f"DROP VIEW IF EXISTS {discard}" in down
