"""Migration 000404: the serving view's financial flags read the financial entity, and the
filing-status view's data_available leg does too.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view.

WHAT 000404 CHANGES (financial slice 4a, spec 2026-09-11 section 10). `has_financial` becomes
"an active row in corpscout.se_company_financial OR a filed report" (the 2026-08-25 widening
on filed reports stays), `fin_bolagsverket` and `fin_esef` become `has(sources, ...)` over the
same active rows (the restated column `bolagsverket_comparative` is Bolagsverket data and
lights that flag too), and the IN-set subqueries on se_bolagsverket_financial_metrics,
esef_financial_metrics and company_identifier leave the view. The same file re-issues
`se_annual_report_filing_status_current` (000282) with its data_available leg reading the
entity's newest active standalone period end instead of se_company_financials_latest.

The serving definition changes and nothing else does, so this is the in-place `ALTER TABLE
... MODIFY QUERY` of 000393, 000396, 000398 and 000403, not the staged swap of 000391/000392:
no `_next`, no `SYSTEM WAIT VIEW` (a refresh takes 13 to 15 minutes against a 300-second
client read timeout), no drop. The filing-status view is a plain view: CREATE OR REPLACE.

The drift pin couples the migration's MODIFY QUERY body to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000404_corpscout_se_financial_readers_entity"
PREVIOUS_MIGRATION = "000403_corpscout_se_companies_serving_no_workplace"
FILING_VIEW = "corpscout.se_annual_report_filing_status_current"
FINANCIAL_ENTITY = "corpscout.se_company_financial"
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


def test_the_builder_reads_the_financial_entity_for_every_financial_flag() -> None:
    """Slice 4a's repoint, on the builder rather than on the file: the three financial arms
    read the entity's ACTIVE rows under FINAL, the register flags read the row's `sources`
    (the restated Bolagsverket column counts as Bolagsverket), the filed-reports arm stays,
    and no financial arm names a source table or company_identifier any more."""
    sql = build_se_companies_serving_sql()

    assert sql.count(f"{FINANCIAL_ENTITY} FINAL") == 3
    assert "toUInt8(fin_entity OR fin_reports) AS has_financial" in sql
    assert "WHERE active = 1 AND hasAny(sources, ['bolagsverket', 'bolagsverket_comparative'])" in sql
    assert sql.count("has(sources, 'esef')") == 2          # the people arm and the financial arm
    assert "FROM corpscout.se_financial_reports" in sql
    for gone in ("se_bolagsverket_financial_metrics", "esef_financial_metrics", "company_identifier"):
        assert gone not in sql, gone


def test_the_pin_is_not_vacuous() -> None:
    body = _modify_query_body(_sql("up"))
    assert len(body) > 2000
    assert "groupArray" in body
    assert "primary_geocode_class" in body
    assert ADDRESS_ROW_SOURCE in body
    assert body.count(f"{FINANCIAL_ENTITY} FINAL") == 3
    assert "se_bolagsverket_financial_metrics" not in body and "company_identifier" not in body
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

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    # The filing-status view, re-issued in place with its data_available leg on the entity.
    filing = _body(statements[4])
    assert filing.startswith(f"CREATE OR REPLACE VIEW {FILING_VIEW} AS")
    assert f"FROM {FINANCIAL_ENTITY} FINAL" in filing and "WHERE active = 1 AND scope = 'standalone'" in filing
    assert "se_company_financials_latest" not in filing
    assert "FROM corpscout.se_annual_report_filing_observations FINAL" in filing
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


def test_the_down_migration_restores_000403s_render_and_000282s_view() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    assert "SYSTEM WAIT VIEW" not in _executable(_sql("down"))
    # The restored query is 000403's, modulo whitespace (_normalized collapses runs of
    # whitespace before comparing, so this is not a character-for-character check) -- the
    # render whose financial flags still read the source tables.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )
    assert FINANCIAL_ENTITY not in _modify_query_body(_sql("down"))
    assert "se_bolagsverket_financial_metrics" in _modify_query_body(_sql("down"))
    # And the filing-status view is 000282's text, verbatim modulo whitespace.
    [original] = [
        _body(s) for s in _statements(_sql_of("000282_corpscout_se_annual_report_filing_status", "up"))
        if _body(s).startswith("CREATE OR REPLACE VIEW")
    ]
    assert _normalized(_body(statements[4])) == _normalized(original)
    assert "FROM corpscout.se_company_financials_latest" in _body(statements[4])


def test_the_up_migration_documents_the_interrupted_repoint_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 404" in up
