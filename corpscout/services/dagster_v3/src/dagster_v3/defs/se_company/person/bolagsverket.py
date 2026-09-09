"""Bolagsverket annual-report signatories -> raw person suggestions (spec 2026-09-09
sections 3.1 and 6).

One suggestion per signature line. se_financial_report_signatories holds one row per
(report, signatory kind, person sequence), so a person who signs two years -- or signs one
report both in the board section and on the certification -- has two slots, which is what
spec 3.1.1 asks for. The slot is the report's source_record_uid plus the table's own
MATERIALIZED signatory_uid; ClickHouse computes both from the row's natural key, so a
re-parse that reproduces the same signature line reproduces the same slot and rewrites the
row in place instead of tombstoning it and inventing a new one.

The universe is se_company_basic_info: 31 of the 577,932 signatory companies have no
basic-info row (measured 2026-09-09) and are dropped here rather than published as company
ids nothing downstream knows.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)

PERSON_SOURCE = "bolagsverket"
BOLAGSVERKET_PERSON_EXTRACTOR_VERSION = "bolagsverket-person-v1"

UNIVERSE_JOIN_SQL = (
    "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "    ON universe.company_id = s.company_id"
)

BOLAGSVERKET_COLUMN_SQL: dict[str, str] = {
    "company_id": "s.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": "concat(s.source_record_uid, ':', toString(s.signatory_uid))",
    "source_record_id": "s.source_record_uid",
    # Bolagsverket delivers the split name, never one string.
    "full_name": NULL_SQL["full_name"],
    "first_name": "nullIf(trim(s.first_name), '')",
    "last_name": "nullIf(trim(s.last_name), '')",
    "birth_year": NULL_SQL["birth_year"],
    "wikidata_id": NULL_SQL["wikidata_id"],
    "role_original": "nullIf(trim(s.role_original), '')",
    # role_kind is the map key in roles.py; 'unknown' is its roleless code.
    "role_key": "nullIf(trim(toString(s.role_kind)), '')",
    # fiscal_year is Int32 in the source and Nullable(UInt16) here (spec 4.3: this is the
    # role year); anything outside UInt16 is a parse artefact, not a year.
    "fiscal_year": "if(s.fiscal_year BETWEEN 1900 AND 2155, toUInt16(s.fiscal_year), CAST(NULL AS Nullable(UInt16)))",
    "role_from": NULL_SQL["role_from"],
    "role_to": NULL_SQL["role_to"],
    "document_ref": "nullIf(s.statement_key, '')",
    # Spec 3.1.2's Bolagsverket extras. The report has no fiscal-year-end column here --
    # fiscal_year (the year itself) is the only period marker the table carries, and it has
    # its own column above.
    "data": (
        "toJSONString(map('signatory_kind', toString(s.signatory_kind), "
        "'statement_key', s.statement_key, 'person_seq', toString(s.person_seq)))"
    ),
}


def bolagsverket_live_sql(*, scoped: bool = False) -> str:
    where_sql = "WHERE (trim(s.first_name) != '' OR trim(s.last_name) != '')"
    if scoped:
        where_sql += "\n    AND s.company_id IN %(company_ids)s"
    return live_select_sql(
        columns=BOLAGSVERKET_COLUMN_SQL,
        from_sql=f"FROM corpscout.se_financial_report_signatories AS s\n{UNIVERSE_JOIN_SQL}",
        where_sql=where_sql,
    )


def bolagsverket_current_sql() -> str:
    """(company_id, observed_at) for `since` only. The change scan is the state hash: the
    whole table is rebuilt on every run with a single resolved_at, so this watermark says
    `all` or `none` and nothing in between."""
    return (
        "SELECT s.company_id AS company_id, max(s.resolved_at) AS observed_at\n"
        "FROM corpscout.se_financial_report_signatories AS s\n"
        f"{UNIVERSE_JOIN_SQL}\n"
        "GROUP BY s.company_id"
    )


def bolagsverket_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=bolagsverket_live_sql())


def bolagsverket_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=bolagsverket_live_sql(scoped=True))


se_company_person_suggestions_bolagsverket = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=BOLAGSVERKET_PERSON_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=bolagsverket_select_sql(),
    changed_scope_override=bolagsverket_changed_scope_sql(),
    deps=[
        dg.AssetKey("se_financial_report_signatories_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every Swedish annual-report signature line as a raw person suggestion in "
        "se_company_person_suggestion (slot = report record uid + signatory uid); a line the "
        "rebuilt source no longer delivers is tombstoned. execute=false previews."
    ),
)
