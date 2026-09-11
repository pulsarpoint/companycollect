"""Ratsit's responsible people -> raw person suggestions (spec 2026-09-11 section 4).

One suggestion per NAMED person of the company's CURRENT Ratsit report. se_ratsit_company
holds one row per (company, result hash, normalizer version) and is never pruned -- a
re-scan appends -- so the current report is picked here: newest normalized_at, ties by the
higher result_sha256, LIMIT 1 BY company_id. The people rows join that report on
(company_id, result_sha256, normalizer_version), so a superseded scan's people can never
reach the suggestion table.

THE SLOT IS RATSIT'S OWN PERSON ID: the trailing token of profile_url
(https://www.ratsit.se/<YYYYMMDD>-<Name>_<Town>/<token>), which the same person carries at
every company -- 191,434 distinct tokens over 277,646 rows on 2026-09-10. Keeping it as the
slot means a re-scan rewrites the person's row in place instead of tombstoning it and
inventing a new one, which is what the fold's slot-keyed reviewer rules need. Two
exceptions: 31 (company, token) pairs carry two rows (a `Delgivningsbar person` who is also
VD or Vice VD), so a token a report repeats is qualified with the lowercased role; and a
named row with no URL falls back to `idx:<person_index>`.

23,432 rows are nameless -- GDPR-limited evidence: a role, no name, no URL. They carry no
identity, so the live branch drops them and they never become suggestions.

THE UNIVERSE IS se_company_basic_info, as it is for bolagsverket.py, esef.py and
wikidata.py: this entity's universe is the folded company, and a Ratsit company the entity
does not know is dropped here rather than published as a company id nothing downstream
recognises. The join sits in the `report` CTE, which is also what `ratsit_current_sql` reads,
so the `since` watermark and the page select agree on who exists.

Ratsit delivers no machine role code, so role_key is NULL and roles.py maps the Swedish
label (SOURCE_ROLE_MAPPINGS["ratsit"]).
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

PERSON_SOURCE = "ratsit"
RATSIT_PERSON_EXTRACTOR_VERSION = "ratsit-person-v1"
# The normalizer version the current report must carry; run_extractor binds select_params
# into the scope query and into every page select.
RATSIT_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}

# extract() returns '' when nothing matches, which is what the slot's first branch tests.
TOKEN_SQL = "extract(ifNull(p.profile_url, ''), '/([A-Za-z0-9_-]+)$')"
# A window function, not a join: the page select already binds %(company_ids)s twice and the
# integration test runs under join_use_nulls 0 AND 1, so a second join is both a binding and
# a nullability risk. ClickHouse computes windows after WHERE, so the count covers the named
# rows only -- the same set the slot is drawn from.
TOKEN_ROWS_SQL = "count() OVER (PARTITION BY r.company_id, r.token)"

# The same universe join bolagsverket.py uses, against the report rather than the people
# rows: one INNER JOIN per statement, and ratsit_current_sql can reuse the identical text.
UNIVERSE_JOIN_SQL = (
    "    INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "        ON universe.company_id = c.company_id"
)

RATSIT_COLUMN_SQL: dict[str, str] = {
    "company_id": "r.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": (
        "multiIf("
        "r.token = '', concat('idx:', toString(r.person_index)), "
        f"{TOKEN_ROWS_SQL} > 1, "
        "concat(r.token, ':', lowerUTF8(trim(r.role_raw))), "
        "r.token)"
    ),
    "source_record_id": (
        "concat('ratsit:', toString(r.result_sha256), ':', toString(r.person_index))"
    ),
    # Ratsit delivers one name string; the normalizer splits it.
    "full_name": "nullIf(trim(r.name_raw), '')",
    "first_name": NULL_SQL["first_name"],
    "last_name": NULL_SQL["last_name"],
    # The profile URL's path starts with the person's birth date (YYYYMMDD).
    "birth_year": (
        "toUInt16OrNull(substring(extract(r.profile_url, "
        "'^https://www\\.ratsit\\.se/(\\d{8})-'), 1, 4))"
    ),
    "wikidata_id": NULL_SQL["wikidata_id"],
    "role_original": "nullIf(trim(r.role_raw), '')",
    # No machine code from this source (spec 4.2); roles.py keys on the label.
    "role_key": NULL_SQL["role_key"],
    # The role is current at the scan date (spec 4.2 and risk 2 of section 6: a re-scan next
    # year adds a year to the same slot, as Wikidata's dateless roles do).
    "fiscal_year": "toYear(ifNull(r.source_date_modified, toDate32(r.normalized_at)))",
    "role_from": NULL_SQL["role_from"],
    "role_to": NULL_SQL["role_to"],
    "document_ref": NULL_SQL["document_ref"],
    # mapFilter over String values coalesced to '': a key whose source value is NULL or
    # empty is ABSENT from the object rather than present as "" or null. map() alone would
    # render "age":null for the 8% of rows without an age.
    "data": (
        "toJSONString(mapFilter((k, v) -> v != '', map("
        "'age', ifNull(toString(r.age), ''), "
        "'identity_available', if(r.identity_available, 'true', 'false'), "
        "'profile_url', r.profile_url, "
        "'display_name_raw', r.display_name_raw, "
        "'ratsit_person_id', r.token, "
        "'external', if(startsWith(lowerUTF8(trim(r.role_raw)), 'extern'), 'true', 'false'))))"
    ),
}


def ratsit_report_cte_sql(*, scoped: bool = False) -> str:
    """CTEs `report` (the current report per company, inside the basic-info universe) and
    `people` (its responsible-people rows, with the profile token computed once).

    `scoped=True` narrows `report` to %(company_ids)s up front, which binds the ids exactly
    once for this whole side of the select and keeps a page from picking the current report
    of all 947,200 companies.
    """
    company_filter = "\n        AND c.company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH report AS (\n"
        "    SELECT\n"
        "        c.company_id AS company_id,\n"
        "        c.result_sha256 AS result_sha256,\n"
        "        c.normalizer_version AS normalizer_version,\n"
        "        c.normalized_at AS normalized_at,\n"
        "        c.source_date_modified AS source_date_modified\n"
        "    FROM corpscout.se_ratsit_company AS c FINAL\n"
        f"{UNIVERSE_JOIN_SQL}\n"
        f"    WHERE c.normalizer_version = %(normalizer_version)s{company_filter}\n"
        "    ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "    LIMIT 1 BY c.company_id\n"
        "),\n"
        "people AS (\n"
        "    SELECT\n"
        "        p.company_id AS company_id,\n"
        "        p.person_index AS person_index,\n"
        "        p.result_sha256 AS result_sha256,\n"
        "        ifNull(p.name, '') AS name_raw,\n"
        "        ifNull(p.display_name_raw, '') AS display_name_raw,\n"
        "        ifNull(p.role, '') AS role_raw,\n"
        "        ifNull(p.profile_url, '') AS profile_url,\n"
        "        p.age AS age,\n"
        "        p.identity_available AS identity_available,\n"
        f"        {TOKEN_SQL} AS token,\n"
        "        report.source_date_modified AS source_date_modified,\n"
        "        report.normalized_at AS normalized_at\n"
        "    FROM corpscout.se_ratsit_responsible_people AS p FINAL\n"
        "    INNER JOIN report\n"
        "        ON report.company_id = p.company_id\n"
        "        AND report.result_sha256 = p.result_sha256\n"
        "        AND report.normalizer_version = p.normalizer_version\n"
        ")\n"
    )


def ratsit_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=RATSIT_COLUMN_SQL,
        from_sql="FROM people AS r",
        where_sql="WHERE trim(r.name_raw) != ''",
        with_sql=ratsit_report_cte_sql(scoped=scoped),
    )


def ratsit_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash.

    The stamp is the CURRENT report's normalized_at, picked the way the live branch picks the
    report -- universe join included, so `since` cannot offer a company the page then drops.
    A max() over every report would be the same number today (the newest report is also the
    newest stamp), but stays honest if Ratsit ever back-fills an older scan.
    """
    return (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "    SELECT\n"
        "        c.company_id AS company_id,\n"
        "        toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at\n"
        "    FROM corpscout.se_ratsit_company AS c FINAL\n"
        f"{UNIVERSE_JOIN_SQL}\n"
        "    WHERE c.normalizer_version = %(normalizer_version)s\n"
        "    ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "    LIMIT 1 BY c.company_id\n"
        ")"
    )


def ratsit_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=ratsit_live_sql())


def ratsit_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=ratsit_live_sql(scoped=True))


se_company_person_suggestions_ratsit = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=RATSIT_PERSON_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_SELECT_PARAMS,
    changed_scope_override=ratsit_changed_scope_sql(),
    deps=[
        # The table-named keys the se_ratsit_normalized multi-asset declares. Never
        # dg.AssetKey("se_ratsit_normalized"): that is the function's name, not a key, and a
        # dep on it makes a phantom node in the graph (spec 4.1).
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_responsible_people"),
        # The universe, exactly as bolagsverket.py and wikidata.py declare it.
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every named person of a company's newest normalized Ratsit report as a raw person "
        "suggestion in se_company_person_suggestion (slot = the profile-URL token, "
        "role-qualified when one report repeats it, idx:<person_index> when the row has no "
        "URL): the name, the birth year from the URL's date, the Swedish role label, the "
        "scan year as the role year, and data with age, identity_available, profile_url, "
        "display_name_raw, the Ratsit person id and an external flag. Nameless rows are "
        "skipped; a slot the newest report no longer delivers is tombstoned. "
        "execute=false previews."
    ),
)
