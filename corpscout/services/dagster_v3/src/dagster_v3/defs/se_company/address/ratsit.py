"""Ratsit's normalized company report -> raw address suggestions (spec 2026-09-11 section 5).

TWO ROW KINDS PER COMPANY. The company's own postal address keeps slot `company` and kind
`postal`; every establishment of the SAME report that carries a street AND a postcode adds a
row in slot `est:<identifier>` with kind `workplace` (846,718 establishment rows on
2026-09-10, 732,626 of them with both; `identifier` is on every row and unique within a
company on all but 2 rows of the CURRENT reports, which get the establishment index appended
-- the raw table repeats an identifier 324 times, but nearly all of those pairs sit in
different, superseded scans of one company and never meet in a page). 288,840 establishments
repeat the company's own postal street and postcode, so the fold merges them into the postal
address and its `kinds` becomes ['postal', 'workplace']; the rest publish as their own
addresses. The largest company has 1,718 establishments.

THE POSTAL TOWN COMES FROM THE REGISTER, NOT FROM RATSIT. Ratsit delivers the MUNICIPALITY as
the locality on 260,862 of 928,491 company addresses (Stockholm for Bromma 9,871, Göteborg
for Västra Frölunda 6,930, Nacka for Saltsjö-Boo 4,575, Gotland for Visby 3,355 ...). The
normalizer's `city` is part of `location_key`, `address_key` and every fold compatibility
test, so such a row folded into its own set beside the register address and geocoded as its
own key -- the ~26k near-duplicate second addresses of the address entity's follow-up list.
`TOWNS_SQL` rebuilds the register's postcode -> town dictionary per page from the same SCB
rows address/scb.py reads (15,698 postcodes; 38 carry more than one spelling, 12 a minority
above 5%), and every delivered row takes its town from there. A postcode the register does
not know -- about 1,000 of the delivered ones today -- keeps Ratsit's own locality, the only
town such a row has. Establishment localities are already the postal town (4 of 732,626
differ), so the dictionary changes nothing for them; it is applied uniformly rather than only
to the company row, so one rule explains every published town.

THE JOIN KEY IS DIGITS ONLY. `replaceRegexpAll(..., '[^0-9]', '')` on both sides, which is
what normalize_se.py's `re.sub(r"\\D", "", ...)` does to the postcode it stores, so a
delivered `111 22` and a register `11122` are the same postcode. The DELIVERED `postal_code`
stays as delivered (trimmed), per spec 5.2.

PER-SLOT TOMBSTONES. With many slots per company, a slot the newest report stops delivering
must be nulled rather than left behind: `suggestions.address_select_sql` adds the
live UNION ALL tombstones branch, on the shape of person/suggestions.py. Ratsit never
deletes companies, so there is no whole-company tombstone.

ITS OWN `current_sql`. basic_info/ratsit.py's stamp is `greatest(normalized_at,
business_description_translated_at)` over se_ratsit_company_translated, which exceeds the
`observed_at` this select writes and would re-select the 213,283 translated companies (as of
2026-09-11) on every address run. This module reads se_ratsit_company directly instead.
"""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import (
    address_select_sql,
    define_address_suggestion_asset,
)
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

ADDRESS_SOURCE = "ratsit"
# v2 (2026-09-12): the register's postal town, one workplace row per establishment, per-slot
# tombstones, and this module's own current_sql.
RATSIT_ADDRESS_EXTRACTOR_VERSION = "ratsit-address-v2"
RATSIT_ADDRESS_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}

# The register's postcode -> postal town dictionary (spec 5.1): the most frequent trimmed
# spelling per digits-only postcode, ties broken by the alphabetically first spelling. No
# table and no migration: it is recomputed from the 1.8M SCB rows every time the text is
# evaluated, which is about TEN times per page -- the `live` CTE is inlined at each of its
# three references and the page runs a count and an insert over it. ~7 s per statement,
# ~20 min over the 95 pages of a full run, comfortably inside max_execution_time. The inner
# aliases are `postal_code_digits`/`town` rather than the column names they derive from --
# `expr(postal_code) AS postal_code` is a cyclic alias in ClickHouse.
TOWNS_SQL = (
    "SELECT\n"
    "    replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
    "    trim(ifNull(post_town, '')) AS town\n"
    "FROM corpscout.se_scb_companies FINAL\n"
    "WHERE has_company = 1\n"
    "    AND replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') != ''\n"
    "    AND trim(ifNull(post_town, '')) != ''\n"
    "GROUP BY postal_code_digits, town\n"
    "ORDER BY count() DESC, town\n"
    "LIMIT 1 BY postal_code_digits"
)

# A window, not a second join: the page select already binds %(company_ids)s three times and
# the integration test runs under join_use_nulls 0 AND 1. ClickHouse computes windows after
# the inner subquery's WHERE, so the count covers only the establishments that actually
# become rows -- the same set the slot is drawn from.
EST_ROWS_SQL = "count() OVER (PARTITION BY est.company_id, est.identifier)"
# 2 rows of the CURRENT reports repeat an identifier inside one company, and those two rows
# would otherwise share a slot, collapse in the ReplacingMergeTree and leave the change scan
# re-selecting the company for ever. (The raw table repeats an identifier 324 times, but the
# other pairs are the SAME establishment seen in two superseded scans of one company, which
# the report join keeps apart -- only a repeat inside ONE report can collide.) The suffix is
# the establishment index, which is unique by construction.
EST_SLOT_SQL = (
    f"if({EST_ROWS_SQL} > 1, "
    "concat('est:', est.identifier, ':', toString(est.establishment_index)), "
    "concat('est:', est.identifier))"
)
# The register's spelling when the postcode is known, Ratsit's locality otherwise. Both
# branches are plain Strings and the ifNull wrappers make the expression read the same under
# join_use_nulls 0 (an unmatched right side is '') and 1 (it is NULL).
POST_TOWN_SQL = "nullIf(if(ifNull(towns.town, '') != '', ifNull(towns.town, ''), r.locality), '')"


def ratsit_report_sql(*, scoped: bool = False) -> str:
    """The current report per company: newest `normalized_at`, ties by the higher
    `result_sha256`, `LIMIT 1 BY company_id`.

    `scoped=True` narrows it to %(company_ids)s up front, so a page picks the current report
    of its 10,000 companies rather than of all 947,200. The text appears twice in the page
    select (the company row and the establishments join), which is why the select binds the
    page three times in total.
    """
    company_filter = "\n    AND c.company_id IN %(company_ids)s" if scoped else ""
    return (
        "SELECT\n"
        "    c.company_id AS company_id,\n"
        "    c.result_sha256 AS result_sha256,\n"
        "    c.normalizer_version AS normalizer_version,\n"
        "    c.normalized_at AS normalized_at,\n"
        "    c.address_street AS address_street,\n"
        "    c.address_postal_code AS address_postal_code,\n"
        "    c.address_locality AS address_locality,\n"
        "    c.address_county AS address_county\n"
        "FROM corpscout.se_ratsit_company AS c FINAL\n"
        f"WHERE c.normalizer_version = %(normalizer_version)s{company_filter}\n"
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "LIMIT 1 BY c.company_id"
    )


def ratsit_establishments_sql(*, scoped: bool = False) -> str:
    """The current report's establishments that carry a street AND a postcode, trimmed once.

    They join the report's own key `(company_id, result_sha256, normalizer_version)`, so a
    superseded scan's workplaces can never reach the suggestion table. `name`, the NACE
    mapping and the employee range are not carried: the address suggestion has no data
    column, and the slot keeps the establishment identifier for a later establishments entity.
    """
    return (
        "SELECT\n"
        "    e.company_id AS company_id,\n"
        "    e.establishment_index AS establishment_index,\n"
        "    trim(ifNull(e.identifier, '')) AS identifier,\n"
        "    trim(ifNull(e.address_street, '')) AS street_address,\n"
        "    trim(ifNull(e.address_postal_code, '')) AS postal_code,\n"
        "    replaceRegexpAll(ifNull(e.address_postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
        "    trim(ifNull(e.address_locality, '')) AS locality,\n"
        "    trim(ifNull(e.address_county, '')) AS county,\n"
        "    report.result_sha256 AS result_sha256,\n"
        "    report.normalized_at AS normalized_at\n"
        "FROM corpscout.se_ratsit_establishments AS e FINAL\n"
        f"INNER JOIN (\n{ratsit_report_sql(scoped=scoped)}\n) AS report\n"
        "    ON report.company_id = e.company_id\n"
        "    AND report.result_sha256 = e.result_sha256\n"
        "    AND report.normalizer_version = e.normalizer_version\n"
        "WHERE trim(ifNull(e.address_street, '')) != '' AND trim(ifNull(e.address_postal_code, '')) != ''"
    )


def ratsit_rows_sql(*, scoped: bool = False) -> str:
    """The company row UNION ALL one row per qualifying establishment, in one shape.

    The company row is emitted whether or not it carries a street (as the v1 extractor did):
    ~18,700 of 947,294 reports have no street, and the normalizer files those as `no_address`,
    which is the record that Ratsit knows the company and delivered nothing usable.
    """
    return (
        "SELECT\n"
        "    report.company_id AS company_id,\n"
        "    'company' AS slot,\n"
        "    concat('ratsit:', toString(report.result_sha256)) AS source_record_uid,\n"
        "    toDateTime64(report.normalized_at, 3, 'UTC') AS observed_at,\n"
        "    'postal' AS kind,\n"
        "    trim(ifNull(report.address_street, '')) AS street_address,\n"
        "    trim(ifNull(report.address_postal_code, '')) AS postal_code,\n"
        "    replaceRegexpAll(ifNull(report.address_postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
        "    trim(ifNull(report.address_locality, '')) AS locality,\n"
        "    trim(ifNull(report.address_county, '')) AS county\n"
        f"FROM (\n{ratsit_report_sql(scoped=scoped)}\n) AS report\n"
        "UNION ALL\n"
        "SELECT\n"
        "    est.company_id AS company_id,\n"
        f"    {EST_SLOT_SQL} AS slot,\n"
        "    concat('ratsit:', toString(est.result_sha256), ':est:', toString(est.establishment_index)) AS source_record_uid,\n"
        "    toDateTime64(est.normalized_at, 3, 'UTC') AS observed_at,\n"
        "    'workplace' AS kind,\n"
        "    est.street_address AS street_address,\n"
        "    est.postal_code AS postal_code,\n"
        "    est.postal_code_digits AS postal_code_digits,\n"
        "    est.locality AS locality,\n"
        "    est.county AS county\n"
        f"FROM (\n{ratsit_establishments_sql(scoped=scoped)}\n) AS est"
    )


def ratsit_live_sql(*, scoped: bool = False) -> str:
    """The thirteen raw suggestion columns, in ADDRESS_SELECT_COLUMNS order, with the
    register's postal town joined on.

    `raw_address` and `care_of` stay NULL: the normalizer PARSES `raw_address` (it is
    Bolagsverket's packed five-part string) and splits the care-of Ratsit glues onto the
    street. `country_code` stays NULL; every Ratsit address is Swedish and the normalizer
    defaults to SE.
    """
    return (
        "SELECT\n"
        "    r.company_id AS company_id,\n"
        f"    '{ADDRESS_SOURCE}' AS source,\n"
        "    r.slot AS slot,\n"
        "    r.source_record_uid AS source_record_uid,\n"
        "    r.observed_at AS observed_at,\n"
        "    r.kind AS kind,\n"
        "    CAST(NULL AS Nullable(String)) AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        "    nullIf(r.street_address, '') AS street_address,\n"
        "    nullIf(r.postal_code, '') AS postal_code,\n"
        f"    {POST_TOWN_SQL} AS post_town,\n"
        "    nullIf(r.county, '') AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        f"FROM (\n{ratsit_rows_sql(scoped=scoped)}\n) AS r\n"
        f"LEFT JOIN (\n{TOWNS_SQL}\n) AS towns ON towns.postal_code_digits = r.postal_code_digits"
    )


def ratsit_current_sql() -> str:
    """(company_id, observed_at) for the change scan and the `since` escape hatch.

    The current report's `normalized_at`, stamped exactly as every live row stamps it. Using
    basic info's translation-aware stamp here (the v1 extractor did) makes `candidate.observed_at`
    permanently greater than the `observed_at` the select writes for the 213,283 translated
    companies (as of 2026-09-11), so the scan re-selects them on every run.
    """
    return (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "SELECT\n"
        "    c.company_id AS company_id,\n"
        "    toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at\n"
        "FROM corpscout.se_ratsit_company AS c FINAL\n"
        "WHERE c.normalizer_version = %(normalizer_version)s\n"
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "LIMIT 1 BY c.company_id\n"
        ")"
    )


def ratsit_select_sql() -> str:
    return address_select_sql(live_sql=ratsit_live_sql(scoped=True), source=ADDRESS_SOURCE)


se_company_address_suggestions_ratsit = define_address_suggestion_asset(
    source=ADDRESS_SOURCE,
    extractor_version=RATSIT_ADDRESS_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_ADDRESS_SELECT_PARAMS,
    deps=[
        # The table-named keys the se_ratsit_normalized multi-asset declares. Never
        # dg.AssetKey("se_ratsit_normalized"): that is the multi-asset function's name, not a
        # key, and a dep on it makes a phantom node in the graph (spec 5.1).
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_establishments"),
        # The register the postal-town dictionary reads -- the key address/scb.py declares.
        dg.AssetKey("sweden_company_scb_companies_clickhouse"),
    ],
    description=(
        "Ratsit's newest normalized report into se_company_address_suggestion: the company "
        "address (kind postal, slot company) plus one workplace row per establishment with a "
        "street and a postcode (slot est:<identifier>, suffixed with the establishment index "
        "when a report repeats the identifier). The postal town comes from the SCB register's "
        "postcode dictionary, not from Ratsit's locality, which is the municipality on about "
        "28% of company addresses; the care-of Ratsit glues onto the street is split by the "
        "normalizer. A slot the newest report no longer delivers is tombstoned with a NULL "
        "row. execute=false previews."
    ),
)
