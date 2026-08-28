"""The `corpscout.se_companies_serving` serving SELECT: one wide denormalized row per company.

The companies/geocoding admin surfaces need a per-company row -- legal name, the company's
current addresses as a JSON array, and a pre-computed geocode summary for the PRIMARY address
-- without paying the FINAL merges on `se_company_address`/`se_company_info`, the served-view
join, and the per-company aggregation on every request. This module is the single source of
truth for that SELECT; migration 000335 materializes it as a refreshable MV and the backoffice
admin companies pages read the materialized table.

WHAT IT AGGREGATES.

- One row per company. `se_company_address` FINAL, `is_current` only, is reduced to a per-company
  JSON array of its current addresses (companies carry 1-2), plus an `address_count`.
- Each address element is a Map(String, String) -- every value stringified so `toJSONString`
  emits a plain JSON object (verified 2026-08-26 to round-trip a/a/o intact). Its geocode fields
  (`geocode_precision`, `geocode_provider`, `latitude`, `longitude`) come from the SE geocode
  SERVING OVERLAY `corpscout.se_address_geocodes_served` (migration 000325, widened by 000327
  -- precise outcomes pass through, unmatched/ambiguous/postal_box identities filled with a
  coarse postcode/city centroid), LEFT-JOINed on `address_id`; `geocode_status` stays the
  value stored on the published row.
- `legal_name` is INNER-JOINed from `se_company_info` FINAL (every addressed company has one --
  0 orphans verified), matching the sibling company list's own name spine.

THE PRIMARY-ADDRESS SUMMARY. `primary_street_address`/`_postal_code`/`_city`/
`primary_geocode_status`/`primary_geocode_class`/`_precision`/`_provider`/`_latitude`/
`_longitude` describe the ONE address the geocoding list treats as the company's own, picked by
the SAME rule that list uses (se-company-geocoding-list.server.ts): a physical
`visiting_or_postal` outranks `visiting`, which outranks a postal-only row, with `address_key`
as the deterministic final tiebreak -- expressed here as the identical `ORDER BY ... LIMIT 1 BY
company_id` idiom rather than an aggregate, so the primary row's own coordinate (which may be
NULL) is carried through verbatim. The street/postcode/city/geocode_status columns are the
primary row's own display fields carried out alongside the geocode summary: the backoffice
geocoding list reads them straight off this table for its Company/Address columns and badge
tooltip, so it never has to re-pick a primary out of the `addresses` JSON (which, being a plain
map array, carries no `address_key` to tiebreak on).

THE CLASS IS COARSE-AWARE. `primary_geocode_class` mirrors the backoffice's
GEOCODE_STATUS_CLASS_EXPR EXACTLY (`_geocode_class_expr` below), so the Task 3 repoint is a
drop-in: the `geocode_provider = 'centroid_fallback'` check for `'coarse'` runs BEFORE the
geocoded-status membership check, because an overlaid row keeps `match_status` inside the
GEOCODED vocabulary (`matched_area`) and only its provider tells it apart from a
building-precise match. Vocabulary: no_outcome / coarse / geocoded / ambiguous / unmatched.

NULL-SAFE JOIN. `se_company_address.address_id` is `Nullable(FixedString(64))`;
`se_address_geocodes_served.address_id` is `FixedString(64)`. The join compares them as
`toString(s.address_id) = ifNull(toString(a.address_id), '')`, so a company row with a NULL
address_id folds to '' and matches no served row (a real id is 64 hex chars) -- the same
overlay-read fallback every absent field takes, answering identically under both
`join_use_nulls` settings.
"""

from datetime import UTC, datetime, timedelta

from dagster_v3.defs.sweden_company.geocode_serving_overlay import (
    GEOCODE_FALLBACK_PROVIDER,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    CLICKHOUSE_DATABASE,
    GEOCODED_STATUSES,
)

COMPANY_ADDRESS_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_address"
COMPANY_INFO_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_info"
SERVED_GEOCODES_TABLE = f"{CLICKHOUSE_DATABASE}.se_address_geocodes_served"

# The address element's Map keys, in emission order. Each value is stringified so the Map is
# homogeneous (Map(String, String)) and toJSONString renders a flat JSON object.
ADDRESS_ELEMENT_KEYS = (
    "street_address",
    "postal_code",
    "city",
    "address_type",
    "address_id",
    "geocode_status",
    "geocode_precision",
    "geocode_provider",
    "latitude",
    "longitude",
)


def _geocode_class_expr(status_column: str, provider_column: str) -> str:
    """The coarse-aware geocode class, IDENTICAL in semantics to the backoffice's
    `geocodeClassExpr` (se-company-geocoding-list.server.ts).

    `status_column` is the row's stored `geocode_status`; `provider_column` is the served
    overlay's `geocode_provider` ('' when the identity carries no served row). The
    centroid-fallback provider is tested BEFORE the geocoded-status membership check so an
    overlaid coarse row -- whose `match_status` is deliberately the GEOCODED value
    `matched_area` -- classifies as `'coarse'` and never as `'geocoded'`.
    """
    geocoded = ", ".join(f"'{status}'" for status in GEOCODED_STATUSES)
    return (
        "multiIf(\n"
        f"    {status_column} = '', 'no_outcome',\n"
        f"    {provider_column} = '{GEOCODE_FALLBACK_PROVIDER}', 'coarse',\n"
        f"    {status_column} IN ({geocoded}), 'geocoded',\n"
        f"    {status_column} = 'ambiguous', 'ambiguous',\n"
        "    'unmatched'\n"
        "  )"
    )


# The primary-address pick, mirrored verbatim from se-company-geocoding-list.server.ts's
# GEOCODING_PUBLISHED_ADDRESS_SQL: a physical visiting_or_postal outranks visiting outranks a
# postal-only row, address_key the deterministic tiebreak. Applied as ORDER BY + LIMIT 1 BY.
_PRIMARY_ORDER_BY = (
    "company_id,\n"
    "    address_type = 'visiting_or_postal' DESC,\n"
    "    address_type = 'visiting' DESC,\n"
    "    address_key ASC"
)


def _address_map_expression() -> str:
    """`map('key', value, ...)` for one address element -- every value stringified."""
    values = {
        "street_address": "ca.street_address",
        "postal_code": "ca.postal_code",
        "city": "ca.city",
        "address_type": "toString(ca.address_type)",
        "address_id": "ca.address_id",
        "geocode_status": "ca.geocode_status",
        "geocode_precision": "ca.geocode_precision",
        "geocode_provider": "ca.geocode_provider",
        "latitude": "ifNull(toString(ca.latitude), '')",
        "longitude": "ifNull(toString(ca.longitude), '')",
    }
    pairs = ",\n      ".join(f"'{key}', {values[key]}" for key in ADDRESS_ELEMENT_KEYS)
    return f"map(\n      {pairs}\n    )"


# --- The consolidated serving view (migration 000335) ----------------------------------------
# corpscout.se_companies_serving SUPERSEDES se_companies_current as the one wide per-company
# row every admin companies list page reads: the info-list columns (name/status/legal form/
# description flag), the datatype presence flags and per-register source flags that used to be
# computed as IN-set subqueries on every backoffice page load (measured ~1.4s/page, owner
# 2026-08-28), AND the address JSON + primary geocode summary se_companies_current carried.
# Base changes from "companies with a current address" (INNER JOIN) to ALL of se_company_info
# (LEFT JOIN): the info list shows every published company, addressed or not.
#
# The presence-set subqueries below are the single source of truth the backoffice's
# COMPANY_SETS used to hand-carry; after the repoint the backoffice reads these as plain
# columns and only the ledger/filter queries keep their own SQL.

SE_COMPANIES_SERVING_VIEW = "se_companies_serving"

BOLAGSVERKET_FINANCIAL_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_bolagsverket_financial_metrics"
)
ESEF_FINANCIAL_SET = (
    f"SELECT ci.company_id FROM {CLICKHOUSE_DATABASE}.company_identifier AS ci "
    "WHERE ci.issuer_scheme = 'lei' AND ci.country_code = 'SE' AND ci.is_current = 1 "
    f"AND ci.issuer_id IN (SELECT upperUTF8(trimBoth(m.lei)) FROM {CLICKHOUSE_DATABASE}.esef_financial_metrics AS m)"
)
FINANCIAL_REPORTS_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_financial_reports"
)
PEOPLE_SET = f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_company_person"
PEOPLE_BOLAGSVERKET_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_company_person_role "
    "WHERE has(sources, 'bolagsverket')"
)
PEOPLE_ESEF_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_company_person_role "
    "WHERE has(sources, 'esef')"
)
DOMAINS_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.company_domains "
    "WHERE country_code = 'SE'"
)

# The registered-activity translation and the status-reason label, absorbed VERBATIM from the
# retired corpscout.se_companies_translated view (migration 000252's rendering) -- migration
# 000338 folds them into the serving view and the follow-up drops the view. The legal-form
# label join is NOT carried over: se_company_info already ships label_en/label_sv (000306).
SE_COMPANIES_TABLE = f"{CLICKHOUSE_DATABASE}.se_companies"
ACTIVITY_TRANSLATION_JOIN = f"""LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM {CLICKHOUSE_DATABASE}.text_translations
    WHERE source_table = 'corpscout.se_companies'
      AND source_column = 'activity_description'
      AND source_lang = 'sv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
  ) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_description, ''))"""
STATUS_REASON_LABEL_JOIN = f"""LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM {CLICKHOUSE_DATABASE}.se_code_labels
    WHERE code_type = 'status_reason'
    GROUP BY code
  ) AS sr ON sr.code = ifNull(c.status_reason, '')"""


def build_se_companies_serving_sql() -> str:
    """One wide row per published SE company: info-list columns, presence flags, source
    flags, current addresses as JSON, and the primary-address geocode summary.

    The inner SELECT computes each presence ARM exactly once (each `IN (...)` builds its
    hash set once per refresh); the outer SELECT derives the composite flags from the arms.
    Address columns come from the same three CTEs se_companies_current used, LEFT-joined so
    a company with no current address still gets a row -- its `addresses` folds to '[]'
    (via coalesce/nullIf, correct under both join_use_nulls settings) and its primary
    summary to the same ''/NULL an absent overlay row produces.

    Flag semantics are ported verbatim from the backoffice's DATATYPE_PRESENCE_EXPR /
    PROFILE_SOURCE_PREDICATES (se-company-info-lists.server.ts, owner ruling 2026-08-25:
    has_financial is extracted metrics OR filed reports; the SCB flag is omitted -- it is 1
    for every row by construction, the reader hard-codes its letter).
    """
    address_map = _address_map_expression()
    return f"""WITH company_addresses AS (
  SELECT
    a.company_id AS company_id,
    a.address_key AS address_key,
    a.address_type AS address_type,
    toUInt8(has(a.sources, 'bolagsverket')) AS from_bolagsverket,
    ifNull(a.street_address, '') AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    ifNull(toString(a.address_id), '') AS address_id,
    toString(a.geocode_status) AS geocode_status,
    ifNull(s.geocode_precision, '') AS geocode_precision,
    ifNull(s.geocode_provider, '') AS geocode_provider,
    s.latitude AS latitude,
    s.longitude AS longitude
  FROM {COMPANY_ADDRESS_TABLE} AS a FINAL
  LEFT JOIN {SERVED_GEOCODES_TABLE} AS s
    ON toString(s.address_id) = ifNull(toString(a.address_id), '')
  WHERE a.is_current
),
primary_address AS (
  SELECT
    company_id,
    street_address AS primary_street_address,
    postal_code AS primary_postal_code,
    city AS primary_city,
    geocode_status AS primary_geocode_status,
    {_geocode_class_expr("geocode_status", "geocode_provider")} AS primary_geocode_class,
    geocode_precision AS primary_geocode_precision,
    geocode_provider AS primary_geocode_provider,
    latitude AS primary_latitude,
    longitude AS primary_longitude
  FROM company_addresses
  ORDER BY {_PRIMARY_ORDER_BY}
  LIMIT 1 BY company_id
),
aggregated AS (
  SELECT
    ca.company_id AS company_id,
    toJSONString(groupArray({address_map})) AS addresses,
    toUInt32(count()) AS address_count,
    toUInt8(max(ca.from_bolagsverket)) AS address_bolagsverket
  FROM company_addresses AS ca
  GROUP BY ca.company_id
)
SELECT
  company_id,
  legal_name,
  status,
  legal_form_code,
  legal_form_label_en,
  legal_form_label_sv,
  activity_description,
  activity_description_en,
  status_reason,
  status_reason_label_en,
  bolagsverket_source_record_uid,
  updated_from_raw_at,
  has_description,
  has_address,
  toUInt8(fin_bolagsverket OR fin_esef OR fin_reports) AS has_financial,
  has_people,
  has_domains,
  toUInt8(address_bolagsverket OR fin_bolagsverket OR people_bolagsverket) AS source_bolagsverket,
  toUInt8(desc_esef OR has_lei OR fin_esef OR people_esef) AS source_esef,
  toUInt8(has_wikidata OR desc_wikidata) AS source_wikidata,
  addresses,
  address_count,
  primary_street_address,
  primary_postal_code,
  primary_city,
  primary_geocode_status,
  primary_geocode_class,
  primary_geocode_precision,
  primary_geocode_provider,
  primary_latitude,
  primary_longitude
FROM (
  SELECT
    i.company_id AS company_id,
    i.legal_name AS legal_name,
    toString(i.status) AS status,
    ifNull(i.legal_form_code, '') AS legal_form_code,
    i.legal_form_label_en AS legal_form_label_en,
    i.legal_form_label_sv AS legal_form_label_sv,
    ifNull(c.activity_description, '') AS activity_description,
    ifNull(act.translated_text, '') AS activity_description_en,
    ifNull(c.status_reason, '') AS status_reason,
    ifNull(sr.label_en, '') AS status_reason_label_en,
    ifNull(c.bolagsverket_source_record_uid, '') AS bolagsverket_source_record_uid,
    ifNull(c.updated_from_raw_at, toDateTime64(0, 3, 'UTC')) AS updated_from_raw_at,
    toUInt8(i.description IS NOT NULL) AS has_description,
    toUInt8(ifNull(agg.address_count, 0) > 0) AS has_address,
    toUInt8(i.company_id IN ({BOLAGSVERKET_FINANCIAL_SET})) AS fin_bolagsverket,
    toUInt8(i.company_id IN ({ESEF_FINANCIAL_SET})) AS fin_esef,
    toUInt8(i.company_id IN ({FINANCIAL_REPORTS_SET})) AS fin_reports,
    toUInt8(i.company_id IN ({PEOPLE_SET})) AS has_people,
    toUInt8(i.company_id IN ({PEOPLE_BOLAGSVERKET_SET})) AS people_bolagsverket,
    toUInt8(i.company_id IN ({PEOPLE_ESEF_SET})) AS people_esef,
    toUInt8(i.company_id IN ({DOMAINS_SET})) AS has_domains,
    toUInt8(has(i.description_sources, 'esef')) AS desc_esef,
    toUInt8(i.lei IS NOT NULL) AS has_lei,
    toUInt8(i.wikidata_id IS NOT NULL) AS has_wikidata,
    toUInt8(has(i.description_sources, 'wikidata')) AS desc_wikidata,
    toUInt8(ifNull(agg.address_bolagsverket, 0)) AS address_bolagsverket,
    coalesce(nullIf(agg.addresses, ''), '[]') AS addresses,
    toUInt32(ifNull(agg.address_count, 0)) AS address_count,
    ifNull(pa.primary_street_address, '') AS primary_street_address,
    ifNull(pa.primary_postal_code, '') AS primary_postal_code,
    ifNull(pa.primary_city, '') AS primary_city,
    ifNull(pa.primary_geocode_status, '') AS primary_geocode_status,
    ifNull(pa.primary_geocode_class, '') AS primary_geocode_class,
    ifNull(pa.primary_geocode_precision, '') AS primary_geocode_precision,
    ifNull(pa.primary_geocode_provider, '') AS primary_geocode_provider,
    pa.primary_latitude AS primary_latitude,
    pa.primary_longitude AS primary_longitude
  FROM {COMPANY_INFO_TABLE} AS i FINAL
  LEFT JOIN {SE_COMPANIES_TABLE} AS c FINAL ON c.company_id = i.company_id
  {ACTIVITY_TRANSLATION_JOIN}
  {STATUS_REASON_LABEL_JOIN}
  LEFT JOIN aggregated AS agg ON agg.company_id = i.company_id
  LEFT JOIN primary_address AS pa ON pa.company_id = i.company_id
)
ORDER BY company_id"""


# Three times the REFRESH EVERY 15 MINUTE interval migration 000335 gives the serving view.
MAX_COMPANIES_SERVING_REFRESH_AGE = timedelta(minutes=45)

SE_COMPANIES_SERVING_REFRESH_SQL = f"""SELECT
    status,
    exception,
    toUnixTimestamp(last_success_time)
FROM system.view_refreshes
WHERE database = '{CLICKHOUSE_DATABASE}'
  AND view = '{SE_COMPANIES_SERVING_VIEW}'"""


# --- Refresh health -------------------------------------------------------------------------
# companies_current_refresh_is_healthy is the generic predicate on system.view_refreshes rows
# (fetched via SE_COMPANIES_SERVING_REFRESH_SQL above); the weekly refresh asset's check
# (companies_current_asset.py) applies it to the serving view. The epoch-int fetch exists
# because last_success_time is a timezone-naive DateTime in the driver -- toUnixTimestamp is
# the unambiguous absolute tick.


def companies_current_refresh_is_healthy(
    *,
    row_found: bool,
    exception: str,
    last_success_epoch_seconds: int | None,
    now: datetime,
) -> bool:
    """Whether corpscout.se_companies_serving is still being refreshed.

    Fails in four ways, each a way the serving surface goes wrong while still answering fast:

    - NO ROW. system.view_refreshes lists every refreshable view ClickHouse knows; a WHERE that
      names one database and one view returning nothing means se_companies_serving is not a
      refreshable view on this server -- migration 000335 was not applied, or something replaced
      the view with a table. `row_found=False` fails.
    - AN EXCEPTION. A view whose refresh throws keeps serving its last good contents at full
      speed, so staleness is the only visible symptom and the age term alone would not report it
      for three hours. This also catches the definer losing SELECT on an input.
    - NEVER SUCCEEDED. `None` means ClickHouse knows the view and has never completed a refresh:
      it answers every request empty and raises nothing. Treating an absent instant as 'no
      evidence, so pass' would make the check silent in exactly the state it exists to catch.
    - STALE. A last success older than three refresh intervals means refreshes have quietly
      stopped landing.
    """
    if not row_found:
        return False
    if exception:
        return False
    if last_success_epoch_seconds is None:
        return False
    last_success = datetime.fromtimestamp(int(last_success_epoch_seconds), tz=UTC)
    return now.astimezone(UTC) - last_success <= MAX_COMPANIES_SERVING_REFRESH_AGE
