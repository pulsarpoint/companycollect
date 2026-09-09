"""The `corpscout.se_companies_serving` serving SELECT: one wide denormalized row per company.

The companies/geocoding admin surfaces need a per-company row -- legal name, the company's
published addresses as a JSON array, and a pre-computed geocode summary for the PRIMARY
address -- without paying the FINAL merges on `se_company_address`/`se_company_basic_info`
and the per-company aggregation on every request. This module is the single source of truth
for that SELECT; migration 000335 materializes it as a refreshable MV, 000391 repoints its
company spine at the folded basic-info row, 000392 repoints its address half at the address
entity, and the backoffice admin companies pages read the materialized table.

WHAT IT AGGREGATES.

- One row per company. `se_company_address` FINAL, `active = 1` only, is reduced to a
  per-company JSON array of its published addresses (companies carry 1-2), plus an
  `address_count`.
- Each address element is a Map(String, String) -- every value stringified so `toJSONString`
  emits a plain JSON object (verified 2026-08-26 to round-trip a/a/o intact). Its geocode
  fields (`geocode_status`, `geocode_precision`, `latitude`, `longitude`) sit ON the entity
  row, written by the address module's geocode step; `geocode_provider` is DERIVED from the
  status (see below) because the entity does not store one.
- `legal_name`, `status`, the legal form and both descriptions come from `se_company_basic_info`
  FINAL, the folded basic-info row (slice 4, 2026-09-08); the register fields (deregistration
  reason, record identity, stamp) from `se_bolagsverket_companies`, the labels from `se_code_labels`.

THE PRIMARY-ADDRESS SUMMARY. `primary_street_address`/`_postal_code`/`_city`/
`primary_geocode_status`/`primary_geocode_class`/`_precision`/`_provider`/`_latitude`/
`_longitude` describe the ONE address the geocoding list treats as the company's own, picked by
the rule that list uses (se-company-geocoding-list.server.ts): a physical `visiting_or_postal`
outranks `visiting`, which outranks a postal-only row, with `address_key` as the deterministic
final tiebreak -- expressed here as the identical `ORDER BY ... LIMIT 1 BY company_id` idiom
rather than an aggregate, so the primary row's own coordinate (which may be NULL) is carried
through verbatim. The entity keeps the kinds as an ARRAY on one row rather than a row per
type, so the kind rank terms are `has(kinds, ...)` membership tests instead of equality on a
scalar `address_type`. AHEAD of them ranks `has_location` (`street_name IS NOT NULL OR box IS
NOT NULL`): a postcode-only or otherwise location-less row must never take the primary slot --
and so the printed street -- from a row that carries a street or a box, whatever its kind.
The street/postcode/city/geocode_status columns are the
primary row's own display fields carried out alongside the geocode summary: the backoffice
geocoding list reads them straight off this table for its Company/Address columns and badge
tooltip, so it never has to re-pick a primary out of the `addresses` JSON (which, being a plain
map array, carries no `address_key` to tiebreak on).

THE CLASS IS COARSE-AWARE. `primary_geocode_class` mirrors the backoffice's
GEOCODE_STATUS_CLASS_EXPR EXACTLY (`_geocode_class_expr` below): the
`geocode_provider = 'centroid_fallback'` check for `'coarse'` runs BEFORE the geocoded-status
membership check, because a centroid row keeps its status inside the GEOCODED vocabulary
(`matched_area`) and only the provider tells it apart from a building-precise match.
Vocabulary: no_outcome / coarse / geocoded / ambiguous / unmatched.

THE DERIVED PROVIDER. The entity stores no provider column, so one is derived from the row:
`matched_area` -- what the postcode/city centroid overlay writes -- is `centroid_fallback`, a
row with no coordinate at all gets `''`, and everything else is `'osm'` (the gazetteer the
address module geocodes against). That reproduces the class the served overlay used to give
every one of these rows, which is why `_geocode_class_expr` needed no change when slice 4a
repointed this view at the entity.

STREET FROM THE PUBLISHED LINE. The entity has no `street_address` column: `normalized_address`
is the display line, `street[, postcode city]`. `_STREET_PART_EXPR` strips the trailing
`, NNN NN Town` and keeps everything before it, so a `c/o` prefix survives -- the mapping
rule every slice-4a reader applies to the entity's line. Normalizer v3's postcode-only lines
(`100 11 Stockholm`) have no comma before the postal part, so the strip alone would keep the
WHOLE line as the street; the expression tests for that shape first and yields '' instead. A
care-of-only line (`c/o AxFast AB, 164 87 Stockholm`) does have a comma and keeps `c/o AxFast
AB` as its street part, by design.
"""

from datetime import UTC, datetime, timedelta

from dagster_v3.defs.se_company.address.constants import GEOCODE_FALLBACK_PROVIDER
from dagster_v3.defs.se_company.common import bolagsverket_record_uid_sql
from dagster_v3.defs.sweden_company.geocode_store import (
    CLICKHOUSE_DATABASE,
    GEOCODED_STATUSES,
)

# The SE address entity (migration 000384, renamed to its final name by 000393). This
# constant is the one place the view names it.
COMPANY_ADDRESS_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_address"
# The SE person entity (migration 000395). This constant is the one place the view names
# it. The table is empty until slice 2's first fold, so the three people flags below read
# 0 for every company between this migration and that fold -- deliberately (spec section
# 8: "the flags read empty tables until the first fold"). It is se_company_person_v2 for
# slices 0 to 3; slice 4 renames it and edits this one line.
COMPANY_PERSON_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_person_v2"
# The company spine and its register/label joins (basic-info slice 4, migration 000391).
BASIC_INFO_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_basic_info"
BOLAGSVERKET_TABLE = f"{CLICKHOUSE_DATABASE}.se_bolagsverket_companies"
CODE_LABELS_TABLE = f"{CLICKHOUSE_DATABASE}.se_code_labels"

# The street part of a published line: everything before a trailing `, NNN NN Town`, and ''
# for a line that is ONLY a postal part (normalizer v3's postcode-only addresses, which carry
# no street and no box -- the strip alone would hand back the whole `100 11 Stockholm` line as
# a street). Written as a raw string so the doubled backslash reaches ClickHouse, whose
# string-literal parser unescapes it back to the regex's `\s`.
_STREET_PART_EXPR = (
    r"if(match(a.normalized_address, '^[0-9]{3} [0-9]{2}[^,]*$'), '', "
    r"replaceRegexpOne(a.normalized_address, ',\\s*[0-9]{3} [0-9]{2}[^,]*$', ''))"
)

# The entity carries no geocode_provider column; the class expression needs one. `matched_area`
# is what the postcode/city centroid overlay writes, so it maps to the fallback provider the
# served view used to stamp; a row with no coordinate has no provider at all.
_GEOCODE_PROVIDER_EXPR = (
    "multiIf("
    f"a.geocode_status = 'matched_area', '{GEOCODE_FALLBACK_PROVIDER}', "
    "a.latitude IS NULL, '', "
    "'osm')"
)

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

    `status_column` is the row's stored `geocode_status`; `provider_column` is the DERIVED
    `geocode_provider` (`_GEOCODE_PROVIDER_EXPR`: `centroid_fallback` for a centroid outcome,
    '' for a row with no coordinate, `osm` otherwise). The centroid-fallback provider is
    tested BEFORE the geocoded-status membership check so a coarse row -- whose status is
    deliberately the GEOCODED value `matched_area` -- classifies as `'coarse'` and never as
    `'geocoded'`.
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


# The primary-address pick, mirrored from se-company-geocoding-list.server.ts's
# GEOCODING_PUBLISHED_ADDRESS_SQL: a physical visiting_or_postal outranks visiting outranks a
# postal-only row, address_key the deterministic tiebreak. Applied as ORDER BY + LIMIT 1 BY.
# The two kind terms are the CTE's `has(kinds, ...)` membership columns -- the entity carries
# every kind of one address on ONE row, so there is no scalar address_type to compare.
#
# A LOCATION LINE OUTRANKS EVERY KIND. Normalizer v3 publishes rows that carry no street and
# no box -- a postcode-only `100 11 Stockholm`, a foreign or otherwise unparsed line -- and
# such a row can perfectly well be the company's `visiting_or_postal` one while a Bolagsverket
# `postal` row carries the actual street. Ranking `has_location` FIRST keeps `primary_street_
# address`/`_city` (what the companies and geocoding lists print) on the row that HAS a
# location, and only then applies the kind ranks among equals.
_PRIMARY_ORDER_BY = (
    "company_id,\n"
    "    has_location DESC,\n"
    "    kind_visiting_or_postal DESC,\n"
    "    kind_visiting DESC,\n"
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
# Base changes from "companies with a current address" (INNER JOIN) to ALL of se_company_basic_info
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
# Active published persons only, read FINAL (ReplacingMergeTree(folded_at)): a person the
# reviewer hid or whose observations all became tombstones must not keep the flag lit.
PEOPLE_SET = f"SELECT company_id FROM {COMPANY_PERSON_TABLE} FINAL WHERE active = 1"
PEOPLE_BOLAGSVERKET_SET = (
    f"SELECT company_id FROM {COMPANY_PERSON_TABLE} FINAL "
    "WHERE active = 1 AND has(sources, 'bolagsverket')"
)
PEOPLE_ESEF_SET = (
    f"SELECT company_id FROM {COMPANY_PERSON_TABLE} FINAL "
    "WHERE active = 1 AND has(sources, 'esef')"
)
DOMAINS_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.company_domains "
    "WHERE country_code = 'SE'"
)
# The company has a resolved EODHD stock-market listing (company_traded_symbols, the
# company_markets module's precomputed FIRDS+GLEIF+EODHD resolve, refreshed daily). Owner
# 2026-08-28: this REPLACES the earlier ESEF-filing signal -- EODHD covers First North/NGM/
# Spotlight listings that never file ESEF (826 SE companies vs ESEF's 403), and it is market
# truth rather than a filing obligation.
PUBLICLY_TRADED_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.company_traded_symbols "
    "WHERE country_code = 'SE'"
)
# Exact-matched SE companies that WON a government contract (UHM + TED), per the same
# company-keyed contracts view the public contracts pages read.
GOVERNMENT_CONTRACTS_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.se_government_contracts"
)
# Companies with a Platsbanken job ad, open now or in the past -- company_job_history is the
# normalized company-keyed layer (000302); one interval row per ad lifetime.
JOB_ADS_SET = (
    f"SELECT company_id FROM {CLICKHOUSE_DATABASE}.company_job_history "
    "WHERE country_code = 'SE'"
)

# The register row behind the main row: deregistration reason (the status-reason code),
# the record identity the extractor hashes, and the register's own stamp. FINAL and the
# has_company scope inside the subquery, so the outer LEFT JOIN sees one row per company.
BOLAGSVERKET_JOIN = f"""LEFT JOIN (
    SELECT company_id, deregistration_reason, source_record_id, source_payload_hash, observed_at
    FROM {BOLAGSVERKET_TABLE} FINAL
    WHERE has_company = 1
  ) AS b ON b.company_id = i.company_id"""
# The curated dictionaries (se_code_labels): what the legal-form code and the
# deregistration reason are called. argMax(version) so a re-seeded label wins.
LEGAL_FORM_LABEL_JOIN = f"""LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en, argMax(label_sv, version) AS label_sv
    FROM {CODE_LABELS_TABLE}
    WHERE code_type = 'legal_form'
    GROUP BY code
  ) AS lf ON lf.code = ifNull(i.legal_form_code, '')"""
STATUS_REASON_LABEL_JOIN = f"""LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM {CODE_LABELS_TABLE}
    WHERE code_type = 'status_reason'
    GROUP BY code
  ) AS sr ON sr.code = ifNull(b.deregistration_reason, '')"""


def build_se_companies_serving_sql() -> str:
    """One wide row per published SE company: info-list columns, presence flags, source
    flags, current addresses as JSON, and the primary-address geocode summary.

    The inner SELECT computes each presence ARM exactly once (each `IN (...)` builds its
    hash set once per refresh); the outer SELECT derives the composite flags from the arms.
    Address columns come from the same three CTEs se_companies_current used, LEFT-joined so
    a company with no published address still gets a row -- its `addresses` folds to '[]'
    (via coalesce/nullIf, correct under both join_use_nulls settings) and its primary
    summary to the same ''/NULL an ungeocoded row produces.

    Flag semantics are ported verbatim from the backoffice's DATATYPE_PRESENCE_EXPR /
    PROFILE_SOURCE_PREDICATES (se-company-info-lists.server.ts, owner ruling 2026-08-25:
    has_financial is extracted metrics OR filed reports; the SCB flag is omitted -- it is 1
    for every row by construction, the reader hard-codes its letter).
    """
    address_map = _address_map_expression()
    return f"""WITH company_addresses AS (
  SELECT
    a.company_id AS company_id,
    toString(a.address_key) AS address_key,
    arrayStringConcat(arrayMap(x -> toString(x), a.kinds), ',') AS address_type,
    toUInt8(has(a.sources, 'bolagsverket')) AS from_bolagsverket,
    {_STREET_PART_EXPR} AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    toString(a.address_key) AS address_id,
    toString(a.geocode_status) AS geocode_status,
    toString(a.geocode_precision) AS geocode_precision,
    {_GEOCODE_PROVIDER_EXPR} AS geocode_provider,
    a.latitude AS latitude,
    a.longitude AS longitude,
    (a.street_name IS NOT NULL OR a.box IS NOT NULL) AS has_location,
    has(a.kinds, 'visiting_or_postal') AS kind_visiting_or_postal,
    has(a.kinds, 'visiting') AS kind_visiting
  FROM {COMPANY_ADDRESS_TABLE} AS a FINAL
  WHERE a.active = 1
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
  is_publicly_traded,
  has_government_contracts,
  has_job_ads,
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
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(lf.label_sv, '') AS legal_form_label_sv,
    ifNull(i.description_sv, '') AS activity_description,
    if(ifNull(i.description_language, '') = 'en', ifNull(i.description, ''), '') AS activity_description_en,
    ifNull(b.deregistration_reason, '') AS status_reason,
    ifNull(sr.label_en, '') AS status_reason_label_en,
    if(ifNull(b.company_id, '') = '', '', ifNull({bolagsverket_record_uid_sql('b')}, '')) AS bolagsverket_source_record_uid,
    ifNull(b.observed_at, toDateTime64(0, 3, 'UTC')) AS updated_from_raw_at,
    toUInt8(i.description IS NOT NULL) AS has_description,
    toUInt8(ifNull(agg.address_count, 0) > 0) AS has_address,
    toUInt8(i.company_id IN ({BOLAGSVERKET_FINANCIAL_SET})) AS fin_bolagsverket,
    toUInt8(i.company_id IN ({ESEF_FINANCIAL_SET})) AS fin_esef,
    toUInt8(i.company_id IN ({FINANCIAL_REPORTS_SET})) AS fin_reports,
    toUInt8(i.company_id IN ({PEOPLE_SET})) AS has_people,
    toUInt8(i.company_id IN ({PEOPLE_BOLAGSVERKET_SET})) AS people_bolagsverket,
    toUInt8(i.company_id IN ({PEOPLE_ESEF_SET})) AS people_esef,
    toUInt8(i.company_id IN ({DOMAINS_SET})) AS has_domains,
    toUInt8(i.company_id IN ({PUBLICLY_TRADED_SET})) AS is_publicly_traded,
    toUInt8(i.company_id IN ({GOVERNMENT_CONTRACTS_SET})) AS has_government_contracts,
    toUInt8(i.company_id IN ({JOB_ADS_SET})) AS has_job_ads,
    toUInt8(i.description_source = 'esef') AS desc_esef,
    toUInt8(i.lei IS NOT NULL) AS has_lei,
    toUInt8(i.wikidata_id IS NOT NULL) AS has_wikidata,
    toUInt8(i.description_source = 'wikidata') AS desc_wikidata,
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
  FROM {BASIC_INFO_TABLE} AS i FINAL
  {BOLAGSVERKET_JOIN}
  {LEGAL_FORM_LABEL_JOIN}
  {STATUS_REASON_LABEL_JOIN}
  LEFT JOIN aggregated AS agg ON agg.company_id = i.company_id
  LEFT JOIN primary_address AS pa ON pa.company_id = i.company_id
)
ORDER BY company_id
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888"""


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
