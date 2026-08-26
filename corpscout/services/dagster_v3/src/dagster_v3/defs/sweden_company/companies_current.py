"""The `corpscout.se_companies_current` serving SELECT: one denormalized row per company.

The companies/geocoding admin surfaces need a per-company row -- legal name, the company's
current addresses as a JSON array, and a pre-computed geocode summary for the PRIMARY address
-- without paying the FINAL merges on `se_company_address`/`se_company_info`, the served-view
join, and the per-company aggregation on every request. This module is the single source of
truth for that SELECT; migration 000326 materializes it as a refreshable MV and the backoffice
list reads the materialized table (Task 3).

WHAT IT AGGREGATES.

- One row per company. `se_company_address` FINAL, `is_current` only, is reduced to a per-company
  JSON array of its current addresses (companies carry 1-2), plus an `address_count`.
- Each address element is a Map(String, String) -- every value stringified so `toJSONString`
  emits a plain JSON object (verified 2026-08-26 to round-trip a/a/o intact). Its geocode fields
  (`geocode_precision`, `geocode_provider`, `latitude`, `longitude`) come from the SE geocode
  SERVING OVERLAY `corpscout.se_address_geocodes_served` (migration 000325 -- precise outcomes
  pass through, unmatched/ambiguous identities filled with a coarse postcode/city centroid),
  LEFT-JOINed on `address_id`; `geocode_status` stays the value stored on the published row.
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


def build_se_companies_current_sql() -> str:
    """One row per SE company: legal name, current addresses as JSON, primary geocode summary.

    A self-contained SELECT the migration wraps in a refreshable MV. `company_addresses`
    enriches each current published address with its served-overlay geocode and its coarse-aware
    class; `primary_address` picks the one summary row per company by the geocoding list's own
    rule; `aggregated` folds every current address into the JSON array and the count. The trailing
    SELECT joins the three plus `se_company_info` for the name.
    """
    address_map = _address_map_expression()
    return f"""WITH company_addresses AS (
  SELECT
    a.company_id AS company_id,
    a.address_key AS address_key,
    a.address_type AS address_type,
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
    toUInt32(count()) AS address_count
  FROM company_addresses AS ca
  GROUP BY ca.company_id
)
SELECT
  agg.company_id AS company_id,
  i.legal_name AS legal_name,
  agg.addresses AS addresses,
  agg.address_count AS address_count,
  pa.primary_street_address AS primary_street_address,
  pa.primary_postal_code AS primary_postal_code,
  pa.primary_city AS primary_city,
  pa.primary_geocode_status AS primary_geocode_status,
  pa.primary_geocode_class AS primary_geocode_class,
  pa.primary_geocode_precision AS primary_geocode_precision,
  pa.primary_geocode_provider AS primary_geocode_provider,
  pa.primary_latitude AS primary_latitude,
  pa.primary_longitude AS primary_longitude
FROM aggregated AS agg
INNER JOIN {COMPANY_INFO_TABLE} AS i FINAL ON i.company_id = agg.company_id
INNER JOIN primary_address AS pa ON pa.company_id = agg.company_id
ORDER BY agg.company_id"""



# --- Serving-view refresh health -------------------------------------------------------------
# The refreshable MV migration 000326 creates has no Dagster asset that recomputes it -- so, as
# with se_address_geocodes_current (migration 000320), the only place its health is visible is
# its refresh state in system.view_refreshes. Task 2b's sweden_companies_current_clickhouse asset
# reads SE_COMPANIES_CURRENT_REFRESH_SQL and reports companies_current_refresh_is_healthy(...);
# the SQL and the predicate live here beside the builder so both are importable and unit-testable
# without pulling in the asset module. This mirrors sweden_address_geocodes_serving_view_refresh_check.

SE_COMPANIES_CURRENT_VIEW = "se_companies_current"

# Three times the REFRESH EVERY 1 HOUR interval migration 000326 gave the view. One interval
# would fire on any refresh that merely ran long; three means two consecutive refreshes had to
# have been missed entirely before this reports unhealthy. Mirrors the 000320 view's budget.
MAX_COMPANIES_CURRENT_REFRESH_AGE = timedelta(hours=3)

# The refresh instant is fetched as an epoch INTEGER, not a DateTime: `last_success_time` is
# Nullable(DateTime) with no timezone in its type, so the driver hands back a NAIVE datetime in
# the SERVER's timezone -- comparing that against a UTC `now` is wrong by the server's offset
# (most of a three-hour budget on a Europe/Stockholm server). toUnixTimestamp is the absolute
# tick and an int crosses the driver unambiguously. Status is selected but never gated: a view
# mid-refresh (Running) is healthy if its LAST success is recent, and one stuck retrying is
# unhealthy for the same reason -- both are answered by exception + last_success_time.
SE_COMPANIES_CURRENT_REFRESH_SQL = f"""SELECT
    status,
    exception,
    toUnixTimestamp(last_success_time)
FROM system.view_refreshes
WHERE database = '{CLICKHOUSE_DATABASE}'
  AND view = '{SE_COMPANIES_CURRENT_VIEW}'"""


def companies_current_refresh_is_healthy(
    *,
    row_found: bool,
    exception: str,
    last_success_epoch_seconds: int | None,
    now: datetime,
) -> bool:
    """Whether corpscout.se_companies_current is still being refreshed.

    Fails in four ways, each a way the serving surface goes wrong while still answering fast:

    - NO ROW. system.view_refreshes lists every refreshable view ClickHouse knows; a WHERE that
      names one database and one view returning nothing means se_companies_current is not a
      refreshable view on this server -- migration 000326 was not applied, or something replaced
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
    return now.astimezone(UTC) - last_success <= MAX_COMPANIES_CURRENT_REFRESH_AGE
