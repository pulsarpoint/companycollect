CREATE DATABASE IF NOT EXISTS corpscout;

-- corpscout.se_companies_current is a NEW per-company serving surface: one denormalized row
-- per SE company -- legal name, the company's current addresses as a JSON array, and a
-- pre-computed geocode summary for the PRIMARY address -- so the companies/geocoding admin
-- pages read a plain table sub-second instead of recomputing the FINAL merges on
-- se_company_address/se_company_info, the served-overlay join, and the per-company JSON
-- aggregation on every request (measured near 20s live).
--
-- WHY A REFRESHABLE MV AND NOT A PLAIN VIEW. The read this holds is the expensive one: two
-- FINAL merges, a LEFT JOIN to the se_address_geocodes_served overlay, a primary-address
-- pick and a per-company groupArray. A plain VIEW would pay all of it on every request, which
-- is the cost this surface exists to remove. A refreshable MV keeps a real MergeTree ORDER BY
-- company_id behind the name, so serving reads a materialized table and ClickHouse recomputes
-- it hourly -- the same shape migration 000320 gave se_address_geocodes_current.
--
-- WHY A PLAIN CREATE AND NOT 000320's STAGED SWAP. 000320 built under a _next name and swapped
-- in one atomic RENAME because it was REPLACING a table that concurrent backoffice readers were
-- already hitting: between a naive RENAME-away and the CREATE the name would vanish
-- (UNKNOWN_TABLE), and a freshly created refreshable MV is EMPTY until its first refresh lands.
-- Both windows only exist when an established reader is mid-cutover. This name is BRAND NEW --
-- nothing reads corpscout.se_companies_current before this migration creates it (Task 3 repoints
-- the backoffice AFTER this ships) -- so there is nothing to swap and no gap to hide. The
-- simplest correct form applies, the same reasoning migration 000325 used for its brand-new
-- sibling view: CREATE once under the final name.
--
-- SYSTEM WAIT VIEW STILL MATTERS. A refreshable MV schedules its first refresh rather than
-- running it inline, so the CREATE returns with the view EMPTY. SYSTEM WAIT VIEW blocks until
-- that first refresh has finished, so `migrate up` only reports done once the view is populated
-- and queryable -- no reader (or the Task 2b asset that force-refreshes it) meets an empty view
-- created by a completed migration. It was applied through the real golang-migrate tooling
-- against a throwaway ClickHouse 26.5 before this file was written, not just through a client.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED. It is the exact rendering
-- of companies_current.build_se_companies_current_sql(), the single source of truth for the
-- per-company serving SELECT. Editing this file without editing that builder -- or the builder
-- without adding a migration -- trips the drift pin in dagster_v3
-- tests/test_se_companies_current_mv.py.
--
-- AFTER THE APPLY. Nothing further is required: the view is populated and ClickHouse refreshes
-- it hourly. `SYSTEM REFRESH VIEW corpscout.se_companies_current` forces a refresh at any later
-- time -- the Task 2b asset issues exactly that (plus a WAIT) after the weekly geocode update so
-- the view is fresh immediately rather than up to an hour later. The asset check
-- sweden_companies_current_serving_view_refresh_check watches this view in system.view_refreshes.
--
-- NO DROP HERE. The view is brand new, so this up-file only CREATEs -- there is no prior name to
-- rename or retire, and no existing reader to leave without a table. The down-file drops only
-- the view this up-file created.

CREATE MATERIALIZED VIEW corpscout.se_companies_current
REFRESH EVERY 1 HOUR
ENGINE = MergeTree
ORDER BY company_id
AS WITH company_addresses AS (
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
  FROM corpscout.se_company_address AS a FINAL
  LEFT JOIN corpscout.se_address_geocodes_served AS s
    ON toString(s.address_id) = ifNull(toString(a.address_id), '')
  WHERE a.is_current
),
primary_address AS (
  SELECT
    company_id,
    multiIf(
    geocode_status = '', 'no_outcome',
    geocode_provider = 'centroid_fallback', 'coarse',
    geocode_status IN ('matched_exact', 'matched_corrected', 'matched_site', 'matched_area', 'matched_street'), 'geocoded',
    geocode_status = 'ambiguous', 'ambiguous',
    'unmatched'
  ) AS primary_geocode_class,
    geocode_precision AS primary_geocode_precision,
    geocode_provider AS primary_geocode_provider,
    latitude AS primary_latitude,
    longitude AS primary_longitude
  FROM company_addresses
  ORDER BY company_id,
    address_type = 'visiting_or_postal' DESC,
    address_type = 'visiting' DESC,
    address_key ASC
  LIMIT 1 BY company_id
),
aggregated AS (
  SELECT
    ca.company_id AS company_id,
    toJSONString(groupArray(map(
      'street_address', ca.street_address,
      'postal_code', ca.postal_code,
      'city', ca.city,
      'address_type', toString(ca.address_type),
      'address_id', ca.address_id,
      'geocode_status', ca.geocode_status,
      'geocode_precision', ca.geocode_precision,
      'geocode_provider', ca.geocode_provider,
      'latitude', ifNull(toString(ca.latitude), ''),
      'longitude', ifNull(toString(ca.longitude), '')
    ))) AS addresses,
    toUInt32(count()) AS address_count
  FROM company_addresses AS ca
  GROUP BY ca.company_id
)
SELECT
  agg.company_id AS company_id,
  i.legal_name AS legal_name,
  agg.addresses AS addresses,
  agg.address_count AS address_count,
  pa.primary_geocode_class AS primary_geocode_class,
  pa.primary_geocode_precision AS primary_geocode_precision,
  pa.primary_geocode_provider AS primary_geocode_provider,
  pa.primary_latitude AS primary_latitude,
  pa.primary_longitude AS primary_longitude
FROM aggregated AS agg
INNER JOIN corpscout.se_company_info AS i FINAL ON i.company_id = agg.company_id
INNER JOIN primary_address AS pa ON pa.company_id = agg.company_id
ORDER BY agg.company_id;

SYSTEM WAIT VIEW corpscout.se_companies_current;
