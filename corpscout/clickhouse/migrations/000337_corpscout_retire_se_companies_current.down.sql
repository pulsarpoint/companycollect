CREATE DATABASE IF NOT EXISTS corpscout;

-- Faithful recreate of migration 000326's refreshable MV, rendering preserved verbatim from
-- that file (the Python builder that produced it was retired together with the up-file's
-- drop). First refresh repopulates it -- SYSTEM WAIT VIEW blocks until it lands. The guard
-- DROP makes the recreate idempotent on a half-applied down.
DROP VIEW IF EXISTS corpscout.se_companies_current;

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
    street_address AS primary_street_address,
    postal_code AS primary_postal_code,
    city AS primary_city,
    geocode_status AS primary_geocode_status,
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
INNER JOIN corpscout.se_company_info AS i FINAL ON i.company_id = agg.company_id
INNER JOIN primary_address AS pa ON pa.company_id = agg.company_id
ORDER BY agg.company_id;

SYSTEM WAIT VIEW corpscout.se_companies_current;
