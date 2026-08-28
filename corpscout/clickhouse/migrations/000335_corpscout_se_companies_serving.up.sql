CREATE DATABASE IF NOT EXISTS corpscout;

-- corpscout.se_companies_serving is the ONE wide per-company serving surface every admin
-- companies list page reads: the info-list columns (legal name, status, legal form, description
-- flag), the datatype presence flags (address / financial / people / domains) and per-register
-- source flags that the backoffice used to recompute as IN-set subqueries on every page load
-- (measured ~1.4s of ClickHouse per load to decorate 50 rows, owner 2026-08-28), plus the
-- address JSON array and primary-address geocode summary that corpscout.se_companies_current
-- (migration 000326) carried. It SUPERSEDES se_companies_current -- the base widens from
-- "companies with a current address" (INNER JOIN) to ALL of se_company_info (LEFT JOIN), so the
-- info list can read every published company off this table -- and se_companies_current is
-- dropped by a follow-up migration once every reader is repointed.
--
-- WHY A REFRESHABLE MV: same reasoning as 000320/000326 -- the SELECT below pays two FINAL
-- merges, seven IN-set builds and a per-company JSON aggregation (measured ~26s for the full
-- corpus) -- a refreshable MV keeps a real MergeTree ORDER BY company_id behind the name so every
-- admin read is a primary-key probe, and ClickHouse recomputes it EVERY 15 MINUTE. Insert-
-- triggered MVs cannot maintain this: several child pipelines publish via EXCHANGE TABLES,
-- which fires no insert triggers. `SYSTEM REFRESH VIEW corpscout.se_companies_serving` forces
-- an immediate refresh -- the weekly geocode asset issues exactly that after publishing.
--
-- PLAIN CREATE, NOT 000320's STAGED SWAP: the name is brand new -- nothing reads
-- corpscout.se_companies_serving before this migration creates it (the backoffice repoint ships
-- after) -- so there is no reader to keep whole across a cutover. SYSTEM WAIT VIEW still
-- follows the CREATE so `migrate up` returns only once the first refresh has populated the
-- view (a freshly created refreshable MV schedules its first refresh instead of running it
-- inline, and would otherwise be empty until it lands).
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED. It is the exact rendering
-- of companies_current.build_se_companies_serving_sql(), the single source of truth for the
-- serving SELECT (including the presence-set subqueries the backoffice used to hand-carry as
-- COMPANY_SETS). Editing this file without editing that builder -- or the builder without a new
-- migration -- trips the drift pin in dagster_v3 tests/test_se_companies_serving_mv.py.
--
-- NO DROP HERE. se_companies_current keeps serving its remaining readers until they are
-- repointed -- its drop is a separate migration with its own zero-reader proof.

CREATE MATERIALIZED VIEW corpscout.se_companies_serving
REFRESH EVERY 15 MINUTE
ENGINE = MergeTree
ORDER BY company_id
AS WITH company_addresses AS (
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
    toUInt8(i.description IS NOT NULL) AS has_description,
    toUInt8(ifNull(agg.address_count, 0) > 0) AS has_address,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_bolagsverket_financial_metrics)) AS fin_bolagsverket,
    toUInt8(i.company_id IN (SELECT ci.company_id FROM corpscout.company_identifier AS ci WHERE ci.issuer_scheme = 'lei' AND ci.country_code = 'SE' AND ci.is_current = 1 AND ci.issuer_id IN (SELECT upperUTF8(trimBoth(m.lei)) FROM corpscout.esef_financial_metrics AS m))) AS fin_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_financial_reports)) AS fin_reports,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person)) AS has_people,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person_role WHERE has(sources, 'bolagsverket'))) AS people_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person_role WHERE has(sources, 'esef'))) AS people_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_domains WHERE country_code = 'SE')) AS has_domains,
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
  FROM corpscout.se_company_info AS i FINAL
  LEFT JOIN aggregated AS agg ON agg.company_id = i.company_id
  LEFT JOIN primary_address AS pa ON pa.company_id = i.company_id
)
ORDER BY company_id;

SYSTEM WAIT VIEW corpscout.se_companies_serving;
