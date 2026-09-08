CREATE DATABASE IF NOT EXISTS corpscout;

-- The address half of the serving view moves off the OLD address chain and onto the SE
-- ADDRESS ENTITY, corpscout.se_company_address_v2 (migration 000384): one row per company
-- and published address, `active = 1` for the published ones, `kinds` an array rather than
-- a row per type, and the geocode outcome -- status, precision, coordinate -- stored ON the
-- row. Slice 4a, task 3 of the 2026-09-08 reader switch: the old se_company_address final
-- table and the se_address_geocodes_served overlay keep running until slice 4b retires them,
-- so nothing here drops or renames either.
--
-- FOUR SHAPE CHANGES INSIDE THE company_addresses CTE:
--   * the served-overlay LEFT JOIN is gone -- latitude/longitude/geocode_precision come off
--     the entity row, so the refresh loses a join over the whole address population
--   * geocode_provider is DERIVED: `matched_area` (what the postcode/city centroid overlay
--     writes) maps to 'centroid_fallback', a row without a coordinate to '', everything else
--     to 'osm'. That keeps a centroid row classifying 'coarse' exactly as the overlaid rows
--     did, so primary_geocode_class is unchanged for every existing row
--   * street_address is the published line minus its trailing `, NNN NN Town` (a `c/o`
--     prefix survives) -- the entity has no street_address column. A normalizer-v3
--     postcode-only line (`100 11 Stockholm`) has no comma to strip at, so that shape is
--     tested for first and yields '' rather than the whole line
--   * the primary pick ranks on has(kinds, 'visiting_or_postal') / has(kinds, 'visiting')
--     instead of equality on a scalar address_type, same three ranks, same address_key
--     tiebreak. address_id in the addresses JSON is now the entity's address_key.
--
-- Staged swap, 000347's pattern for 000320's reason: the serving name has LIVE readers, so
-- the repointed view builds under _next, its first refresh is waited on, and ONE atomic
-- RENAME swaps both names. The retired view keeps its machinery for the down file, and
-- a follow-up drops it as 000345/000348 did.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

SYSTEM STOP VIEW corpscout.se_companies_serving;

-- The cadence is restated from 000366 (hourly at :45, staggered off the address-geocode
-- refresh at :00): a CREATE does not inherit the live view's refresh clause.
CREATE MATERIALIZED VIEW corpscout.se_companies_serving_next
REFRESH EVERY 1 HOUR OFFSET 45 MINUTE
ENGINE = MergeTree
ORDER BY company_id
AS WITH company_addresses AS (
  SELECT
    a.company_id AS company_id,
    toString(a.address_key) AS address_key,
    arrayStringConcat(arrayMap(x -> toString(x), a.kinds), ',') AS address_type,
    toUInt8(has(a.sources, 'bolagsverket')) AS from_bolagsverket,
    if(match(a.normalized_address, '^[0-9]{3} [0-9]{2}[^,]*$'), '', replaceRegexpOne(a.normalized_address, ',\\s*[0-9]{3} [0-9]{2}[^,]*$', '')) AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    toString(a.address_key) AS address_id,
    toString(a.geocode_status) AS geocode_status,
    toString(a.geocode_precision) AS geocode_precision,
    multiIf(a.geocode_status = 'matched_area', 'centroid_fallback', a.latitude IS NULL, '', 'osm') AS geocode_provider,
    a.latitude AS latitude,
    a.longitude AS longitude,
    has(a.kinds, 'visiting_or_postal') AS kind_visiting_or_postal,
    has(a.kinds, 'visiting') AS kind_visiting
  FROM corpscout.se_company_address_v2 AS a FINAL
  WHERE a.active = 1
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
    kind_visiting_or_postal DESC,
    kind_visiting DESC,
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
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_bolagsverket_financial_metrics)) AS fin_bolagsverket,
    toUInt8(i.company_id IN (SELECT ci.company_id FROM corpscout.company_identifier AS ci WHERE ci.issuer_scheme = 'lei' AND ci.country_code = 'SE' AND ci.is_current = 1 AND ci.issuer_id IN (SELECT upperUTF8(trimBoth(m.lei)) FROM corpscout.esef_financial_metrics AS m))) AS fin_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_financial_reports)) AS fin_reports,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person)) AS has_people,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person_role WHERE has(sources, 'bolagsverket'))) AS people_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person_role WHERE has(sources, 'esef'))) AS people_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_domains WHERE country_code = 'SE')) AS has_domains,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_traded_symbols WHERE country_code = 'SE')) AS is_publicly_traded,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_government_contracts)) AS has_government_contracts,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_job_history WHERE country_code = 'SE')) AS has_job_ads,
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
  LEFT JOIN corpscout.se_companies AS c FINAL ON c.company_id = i.company_id
  LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_companies'
      AND source_column = 'activity_description'
      AND source_lang = 'sv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
  ) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_description, ''))
  LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM corpscout.se_code_labels
    WHERE code_type = 'status_reason'
    GROUP BY code
  ) AS sr ON sr.code = ifNull(c.status_reason, '')
  LEFT JOIN aggregated AS agg ON agg.company_id = i.company_id
  LEFT JOIN primary_address AS pa ON pa.company_id = i.company_id
)
ORDER BY company_id
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888;

SYSTEM WAIT VIEW corpscout.se_companies_serving_next;

RENAME TABLE
    corpscout.se_companies_serving TO corpscout.se_companies_serving_retired,
    corpscout.se_companies_serving_next TO corpscout.se_companies_serving;
