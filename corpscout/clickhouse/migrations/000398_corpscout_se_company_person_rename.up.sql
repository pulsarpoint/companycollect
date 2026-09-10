CREATE DATABASE IF NOT EXISTS corpscout;

-- THE PERSON ENTITY TAKES ITS FINAL NAME (spec 2026-09-09 section 3.3, slice 4).
-- corpscout.se_company_person_v2 -- built by 000396 beside the 2026-08-19 model, filled by
-- slice 2's fold (1.13M persons over 578,289 companies) and read by slice 3's backoffice --
-- becomes corpscout.se_company_person. ONE PAIR IN THE RENAME, not two: person slice 0
-- dropped the old table of that name by hand on 2026-09-09, so the target name is free and
-- there is nothing to park under _legacy the way the address rename (000393) had to.
--
-- NO STAGED _next SWAP, and that is the difference from 000391 and 000392. Those two
-- REPLACED the serving view's definition, which meant building a second view and swapping
-- names. This migration changes only the TABLE NAME the same definition reads, and ALTER
-- TABLE's MODIFY-QUERY clause does that in place: the refreshable view keeps the rows it is
-- already serving and its next scheduled refresh (hourly at :45, migration 000366) runs the
-- new query. Proven on prod 2026-09-08 by 000393 and again on 2026-09-09 by 000396. So
-- there is no _next to populate and no refresh wait to sit through.
--
-- THE VIEW IS STOPPED FIRST because between the RENAME and the MODIFY-QUERY step its stored
-- query names a table that no longer exists, so a refresh landing in that window would
-- fail -- a stopped view cannot refresh at all.
--
-- APPLY THIS OUTSIDE THE :45 REFRESH WINDOW. The refresh takes 13 to 15 minutes, so start
-- the migration just after one finishes (about :00) and the STOP cannot interrupt a run.
--
-- IF THE MIGRATE CLIENT DROPS between the STOP and the START, the view is left stopped and
-- serving its last contents at full speed with nothing raising anywhere. Recovery is by
-- hand: check corpscout.se_companies_serving in system.view_refreshes, run SYSTEM START VIEW
-- corpscout.se_companies_serving, then migrate force 398 so the ledger records where the
-- database actually is. The person design doc's runbook section has the full sequence.
--
-- THE LEDGER KEEPS 000396's DDL UNDER THE OLD NAME. Under the dev-phase ledger policy a
-- historical file is history: 000396 still declares se_company_person_v2, and the tests that
-- read that DDL replay this rename themselves.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

SYSTEM STOP VIEW corpscout.se_companies_serving;

RENAME TABLE corpscout.se_company_person_v2 TO corpscout.se_company_person;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
WITH company_addresses AS (
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
    (a.street_name IS NOT NULL OR a.box IS NOT NULL) AS has_location,
    has(a.kinds, 'visiting_or_postal') AS kind_visiting_or_postal,
    has(a.kinds, 'visiting') AS kind_visiting
  FROM corpscout.se_company_address AS a FINAL
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
    has_location DESC,
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
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(lf.label_sv, '') AS legal_form_label_sv,
    ifNull(i.description_sv, '') AS activity_description,
    if(ifNull(i.description_language, '') = 'en', ifNull(i.description, ''), '') AS activity_description_en,
    ifNull(b.deregistration_reason, '') AS status_reason,
    ifNull(sr.label_en, '') AS status_reason_label_en,
    if(ifNull(b.company_id, '') = '', '', ifNull(lower(hex(SHA256(concat('company-source-record-v1\nstructured\n', 'sweden_bolagsverket', '\nregistry_company\n', b.source_record_id, '\n', lowerUTF8(b.source_payload_hash))))), '')) AS bolagsverket_source_record_uid,
    ifNull(b.observed_at, toDateTime64(0, 3, 'UTC')) AS updated_from_raw_at,
    toUInt8(i.description IS NOT NULL) AS has_description,
    toUInt8(ifNull(agg.address_count, 0) > 0) AS has_address,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_bolagsverket_financial_metrics)) AS fin_bolagsverket,
    toUInt8(i.company_id IN (SELECT ci.company_id FROM corpscout.company_identifier AS ci WHERE ci.issuer_scheme = 'lei' AND ci.country_code = 'SE' AND ci.is_current = 1 AND ci.issuer_id IN (SELECT upperUTF8(trimBoth(m.lei)) FROM corpscout.esef_financial_metrics AS m))) AS fin_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_financial_reports)) AS fin_reports,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1)) AS has_people,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'bolagsverket'))) AS people_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'esef'))) AS people_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_domains WHERE country_code = 'SE')) AS has_domains,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_traded_symbols WHERE country_code = 'SE')) AS is_publicly_traded,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_government_contracts)) AS has_government_contracts,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_job_history WHERE country_code = 'SE')) AS has_job_ads,
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
  FROM corpscout.se_company_basic_info AS i FINAL
  LEFT JOIN (
    SELECT company_id, deregistration_reason, source_record_id, source_payload_hash, observed_at
    FROM corpscout.se_bolagsverket_companies FINAL
    WHERE has_company = 1
  ) AS b ON b.company_id = i.company_id
  LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en, argMax(label_sv, version) AS label_sv
    FROM corpscout.se_code_labels
    WHERE code_type = 'legal_form'
    GROUP BY code
  ) AS lf ON lf.code = ifNull(i.legal_form_code, '')
  LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM corpscout.se_code_labels
    WHERE code_type = 'status_reason'
    GROUP BY code
  ) AS sr ON sr.code = ifNull(b.deregistration_reason, '')
  LEFT JOIN aggregated AS agg ON agg.company_id = i.company_id
  LEFT JOIN primary_address AS pa ON pa.company_id = i.company_id
)
ORDER BY company_id
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888;

SYSTEM START VIEW corpscout.se_companies_serving;
