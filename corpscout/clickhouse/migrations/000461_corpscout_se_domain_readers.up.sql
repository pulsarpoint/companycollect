CREATE DATABASE IF NOT EXISTS corpscout;

-- First deploy this migration, then the application/Dagster/crawler readers.
-- The next migration removes the old tables only after those readers are deployed.
-- Associations and reviews remain owned by se_company_domain and its rule table.
CREATE OR REPLACE VIEW corpscout.se_company_domain_resolved AS
WITH reviewed AS (
    SELECT
        'SE' AS country_code, d.company_id AS company_id, d.root_domain AS root_domain,
        d.website_url AS website_url, d.website_host AS website_host,
        d.supporting_sources AS supporting_sources, d.sources AS source_names, arrayMap(value -> toFloat32(value), d.source_confidences) AS source_confidences,
        d.source_record_ids AS source_record_ids, d.source_urls AS source_urls,
        d.confidence_bases AS confidence_bases, toFloat32(d.confidence) AS suggested_confidence,
        d.is_primary AS base_primary, d.evidence_hash AS evidence_fingerprint,
        if(r.company_id != '', if(r.removed, 'unreviewed', r.action), d.review_status) AS review_status,
        if(r.company_id != '', r.note, d.review_note) AS review_note,
        if(r.company_id != '', r.decided_by, d.reviewed_by) AS reviewed_by,
        if(r.company_id != '', r.decided_at, d.reviewed_at) AS reviewed_at,
        if(r.company_id != '', r.evidence_hash, d.reviewed_evidence_hash) AS reviewed_evidence_fingerprint,
        toUInt8(multiIf(
            r.company_id != '' AND r.removed = 0 AND r.action IN ('confirmed_primary', 'confirmed_related'), 1,
            r.company_id != '' AND r.removed = 0 AND r.action = 'rejected', 0,
            r.company_id != '' AND r.removed = 1 AND d.association_source = 'reviewer', 0,
            d.active
        )) AS is_active,
        d.first_seen_at AS first_seen_at, d.last_seen_at AS last_seen_at,
        greatest(d.folded_at, ifNull(r.decided_at, d.folded_at)) AS resolved_at
    FROM corpscout.se_company_domain AS d FINAL
    LEFT JOIN corpscout.se_company_domain_rule AS r FINAL
        ON r.company_id = d.company_id AND r.root_domain = d.root_domain
)
SELECT
    country_code, company_id, root_domain, website_url, website_host, source_names,
    source_confidences, source_record_ids, source_urls, confidence_bases, suggested_confidence,
    toUInt8(is_active AND row_number() OVER (
        PARTITION BY company_id ORDER BY is_active DESC, review_status = 'confirmed_primary' DESC,
        base_primary DESC, suggested_confidence DESC, root_domain
    ) = 1) AS suggested_primary,
    evidence_fingerprint, review_status, review_note, reviewed_by, reviewed_at,
    reviewed_evidence_fingerprint, is_active, first_seen_at, last_seen_at, resolved_at, supporting_sources
FROM reviewed;

CREATE OR REPLACE VIEW corpscout.website_domain_relationship_inputs AS
WITH owners AS (
    SELECT replaceRegexpOne(lowerUTF8(trim(TRAILING '.' FROM website_host)), '^www[.]', '') AS source_host,
        any(country_code) AS owner_country_code, any(company_id) AS owner_company_id
    FROM corpscout.se_company_domain_resolved
    WHERE is_active = 1 AND website_host != ''
    GROUP BY source_host
    HAVING uniqExact(tuple(country_code, company_id)) = 1
), occurrences AS (
    SELECT result_id, domain AS crawl_domain, result_kind, source_path, finished_at,
        arrayJoin(JSONExtractArrayRaw(ifNull(external_links, '[]'))) AS evidence,
        JSONExtractString(evidence, 'source_host') AS source_host,
        JSONExtractString(evidence, 'destination_domain') AS registrable_domain
    FROM corpscout.website_crawl_results_latest
    WHERE result_kind IN ('crawl', 'analysis')
)
SELECT r.result_id AS result_id, r.crawl_domain AS crawl_domain,
    r.result_kind AS result_kind, r.source_path AS source_path,
    r.source_host AS source_host, o.owner_country_code AS country_code, o.owner_company_id AS company_id,
    c.legal_name AS reporting_entity_name, r.registrable_domain AS registrable_domain,
    max(coalesce(parseDateTime64BestEffortOrNull(JSONExtractString(r.evidence, 'fetched_at'), 6, 'UTC'),
        r.finished_at, toDateTime64(0, 6, 'UTC'))) AS captured_at,
    min(JSONExtractString(r.evidence, 'source_url')) AS source_url,
    concat('[', arrayStringConcat(arraySort(groupUniqArray(r.evidence)), ','), ']') AS evidence_json
FROM occurrences AS r
INNER JOIN owners AS o ON o.source_host = r.source_host
INNER JOIN corpscout.se_company_basic_info AS c FINAL ON c.company_id = o.owner_company_id AND o.owner_country_code = 'SE'
WHERE JSONExtractString(r.evidence, 'context_version') = 'website-domain-context-v1'
    AND r.registrable_domain != ''
GROUP BY r.result_id, r.crawl_domain, r.result_kind, r.source_path,
    r.source_host, o.owner_country_code, o.owner_company_id, c.legal_name, r.registrable_domain;

SYSTEM STOP VIEW corpscout.se_companies_serving;

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
  WHERE a.active = 1 AND NOT (a.kinds = ['workplace'])
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
  toUInt8(fin_entity OR fin_reports) AS has_financial,
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
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_financial FINAL WHERE active = 1)) AS fin_entity,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_financial FINAL WHERE active = 1 AND hasAny(sources, ['bolagsverket', 'bolagsverket_comparative']))) AS fin_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_financial FINAL WHERE active = 1 AND has(sources, 'esef'))) AS fin_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_financial_reports)) AS fin_reports,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1)) AS has_people,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'bolagsverket'))) AS people_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'esef'))) AS people_esef,
    toUInt8(i.company_id IN (SELECT domains.company_id
      FROM corpscout.se_company_domain_resolved AS domains
      INNER JOIN corpscout.se_company_domain AS entity FINAL
        ON entity.company_id = domains.company_id
        AND entity.root_domain = domains.root_domain
      WHERE domains.country_code = 'SE'
        AND domains.review_status != 'rejected'
        AND (domains.is_active = 1 OR entity.inactive_reason = 'unverified'))) AS has_domains,
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
